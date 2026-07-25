"""Unit tests for the BigQuery connector (rc17 stage-then-apply write path).

Covers the four dialect render/land hooks (``stage_table_sql``,
``merge_statement_sql`` — including the all-keys insert-only degradation —
``empty_table_sql``, and ``bulk_land`` orchestration), the load-direction
type rendering (``render_column_type`` NUMERIC/BIGNUMERIC arithmetic and
Duration/Interval rejection), the nested-column JSON serialization
(``_jsonify_nested_columns`` and its leaf validation), and the load-job
machinery (deterministic job-id chain, orphan drain, submit/attach,
credential recovery, and Google-error classification).

The CDK is stubbed in conftest; ``google.cloud.bigquery`` / ``google.auth``
are real (install per requirements.txt to run this suite).
"""

import io
import json
import math

import pyarrow as pa
import pytest

from google.api_core import exceptions as gexc
from google.auth import exceptions as authexc

from cdk.sql.dialects import TableAddress  # stubbed in conftest
from cdk.adbc_registry import AdbcConfigurationError  # stubbed in conftest

import connector as bq
from connector import BigQueryDialect


# --------------------------------------------------------------------------
# Dialect render hooks
# --------------------------------------------------------------------------


class TestStageTableSql:
    def setup_method(self):
        self.dialect = BigQueryDialect()

    def test_renders_create_table_like_three_level(self):
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        target = TableAddress(table="orders", schema="ds", catalog="proj")
        sql = self.dialect.stage_table_sql(stage, target, temp=False)
        assert sql == (
            "CREATE TABLE `proj`.`ds`.`_stg` LIKE `proj`.`ds`.`orders`"
        )

    def test_no_temporary_keyword_for_real_scope(self):
        stage = TableAddress(table="_stg", schema="ds")
        target = TableAddress(table="orders", schema="ds")
        sql = self.dialect.stage_table_sql(stage, target, temp=False)
        assert "TEMP" not in sql.upper()
        assert sql.startswith("CREATE TABLE ")


class TestMergeStatementSql:
    def setup_method(self):
        self.dialect = BigQueryDialect()
        self.stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        self.target = TableAddress(table="orders", schema="ds", catalog="proj")

    def test_renders_merge_on_conflict_keys(self):
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["id"], columns=["id", "total", "status"],
        )
        assert sql == (
            "MERGE INTO `proj`.`ds`.`orders` t USING `proj`.`ds`.`_stg` s "
            "ON t.`id` = s.`id` "
            "WHEN MATCHED THEN UPDATE SET `total` = s.`total`, "
            "`status` = s.`status` "
            "WHEN NOT MATCHED THEN INSERT (`id`, `total`, `status`) "
            "VALUES (s.`id`, s.`total`, s.`status`)"
        )

    def test_update_set_excludes_keys_and_is_unqualified(self):
        # GoogleSQL rejects a target-alias prefix on UPDATE SET targets.
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["id"], columns=["id", "total"],
        )
        set_clause = sql.split("UPDATE SET", 1)[1].split("WHEN NOT MATCHED")[0]
        assert "`id` =" not in set_clause  # key never updated
        assert "`total` = s.`total`" in set_clause
        assert "t.`total`" not in set_clause  # SET target unqualified

    def test_on_clause_uses_actual_conflict_key_not_hardcoded_id(self):
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["seq"], columns=["seq", "v"],
        )
        on_region = sql.split(" ON ", 1)[1].split(" WHEN ", 1)[0]
        assert "t.`seq` = s.`seq`" in on_region

    def test_composite_conflict_keys(self):
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["a", "b"], columns=["a", "b", "v"],
        )
        on_region = sql.split(" ON ", 1)[1].split(" WHEN ", 1)[0]
        assert on_region == "t.`a` = s.`a` AND t.`b` = s.`b`"

    def test_all_keys_degrades_to_insert_only_no_when_matched(self):
        # Conformance: when every landed column is a conflict key, the
        # MERGE must omit WHEN MATCHED entirely (insert-only).
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["id"], columns=["id"],
        )
        assert "WHEN MATCHED" not in sql
        assert "WHEN NOT MATCHED THEN INSERT" in sql

    def test_no_foreign_merge_tokens(self):
        sql = self.dialect.merge_statement_sql(
            self.stage, self.target,
            conflict_keys=["id"], columns=["id", "v"],
        )
        assert "MERGE" in sql
        assert "ON CONFLICT" not in sql
        assert "ON DUPLICATE KEY" not in sql


class TestEmptyTableSql:
    def test_renders_delete_where_true_not_truncate(self):
        dialect = BigQueryDialect()
        target = TableAddress(table="orders", schema="ds", catalog="proj")
        sql = dialect.empty_table_sql(target)
        assert sql == "DELETE FROM `proj`.`ds`.`orders` WHERE TRUE"
        assert "TRUNCATE" not in sql.upper()
        assert "DELETE" in sql


class TestInformationSchemaRef:
    def setup_method(self):
        self.dialect = BigQueryDialect()

    def test_schemata_project_scoped_uppercase(self):
        ref = self.dialect.information_schema_ref("schemata", catalog="proj")
        assert ref == "`proj`.INFORMATION_SCHEMA.SCHEMATA"

    def test_schemata_unqualified_when_no_catalog(self):
        ref = self.dialect.information_schema_ref("schemata")
        assert ref == "INFORMATION_SCHEMA.SCHEMATA"

    def test_tables_dataset_scoped_uppercase(self):
        ref = self.dialect.information_schema_ref(
            "tables", catalog="proj", schema="ds"
        )
        assert ref == "`proj`.`ds`.INFORMATION_SCHEMA.TABLES"


class TestRenderColumnType:
    def setup_method(self):
        self.dialect = BigQueryDialect()
        self.mapper = object()  # unused on the paths under test

    def test_small_decimal_renders_numeric(self):
        assert (
            self.dialect.render_column_type("Decimal128(18, 2)", self.mapper)
            == "NUMERIC(18, 2)"
        )

    def test_large_precision_renders_bignumeric(self):
        # precision 50 > NUMERIC's 38; integer digits 30 <= BIGNUMERIC's 38.
        assert (
            self.dialect.render_column_type("Decimal256(50, 20)", self.mapper)
            == "BIGNUMERIC(50, 20)"
        )

    def test_numeric_integer_digit_bound_pushes_to_bignumeric(self):
        # precision 38, scale 0 -> 38 integer digits > NUMERIC's 29 bound.
        assert (
            self.dialect.render_column_type("Decimal128(38, 0)", self.mapper)
            == "BIGNUMERIC(38, 0)"
        )

    def test_invalid_decimal_shape_raises(self):
        with pytest.raises(ValueError, match="renderable BigQuery decimal"):
            self.dialect.render_column_type("Decimal128(2, 5)", self.mapper)

    def test_out_of_range_decimal_raises(self):
        with pytest.raises(ValueError, match="neither NUMERIC"):
            self.dialect.render_column_type("Decimal256(76, 76)", self.mapper)

    @pytest.mark.parametrize("canonical", ["Duration(SECOND)", "Interval(MONTH_DAY_NANO)"])
    def test_duration_interval_rejected(self, canonical):
        with pytest.raises(ValueError, match="no loadable BigQuery"):
            self.dialect.render_column_type(canonical, self.mapper)

    def test_other_canonicals_delegate_to_base(self):
        # The stub base returns a marker so delegation is observable.
        assert (
            self.dialect.render_column_type("Utf8", self.mapper)
            == "<delegated:Utf8>"
        )


# --------------------------------------------------------------------------
# Nested-column JSON serialization (issue #8)
# --------------------------------------------------------------------------


def _one_col_batch(values, dtype, name="c"):
    return pa.RecordBatch.from_arrays([pa.array(values, type=dtype)], names=[name])


def _jsonified(values, dtype):
    out = bq._jsonify_nested_columns(_one_col_batch(values, dtype))
    return out.column(0).to_pylist()


class TestJsonifyNestedColumns:
    def test_no_nested_columns_returns_same_batch(self):
        batch = _one_col_batch([1, 2, 3], pa.int64())
        assert bq._jsonify_nested_columns(batch) is batch

    def test_struct_becomes_json_object(self):
        dtype = pa.struct([("a", pa.int64()), ("b", pa.string())])
        [text] = _jsonified([{"a": 1, "b": "x"}], dtype)
        assert json.loads(text) == {"a": 1, "b": "x"}

    def test_list_becomes_json_array(self):
        assert _jsonified([[1, 2, 3]], pa.list_(pa.int64())) == ["[1,2,3]"]

    def test_map_becomes_json_object(self):
        dtype = pa.map_(pa.string(), pa.int64())
        [text] = _jsonified([[("k1", 1), ("k2", 2)]], dtype)
        assert json.loads(text) == {"k1": 1, "k2": 2}

    def test_null_slot_preserved_as_null_not_text(self):
        assert _jsonified([None, [1]], pa.list_(pa.int64())) == [None, "[1]"]

    def test_output_is_large_string(self):
        out = bq._jsonify_nested_columns(
            _one_col_batch([[1]], pa.list_(pa.int64()))
        )
        assert out.schema.field(0).type == pa.large_string()

    def test_field_nullability_preserved(self):
        arr = pa.array([[1]], type=pa.list_(pa.int64()))
        schema = pa.schema([pa.field("c", pa.list_(pa.int64()), nullable=False)])
        batch = pa.RecordBatch.from_arrays([arr], schema=schema)
        out = bq._jsonify_nested_columns(batch)
        assert out.schema.field("c").nullable is False

    def test_non_finite_float_value_becomes_null(self):
        [text] = _jsonified([[1.0, math.inf, math.nan]], pa.list_(pa.float64()))
        assert json.loads(text) == [1.0, None, None]

    def test_decimal_value_serialized_as_string(self):
        import decimal

        dtype = pa.list_(pa.decimal128(10, 2))
        [text] = _jsonified([[decimal.Decimal("1.50")]], dtype)
        assert json.loads(text) == ["1.50"]

    def test_binary_value_base64(self):
        import base64

        [text] = _jsonified([[b"\x00\x01"]], pa.list_(pa.binary()))
        assert json.loads(text) == [base64.b64encode(b"\x00\x01").decode()]

    def test_timestamp_value_isoformat(self):
        dtype = pa.list_(pa.timestamp("us"))
        import datetime

        [text] = _jsonified([[datetime.datetime(2026, 1, 2, 3, 4, 5)]], dtype)
        assert "2026-01-02T03:04:05" in json.loads(text)[0]

    # ---- rejection cases (fail loud, never silent corruption) ----

    def test_nested_duration_rejected(self):
        with pytest.raises(ValueError, match="no faithful JSON"):
            _jsonified([[1]], pa.list_(pa.duration("s")))

    def test_nested_union_rejected(self):
        union = pa.union(
            [pa.field("a", pa.int64()), pa.field("b", pa.string())], mode="sparse"
        )
        with pytest.raises(ValueError, match="no faithful JSON"):
            bq._validate_json_leaf_types(pa.list_(union), "c", [])

    def test_non_scalar_map_key_rejected(self):
        key = pa.struct([("x", pa.int64())])
        dtype = pa.map_(key, pa.int64())
        with pytest.raises(ValueError, match="scalar JSON object key"):
            bq._validate_json_leaf_types(dtype, "c", [])

    def test_non_finite_float_map_key_raises(self):
        dtype = pa.map_(pa.float64(), pa.int64())
        with pytest.raises(ValueError, match="cannot become a JSON object key"):
            _jsonified([[(math.nan, 1)]], dtype)

    def test_colliding_map_keys_raise(self):
        # Distinct source keys whose JSON-text forms coincide.
        dtype = pa.map_(pa.int64(), pa.int64())
        with pytest.raises(ValueError, match="collide"):
            bq._json_safe_value([(1, 10), (1, 20)], dtype)

    def test_clean_leaf_tree_collects_no_exotic(self):
        exotic: list[str] = []
        dtype = pa.struct(
            [
                ("i", pa.int64()),
                ("s", pa.string()),
                ("m", pa.map_(pa.string(), pa.list_(pa.float64()))),
            ]
        )
        bq._validate_json_leaf_types(dtype, "c", exotic)
        assert exotic == []


# --------------------------------------------------------------------------
# Load-job id chain
# --------------------------------------------------------------------------


class TestLoadJobIdPrefix:
    def test_deterministic_for_same_stage_and_payload(self):
        a = bq._load_job_id_prefix("bstage1", b"payload")
        b = bq._load_job_id_prefix("bstage1", b"payload")
        assert a == b
        assert a.startswith("analitiq_")

    def test_differs_on_payload(self):
        assert bq._load_job_id_prefix("s", b"x") != bq._load_job_id_prefix("s", b"y")

    def test_differs_on_stage(self):
        assert bq._load_job_id_prefix("s1", b"x") != bq._load_job_id_prefix("s2", b"x")


def _job(state="DONE", error_result=None, result_exc=None):
    from unittest.mock import MagicMock

    j = MagicMock()
    j.state = state
    j.error_result = error_result
    j.job_id = "analitiq_x_n"
    if result_exc is not None:
        j.result.side_effect = result_exc
    return j


class TestDrainLoadJobChain:
    def _client(self, get_job_side_effect):
        from unittest.mock import MagicMock

        c = MagicMock()
        c.get_job.side_effect = get_job_side_effect
        return c

    def test_empty_chain_returns_zero(self):
        c = self._client([gexc.NotFound("no")])
        assert bq._drain_load_job_chain(c, "p", location=None) == 0

    def test_all_terminal_returns_next_index(self):
        c = self._client([_job(), _job(), gexc.NotFound("no")])
        assert bq._drain_load_job_chain(c, "p", location="US") == 2

    def test_last_in_flight_is_attached(self):
        j = _job(state="RUNNING")
        c = self._client([j, gexc.NotFound("no")])
        assert bq._drain_load_job_chain(c, "p", location=None) == 1
        j.result.assert_called_once()

    def test_poll_failure_but_reload_done_advances(self):
        j = _job(state="RUNNING", result_exc=RuntimeError("poll"))

        def _reload():
            j.state = "DONE"

        j.reload.side_effect = _reload
        j.error_result = {"reason": "x"}  # terminal failure, commits nothing
        c = self._client([j, gexc.NotFound("no")])
        assert bq._drain_load_job_chain(c, "p", location=None) == 1

    def test_poll_failure_and_still_live_reraises(self):
        j = _job(state="RUNNING", result_exc=RuntimeError("poll"))
        j.reload.return_value = None  # state stays RUNNING
        c = self._client([j, gexc.NotFound("no")])
        with pytest.raises(RuntimeError, match="poll"):
            bq._drain_load_job_chain(c, "p", location=None)

    def test_chain_cap_raises(self):
        c = self._client(lambda *a, **k: _job())  # never NotFound
        c.get_job.side_effect = None
        c.get_job.return_value = _job()
        with pytest.raises(RuntimeError, match="reached"):
            bq._drain_load_job_chain(c, "p", location=None)


class TestSubmitAndPoll:
    def test_write_append_create_never(self):
        from unittest.mock import MagicMock
        from google.cloud import bigquery

        c = MagicMock()
        c.load_table_from_file.return_value = MagicMock()
        bq._submit_and_poll(
            c, io.BytesIO(b"x"), MagicMock(), job_id="j0", location="US"
        )
        _, kwargs = c.load_table_from_file.call_args
        assert kwargs["job_id"] == "j0"
        assert kwargs["location"] == "US"
        jc = kwargs["job_config"]
        assert jc.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
        assert jc.create_disposition == bigquery.CreateDisposition.CREATE_NEVER
        assert jc.source_format == bigquery.SourceFormat.PARQUET

    def test_conflict_attaches_to_existing_job(self):
        from unittest.mock import MagicMock

        c = MagicMock()
        c.load_table_from_file.side_effect = gexc.Conflict("exists")
        existing = MagicMock()
        c.get_job.return_value = existing
        bq._submit_and_poll(
            c, io.BytesIO(b"x"), MagicMock(), job_id="j0", location=None
        )
        c.get_job.assert_called_once_with("j0", location=None)
        existing.result.assert_called_once()


# --------------------------------------------------------------------------
# Credential recovery
# --------------------------------------------------------------------------


def _opt_from(mapping):
    return lambda key: mapping.get(key, "")


class TestBuildGoogleCredentials:
    def test_none_authtype_points_at_driver_floor(self):
        with pytest.raises(AdbcConfigurationError, match="1.11.0"):
            bq._build_google_credentials(None, _opt_from({}))

    def test_empty_authtype_is_wiring_defect(self):
        with pytest.raises(AdbcConfigurationError, match="never reached the driver"):
            bq._build_google_credentials("", _opt_from({}))

    def test_service_invalid_json_rejected(self):
        opt = _opt_from({bq._OPT_AUTH_CREDENTIALS: "{not json"})
        with pytest.raises(AdbcConfigurationError, match="not valid JSON"):
            bq._build_google_credentials("x.json_credential_string", opt)

    def test_service_non_object_json_rejected(self):
        opt = _opt_from({bq._OPT_AUTH_CREDENTIALS: "[1, 2]"})
        with pytest.raises(AdbcConfigurationError, match="not a JSON object"):
            bq._build_google_credentials("x.json_credential_string", opt)

    def test_service_missing_credentials_rejected(self):
        opt = _opt_from({bq._OPT_AUTH_CREDENTIALS: ""})
        with pytest.raises(AdbcConfigurationError, match="appears to be\n?\\s*unset|unset"):
            bq._build_google_credentials("x.json_credential_string", opt)

    def test_service_valid_returns_project(self, monkeypatch):
        from unittest.mock import MagicMock

        info = {"project_id": "proj-123", "type": "service_account"}
        opt = _opt_from({bq._OPT_AUTH_CREDENTIALS: json.dumps(info)})
        monkeypatch.setattr(
            "google.oauth2.service_account.Credentials.from_service_account_info",
            lambda info, scopes: MagicMock(name="creds"),
        )
        creds, project = bq._build_google_credentials(
            "adbc.bigquery.sql.auth_type.json_credential_string", opt
        )
        assert project == "proj-123"
        assert creds is not None

    def test_user_valid(self):
        opt = _opt_from(
            {
                bq._OPT_CLIENT_ID: "cid",
                bq._OPT_CLIENT_SECRET: "sec",
                bq._OPT_REFRESH_TOKEN: "rt",
            }
        )
        creds, project = bq._build_google_credentials(
            "adbc.bigquery.sql.auth_type.user_authentication", opt
        )
        assert project == ""
        assert creds.refresh_token == "rt"

    def test_user_missing_fields_rejected(self):
        opt = _opt_from({bq._OPT_CLIENT_ID: "cid"})  # secret/token unset
        with pytest.raises(AdbcConfigurationError, match="requires client_id"):
            bq._build_google_credentials(
                "adbc.bigquery.sql.auth_type.user_authentication", opt
            )

    def test_unsupported_authtype_rejected(self):
        with pytest.raises(AdbcConfigurationError, match="unsupported driver auth_type"):
            bq._build_google_credentials("something_else", _opt_from({}))


# --------------------------------------------------------------------------
# Google-error classification
# --------------------------------------------------------------------------


class TestErrorClassification:
    def test_reasons_extracted(self):
        exc = gexc.Forbidden("t", errors=[{"reason": "rateLimitExceeded"}])
        assert "rateLimitExceeded" in bq._google_error_reasons(exc)

    def test_transport_error_is_retryable(self):
        assert bq._is_deterministic_google_error(authexc.TransportError("net")) is False

    def test_refresh_error_invalid_grant_is_deterministic(self):
        assert bq._is_deterministic_google_error(authexc.RefreshError("invalid_grant")) is True

    def test_refresh_error_unknown_is_retryable(self):
        assert bq._is_deterministic_google_error(authexc.RefreshError("hiccup")) is False

    def test_other_auth_error_is_deterministic(self):
        assert bq._is_deterministic_google_error(authexc.GoogleAuthError("bad")) is True

    def test_throttling_403_is_retryable(self):
        exc = gexc.Forbidden("throttled", errors=[{"reason": "rateLimitExceeded"}])
        assert bq._is_deterministic_google_error(exc) is False

    def test_not_found_404_is_deterministic(self):
        assert bq._is_deterministic_google_error(gexc.NotFound("gone")) is True

    def test_server_error_5xx_is_retryable(self):
        assert bq._is_deterministic_google_error(gexc.InternalServerError("boom")) is False


# --------------------------------------------------------------------------
# bulk_land orchestration (helpers monkeypatched for isolation)
# --------------------------------------------------------------------------


class TestBulkLand:
    def _batch(self, n=3):
        return pa.RecordBatch.from_arrays(
            [pa.array(list(range(n)))], names=["id"]
        )

    def _patch_client(self, monkeypatch):
        from unittest.mock import MagicMock

        client = MagicMock(name="bq_client")
        client.project = "proj"
        monkeypatch.setattr(
            bq, "_build_bigquery_client", lambda conn: (client, "proj")
        )
        monkeypatch.setattr(bq, "_resolve_dataset_location", lambda c, d: "US")
        return client

    def test_schema_less_stage_is_fatal(self):
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="")  # no dataset
        with pytest.raises(AdbcConfigurationError, match="no dataset"):
            dialect.bulk_land(object(), stage, self._batch(), runtime=None)

    def test_first_attempt_submits_index_zero(self, monkeypatch):
        client = self._patch_client(monkeypatch)
        monkeypatch.setattr(
            bq, "_drain_load_job_chain", lambda c, p, *, location: 0
        )
        submitted = {}
        monkeypatch.setattr(
            bq,
            "_submit_and_poll",
            lambda c, buf, dest, *, job_id, location: submitted.update(job_id=job_id),
        )
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        assert dialect.bulk_land(object(), stage, self._batch(), runtime=None) is True
        assert submitted["job_id"].endswith("_0")
        client.close.assert_called_once()

    def test_retry_reuses_when_stage_already_holds_batch(self, monkeypatch):
        client = self._patch_client(monkeypatch)
        monkeypatch.setattr(
            bq, "_drain_load_job_chain", lambda c, p, *, location: 1
        )
        monkeypatch.setattr(
            bq, "_stage_row_count", lambda c, d, *, location: 3  # == batch rows
        )
        called = {"submit": False}
        monkeypatch.setattr(
            bq,
            "_submit_and_poll",
            lambda *a, **k: called.update(submit=True),
        )
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        assert dialect.bulk_land(object(), stage, self._batch(3), runtime=None) is True
        assert called["submit"] is False  # reused, not re-submitted
        client.close.assert_called_once()

    def test_retry_resubmits_when_stage_empty(self, monkeypatch):
        self._patch_client(monkeypatch)
        monkeypatch.setattr(
            bq, "_drain_load_job_chain", lambda c, p, *, location: 2
        )
        monkeypatch.setattr(
            bq, "_stage_row_count", lambda c, d, *, location: 0
        )
        submitted = {}
        monkeypatch.setattr(
            bq,
            "_submit_and_poll",
            lambda c, buf, dest, *, job_id, location: submitted.update(job_id=job_id),
        )
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        assert dialect.bulk_land(object(), stage, self._batch(3), runtime=None) is True
        assert submitted["job_id"].endswith("_2")

    def test_serialization_failure_is_fatal_and_closes_client(self, monkeypatch):
        client = self._patch_client(monkeypatch)

        def _boom(batch):
            raise ValueError("bad nested value")

        monkeypatch.setattr(bq, "_jsonify_nested_columns", _boom)
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        with pytest.raises(AdbcConfigurationError, match="before upload"):
            dialect.bulk_land(object(), stage, self._batch(), runtime=None)
        client.close.assert_called_once()

    def test_deterministic_google_error_wrapped_fatal(self, monkeypatch):
        self._patch_client(monkeypatch)
        monkeypatch.setattr(
            bq,
            "_drain_load_job_chain",
            lambda c, p, *, location: (_ for _ in ()).throw(gexc.NotFound("gone")),
        )
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        with pytest.raises(AdbcConfigurationError, match="failed deterministically"):
            dialect.bulk_land(object(), stage, self._batch(), runtime=None)

    def test_retryable_google_error_propagates(self, monkeypatch):
        self._patch_client(monkeypatch)
        monkeypatch.setattr(
            bq,
            "_drain_load_job_chain",
            lambda c, p, *, location: (_ for _ in ()).throw(
                gexc.InternalServerError("boom")
            ),
        )
        dialect = BigQueryDialect()
        stage = TableAddress(table="_stg", schema="ds", catalog="proj")
        with pytest.raises(gexc.InternalServerError):
            dialect.bulk_land(object(), stage, self._batch(), runtime=None)
