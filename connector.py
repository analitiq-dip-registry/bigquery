"""Google BigQuery connector - dialect + connector class for the Analitiq CDK.

BigQuery runs on the first-class ADBC BigQuery driver (``transport_type:
adbc``) for connect, SQL execution, discovery, and Arrow reads. BigQuery is
an HTTPS REST service - there is no host/port, username, or password - and
traffic to ``googleapis.com`` is always TLS with no selectable mode, so
there is no ``ssl_mode`` input and no ``build_tls_connect_arg`` hook. All
connection state is carried in the transport's ``adbc.bigquery.sql.*``
``db_kwargs``.

**Writes ride the CDK's stage-then-apply write path (ADR
sql-write-path-v2) with a BigQuery load job as the stage-fill mechanism.**
The engine runs one uniform stepwise cycle for every write mode: pre-flight
``DROP`` the batch's stage table, ``CREATE`` it shaped like the target
(``stage_table_sql`` -> ``CREATE TABLE ... LIKE``), fill it (``bulk_land``),
optionally empty the target on a truncate-insert's first batch
(``empty_table_sql`` -> ``DELETE ... WHERE TRUE``), then apply the stage to
the target with the mode statement - an engine-rendered ``INSERT ...
SELECT`` for append / truncate-insert, or this dialect's
``merge_statement_sql`` (GoogleSQL ``MERGE``) for upsert - and finally
``DROP`` the stage. The connector declares this shape in
``definition/connector.json`` ``sql_capabilities`` (``bulk_load.adbc:
load_job``, ``stage.scope: real``, ``merge_form: merge``, ``catalog:
full``); without that block the engine fails every write at
``configure_schema`` time.

The BigQuery-specific write mechanism lives in ``BigQueryDialect.bulk_land``:
the ADBC driver implements no ``adbc.ingest.*`` bulk path
(``cursor.adbc_ingest`` fails on the first option) and per-row DML INSERT is
a BigQuery anti-pattern (quota-bound, slow, costly), so each cast batch is
written to an in-memory Parquet buffer and shipped as a **load job** (direct
media upload - no GCS staging bucket) into the freshly-created stage table.

* **Idempotent, orphan-drained load jobs** - the engine gives each batch a
  collision-proof stage table whose name embeds ``sha256(run_id|stream_id|
  batch_seq|target)`` and drops+recreates it on every attempt.
  ``bulk_land`` submits its load under a DETERMINISTIC job-id chain
  (``analitiq_<token>_0``, ``_1``, ...) whose token hashes that stage
  identity and the exact Parquet payload. BigQuery load jobs are idempotent
  by job id, so a client-side polling timeout WITHIN a call re-attaches to
  the running job instead of re-submitting. ACROSS engine retries (which
  drop+recreate the stage) the chain walk first drains any prior in-flight
  job of this batch to a terminal state - closing the window where an
  abandoned job commits into the recreated same-named stage after a fresh
  load already filled it - and then, if a drained job already landed this
  exact payload into the current stage incarnation (row count matches),
  reuses it rather than double-loading; otherwise it submits a fresh chain
  id. Row idempotency across attempts otherwise lives in the engine's mode
  statement (``MERGE`` for upsert; the anti-join ``INSERT ... SELECT`` for
  keyed insert).
* **Credentials** - recovered per call from the SAME connection state the
  transport was materialized with, via ``GetOption`` on the live ADBC
  database handle (service-account JSON for ``auth_type=service``; client
  id/secret/refresh token for ``auth_type=user``). The load-job client is
  built inside the call and discarded on return - never cached - per the
  ADR's ``load_job`` mechanism contract. No second credential input exists.
* **Load-job type limits** - Json-family canonicals (``Json``, ``Object``,
  the nested ``List``/``Struct``/``Map`` forms) render ``STRING`` DDL: the
  CDK ships ``Json`` values as Parquet ``STRING``, and BigQuery batch load
  jobs cannot populate ``JSON`` columns from Parquet. Columns that
  materialize as genuine Arrow struct/list/map values (e.g. an endpoint
  column declared with a real ``properties``/``items`` sub-schema) are
  serialized to JSON text before the Parquet write
  (``_jsonify_nested_columns``), so every Json-family column lands as
  ``STRING`` regardless of how it materialized; leaves JSON text would
  silently corrupt (nested duration/interval/union, non-scalar map
  keys) fail loud at write time instead. ``Duration`` and ``Interval``
  canonicals are rejected at CREATE TABLE time (``render_column_type``):
  BigQuery INTERVAL cannot be populated by a Parquet load job at all.

Everything else BigQuery-specific lives here:

* **Backtick quoting** - BigQuery reads ``\"...\"`` as a string literal, so
  identifiers are quoted with backticks (``quote_char``).
* **Three-level addressing** - project -> dataset -> table; declared as
  ``sql_capabilities.catalog: full`` so a destination may name its own
  project.
* **NOT ENFORCED primary keys** - BigQuery's parser requires the
  ``NOT ENFORCED`` qualifier on a PRIMARY KEY clause and never enforces
  uniqueness (``pk_not_enforced``).
* **Schema-scoped INFORMATION_SCHEMA** - BigQuery's metadata views are
  addressed per dataset (``project.dataset.INFORMATION_SCHEMA.TABLES``) and
  the prefix must be uppercase; the ``information_schema_ref`` override
  composes that path for all four builtin discovery queries.
  ``INFORMATION_SCHEMA.SCHEMATA`` is project-scoped (composed from the
  catalog alone) and region-scoped through the query job's location - set
  the ``location`` connection parameter for non-US datasets.
* **NUMERIC/BIGNUMERIC render arithmetic** - write-direction logic
  ``type-map-write.json`` cannot express. NUMERIC (precision <= 38, scale
  <= 9, integer digits ``precision - scale`` <= 29) vs BIGNUMERIC
  (precision <= 76, scale <= 38, integer digits ``precision - scale`` <=
  38) is chosen from BOTH a Decimal's precision and scale, so a plain regex
  rule would emit an out-of-range ``NUMERIC(30, 15)``. Everything else
  delegates back to the write map via ``super().render_column_type``.

The read path still compiles paged ``SELECT``s through SQLAlchemy Core and
resolves this dialect by name via
``sqlalchemy.dialects.registry.load('bigquery')``, so ``sqlalchemy-bigquery``
is a required runtime dependency (see ``requirements.txt``) even though the
data transport is ADBC.

Registered under connector_id ``bigquery`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from cdk.adbc_registry import AdbcConfigurationError
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map import normalize_canonical_type

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any

    from cdk.sql.dialects import TableAddress
    from cdk.type_map.mapper import TypeMapper
    from google.cloud import bigquery

logger = logging.getLogger(__name__)

#: OAuth scope for load jobs and queries (the ADBC driver uses the same).
_BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"
#: Google's OAuth 2.0 token endpoint, used by the user-credential refresh flow.
_GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"

#: ADBC BigQuery driver database options read back via ``GetOption``.
#: adbc-driver-bigquery >= 1.11.0 (the requirements floor) implements
#: ``(*databaseImpl).GetOption`` for all of these.
_OPT_AUTH_TYPE = "adbc.bigquery.sql.auth_type"
_OPT_AUTH_CREDENTIALS = "adbc.bigquery.sql.auth_credentials"
_OPT_CLIENT_ID = "adbc.bigquery.sql.auth.client_id"
_OPT_CLIENT_SECRET = "adbc.bigquery.sql.auth.client_secret"
_OPT_REFRESH_TOKEN = "adbc.bigquery.sql.auth.refresh_token"
_OPT_PROJECT_ID = "adbc.bigquery.sql.project_id"
_OPT_LOCATION = "adbc.bigquery.sql.location"
#: The driver's billing/quota project option (set from billing_project_id).
#: Readable via GetOption on the >= 1.11.0 driver, so the load-job client can
#: recover the billing project from the live connection without the runtime.
_OPT_QUOTA_PROJECT = "adbc.bigquery.sql.auth.quota_project"

#: HTTP status codes on Google API call errors that CAN heal between
#: retries. Consulted only after the reason-based classification.
_RETRYABLE_HTTP_CODES = frozenset({408, 429})
#: Google API error ``reason`` codes that are transient per BigQuery's
#: error-table guidance (retry with backoff). The throttling pair matters
#: most: BigQuery maps rateLimitExceeded / quotaExceeded to HTTP 403,
#: which a bare 4xx rule would misread as a deterministic config defect.
_RETRYABLE_GOOGLE_REASONS = frozenset(
    {
        "rateLimitExceeded",
        "quotaExceeded",
        "backendError",
        "internalError",
        # Google's error table says retry-with-backoff for this one too,
        # even though the client library maps it to HTTP 400.
        "tableUnavailable",
    }
)
#: RFC 6749 token-endpoint error codes that cannot heal between retries
#: (revoked/expired grant, bad client credentials, malformed request).
#: A RefreshError naming anything else stays retryable.
_DETERMINISTIC_OAUTH_ERRORS = frozenset(
    {
        "access_denied",
        "invalid_client",
        "invalid_grant",
        "invalid_request",
        "invalid_scope",
        "unauthorized_client",
        "unsupported_grant_type",
    }
)


#: Upper bound on a single batch's deterministic load-job id chain
#: (``<prefix>_0`` ... ``<prefix>_N``). The chain grows at most one link
#: per engine retry, so a real chain never approaches this; the cap is a
#: runaway backstop that raises retryable instead of walking forever.
_MAX_LOAD_JOB_CHAIN = 64


def _pa_type_check(name: str, dtype: pa.DataType) -> bool:
    """Apply a ``pa.types`` predicate that may not exist on older wheels.

    ``is_list_view`` / ``is_large_list_view`` / ``is_run_end_encoded``
    arrived in newer pyarrow releases; the CDK owns the pyarrow pin, so
    this connector degrades gracefully (the predicate reports False and
    the type simply is not treated) instead of failing at import.
    """
    check = getattr(pa.types, name, None)
    return check is not None and check(dtype)


def _is_listish(dtype: pa.DataType) -> bool:
    """True for every Arrow list flavor that materializes a Python list.

    Includes the view variants: they serialize to Parquet as LIST groups
    just like plain lists, so leaving them out would reproduce the exact
    unloadable-column failure this serialization exists to prevent.
    """
    return (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_fixed_size_list(dtype)
        or _pa_type_check("is_list_view", dtype)
        or _pa_type_check("is_large_list_view", dtype)
    )


def _needs_json_serialization(dtype: pa.DataType) -> bool:
    """True when a column of *dtype* must become JSON text to be loadable.

    A genuine struct/list/map column loads into neither a ``JSON`` nor a
    ``STRING`` BigQuery column via a Parquet load job, while the
    Json-family DDL this connector renders is ``STRING``. Deliberately
    narrower than ``pa.types.is_nested``: a TOP-LEVEL union is not a
    Json-family materialization and stays untouched - it fails loud
    client-side at the Parquet write (``ArrowNotImplementedError``,
    wrapped fatal by the load method's pre-upload guard), not at the
    load job. A union nested INSIDE a selected column is rejected by
    ``_validate_json_leaf_types`` instead.
    """
    return (
        pa.types.is_struct(dtype)
        or pa.types.is_map(dtype)
        or _is_listish(dtype)
    )


#: Leaf families with a faithful JSON text form. Anything outside this
#: set (and not rejected outright) degrades to ``str(value)`` - which
#: ``_validate_json_leaf_types`` surfaces as a per-column WARNING so the
#: degradation is never silent.
_JSON_CLEAN_LEAF_CHECKS = (
    pa.types.is_null,
    pa.types.is_boolean,
    pa.types.is_integer,
    pa.types.is_floating,
    pa.types.is_decimal,
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_binary,
    pa.types.is_large_binary,
    pa.types.is_fixed_size_binary,
    pa.types.is_date,
    pa.types.is_time,
    pa.types.is_timestamp,
)


def _validate_json_leaf_types(
    dtype: pa.DataType, path: str, exotic: list[str]
) -> None:
    """Validate a Json-family column's type tree before serialization.

    Raises ``ValueError`` (wrapped deterministic/fatal by the caller's
    pre-upload guard) for leaves JSON text would silently corrupt,
    mirroring ``render_column_type``'s loud-rejection policy for the
    same families at top level:

    * duration / interval - ``str(timedelta)`` is not ISO-8601 (negative
      values render the misleading ``-1 day, 23:59:59``) and
      MonthDayNano renders as a Python repr; a FLAT column of these
      types already fails loud at CREATE TABLE time, so a nested one
      must not silently degrade;
    * unions and any other unrecognized nested form - the value walk
      would ``str()`` container values into Python-repr pseudo-JSON,
      row-dependently;
    * nested map KEY types - a JSON object key must be scalar text.

    Leaves outside ``_JSON_CLEAN_LEAF_CHECKS`` that are not corrupting
    (extension-ish scalars) are collected into *exotic* so the caller
    can WARN once per column about the ``str(value)`` degradation.
    """
    if pa.types.is_dictionary(dtype) or _pa_type_check(
        "is_run_end_encoded", dtype
    ):
        # to_pylist yields decoded logical values; validate those.
        _validate_json_leaf_types(dtype.value_type, path, exotic)
        return
    if pa.types.is_struct(dtype):
        for field in dtype:
            _validate_json_leaf_types(
                field.type, f"{path}.{field.name}", exotic
            )
        return
    if pa.types.is_map(dtype):
        if pa.types.is_nested(dtype.key_type):
            raise ValueError(
                f"column path {path!r} maps from Arrow key type "
                f"{dtype.key_type}, which cannot become a scalar JSON "
                "object key; cast the source column upstream"
            )
        _validate_json_leaf_types(dtype.key_type, f"{path}<key>", exotic)
        _validate_json_leaf_types(dtype.item_type, f"{path}<item>", exotic)
        return
    if _is_listish(dtype):
        _validate_json_leaf_types(dtype.value_type, f"{path}[]", exotic)
        return
    if (
        pa.types.is_duration(dtype)
        or pa.types.is_interval(dtype)
        or pa.types.is_union(dtype)
        or pa.types.is_nested(dtype)
    ):
        raise ValueError(
            f"column path {path!r} has Arrow type {dtype}, which has no "
            "faithful JSON text form on the Parquet load-job write path; "
            "cast the source column upstream (e.g. duration -> Int64 raw "
            "count or Utf8 ISO-8601 text)"
        )
    if not any(check(dtype) for check in _JSON_CLEAN_LEAF_CHECKS):
        exotic.append(f"{path}: {dtype}")


def _json_safe_scalar(value: Any) -> Any:
    """Convert one leaf value from ``to_pylist`` output to a JSON value.

    Mirrors the common JSON conventions for types JSON cannot represent
    natively: temporal values become ISO-8601 text (aware timestamps keep
    their offset), Decimals become strings (float would silently lose
    precision), binary becomes base64 text, and non-finite floats become
    null (strict JSON has no NaN/Infinity, and BigQuery's ``PARSE_JSON``
    rejects them). The ``str`` fallback keeps the conversion total for
    the exotic scalar leaves ``_validate_json_leaf_types`` tolerates -
    corrupting families never reach here (rejected at type level), and
    the degradation is WARNING-logged per column, never silent.
    """
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _json_safe_map_key(key: Any, key_type: pa.DataType) -> str:
    """JSON object key for one Arrow map key: scalar text, loud on loss.

    Keys deliberately do NOT ride the value path: the value rules turn
    non-finite floats into null, which for a KEY would collapse every
    NaN/Infinity key into the literal text ``'None'``, silently merging
    distinct entries. A key that cannot keep its identity as text fails
    the batch (deterministic - wrapped fatal by the pre-upload guard).
    """
    if key is None or (isinstance(key, float) and not math.isfinite(key)):
        raise ValueError(
            f"Arrow map key {key!r} cannot become a JSON object key; "
            "cast the source column upstream"
        )
    safe = _json_safe_value(key, key_type)
    return safe if isinstance(safe, str) else str(safe)


def _json_safe_value(value: Any, dtype: pa.DataType) -> Any:
    """Recursively convert a ``to_pylist`` value to a JSON-dumpable one.

    Walks the Arrow type alongside the Python value so map entries are
    recognized structurally: ``to_pylist`` yields a map as a list of
    ``(key, item)`` tuples, which must become a JSON object (keys via
    ``_json_safe_map_key`` - JSON object keys are always strings, and
    key collisions after text coercion fail loud instead of silently
    last-wins merging), never a list of two-element arrays. Struct
    fields are read by subscript: ``to_pylist`` always yields every
    field, so a missing key is a violated invariant that must raise
    (``KeyError`` is in the pre-upload guard's catch), not degrade to
    null.
    """
    if value is None:
        return None
    if pa.types.is_dictionary(dtype) or _pa_type_check(
        "is_run_end_encoded", dtype
    ):
        return _json_safe_value(value, dtype.value_type)
    if pa.types.is_struct(dtype):
        return {
            field.name: _json_safe_value(value[field.name], field.type)
            for field in dtype
        }
    if pa.types.is_map(dtype):
        entries = {}
        for key, item in value:
            safe_key = _json_safe_map_key(key, dtype.key_type)
            if safe_key in entries:
                raise ValueError(
                    f"map keys collide as JSON object keys: {safe_key!r} "
                    "(duplicate source keys, or distinct keys whose text "
                    "forms coincide); JSON objects cannot hold both"
                )
            entries[safe_key] = _json_safe_value(item, dtype.item_type)
        return entries
    if _is_listish(dtype):
        return [_json_safe_value(item, dtype.value_type) for item in value]
    return _json_safe_scalar(value)


def _jsonify_nested_columns(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Serialize struct/list/map columns to JSON text before Parquet.

    The write map renders every Json-family canonical (``Json``,
    ``Object``, the nested ``List``/``Struct``/``Map`` forms) as
    ``STRING`` DDL, and opaque ``Json`` values already ship as Parquet
    ``STRING`` - that round-trips cleanly. But an endpoint column
    declared ``Object`` with a real ``properties`` sub-schema (or
    ``List`` with ``items``) materializes as a genuine Arrow struct/list
    in the cast batch, which a Parquet load job can populate into
    neither a ``JSON`` nor a ``STRING`` column - the load job fails.
    Serializing those columns to compact JSON text here makes every
    Json-family column land as ``STRING`` regardless of how it
    materialized. Null slots stay NULL (not the text ``"null"``); the
    original field nullability and any schema/field metadata are
    preserved. Each column's type tree is validated first
    (``_validate_json_leaf_types``): leaves JSON text would silently
    corrupt (duration/interval/union, non-scalar map keys) fail loud
    and deterministic, and tolerated exotic leaves are WARNING-logged
    once per column before they degrade to their text form. The text
    lands as ``large_string`` - the same Arrow type opaque ``Json``
    values ship as, and uncapped where ``string`` tops out at 2GB per
    column per batch.
    """
    nested_indices = [
        index
        for index in range(batch.num_columns)
        if _needs_json_serialization(batch.schema.field(index).type)
    ]
    if not nested_indices:
        return batch
    columns = list(batch.columns)
    fields = [batch.schema.field(index) for index in range(batch.num_columns)]
    for index in nested_indices:
        field = fields[index]
        exotic: list[str] = []
        _validate_json_leaf_types(field.type, field.name, exotic)
        if exotic:
            logger.warning(
                "BigQuery JSON serialization of column %r: leaf types "
                "with no canonical JSON form degrade to their Python "
                "text form: %s",
                field.name,
                "; ".join(exotic),
            )
        texts = [
            None
            if value is None
            else json.dumps(
                _json_safe_value(value, field.type),
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for value in columns[index].to_pylist()
        ]
        columns[index] = pa.array(texts, type=pa.large_string())
        fields[index] = pa.field(
            field.name,
            pa.large_string(),
            nullable=field.nullable,
            metadata=field.metadata,
        )
    return pa.RecordBatch.from_arrays(
        columns, schema=pa.schema(fields, metadata=batch.schema.metadata)
    )


# ---- load-job stage-fill machinery (BigQueryDialect.bulk_land helpers) ------


def _build_google_credentials(
    auth_type: str | None, opt: Callable[[str], str | None]
) -> tuple[Any, str]:
    """Build google-auth credentials from the driver's auth options.

    Returns ``(credentials, embedded_project)`` - the project is only
    non-empty for a service-account key (which embeds ``project_id``).
    The two accepted driver auth types are exactly the two the
    connection contract's ``auth_type`` enum can produce via the
    transport's lookup (``service`` / ``user``).

    ``opt`` results are tri-state: a value, ``''`` (the option is
    unset), or ``None`` (``GetOption`` itself raised - traceback at
    DEBUG). The terminal errors keep the two failure directions
    apart: ``None`` points at the driver install (a wheel predating
    the >= 1.11.0 GetOption floor), ``''`` at the connection
    configuration.
    """
    if auth_type is None:
        raise AdbcConfigurationError(
            "BigQuery load jobs: reading auth_type back from the live "
            "ADBC connection failed (GetOption raised; traceback at "
            "DEBUG). The installed adbc-driver-bigquery most likely "
            "predates the >= 1.11.0 floor that implements database "
            "GetOption - fix the driver install, not the connection "
            "configuration"
        )
    if not auth_type:
        raise AdbcConfigurationError(
            "BigQuery load jobs: the ADBC driver returned an empty "
            "auth_type from the live connection - the connection's "
            "auth_type parameter never reached the driver (transport "
            "wiring defect, not an operator error)"
        )
    if auth_type.endswith("json_credential_string"):
        raw = opt(_OPT_AUTH_CREDENTIALS)
        if raw is None:
            raise AdbcConfigurationError(
                "BigQuery load jobs: GetOption raised while reading "
                "auth_credentials back from the live ADBC connection "
                "(traceback at DEBUG); adbc-driver-bigquery >= 1.11.0 "
                "implements it - fix the driver install"
            )
        if not raw:
            raise AdbcConfigurationError(
                "BigQuery load jobs: the ADBC driver returned no "
                "auth_credentials for auth_type=service; the "
                "connection's auth_json_credential secret appears to be "
                "unset"
            )
        try:
            info = json.loads(raw)
        except ValueError as exc:
            raise AdbcConfigurationError(
                "auth_json_credential is not valid JSON (expected the "
                "full service-account key file contents)"
            ) from exc
        if not isinstance(info, dict):
            # A JSON array/string/number parses fine but would raise
            # AttributeError inside google-auth - which is not in the
            # deterministic wrap and would ack RETRYABLE forever.
            raise AdbcConfigurationError(
                "auth_json_credential is not a JSON object (expected "
                "the full service-account key file contents)"
            )
        from google.oauth2 import service_account

        try:
            credentials = (
                service_account.Credentials.from_service_account_info(
                    info, scopes=[_BIGQUERY_SCOPE]
                )
            )
        except (ValueError, TypeError) as exc:
            # google-auth raises MalformedError (a GoogleAuthError) for
            # missing fields, but the cryptography layer raises a plain
            # ValueError for undeserializable private_key material -
            # deterministic bad input that must not ack RETRYABLE.
            raise AdbcConfigurationError(
                "auth_json_credential could not be loaded as a "
                f"service-account key: {exc}"
            ) from exc
        return credentials, str(info.get("project_id") or "")
    if auth_type.endswith("user_authentication"):
        options = {
            "client_id": opt(_OPT_CLIENT_ID),
            "client_secret": opt(_OPT_CLIENT_SECRET),
            "refresh_token": opt(_OPT_REFRESH_TOKEN),
        }
        unreadable = sorted(k for k, v in options.items() if v is None)
        if unreadable:
            raise AdbcConfigurationError(
                "BigQuery load jobs: GetOption raised while reading "
                f"{', '.join(unreadable)} back from the live ADBC "
                "connection (traceback at DEBUG); adbc-driver-bigquery "
                ">= 1.11.0 implements it - fix the driver install"
            )
        missing = sorted(k for k, v in options.items() if not v)
        if missing:
            raise AdbcConfigurationError(
                "BigQuery load jobs: auth_type=user requires client_id, "
                "client_secret, and refresh_token; the connection left "
                f"{', '.join(missing)} unset"
            )
        from google.oauth2.credentials import Credentials as UserCredentials

        credentials = UserCredentials(
            token=None,
            refresh_token=options["refresh_token"],
            client_id=options["client_id"],
            client_secret=options["client_secret"],
            token_uri=_GOOGLE_TOKEN_URI,
            scopes=[_BIGQUERY_SCOPE],
        )
        return credentials, ""
    raise AdbcConfigurationError(
        f"BigQuery load jobs: unsupported driver auth_type {auth_type!r}; "
        "this connector supports auth_type=service (service-account JSON "
        "key) and auth_type=user (OAuth user credentials)"
    )


def _build_bigquery_client(conn: Any) -> tuple[bigquery.Client, str]:
    """Build a google-cloud-bigquery client from the live ADBC connection.

    Credentials and connection parameters are read back from the SAME
    connection state the ADBC transport was materialized with, via
    ``GetOption`` on the database handle (``conn.adbc_database``): the
    service-account JSON for ``auth_type=service``, the client
    id/secret/refresh token for ``auth_type=user``, and the ``project_id``
    / ``quota_project`` (billing) / ``location`` options. Built fresh per
    ``bulk_land`` call and discarded on return - no client is cached (the
    ADR's ``load_job`` mechanism contract) - so this reads no runtime
    config and holds no per-connector state.

    Returns ``(client, data_project)``. *data_project* is the
    dataset-owning project (the connection's ``project_id``, else the
    service-account key's), which may differ from the client's billing
    project.
    """
    from google.cloud import bigquery

    database = conn.adbc_database

    def opt(key: str) -> str | None:
        """Driver option via GetOption: '' = unset, None = call raised."""
        try:
            return database.get_option(key) or ""
        except Exception:
            # Distinguishable from '' so the credential builder can point
            # at the driver install (a pre-GetOption wheel) instead of
            # misreporting a connection-config defect.
            logger.debug(
                "GetOption(%r) raised on the ADBC BigQuery database "
                "handle; treating the option as unreadable",
                key,
                exc_info=True,
            )
            return None

    credentials, key_project = _build_google_credentials(opt(_OPT_AUTH_TYPE), opt)
    data_project = opt(_OPT_PROJECT_ID) or key_project
    billing_project = opt(_OPT_QUOTA_PROJECT) or data_project
    if not billing_project:
        raise AdbcConfigurationError(
            "BigQuery load jobs need a GCP project: set project_id (or "
            "billing_project_id) on the connection, or authenticate with "
            "a service-account key that embeds project_id"
        )
    location = opt(_OPT_LOCATION) or None
    client = bigquery.Client(
        project=billing_project, credentials=credentials, location=location
    )
    return client, (data_project or billing_project)


def _resolve_dataset_location(
    client: bigquery.Client, destination: bigquery.TableReference
) -> str | None:
    """Resolve the destination dataset's location for job get/insert.

    ``jobs.insert`` infers the location from the dataset, but
    ``jobs.get`` requires it outside the US/EU multi-regions - a
    location-blind lookup returns NotFound for jobs that exist, which
    would silently disable the attach/drain machinery for regional
    datasets. A dataset's location is immutable, but the client is built
    per call, so this resolves per call (one cheap metadata GET; the
    load job that follows dwarfs it). Resolution failures propagate
    through ``bulk_land``'s classification (a missing dataset is
    deterministic; network trouble retryable).
    """
    from google.cloud import bigquery

    return (
        client.get_dataset(
            bigquery.DatasetReference(destination.project, destination.dataset_id)
        ).location
        or ""
    ) or None


def _stage_row_count(
    client: bigquery.Client,
    destination: bigquery.TableReference,
    *,
    location: str | None,
) -> int:
    """Live ``SELECT COUNT(*)`` of the stage table.

    Run only on a retry (a prior chain job existed) to decide whether a
    drained prior job already landed this batch into the current stage
    incarnation. A live query is authoritative and matches the engine's
    own post-``bulk_land`` ``SELECT COUNT(*)`` verify - unlike
    ``get_table().num_rows``, whose table metadata is eventually
    consistent and can lag a just-committed load, which would risk a
    double-load the engine's verify then FATALs.
    """
    fqn = (
        f"`{destination.project}`.`{destination.dataset_id}`"
        f".`{destination.table_id}`"
    )
    sql = f"SELECT COUNT(*) FROM {fqn}"  # nosec B608
    rows = list(client.query(sql, location=location).result())
    return int(rows[0][0]) if rows else 0


def _load_job_id_prefix(
    stage_table_name: str, parquet_payload: bytes | memoryview
) -> str:
    """Deterministic load-job id prefix for a stage-fill.

    Binds the engine's collision-proof stage identity
    (``stage_table_name`` already embeds ``sha256(run_id|stream_id|
    batch_seq|target)``) and a SHA-256 of the exact Parquet payload, so
    an id can only ever attach to a job that loaded these very bytes into
    this very stage. Engine retries resend the identical record batch
    (and the cast + Parquet encode are deterministic in-process), so
    retries land on the same chain; a restarted read that produces
    different bytes gets a different chain and simply loads them. 32 hex
    chars (128 bits) keep accidental collision with any other job in the
    project's global, long-retention job namespace out of consideration.
    """
    content_hash = hashlib.sha256(parquet_payload).hexdigest()
    token = hashlib.sha256(
        f"{stage_table_name}|{content_hash}".encode()
    ).hexdigest()[:32]
    return f"analitiq_{token}"


def _job_terminal_state(job: Any) -> tuple[bool, bool]:
    """(reached DONE, succeeded) for *job*, refreshed best-effort.

    The reload targets the job's own stored project/location, so no
    location plumbing is needed. When the reload itself fails, the
    job's LOCALLY cached state still decides: ``result()`` refreshes
    the job before raising a stored job error, so a locally-DONE job
    with an ``error_result`` is provably terminal even without a
    fresh reload - without this fallback, a network blip on the
    reload would convert a burnable terminal failure into a spurious
    "possibly live" re-raise. A pure polling failure leaves the local
    state non-DONE, preserving the conservative re-raise exactly where
    it matters.
    """
    try:
        job.reload()
    except Exception:
        logger.debug(
            "BigQuery job %s state reload failed after a polling "
            "error; falling back to the locally cached job state",
            getattr(job, "job_id", "<unknown>"),
            exc_info=True,
        )
    done = job.state == "DONE"
    return done, done and job.error_result is None


def _drain_load_job_chain(
    client: bigquery.Client, job_id_prefix: str, *, location: str | None
) -> int:
    """Drain this batch's load-job id chain and return the next free index.

    Walks ``<prefix>_0``, ``<prefix>_1``, ... until an id does not exist,
    attaching to (polling to terminal) the last one if it is still in
    flight. The engine serializes the whole stage cycle per backend, so
    only the last id can be non-terminal, and it is an abandoned job of a
    PRIOR attempt of this same batch (a client-side polling timeout, an
    ack-budget cancellation, a container restart). Draining it to a
    terminal state before ``bulk_land`` submits a fresh id closes the
    window where that job commits into the freshly-recreated same-named
    stage after a fresh load already filled it.

    The except branch distinguishes THE JOB failing from OUR POLLING
    failing: after a poll error the job is reloaded, and a job that did
    not provably reach DONE re-raises (the batch acks RETRYABLE and the
    next attempt re-attaches to the same id) rather than being advanced
    past while possibly still live. A job that reached a terminal state -
    committed or failed - is left for ``bulk_land``'s stage row-count
    guard to act on: reuse its rows if it filled the current stage
    incarnation, else submit fresh (a failed load commits nothing).
    Bounded by ``_MAX_LOAD_JOB_CHAIN`` as a runaway backstop.
    """
    from google.api_core import exceptions as api_exceptions

    index = 0
    last_job: Any = None
    while True:
        if index >= _MAX_LOAD_JOB_CHAIN:
            raise RuntimeError(
                f"BigQuery load-job chain {job_id_prefix} reached "
                f"{_MAX_LOAD_JOB_CHAIN} ids without draining; refusing to "
                "extend it this attempt. Investigate why this stage's load "
                "jobs keep failing transiently (all chain ids share this "
                "prefix in the project's job history); the engine's retry "
                "cap bounds further attempts"
            )
        try:
            job = client.get_job(f"{job_id_prefix}_{index}", location=location)
        except api_exceptions.NotFound:
            break
        if last_job is not None and last_job.state != "DONE":
            # Violates the chain invariant; observable, not fatal - the
            # walk still only acts on the last job.
            logger.warning(
                "BigQuery load-job chain %s has a non-terminal non-last "
                "job %s (state %s); the chain invariant expects only the "
                "last id to be in flight",
                job_id_prefix,
                last_job.job_id,
                last_job.state,
            )
        last_job = job
        index += 1
    if last_job is not None and last_job.state != "DONE":
        logger.info(
            "Attaching to in-flight BigQuery load job %s from a previous "
            "attempt (client-side polling was interrupted; the job kept "
            "running server-side)",
            last_job.job_id,
        )
        try:
            last_job.result()
        except Exception:
            done, _succeeded = _job_terminal_state(last_job)
            if not done:
                # OUR polling (or the reload) failed - the job may still
                # be running. Never advance past a live job: re-raise so
                # the engine backs off and the next attempt re-attaches.
                raise
            # Terminal (committed or failed): fall through. bulk_land's
            # stage row-count guard decides reuse vs. fresh submit.
    return index


def _submit_and_poll(
    client: bigquery.Client,
    buffer: io.BytesIO,
    destination: bigquery.TableReference,
    *,
    job_id: str,
    location: str | None,
) -> None:
    """Submit one ``WRITE_APPEND`` Parquet load job into the stage and poll.

    ``WRITE_APPEND`` + ``CREATE_NEVER`` preserve the stage's
    ``CREATE TABLE ... LIKE`` schema (column types and default value
    expressions) and never re-infer it from the Parquet file, so columns
    the batch did not land keep their DEFAULT and a missing stage is a
    loud defect, not a silent Parquet-inferred re-creation. An
    ``Already Exists: Job`` conflict means a submission of this very id
    already reached the server - an HTTP-layer retry of this attempt's
    own insert, or a job the drain walk raced - and it loaded this
    payload into this stage, so attach and poll it.
    """
    from google.api_core import exceptions as api_exceptions
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )
    buffer.seek(0)
    try:
        client.load_table_from_file(
            buffer,
            destination,
            job_config=job_config,
            job_id=job_id,
            location=location,
        ).result()
    except api_exceptions.Conflict:
        logger.info(
            "BigQuery load job %s already exists; attaching to the prior "
            "submission",
            job_id,
        )
        client.get_job(job_id, location=location).result()


def _google_error_reasons(exc: BaseException) -> frozenset[str]:
    """Collect the per-error ``reason`` codes off a Google API error.

    google-cloud-bigquery surfaces the job/API error list as
    ``exc.errors`` - a list of dicts with ``reason`` / ``message``
    keys. Absent or unparseable entries yield the empty set, which
    falls back to status-code classification.
    """
    reasons: set[str] = set()
    for err in getattr(exc, "errors", None) or ():
        if isinstance(err, Mapping):
            reason = err.get("reason")
            if reason:
                reasons.add(str(reason))
    return frozenset(reasons)


def _is_deterministic_refresh_error(exc: BaseException) -> bool:
    """True when a RefreshError names a deterministic OAuth error.

    google-auth marks transient refresh failures (token-endpoint 5xx,
    ``server_error`` / ``temporarily_unavailable``) with
    ``retryable=True`` - honor that first. Otherwise look for a
    deterministic RFC 6749 error code in the response payload (a
    Mapping in ``args``) or the message text. A refresh failure with
    no recognizable code stays retryable: a few wasted retries on a
    dead credential cost far less than fatally failing a healthy
    stream on a transient wobble.
    """
    if getattr(exc, "retryable", False):
        return False
    for arg in exc.args:
        if isinstance(arg, Mapping):
            code = str(arg.get("error") or "")
            if code:
                return code in _DETERMINISTIC_OAUTH_ERRORS
    text = str(exc)
    return any(code in text for code in _DETERMINISTIC_OAUTH_ERRORS)


def _is_deterministic_google_error(exc: BaseException) -> bool:
    """True when a Google-side failure cannot heal between retries.

    ``bulk_land`` acks RETRYABLE for anything not
    ``AdbcConfigurationError``-shaped, so this predicate decides
    retry-with-backoff vs fail-fatal:

    * ``TransportError`` (network trouble reaching Google's OAuth
      endpoint) - never deterministic.
    * ``RefreshError`` - deterministic only when it names a
      deterministic OAuth error code (``invalid_grant`` etc.);
      token-endpoint 5xx and unclassifiable refresh failures stay
      retryable. Realistic under auth_type=user: the client is
      rebuilt (and refreshes) on every call.
    * Other ``GoogleAuthError`` (malformed / missing credential
      material) - deterministic.
    * ``GoogleAPICallError`` - classified by error *reason* first:
      BigQuery maps throttling (rateLimitExceeded / quotaExceeded) to
      HTTP 403, which a bare 4xx rule would misread as a config
      defect; Google's guidance for those is retry with backoff.
      Unambiguously deterministic reasons (accessDenied, invalid,
      notFound, duplicate) carry non-408/429 4xx codes and fall
      through to the status rule. 5xx stays retryable.
    """
    from google.api_core import exceptions as api_exceptions
    from google.auth import exceptions as auth_exceptions

    if isinstance(exc, auth_exceptions.TransportError):
        return False
    if isinstance(exc, auth_exceptions.RefreshError):
        return _is_deterministic_refresh_error(exc)
    if isinstance(exc, auth_exceptions.GoogleAuthError):
        return True
    if isinstance(exc, api_exceptions.GoogleAPICallError):
        if _google_error_reasons(exc) & _RETRYABLE_GOOGLE_REASONS:
            return False
        code = getattr(exc, "code", None)
        return (
            isinstance(code, int)
            and 400 <= code < 500
            and code not in _RETRYABLE_HTTP_CODES
        )
    return False


class BigQueryDialect(SqlDialect):
    """BigQuery SQL strategy: backtick quoting, three-level (project ->
    dataset -> table) addressing, NOT ENFORCED primary keys, dataset-scoped
    INFORMATION_SCHEMA composition, the ``CREATE TABLE ... LIKE`` stage
    clone for the MERGE upsert, and load-job-aware type rendering (the
    NUMERIC/BIGNUMERIC precision-range choice; Duration/Interval
    rejection)."""

    name = "bigquery"
    #: BigQuery reads double quotes as string literals; identifiers use backticks.
    quote_char = "`"
    #: BigQuery never enforces PK uniqueness and its parser requires the
    #: ``NOT ENFORCED`` qualifier on the PRIMARY KEY clause.
    pk_not_enforced = True
    #: The per-dataset INFORMATION_SCHEMA pseudo-schema is hidden from discovery.
    #: (Three-level project -> dataset -> table addressing and the MERGE upsert
    #: are declared in connector.json ``sql_capabilities`` - ``catalog: full``
    #: and ``merge_form: merge`` - not as dialect flags.)
    system_schemas = ("INFORMATION_SCHEMA",)

    #: Decimal128/Decimal256 canonical (post-``normalize_canonical_type``
    #: form; the tolerant whitespace padding is defense in depth).
    _DECIMAL_RE = re.compile(
        r"^Decimal(?:128|256)\(\s*(?P<p>\d+)\s*,\s*(?P<s>\d+)\s*\)$"
    )
    #: Canonical families with no loadable BigQuery representation on the
    #: Parquet load-job write path. ``render_column_type`` rejects them at
    #: CREATE TABLE time; they are deliberately absent from
    #: type-map-write.json (a takeover, not a coverage gap).
    _UNLOADABLE_TEMPORAL_RE = re.compile(r"^(?P<family>Duration|Interval)\b")

    # ---- discovery: dataset-scoped INFORMATION_SCHEMA ----------------------
    def information_schema_ref(
        self, view: str, *, catalog: str = "", schema: str = ""
    ) -> str:
        """Compose BigQuery's scoped ``INFORMATION_SCHEMA`` path.

        The ANSI base emits the session-local, lowercase
        ``information_schema.<view>``, which BigQuery rejects: its metadata
        views are addressed per scope and the ``INFORMATION_SCHEMA`` prefix
        must be uppercase. This override composes:

        * ``schemata`` (schemas_query, no *schema* argument) ->
          ``INFORMATION_SCHEMA.SCHEMATA`` or
          ``` `project`.INFORMATION_SCHEMA.SCHEMATA ``` - project-scoped;
          the unqualified form resolves against the connection's default
          project (``adbc.bigquery.sql.project_id``). Region scoping comes
          from the query job's location (``adbc.bigquery.sql.location``).
        * ``tables`` / ``columns`` / ``table_constraints`` /
          ``key_column_usage`` (dataset-scoped queries pass *schema*) ->
          ``` `dataset`.INFORMATION_SCHEMA.<VIEW> ``` or
          ``` `project`.`dataset`.INFORMATION_SCHEMA.<VIEW> ```.

        The base queries' ``table_schema = ?`` / ``catalog_name = ?``
        filters remain correct against these views (the columns exist and
        carry the dataset / project id), so only the FROM path needs
        overriding. ``system_schemas`` filtering composes cleanly:
        ``SCHEMATA`` lists real datasets only, so the ``schema_name NOT IN
        ('INFORMATION_SCHEMA')`` guard is a harmless belt-and-braces.
        """
        self._check_catalog(catalog)
        qualifiers = [self.quote_ident(part) for part in (catalog, schema) if part]
        return ".".join([*qualifiers, f"INFORMATION_SCHEMA.{view.upper()}"])

    # ---- stage-then-apply write renderings (ADR sql-write-path-v2) ----------
    def stage_table_sql(
        self, stage: TableAddress, target: TableAddress, *, temp: bool
    ) -> str:
        """``CREATE`` the batch's stage table shaped like *target*.

        The connector declares ``sql_capabilities.stage.scope: real``
        (BigQuery has no usable session-temp table semantics for a
        load-job target), so the engine derives *temp* as False and this
        renders a plain ``CREATE TABLE``. ``CREATE TABLE ... LIKE`` clones
        the target's column definitions - including default value
        expressions - with no rows; ``bulk_land`` fills the clone, then
        the engine's mode statement applies it to the target.
        """
        return (
            f"CREATE TABLE {self.quote_table(stage)} "
            f"LIKE {self.quote_table(target)}"
        )

    def merge_statement_sql(
        self,
        stage: TableAddress,
        target: TableAddress,
        conflict_keys: Sequence[str],
        columns: Sequence[str],
    ) -> str:
        """Render BigQuery's GoogleSQL ``MERGE`` upsert (stage -> target).

        ``conflict_keys`` are the match keys (BigQuery PKs are NOT
        ENFORCED - the MERGE needs only the ``ON`` clause); ``columns``
        are the landed columns in stage order. The updated columns are
        ``columns`` minus the keys: columns the target has but the batch
        did not land keep their stored value on matched rows and their
        DEFAULT on inserted ones. When every landed column is a conflict
        key there is nothing to update, so the ``WHEN MATCHED`` clause is
        omitted - a ``WHEN NOT MATCHED THEN INSERT``-only MERGE, BigQuery's
        insert-only degradation (matched rows stay untouched, never an
        error). GoogleSQL rejects a target-alias prefix on ``UPDATE SET``
        assignment targets, so the SET items are unqualified
        (``col = s.col``); the ``ON`` / ``INSERT`` clauses keep the
        alias-qualified form.
        """
        stage_q = self.quote_table(stage)
        target_q = self.quote_table(target)
        keys = set(conflict_keys)
        update_cols = [c for c in columns if c not in keys]
        on_clause = " AND ".join(
            f"t.{self.quote_ident(k)} = s.{self.quote_ident(k)}"
            for k in conflict_keys
        )
        insert_cols = ", ".join(self.quote_ident(c) for c in columns)
        insert_vals = ", ".join(f"s.{self.quote_ident(c)}" for c in columns)
        statement = (
            f"MERGE INTO {target_q} t USING {stage_q} s "  # nosec B608
            f"ON {on_clause} "
        )
        if update_cols:
            set_clause = ", ".join(
                f"{self.quote_ident(c)} = s.{self.quote_ident(c)}"
                for c in update_cols
            )
            statement += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        statement += (
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
            f"VALUES ({insert_vals})"
        )
        return statement

    def empty_table_sql(self, target: TableAddress) -> str:
        """Empty *target* before a truncate-insert's first batch.

        BigQuery's ``DELETE`` requires a ``WHERE`` clause (it rejects a
        bare ``DELETE FROM t``), and ``TRUNCATE`` implicitly commits and
        would break the stage cycle's shape, so the always-true predicate
        ``WHERE TRUE`` is the conformant empty-all form (a ``DELETE`` that
        removes every row on the same connection, inside the cycle).
        """
        return f"DELETE FROM {self.quote_table(target)} WHERE TRUE"

    def bulk_land(
        self,
        conn: Any,
        stage: TableAddress,
        batch: pa.RecordBatch,
        *,
        runtime: Any,
    ) -> bool:
        """Fill the freshly-created *stage* table with *batch* via a load job.

        The BigQuery ADBC driver implements no ``adbc.ingest.*`` bulk path
        and per-row DML INSERT is a BigQuery anti-pattern, so this dialect
        declares ``sql_capabilities.bulk_load.adbc: load_job`` and lands
        each batch as a Parquet **load job** (direct media upload - no GCS
        staging bucket) into the stage the engine already created via
        ``stage_table_sql``. The load-job client is built from the live
        ADBC connection state (``conn``) and discarded on return; *runtime*
        is unused (all connection state is recovered from ``conn``).

        Struct/list/map columns are serialized to JSON text first
        (``_jsonify_nested_columns`` - their DDL is ``STRING``, which a
        Parquet nested column cannot populate). The batch is written to an
        in-memory Parquet buffer and submitted under a deterministic,
        orphan-drained job-id chain (``_load_job_id_prefix`` /
        ``_drain_load_job_chain``): a within-call polling timeout attaches
        to the running job, and across engine retries any abandoned prior
        job of this batch is drained to a terminal state before a fresh id
        is submitted - reusing its rows if it already landed this exact
        payload into the current stage incarnation (row count matches),
        else submitting fresh.

        Returns ``True`` (landed) so the backend skips its executemany
        fallback. Failure classification: serialization defects and Google
        errors that cannot heal between retries raise
        ``AdbcConfigurationError`` (the batch fails FATAL); everything else
        propagates and acks RETRYABLE. The per-call client is always closed.
        """
        from google.cloud import bigquery

        del runtime  # credentials/parameters are recovered from conn

        if not stage.schema:
            # Defense in depth: the engine rejects schema-less ADBC
            # destinations at configure_schema time, so the normal
            # lifecycle cannot reach this raise.
            raise AdbcConfigurationError(
                f"BigQuery load job for {stage} has no dataset; ADBC "
                "destinations require a dataset (database_object.schema)"
            )
        client: bigquery.Client | None = None
        try:
            client, data_project = _build_bigquery_client(conn)
            try:
                destination = bigquery.TableReference(
                    bigquery.DatasetReference(
                        stage.catalog or data_project or client.project,
                        stage.schema,
                    ),
                    stage.table,
                )
                loadable_batch = _jsonify_nested_columns(batch)
                buffer = io.BytesIO()
                pq.write_table(pa.Table.from_batches([loadable_batch]), buffer)
            except (
                pa.ArrowException,
                ValueError,
                TypeError,
                KeyError,
                RecursionError,
            ) as exc:
                if isinstance(exc, pa.ArrowMemoryError):
                    # Transient memory pressure, not a deterministic input
                    # defect - leave it retryable. (A plain MemoryError
                    # from to_pylist materializing a huge nested column
                    # escapes this wrap entirely and also acks RETRYABLE.)
                    raise
                # Deterministic client-side failure: a malformed
                # project/dataset/table id, an Arrow type Parquet cannot
                # store, or a nested value that resists JSON serialization
                # (a rejected leaf family, a colliding map key, a violated
                # struct-shape invariant via KeyError, or nesting past
                # Python's recursion limit). pyarrow's
                # ArrowNotImplementedError subclasses NotImplementedError -
                # not a PEP-249 fatal name - so without this wrap it would
                # ack RETRYABLE and the batch would retry forever.
                raise AdbcConfigurationError(
                    f"{type(exc).__name__}: BigQuery load job for {stage} "
                    "failed before upload (building the table reference / "
                    "JSON-serializing nested columns / serializing the "
                    f"batch to Parquet): {exc}"
                ) from exc
            location = _resolve_dataset_location(client, destination)
            job_id_prefix = _load_job_id_prefix(stage.table, buffer.getbuffer())
            next_index = _drain_load_job_chain(
                client, job_id_prefix, location=location
            )
            if next_index > 0:
                # A prior attempt's job existed and was drained to a
                # terminal state above. If it already landed this exact
                # payload into the CURRENT stage incarnation (the engine
                # drops+recreates the stage each attempt, and load jobs
                # commit atomically, so a live count is 0 or this batch),
                # reuse it rather than double-loading; else submit fresh. A
                # live COUNT (not the eventually-consistent table metadata)
                # is authoritative and agrees with the engine's own
                # post-bulk_land SELECT COUNT(*) verify.
                landed_rows = _stage_row_count(
                    client, destination, location=location
                )
                if landed_rows == batch.num_rows:
                    logger.info(
                        "BigQuery stage %s already holds this batch (%d "
                        "rows) from a drained prior attempt of chain %s; "
                        "reusing it instead of re-submitting",
                        stage,
                        batch.num_rows,
                        job_id_prefix,
                    )
                    return True
            _submit_and_poll(
                client,
                buffer,
                destination,
                job_id=f"{job_id_prefix}_{next_index}",
                location=location,
            )
            return True
        except AdbcConfigurationError:
            raise
        except Exception as exc:
            if _is_deterministic_google_error(exc):
                raise AdbcConfigurationError(
                    f"{type(exc).__name__}: BigQuery load job into {stage} "
                    f"failed deterministically: {exc}"
                ) from exc
            raise
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    logger.debug(
                        "BigQuery load-job client close failed", exc_info=True
                    )

    # ---- column type rendering ---------------------------------------------
    def render_column_type(
        self,
        canonical: str,
        type_mapper: TypeMapper,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> str:
        """Render a canonical Arrow type to BigQuery DDL.

        Two families need code - both because the write path is a Parquet
        load job, which the declarative write map cannot reason about:

        * **Decimal128/Decimal256**: NUMERIC vs BIGNUMERIC is chosen from
          BOTH precision and scale - including each type's integer-digit
          bound (``precision - scale``) - so a plain regex rule would emit
          an out-of-range ``NUMERIC(30, 15)``. An invalid decimal shape
          (precision < 1, or scale > precision - both reachable
          cross-source; PostgreSQL 15+ allows declared scale > precision)
          and a Decimal that fits neither native range fail loud at render
          time rather than emitting invalid DDL.
        * **Duration/Interval**: rejected outright. BigQuery INTERVAL
          columns cannot be populated by a Parquet load job - Arrow
          interval values have no Parquet encoding (the Parquet writer
          itself raises), and Arrow duration serializes to Parquet INT64,
          which BigQuery refuses to load into INTERVAL. Rendering INTERVAL
          DDL anyway would create a table whose loads can never succeed,
          so this fails at CREATE TABLE time - deterministic, before the
          table exists - with a message naming the upstream fix.

        The canonical is normalized first (``normalize_canonical_type``)
        so every spelling the base mapper would accept takes these same
        paths instead of bypassing them. Every other canonical delegates
        to ``type-map-write.json`` through the base implementation.
        """
        normalized = normalize_canonical_type(canonical)
        unloadable = self._UNLOADABLE_TEMPORAL_RE.match(normalized)
        if unloadable is not None:
            if unloadable.group("family") == "Interval":
                detail = (
                    "Arrow interval values have no Parquet encoding "
                    "(pyarrow's Parquet writer raises on "
                    "month_day_nano_interval)"
                )
            else:
                detail = (
                    "Arrow duration serializes to Parquet INT64, which "
                    "BigQuery refuses to load into an INTERVAL column"
                )
            raise ValueError(
                f"{self.name}: canonical type {canonical!r} has no loadable "
                f"BigQuery representation on the Parquet load-job write "
                f"path ({detail}); failing at CREATE TABLE time, before an "
                "unloadable table exists. Cast the source column upstream "
                "- e.g. to Utf8 (ISO-8601 text) or, for Duration, Int64 "
                "(a raw count in the declared unit)"
            )
        match = self._DECIMAL_RE.match(normalized)
        if match is not None:
            precision = int(match.group("p"))
            scale = int(match.group("s"))
            if precision < 1 or scale > precision:
                raise ValueError(
                    f"{self.name}: Decimal(precision={precision}, scale={scale}) "
                    "is not a renderable BigQuery decimal shape: precision must "
                    "be >= 1 and scale must not exceed precision"
                )
            # BigQuery NUMERIC/DECIMAL: precision <= 38, scale <= 9, and
            # integer digits (precision - scale) <= 29.
            if precision <= 38 and scale <= 9 and (precision - scale) <= 29:
                return f"NUMERIC({precision}, {scale})"
            # BigQuery BIGNUMERIC/BIGDECIMAL: precision <= 76, scale <= 38,
            # and integer digits (precision - scale) <= 38.
            if precision <= 76 and scale <= 38 and (precision - scale) <= 38:
                return f"BIGNUMERIC({precision}, {scale})"
            raise ValueError(
                f"{self.name}: Decimal(precision={precision}, scale={scale}) "
                "fits neither NUMERIC (precision <= 38, scale <= 9, integer "
                "digits precision - scale <= 29) nor BIGNUMERIC (precision "
                "<= 76, scale <= 38, integer digits precision - scale <= 38); "
                "no lossless native decimal type exists"
            )
        return super().render_column_type(canonical, type_mapper, params=params)


class BigQueryConnector(GenericSQLConnector):
    """Google BigQuery connector: the CDK SQL base wired to the BigQuery
    dialect.

    Reads and discovery ride the ADBC BigQuery driver and the shared
    SQLAlchemy read path; writes ride the CDK stage-then-apply cycle (ADR
    sql-write-path-v2) with ``BigQueryDialect.bulk_land`` (a Parquet load
    job) as the stage-fill mechanism - the BigQuery ADBC driver implements
    no ``adbc.ingest.*`` bulk path. Every write capability is declared in
    ``definition/connector.json`` ``sql_capabilities``; this class only
    binds the dialect.
    """

    dialect_class = BigQueryDialect
