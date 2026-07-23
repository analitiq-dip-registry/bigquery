"""Google BigQuery connector - dialect + connector class for the Analitiq CDK.

BigQuery runs on the first-class ADBC BigQuery driver (``transport_type:
adbc``) for connect, SQL execution, discovery, and Arrow reads. BigQuery is
an HTTPS REST service - there is no host/port, username, or password - and
traffic to ``googleapis.com`` is always TLS with no selectable mode, so
there is no ``ssl_mode`` input and no ``build_tls_connect_arg`` hook. All
connection state is carried in the transport's ``adbc.bigquery.sql.*``
``db_kwargs``.

**Writes do NOT use the base ADBC ingest path.** The BigQuery ADBC driver
implements no ``adbc.ingest.*`` statement options (``cursor.adbc_ingest``
fails on the first option), and per-row DML INSERT is a BigQuery
anti-pattern (quota-bound, slow, costly). This connector instead ships each
cast Arrow batch as an in-memory Parquet buffer submitted as a BigQuery
**load job** (direct media upload - no GCS staging bucket):

* **Append / truncate-insert** - ``_adbc_only_ingest_sync`` is overridden
  to run a ``WRITE_APPEND`` load job into the target table (TRUNCATE still
  runs as SQL over the ADBC connection).
* **Upsert / keyless-insert dedup** - ``_merge_ingest_locked_sync`` is
  overridden to keep the base's exact stage lifecycle (pre-flight ``DROP
  TABLE IF EXISTS`` -> ``CREATE TABLE ... LIKE`` clone -> fill -> ``MERGE
  INTO`` -> ``DROP``) with the stage fill replaced by a load job. MERGE on
  the declared conflict keys works with BigQuery's NOT ENFORCED primary
  keys.
* **Credentials** - recovered from the SAME connection state the transport
  was materialized with, via ``GetOption`` on the live ADBC database handle
  (service-account JSON for ``auth_type=service``; client id/secret/refresh
  token for ``auth_type=user``). No second credential input exists.

Everything else BigQuery-specific lives here:

* **Backtick quoting** - BigQuery reads ``\"...\"`` as a string literal, so
  identifiers are quoted with backticks (``quote_char``).
* **Three-level addressing** - project -> dataset -> table; the project is a
  per-statement addressable catalog (``supports_catalog_addressing``).
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
* **NUMERIC/BIGNUMERIC render arithmetic** - the single write-direction
  logic ``type-map-write.json`` cannot express. NUMERIC (precision <= 38,
  scale <= 9, integer digits ``precision - scale`` <= 29) vs BIGNUMERIC
  (precision <= 76, scale <= 38, integer digits ``precision - scale`` <=
  38) is chosen from BOTH a Decimal's precision and scale, so a plain regex
  rule would emit an out-of-range ``NUMERIC(30, 15)``. Everything else
  delegates back to the write map via ``super().render_column_type``.

The read path still compiles paged ``SELECT``s through SQLAlchemy Core and
resolves this dialect by name via
``sqlalchemy.dialects.registry.load(\"bigquery\")``, so ``sqlalchemy-bigquery``
is a required runtime dependency (see ``requirements.txt``) even though the
data transport is ADBC.

Registered under connector_id ``bigquery`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from cdk.adbc_registry import AdbcConfigurationError
from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector
from cdk.type_map import normalize_canonical_type

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
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

#: PEP-249 exception class names whose failures cannot heal between retries.
#: Mirrors the base ADBC write path's fatal classification (the base's
#: helpers are module-private, so the two-line check is replicated here).
_FATAL_DBAPI_ERROR_NAMES = frozenset(
    {"ProgrammingError", "NotSupportedError", "IntegrityError", "DataError"}
)
#: HTTP status codes on Google API call errors that CAN heal between retries.
_RETRYABLE_HTTP_CODES = frozenset({408, 429})


def _close_cursor_quietly(cursor: Any) -> None:
    """Close an ADBC cursor, downgrading a close failure to a debug log."""
    try:
        cursor.close()
    except Exception:
        logger.debug("ADBC cursor close failed", exc_info=True)


def _is_fatal_dbapi_error(exc: BaseException) -> bool:
    """True when *exc* is a PEP-249 failure class retries cannot heal."""
    return any(cls.__name__ in _FATAL_DBAPI_ERROR_NAMES for cls in type(exc).__mro__)


def _as_fatal(exc: BaseException) -> AdbcConfigurationError:
    """Wrap a fatal DBAPI error so the engine stops retrying the batch."""
    wrapped = AdbcConfigurationError(f"{type(exc).__name__}: {exc}")
    wrapped.__cause__ = exc
    return wrapped


class BigQueryDialect(SqlDialect):
    """BigQuery SQL strategy: backtick quoting, three-level (project ->
    dataset -> table) addressing, NOT ENFORCED primary keys, dataset-scoped
    INFORMATION_SCHEMA composition, the ``CREATE TABLE ... LIKE`` stage
    clone for the MERGE upsert, and the NUMERIC/BIGNUMERIC precision-range
    render override."""

    name = "bigquery"
    #: BigQuery reads double quotes as string literals; identifiers use backticks.
    quote_char = "`"
    #: BigQuery never enforces PK uniqueness and its parser requires the
    #: ``NOT ENFORCED`` qualifier on the PRIMARY KEY clause.
    pk_not_enforced = True
    #: Three-level object hierarchy (project -> dataset -> table): the project
    #: is a per-statement addressable catalog.
    supports_catalog_addressing = True
    #: The per-dataset INFORMATION_SCHEMA pseudo-schema is hidden from discovery.
    system_schemas = ("INFORMATION_SCHEMA",)
    #: Upsert runs as stage-clone + MERGE. The connector class supplies the
    #: stage *fill* (a Parquet load job - the BigQuery ADBC driver has no
    #: ``adbc.ingest.*`` path); this flag routes upsert and keyless-insert
    #: dedup through that MERGE machinery.
    supports_upsert_adbc = True

    #: Decimal128/Decimal256 canonical (post-``normalize_canonical_type``
    #: form; the tolerant ``\\s*`` padding is defense in depth).
    _DECIMAL_RE = re.compile(
        r"^Decimal(?:128|256)\(\s*(?P<p>\d+)\s*,\s*(?P<s>\d+)\s*\)$"
    )

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

    # ---- ADBC-only write path ----------------------------------------------
    def adbc_stage_table_sql(
        self, stage_qualified: str, target_qualified: str
    ) -> str:
        """Stage table for the MERGE upsert: clone the target's schema.

        BigQuery's ``CREATE TABLE ... LIKE`` copies the source table's
        column definitions (including default value expressions) with no
        data; the connector's load job fills this clone before ``MERGE
        INTO`` the target.
        """
        return f"CREATE TABLE {stage_qualified} LIKE {target_qualified}"

    # ---- column type rendering ---------------------------------------------
    def render_column_type(
        self,
        canonical: str,
        type_mapper: TypeMapper,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> str:
        """Render a canonical Arrow type to BigQuery DDL.

        Only the Decimal family needs code: NUMERIC vs BIGNUMERIC is chosen
        from BOTH precision and scale - including each type's integer-digit
        bound (``precision - scale``) - which the declarative write map
        cannot express. The canonical is normalized first
        (``normalize_canonical_type``) so every spelling the base mapper
        would accept takes this same path instead of bypassing it. An
        invalid decimal shape (precision < 1, or scale > precision - both
        reachable cross-source; PostgreSQL 15+ allows declared scale >
        precision) and a Decimal that fits neither native range fail loud
        at render time rather than emitting invalid DDL. Every other
        canonical delegates to ``type-map-write.json`` through the base
        implementation.
        """
        match = self._DECIMAL_RE.match(normalize_canonical_type(canonical))
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
    dialect, with the ADBC ingest steps replaced by BigQuery load jobs
    (the sanctioned thick-path override - the BigQuery ADBC driver
    implements no ``adbc.ingest.*`` bulk path)."""

    dialect_class = BigQueryDialect

    def __init__(self) -> None:
        super().__init__()
        # Lazily-built google-cloud-bigquery client for the load-job write
        # path. Built and used only under the base's _adbc_op_lock (all
        # write entry points hold it), so no extra lock is needed. Dropped
        # on any load failure so the next attempt rebuilds it from the live
        # connection state; closed on disconnect.
        self._bq_client: bigquery.Client | None = None
        # Data project (dataset owner) resolved alongside the client: the
        # connection's project_id, else the driver's, else the
        # service-account key's. May differ from the client's billing
        # project (billing_project_id).
        self._bq_data_project: str = ""

    # ---- write path: load jobs replace cursor.adbc_ingest ------------------
    def _adbc_only_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        address: TableAddress,
    ) -> None:
        """Append one cast batch to the target via a BigQuery load job.

        Overrides the base's ``cursor.adbc_ingest`` append (the BigQuery
        ADBC driver rejects every ``adbc.ingest.*`` statement option, and
        DML INSERT writes are quota-bound and costly). Serves keyed
        insert, and truncate-insert's append phase via the base's
        ``_truncate_then_ingest_sync`` (the TRUNCATE itself still runs as
        SQL over the ADBC connection). The lock acquire mirrors the base:
        ``_adbc_op_lock`` is reentrant for the truncate-then-ingest
        composition.
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            self._load_batch_via_load_job_sync(conn, cast_batch, address)

    def _merge_ingest_locked_sync(
        self,
        cast_batch: pa.RecordBatch,
        target_qualified: str,
        stage_qualified: str,
        stage_address: TableAddress,
        all_columns: list[str],
        conflict_keys: list[str],
        update_cols: list[str],
        *,
        insert_only: bool = False,
    ) -> None:
        """Upsert body with the stage ingest replaced by a load job.

        Keeps the base's statement sequence and idempotency shape exactly:
        pre-flight ``DROP TABLE IF EXISTS`` (so a retry of the same batch
        finds a clean slate), ``CREATE TABLE ... LIKE`` stage clone, fill
        the stage, ``MERGE INTO`` the target on *conflict_keys* (BigQuery
        PKs are NOT ENFORCED - the MERGE needs only the ON clause), DROP
        the stage, with best-effort cleanup on failure. Only the fill step
        differs: a Parquet load job instead of ``cursor.adbc_ingest``. One
        deliberate SQL deviation from the base composition: GoogleSQL
        rejects a target-alias prefix on MERGE's ``UPDATE SET`` assignment
        targets, so the SET items are unqualified (``SET col = s.col``);
        the ON / INSERT clauses keep the alias-qualified form. Runs with
        ``_adbc_op_lock`` held (acquired by the base's
        ``_merge_ingest_sync``, which also derives the collision-proof
        stage name and calls here).
        """
        conn = self._reopen_adbc_if_needed_sync()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                cursor.execute(
                    self.dialect.adbc_stage_table_sql(
                        stage_qualified, target_qualified
                    )
                )
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)

            # Fill the stage (replaces the base's cursor.adbc_ingest).
            self._load_batch_via_load_job_sync(conn, cast_batch, stage_address)

            on_clause = " AND ".join(
                f"t.{self.dialect.quote_ident(k)} = s.{self.dialect.quote_ident(k)}"
                for k in conflict_keys
            )
            set_clause = ", ".join(
                f"{self.dialect.quote_ident(c)} = s.{self.dialect.quote_ident(c)}"
                for c in update_cols
            )
            insert_cols = ", ".join(
                self.dialect.quote_ident(c) for c in all_columns
            )
            insert_vals = ", ".join(
                f"s.{self.dialect.quote_ident(c)}" for c in all_columns
            )
            merge_sql = (
                f"MERGE INTO {target_qualified} t USING {stage_qualified} s "
                f"ON {on_clause} "
            )
            if update_cols and not insert_only:
                merge_sql += (
                    f"WHEN MATCHED THEN UPDATE SET {set_clause} "  # nosec B608
                )
            merge_sql += (
                f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                f"VALUES ({insert_vals})"
            )
            cursor = conn.cursor()
            try:
                cursor.execute(merge_sql)
                conn.commit()
            finally:
                _close_cursor_quietly(cursor)
        except Exception as exc:
            # Best-effort stage cleanup on the local ``conn`` (never the
            # shared attribute), mirroring the base: a concurrent poison by
            # another thread cannot turn this into a use-after-poison race.
            try:
                drop_cursor = conn.cursor()
                try:
                    drop_cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                    conn.commit()
                finally:
                    _close_cursor_quietly(drop_cursor)
            except Exception:
                logger.warning(
                    "BigQuery stage table %s left behind after MERGE failure; "
                    "the next retry's pre-flight DROP IF EXISTS will clean it up",
                    stage_qualified,
                    exc_info=True,
                )
            self._poison_adbc_connection()
            if isinstance(exc, AdbcConfigurationError):
                raise
            if _is_fatal_dbapi_error(exc):
                raise _as_fatal(exc) from exc
            raise
        # Success path - DROP the stage so subsequent writes start clean. A
        # failed DROP is cleaned up by the next retry's pre-flight
        # DROP IF EXISTS, so idempotency survives even a persistent failure.
        try:
            drop_cursor = conn.cursor()
            try:
                drop_cursor.execute(f"DROP TABLE IF EXISTS {stage_qualified}")
                conn.commit()
            finally:
                _close_cursor_quietly(drop_cursor)
        except Exception:
            logger.warning(
                "BigQuery stage table %s post-MERGE DROP failed; the next "
                "retry of this batch will clean it up via pre-flight "
                "DROP IF EXISTS",
                stage_qualified,
                exc_info=True,
            )

    # ---- load-job machinery -------------------------------------------------
    def _load_batch_via_load_job_sync(
        self,
        conn: Any,
        cast_batch: pa.RecordBatch,
        address: TableAddress,
    ) -> None:
        """Run one ``WRITE_APPEND`` Parquet load job into *address*.

        Called under ``_adbc_op_lock``. The batch is written to an
        in-memory Parquet buffer and shipped as a direct media upload -
        no GCS staging bucket, no new connection inputs. Deterministic
        failures (Google API 4xx other than 408/429, credential errors)
        raise ``AdbcConfigurationError`` so the engine fails the batch
        fatally instead of retrying forever; transient failures re-raise
        unchanged and stay retryable. Any failure drops the cached client
        so the next attempt rebuilds it from the live connection state.
        """
        from google.cloud import bigquery

        if not address.schema:
            raise AdbcConfigurationError(
                f"BigQuery load job for {address} has no dataset; ADBC "
                "destinations require database_object.schema (the dataset) "
                "on the endpoint"
            )
        try:
            client = self._bigquery_client_sync(conn)
            destination = bigquery.TableReference(
                bigquery.DatasetReference(
                    address.catalog or self._bq_data_project or client.project,
                    address.schema,
                ),
                address.table,
            )
            buffer = io.BytesIO()
            pq.write_table(pa.Table.from_batches([cast_batch]), buffer)
            buffer.seek(0)
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                # The engine creates destination tables via DDL before the
                # first batch; a missing table is a defect that must fail
                # loud, never be re-created from a Parquet-inferred schema.
                create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            )
            client.load_table_from_file(
                buffer, destination, job_config=job_config
            ).result()
        except AdbcConfigurationError:
            self._drop_bigquery_client_sync()
            raise
        except Exception as exc:
            self._drop_bigquery_client_sync()
            if self._is_deterministic_google_error(exc):
                wrapped = AdbcConfigurationError(
                    f"{type(exc).__name__}: BigQuery load job into "
                    f"{address} failed deterministically: {exc}"
                )
                wrapped.__cause__ = exc
                raise wrapped from exc
            raise

    def _bigquery_client_sync(self, conn: Any) -> bigquery.Client:
        """Return the cached load-job client, building it on first use.

        Credentials come from the SAME connection state the ADBC transport
        was materialized with, read back from the live database handle via
        ``GetOption`` (``conn.adbc_database``): the service-account JSON
        for ``auth_type=service``, the client id/secret/refresh token for
        ``auth_type=user``. Non-secret settings (project, billing project,
        location) come from ``connection.parameters`` through the runtime's
        public ``raw_config``, with the driver's own options as fallback.
        The client's project is the billing project
        (``billing_project_id``, else the data project) - the same split
        the transport's ``adbc.bigquery.sql.auth.quota_project`` declares.
        Called only under ``_adbc_op_lock``.
        """
        if self._bq_client is not None:
            return self._bq_client

        from google.cloud import bigquery

        database = conn.adbc_database

        def opt(key: str) -> str:
            try:
                return database.get_option(key) or ""
            except Exception:
                # An unset/unknown option. GetOption itself ships with the
                # adbc-driver-bigquery >= 1.11.0 floor; required options are
                # re-checked (and fail loud) by the credential builder.
                return ""

        credentials, key_project = self._build_google_credentials(
            opt(_OPT_AUTH_TYPE), opt
        )

        parameters: dict[str, Any] = {}
        if self._runtime is not None:
            parameters = dict(self._runtime.raw_config.get("parameters") or {})
        data_project = (
            str(parameters.get("project_id") or "")
            or opt(_OPT_PROJECT_ID)
            or key_project
        )
        billing_project = (
            str(parameters.get("billing_project_id") or "") or data_project
        )
        if not billing_project:
            raise AdbcConfigurationError(
                "BigQuery load jobs need a GCP project: set project_id (or "
                "billing_project_id) on the connection, or authenticate with "
                "a service-account key that embeds project_id"
            )
        location = str(parameters.get("location") or "") or opt(_OPT_LOCATION) or None

        client = bigquery.Client(
            project=billing_project, credentials=credentials, location=location
        )
        self._bq_client = client
        self._bq_data_project = data_project or billing_project
        return client

    def _build_google_credentials(
        self, auth_type: str, opt: Callable[[str], str]
    ) -> tuple[Any, str]:
        """Build google-auth credentials from the driver's auth options.

        Returns ``(credentials, embedded_project)`` - the project is only
        non-empty for a service-account key (which embeds ``project_id``).
        The two accepted driver auth types are exactly the two the
        connection contract's ``auth_type`` enum can produce via the
        transport's lookup (``service`` / ``user``).
        """
        if auth_type.endswith("json_credential_string"):
            raw = opt(_OPT_AUTH_CREDENTIALS)
            if not raw:
                raise AdbcConfigurationError(
                    "BigQuery load jobs: the ADBC driver returned no "
                    "auth_credentials for auth_type=service "
                    "(adbc-driver-bigquery >= 1.11.0 is required)"
                )
            try:
                info = json.loads(raw)
            except ValueError as exc:
                raise AdbcConfigurationError(
                    "auth_json_credential is not valid JSON (expected the "
                    "full service-account key file contents)"
                ) from exc
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=[_BIGQUERY_SCOPE]
            )
            return credentials, str(info.get("project_id") or "")
        if auth_type.endswith("user_authentication"):
            client_id = opt(_OPT_CLIENT_ID)
            client_secret = opt(_OPT_CLIENT_SECRET)
            refresh_token = opt(_OPT_REFRESH_TOKEN)
            if not (client_id and client_secret and refresh_token):
                raise AdbcConfigurationError(
                    "BigQuery load jobs: auth_type=user requires client_id, "
                    "client_secret, and refresh_token; the ADBC driver "
                    "returned an incomplete set (adbc-driver-bigquery >= "
                    "1.11.0 is required)"
                )
            from google.oauth2.credentials import Credentials as UserCredentials

            credentials = UserCredentials(
                token=None,
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                token_uri=_GOOGLE_TOKEN_URI,
                scopes=[_BIGQUERY_SCOPE],
            )
            return credentials, ""
        raise AdbcConfigurationError(
            f"BigQuery load jobs: unsupported driver auth_type {auth_type!r}; "
            "this connector supports auth_type=service (service-account JSON "
            "key) and auth_type=user (OAuth user credentials)"
        )

    @staticmethod
    def _is_deterministic_google_error(exc: BaseException) -> bool:
        """True when a Google-side failure cannot heal between retries.

        Credential/refresh failures and client-side (4xx) API errors other
        than 408/429 are deterministic against an identical request; 5xx
        and network errors stay retryable.
        """
        from google.api_core import exceptions as api_exceptions
        from google.auth import exceptions as auth_exceptions

        if isinstance(exc, auth_exceptions.GoogleAuthError):
            return True
        if isinstance(exc, api_exceptions.GoogleAPICallError):
            code = getattr(exc, "code", None)
            return (
                isinstance(code, int)
                and 400 <= code < 500
                and code not in _RETRYABLE_HTTP_CODES
            )
        return False

    def _drop_bigquery_client_sync(self) -> None:
        """Drop (and close) the cached load-job client after a failure.

        The next write rebuilds it from the live ADBC connection state,
        mirroring the base's poison-and-reopen pattern for the ADBC handle.
        """
        client, self._bq_client = self._bq_client, None
        if client is not None:
            try:
                client.close()
            except Exception:
                logger.debug("BigQuery client close failed", exc_info=True)

    async def disconnect(self) -> None:
        """Close the load-job client, then run the base ADBC disconnect."""
        client = self._bq_client
        self._bq_client = None
        if client is not None:
            try:
                await asyncio.to_thread(client.close)
            except Exception:
                logger.warning(
                    "BigQuery load-job client close failed during disconnect",
                    exc_info=True,
                )
        await super().disconnect()
