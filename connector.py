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
* **Load-job type limits** - Json-family canonicals (``Json``, ``Object``,
  the nested ``List``/``Struct``/``Map`` forms) render ``STRING`` DDL: the
  CDK ships ``Json`` values as Parquet ``STRING``, and BigQuery batch load
  jobs cannot populate ``JSON`` columns from Parquet. ``Duration`` and
  ``Interval`` canonicals are rejected at CREATE TABLE time
  (``render_column_type``): BigQuery INTERVAL cannot be populated by a
  Parquet load job at all.

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

import asyncio
import io
import json
import logging
import re
from collections.abc import Mapping
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

#: PEP-249 exception class names whose failures cannot heal between retries.
#: Mirrors the base ADBC write path's fatal classification (the base's
#: helpers are module-private, so the two-line check is replicated here).
_FATAL_DBAPI_ERROR_NAMES = frozenset(
    {"ProgrammingError", "NotSupportedError", "IntegrityError", "DataError"}
)
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


def _close_cursor_quietly(cursor: Any) -> None:
    """Close an ADBC cursor best-effort, never masking a live error.

    Local copy of the base's private ``_adbc_utils`` helper, kept at
    WARNING parity with it: the swallowed close failure is a potential
    server-side resource leak (BigQuery session, gRPC context) an operator
    may need to act on, so it must not hide at DEBUG.
    """
    try:
        cursor.close()
    except Exception:
        logger.warning(
            "ADBC cursor close failed -- potential server-side resource leak",
            exc_info=True,
        )


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
    clone for the MERGE upsert, and load-job-aware type rendering (the
    NUMERIC/BIGNUMERIC precision-range choice; Duration/Interval
    rejection)."""

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
    dialect, with the ADBC ingest steps replaced by BigQuery load jobs
    (the sanctioned thick-path override - the BigQuery ADBC driver
    implements no ``adbc.ingest.*`` bulk path)."""

    dialect_class = BigQueryDialect

    def __init__(self) -> None:
        super().__init__()
        # Lazily-built google-cloud-bigquery client for the load-job write
        # path. Built and used only under the base's _adbc_op_lock (all
        # write entry points hold it). disconnect() also swaps it out
        # WITHOUT the lock: that is safe only because the engine lifecycle
        # never overlaps disconnect with an in-flight write_batch (writes
        # drain before teardown) - stated explicitly because nothing in
        # this class enforces it. Dropped on any load failure so the next
        # attempt rebuilds it from the live connection state; closed on
        # disconnect.
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
        insert and truncate-insert appends: only the FIRST
        truncate-insert batch routes through the base's
        ``_truncate_then_ingest_sync`` (TRUNCATE as SQL over the ADBC
        connection, then this method); every subsequent batch of the
        stream calls here directly. The lock acquire mirrors the base:
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
                    "BigQuery stage table %s left behind after MERGE "
                    "failure; a retryable failure is cleaned up by the next "
                    "attempt's pre-flight DROP IF EXISTS, but after a FATAL "
                    "failure the table must be dropped manually",
                    stage_qualified,
                    exc_info=True,
                )
            self._poison_adbc_connection()
            if isinstance(exc, AdbcConfigurationError):
                raise
            if _is_fatal_dbapi_error(exc):
                raise _as_fatal(exc) from exc
            raise
        # Success path - DROP the stage so it does not outlive the batch.
        # The stage name embeds this batch's sequence number and the batch
        # is acked right after this method returns, so nothing ever
        # retries it: the pre-flight DROP IF EXISTS only serves retries of
        # FAILED batches and will never reach this table. Try twice, then
        # log honestly that the table is orphaned.
        dropped = False
        for attempt in (1, 2):
            try:
                drop_cursor = conn.cursor()
                try:
                    drop_cursor.execute(
                        f"DROP TABLE IF EXISTS {stage_qualified}"
                    )
                    conn.commit()
                    dropped = True
                finally:
                    _close_cursor_quietly(drop_cursor)
            except Exception:
                logger.debug(
                    "post-MERGE DROP of BigQuery stage table %s failed "
                    "(attempt %d/2)",
                    stage_qualified,
                    attempt,
                    exc_info=True,
                )
            if dropped:
                break
        if not dropped:
            logger.warning(
                "BigQuery stage table %s could not be dropped after a "
                "successful MERGE (two attempts; tracebacks at DEBUG). The "
                "batch is acked and never retried, so no automatic cleanup "
                "reaches this table: it is orphaned - a full copy of this "
                "batch - until dropped manually or expired by the "
                "dataset's default table expiration",
                stage_qualified,
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
        no GCS staging bucket, no new connection inputs.

        Failure classification (the base ``write_batch`` acks RETRYABLE
        for anything not ``AdbcConfigurationError``-shaped, and retries
        have no cap):

        * Building the table reference and serializing the batch to
          Parquet involve no network - any failure there is deterministic
          against the same batch and wraps in ``AdbcConfigurationError``.
        * Google API errors classify by error *reason* first (BigQuery's
          403-mapped throttling retries with backoff), then by HTTP
          status; credential errors distinguish transport/transient
          refresh failures (retryable) from bad credential material
          (fatal). See ``_is_deterministic_google_error``.

        Any failure drops the cached client so the next attempt rebuilds
        it from the live connection state.
        """
        from google.cloud import bigquery

        if not address.schema:
            # Defense in depth: the base already rejects schema-less ADBC
            # destinations at configure_schema time (_destination_address
            # returns None before DDL), so the normal lifecycle cannot
            # reach this raise.
            raise AdbcConfigurationError(
                f"BigQuery load job for {address} has no dataset; ADBC "
                "destinations require database_object.schema (the dataset) "
                "on the endpoint"
            )
        try:
            client = self._bigquery_client_sync(conn)
            try:
                destination = bigquery.TableReference(
                    bigquery.DatasetReference(
                        address.catalog
                        or self._bq_data_project
                        or client.project,
                        address.schema,
                    ),
                    address.table,
                )
                buffer = io.BytesIO()
                pq.write_table(pa.Table.from_batches([cast_batch]), buffer)
            except (pa.ArrowException, ValueError, TypeError) as exc:
                if isinstance(exc, pa.ArrowMemoryError):
                    # Transient memory pressure, not a deterministic
                    # input defect - leave it retryable.
                    raise
                # Deterministic client-side failure: a malformed
                # project/dataset/table id, or an Arrow type Parquet
                # cannot store. pyarrow's ArrowNotImplementedError
                # subclasses NotImplementedError - not a PEP-249 fatal
                # name - so without this wrap it would ack RETRYABLE and
                # the batch would retry forever.
                raise AdbcConfigurationError(
                    f"{type(exc).__name__}: BigQuery load job for "
                    f"{address} failed before upload (building the table "
                    f"reference / serializing the batch to Parquet): {exc}"
                ) from exc
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
                raise AdbcConfigurationError(
                    f"{type(exc).__name__}: BigQuery load job into "
                    f"{address} failed deterministically: {exc}"
                ) from exc
            raise

    def _bigquery_client_sync(self, conn: Any) -> bigquery.Client:
        """Return the cached load-job client, building it on first use.

        Credentials come from the SAME connection state the ADBC transport
        was materialized with, read back from the live database handle via
        ``GetOption`` (``conn.adbc_database``): the service-account JSON
        for ``auth_type=service``, the client id/secret/refresh token for
        ``auth_type=user``. The data project comes from
        ``connection.parameters`` through the runtime's public
        ``raw_config``, falling back to the driver's project_id option and
        then the service-account key's embedded project. The billing
        project (the ``bigquery.Client``'s ``project``) is the
        connection's ``billing_project_id``, else the data project - the
        driver's ``quota_project`` option IS readable via ``GetOption``,
        but the parameters path is the same source that materialized it,
        so the connector deliberately does not read it back. ``location``
        falls back to the driver's location option. Called only under
        ``_adbc_op_lock``.
        """
        if self._bq_client is not None:
            return self._bq_client

        from google.cloud import bigquery

        database = conn.adbc_database

        def opt(key: str) -> str | None:
            """Driver option via GetOption: '' = unset, None = call raised."""
            try:
                return database.get_option(key) or ""
            except Exception:
                # Distinguishable from '' so the credential builder can
                # point at the driver install (a pre-GetOption wheel)
                # instead of misreporting a connection-config defect.
                logger.debug(
                    "GetOption(%r) raised on the ADBC BigQuery database "
                    "handle; treating the option as unreadable",
                    key,
                    exc_info=True,
                )
                return None

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
        self, auth_type: str | None, opt: Callable[[str], str | None]
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

    # ---- failure classification --------------------------------------------
    @classmethod
    def _is_deterministic_google_error(cls, exc: BaseException) -> bool:
        """True when a Google-side failure cannot heal between retries.

        The base ``write_batch`` acks RETRYABLE for anything not
        ``AdbcConfigurationError``-shaped, so this predicate decides
        retry-with-backoff vs fail-fatal:

        * ``TransportError`` (network trouble reaching Google's OAuth
          endpoint) - never deterministic.
        * ``RefreshError`` - deterministic only when it names a
          deterministic OAuth error code (``invalid_grant`` etc.);
          token-endpoint 5xx and unclassifiable refresh failures stay
          retryable. Realistic under auth_type=user: the client is
          dropped on every failure and refreshes on every rebuild.
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
            return cls._is_deterministic_refresh_error(exc)
        if isinstance(exc, auth_exceptions.GoogleAuthError):
            return True
        if isinstance(exc, api_exceptions.GoogleAPICallError):
            if cls._google_error_reasons(exc) & _RETRYABLE_GOOGLE_REASONS:
                return False
            code = getattr(exc, "code", None)
            return (
                isinstance(code, int)
                and 400 <= code < 500
                and code not in _RETRYABLE_HTTP_CODES
            )
        return False

    @staticmethod
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

    @staticmethod
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
        """Close the load-job client, then run the base ADBC disconnect.

        Mirrors the base's cancellation shape: a ``CancelledError`` during
        the client close is remembered - never allowed to skip
        ``super().disconnect()``, which releases the ADBC connection and
        the runtime - and re-raised after the base teardown so the
        caller's cancellation is honored.
        """
        cancelled: BaseException | None = None
        client = self._bq_client
        self._bq_client = None
        if client is not None:
            try:
                await asyncio.to_thread(client.close)
            except asyncio.CancelledError as exc:
                logger.error(
                    "BigQuery load-job client close cancelled during "
                    "disconnect; its HTTP session may leak"
                )
                cancelled = exc
            except Exception:
                logger.warning(
                    "BigQuery load-job client close failed during disconnect",
                    exc_info=True,
                )
        await super().disconnect()
        if cancelled is not None:
            raise cancelled
