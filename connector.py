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
* **Idempotent load jobs** - every load dispatched through
  ``write_batch`` submits under a DETERMINISTIC job id chain
  (``analitiq_<token>_0``, ``_1``, ...) whose token hashes the batch
  identity the CDK base already fingerprints into its stage token
  (``run_id|stream_id|batch_seq``), the exact Parquet payload, and -
  for truncate-insert append batches - a per-table truncate epoch.
  BigQuery load jobs are idempotent by job id, so a retry whose
  predecessor's client-side polling timed out ATTACHES to the
  still-running job (or, on pure appends, resumes the committed one)
  instead of re-submitting the rows: no duplicate appends, no
  double-filled MERGE stage. Destructive steps (the first
  truncate-insert batch's TRUNCATE, the MERGE stage DROP/CREATE) run
  only after THIS batch's chain is drained to a terminal state, and
  never resume a prior success afterwards - so no job of the batch
  being written can commit into a freshly truncated/recreated table or
  stand in for rows a TRUNCATE just wiped. Scope is per batch: for the
  MERGE stage the guarantee is absolute (the stage name embeds the
  batch identity, so only this batch's chain ever targets it); for
  TRUNCATE, an abandoned still-running job of a DIFFERENT batch or run
  remains a residual at-least-once window (it requires that batch to
  exhaust its capped retries with the job still live, plus a read
  restart - as on main, where every abandoned job had this power).
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
import hashlib
import io
import json
import logging
import re
import uuid
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, NamedTuple

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


#: Upper bound on a single batch's deterministic load-job id chain
#: (``<prefix>_0`` ... ``<prefix>_N``). The chain grows at most one link
#: per engine retry, so a real chain never approaches this; the cap is a
#: runaway backstop that raises retryable instead of walking forever.
_MAX_LOAD_JOB_CHAIN = 64

#: The CDK's truncate-insert write-mode name. Load-bearing: the epoch
#: salt applies exactly when the base's own truncate branch would (the
#: base compares the same string, generic.py's _write_batch_adbc_only).
_TRUNCATE_INSERT = "truncate_insert"


@dataclass(frozen=True)
class _BatchWriteContext:
    """Identity of the batch currently flowing through the write path.

    Exactly the inputs the CDK base hashes into its collision-proof
    stage token (``run_id|stream_id|batch_seq``), plus the write mode
    (which decides whether the truncate epoch salts the load-job id).
    """

    run_id: str
    stream_id: str
    batch_seq: int
    write_mode: str

    @property
    def salts_truncate_epoch(self) -> bool:
        """True when this batch's job ids embed the truncate epoch.

        Exactly the truncate-insert APPEND batches (``batch_seq >= 2``):
        the first batch runs the TRUNCATE itself and never resumes a
        prior success, so its chain needs no salt - and must stay
        unsalted so its own retries can find it across epoch rotations.
        """
        return self.write_mode == _TRUNCATE_INSERT and self.batch_seq > 1


class _ChainState(NamedTuple):
    """Terminal state of a batch's drained load-job id chain.

    Three reachable shapes: ``(0, False)`` - empty chain, submit the
    first id; ``(n, False)`` - the last job reached a terminal failure,
    its id is burned, submit ``_n``; ``(n, True)`` - the last job
    committed (implies ``next_index >= 1``). A deterministic failure on
    a non-destructive path is delivered by exception instead.
    """

    next_index: int
    last_succeeded: bool


#: Per-batch write context threaded from the async dispatcher
#: (``_write_batch_adbc_only``) into the sync ingest methods, whose base
#: signatures carry only the cast batch and address. Safety rests on the
#: contextvars snapshot alone: ``asyncio.to_thread`` copies the calling
#: task's context at dispatch time, so every sync ingest call reads
#: exactly the value set by the ``_write_batch_adbc_only`` invocation
#: that dispatched it - even with concurrent streams on this shared
#: handler instance, and even if an abandoned attempt's worker thread
#: outlives its ack deadline and overlaps a newer attempt.
_CURRENT_BATCH: ContextVar[_BatchWriteContext | None] = ContextVar(
    "bigquery_current_batch", default=None
)


def _epoch_key(address: TableAddress) -> tuple[str, str, str]:
    """Truncate-epoch dict key: the destination table's identity.

    Keyed by table rather than stream because the hazard the epoch
    guards is per-table (a TRUNCATE empties one table), and because the
    stream id is not available on every TRUNCATE entry point (the
    base's empty-first-batch ``_truncate_only`` path runs outside the
    ``_write_batch_adbc_only`` dispatch and its ContextVar).
    """
    return (address.catalog or "", address.schema or "", address.table)


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
        # Per-table truncate epoch: rotated on EVERY TRUNCATE (the
        # _adbc_truncate_sync override is the single choke point both
        # truncate entry points share - the first-batch
        # truncate-then-ingest path AND the base's empty-first-batch
        # _truncate_only path), salted into the deterministic load-job
        # ids of truncate_insert append batches (batch_seq >= 2).
        # batch_seq restarts at 1 when the engine restarts a read and
        # the first batch re-runs TRUNCATE - without the epoch, an
        # append batch of the new refresh incarnation could attach to
        # the identical-content job of the OLD incarnation (whose rows
        # that TRUNCATE just wiped) and silently skip its load. Keyed
        # by destination table (_epoch_key), not stream: the hazard is
        # per-table and the stream id is not available on the
        # _truncate_only path. In-memory only, matching the hazard's
        # scope; mutated only under _adbc_op_lock.
        self._bq_truncate_epochs: dict[tuple[str, str, str], str] = {}
        # Dataset locations resolved via get_dataset, cached per
        # (project, dataset). BigQuery's jobs.get requires the job
        # location outside the US/EU multi-regions, while jobs.insert
        # infers it from the dataset - so the attach/resume machinery
        # must pass it explicitly or regional retries would see
        # NotFound for jobs that exist. A dataset's location is
        # immutable in BigQuery, so the cache never invalidates and
        # deliberately survives client rebuilds.
        self._bq_dataset_locations: dict[tuple[str, str], str] = {}

    # ---- write path: load jobs replace cursor.adbc_ingest ------------------
    async def _write_batch_adbc_only(
        self,
        state: Any,
        run_id: str,
        stream_id: str,
        batch_seq: int,
        record_batch: pa.RecordBatch,
        truncate_now: bool,
    ) -> None:
        """Thread the batch identity into the sync ingest overrides.

        The base derives its collision-proof stage token from
        ``(run_id, stream_id, batch_seq)`` but hands the sync ingest
        methods only the cast batch and address; the deterministic
        load-job ids need that same identity. This override publishes it
        via ``_CURRENT_BATCH`` (see the ContextVar's own comment for why
        that is safe under concurrent streams) and delegates everything
        else to the base dispatch.
        """
        token = _CURRENT_BATCH.set(
            _BatchWriteContext(
                run_id=run_id,
                stream_id=stream_id,
                batch_seq=batch_seq,
                write_mode=state.write_mode,
            )
        )
        try:
            await super()._write_batch_adbc_only(
                state,
                run_id,
                stream_id,
                batch_seq,
                record_batch,
                truncate_now,
            )
        finally:
            _CURRENT_BATCH.reset(token)

    def _adbc_only_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        address: TableAddress,
    ) -> None:
        """Append one cast batch to the target via a BigQuery load job.

        Overrides the base's ``cursor.adbc_ingest`` append (the BigQuery
        ADBC driver rejects every ``adbc.ingest.*`` statement option, and
        DML INSERT writes are quota-bound and costly). Serves keyed
        insert and truncate-insert appends after the first batch (the
        FIRST truncate-insert batch routes through this class's
        ``_truncate_then_ingest_sync`` override instead). Pure appends
        resume a previously committed load job (no destructive
        preparation). The justification differs by mode: for keyed
        inserts nothing ever removes appended rows, so a chain success
        proves this exact payload already sits in the target exactly
        once; for truncate-insert appends the epoch salt in the job id
        guarantees a chain success post-dates the last TRUNCATE
        (``_adbc_truncate_sync`` rotates the epoch), so the resumed
        rows cannot have been wiped.
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            self._load_batch_via_load_job_sync(conn, cast_batch, address)

    def _adbc_truncate_sync(self, address: TableAddress) -> None:
        """TRUNCATE via the base, then rotate the table's truncate epoch.

        The single choke point every TRUNCATE flows through - both the
        first-batch ``_truncate_then_ingest_sync`` composition and the
        base's empty-first-batch ``_truncate_only`` short-circuit
        (which runs OUTSIDE the ``_write_batch_adbc_only`` dispatch, so
        it cannot rely on ``_CURRENT_BATCH``; the epoch dict is keyed
        by table for exactly that reason). The invariant this enforces:
        no code path that empties the target leaves its epoch
        unchanged - otherwise an identical-content append batch of the
        new refresh incarnation could resume the wiped incarnation's
        committed job and silently skip its load.
        """
        with self._adbc_op_lock:
            super()._adbc_truncate_sync(address)
            self._bq_truncate_epochs[_epoch_key(address)] = uuid.uuid4().hex

    def _truncate_then_ingest_sync(
        self,
        cast_batch: pa.RecordBatch,
        address: TableAddress,
    ) -> None:
        """TRUNCATE, then load the first truncate-insert batch, idempotently.

        Overrides the base composition (TRUNCATE via SQL, then the plain
        append ingest) because that shape interacts with deterministic
        load-job ids: the engine re-runs TRUNCATE on every retry of the
        first batch (``truncate_now`` is recomputed as ``batch_seq ==
        1``), so

        * an abandoned still-running load job from a previous attempt
          could commit AFTER this attempt's TRUNCATE, duplicating the
          batch (the polling-timeout race this chain scheme exists to
          close), and
        * a job that already committed on a previous attempt has just
          had its rows wiped by this attempt's TRUNCATE, so a chain
          success must NOT count as "the batch is in the table"
          (``resume_committed=False``).

        The fix is drain-then-truncate-then-fresh-load, expressed via
        the load method's destructive-preparation hook: the job chain
        drains to a terminal state first (any in-flight commit lands
        BEFORE the TRUNCATE and is wiped with everything else), then
        the hook runs this class's ``_adbc_truncate_sync`` (TRUNCATE
        plus epoch rotation, so append batches of the wiped refresh
        incarnation can never satisfy this incarnation's job ids), and
        a fresh job id always loads the current payload.
        ``_adbc_op_lock`` is reentrant for the inner acquires,
        mirroring the base.
        """
        with self._adbc_op_lock:
            conn = self._reopen_adbc_if_needed_sync()
            self._load_batch_via_load_job_sync(
                conn,
                cast_batch,
                address,
                destructive_prepare=lambda: self._adbc_truncate_sync(address),
            )

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

        Keeps the base's statement sequence and idempotency shape:
        pre-flight ``DROP TABLE IF EXISTS`` (so a retry of the same batch
        finds a clean slate), ``CREATE TABLE ... LIKE`` stage clone, fill
        the stage, ``MERGE INTO`` the target on *conflict_keys* (BigQuery
        PKs are NOT ENFORCED - the MERGE needs only the ON clause), DROP
        the stage, with best-effort cleanup on failure. Two deliberate
        deviations from the base composition:

        * The fill is a Parquet load job instead of
          ``cursor.adbc_ingest``, and the pre-flight DROP/CREATE runs
          INSIDE the load sequence (the destructive-preparation hook),
          after the batch's deterministic job chain has drained to a
          terminal state - so an abandoned still-running load job from
          a previous attempt can never commit into the freshly
          recreated stage (the double-fill that made ``MERGE``
          multi-match). A chain success is never resumed on this path:
          the recreate just wiped the stage, so a fresh job id always
          re-fills it.
        * GoogleSQL rejects a target-alias prefix on MERGE's ``UPDATE
          SET`` assignment targets, so the SET items are unqualified
          (``SET col = s.col``); the ON / INSERT clauses keep the
          alias-qualified form.

        Runs with ``_adbc_op_lock`` held (acquired by the base's
        ``_merge_ingest_sync``, which also derives the collision-proof
        stage name and calls here).
        """
        conn = self._reopen_adbc_if_needed_sync()
        try:

            def recreate_stage() -> None:
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
            self._load_batch_via_load_job_sync(
                conn,
                cast_batch,
                stage_address,
                destructive_prepare=recreate_stage,
            )

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
        *,
        destructive_prepare: Callable[[], None] | None = None,
    ) -> None:
        """Load one cast batch into *address* via an idempotent job chain.

        Called under ``_adbc_op_lock``. The batch is written to an
        in-memory Parquet buffer and shipped as a direct media upload -
        no GCS staging bucket, no new connection inputs - under a
        DETERMINISTIC job id (see ``_load_job_id_prefix``). BigQuery
        load jobs are idempotent by job id, which closes the
        polling-timeout duplication race: when a previous attempt's
        client-side polling died (classified retryable) while the job
        kept running server-side, this attempt finds that job in the
        chain and ATTACHES to it instead of re-submitting the same rows.

        Sequence: serialize -> drain the chain (``<prefix>_0``,
        ``<prefix>_1``, ... - a still-running last job is polled to its
        terminal state) -> maybe resume -> *destructive_prepare* ->
        submit the next id -> poll.

        *destructive_prepare* is the whole mode switch, and ``None`` vs
        set are the only two states:

        * ``None`` - pure append. A chain whose last job committed
          means this payload provably sits in the target exactly once:
          return without loading anything.
        * set - the caller must destroy-and-recreate the load target
          first (the first truncate-insert batch's TRUNCATE; the MERGE
          fill's stage DROP/CREATE). A prior chain success then proves
          nothing about the CURRENT table contents, so the hook runs
          and a fresh job id always loads the current payload. The
          drain-before-hook ordering is load-bearing: the destructive
          step only runs once no job of THIS batch's chain can commit
          after it. (Jobs of other abandoned batches are outside the
          chain's scope - see the module docstring's residual-window
          note.)

        Collapsing "resume?" and "prepare" into the one parameter makes
        the two corrupt combinations unrepresentable: resuming past a
        pending destructive step, and fresh-submitting a committed pure
        append.

        Without batch context (a direct call outside the
        ``_write_batch_adbc_only`` dispatch - defensive only) the job id
        falls back to a client-generated one: the pre-idempotency,
        at-least-once behavior, WARNING-logged because idempotency is
        silently absent.

        Failure classification (the base ``write_batch`` acks RETRYABLE
        for anything not ``AdbcConfigurationError``-shaped):

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
            location = self._bq_dataset_location_sync(client, destination)
            job_id_prefix = self._load_job_id_prefix(
                buffer.getbuffer(), address
            )
            if job_id_prefix is None:
                logger.warning(
                    "BigQuery load into %s has no batch write context; "
                    "falling back to a client-generated job id - this "
                    "load is NOT idempotent across polling-timeout "
                    "retries (at-least-once)",
                    address,
                )
                if destructive_prepare is not None:
                    destructive_prepare()
                self._submit_and_poll_load_job(
                    client,
                    buffer,
                    destination,
                    job_id=None,
                    location=location,
                    destructive=destructive_prepare is not None,
                )
                return
            chain = self._drain_load_job_chain_sync(
                client,
                job_id_prefix,
                location=location,
                destructive=destructive_prepare is not None,
            )
            if chain.last_succeeded and destructive_prepare is None:
                logger.info(
                    "BigQuery load job %s_%d already loaded this batch "
                    "into %s on a previous attempt; resuming its success "
                    "instead of re-submitting",
                    job_id_prefix,
                    chain.next_index - 1,
                    address,
                )
                return
            if destructive_prepare is not None:
                destructive_prepare()
            self._submit_and_poll_load_job(
                client,
                buffer,
                destination,
                job_id=f"{job_id_prefix}_{chain.next_index}",
                location=location,
                destructive=destructive_prepare is not None,
            )
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

    def _load_job_id_prefix(
        self,
        parquet_payload: bytes | memoryview,
        address: TableAddress,
    ) -> str | None:
        """Deterministic load-job id prefix for the current batch.

        ``None`` when no batch context is set (a direct call outside the
        ``_write_batch_adbc_only`` dispatch) - the caller then falls
        back to a client-generated job id.

        The token binds:

        * the batch identity ``run_id|stream_id|batch_seq`` - the same
          inputs the CDK base hashes into its collision-proof stage
          token;
        * the target table's truncate epoch, for truncate_insert
          batches after the first (``salts_truncate_epoch``; see the
          ``_bq_truncate_epochs`` comment for the wipe hazard it
          closes). Generated on demand so a destination restart
          mid-stream starts a fresh epoch - degrading those batches to
          the pre-idempotency at-least-once behavior instead of risking
          a false resume;
        * a SHA-256 of the exact Parquet payload, so an id can only
          ever attach to a job that loaded these very bytes. Engine
          retries resend the identical record batch (and the cast and
          Parquet encode are deterministic in-process), so retries land
          on the same chain; a restarted read that produces different
          bytes gets a different chain and simply loads them.

        32 hex chars (128 bits) keep accidental collision with any
        other job in the project's global, long-retention job namespace
        out of consideration.
        """
        ctx = _CURRENT_BATCH.get()
        if ctx is None:
            return None
        epoch = ""
        if ctx.salts_truncate_epoch:
            epoch = self._bq_truncate_epochs.setdefault(
                _epoch_key(address), uuid.uuid4().hex
            )
        content_hash = hashlib.sha256(parquet_payload).hexdigest()
        token = hashlib.sha256(
            f"{ctx.run_id}|{ctx.stream_id}|{ctx.batch_seq}|{epoch}|"
            f"{content_hash}".encode()
        ).hexdigest()[:32]
        return f"analitiq_{token}"

    def _bq_dataset_location_sync(
        self, client: bigquery.Client, destination: bigquery.TableReference
    ) -> str | None:
        """Resolve (and cache) the destination dataset's location.

        ``jobs.insert`` infers the location from the dataset, but
        ``jobs.get`` requires it outside the US/EU multi-regions - a
        location-blind lookup returns NotFound for jobs that exist,
        which would silently disable the attach/resume machinery for
        regional datasets and FATAL the Conflict fallback. Resolution
        failures propagate through the caller's classification (a
        missing dataset is deterministic; network trouble retryable).
        """
        from google.cloud import bigquery

        key = (destination.project, destination.dataset_id)
        location = self._bq_dataset_locations.get(key)
        if location is None:
            location = (
                client.get_dataset(
                    bigquery.DatasetReference(*key)
                ).location
                or ""
            )
            self._bq_dataset_locations[key] = location
        return location or None

    def _drain_load_job_chain_sync(
        self,
        client: bigquery.Client,
        job_id_prefix: str,
        *,
        location: str | None,
        destructive: bool,
    ) -> _ChainState:
        """Walk this batch's job-id chain to a terminal state.

        Only the LAST existing job can be non-terminal - a new id is
        only ever submitted after the walk drained its predecessor to a
        terminal state - so the walk polls that one job to completion
        when needed: the "attach" that replaces re-submission.

        The except branch distinguishes THE JOB failing from OUR
        POLLING failing, because burning a live job's id reopens the
        duplication race this chain exists to close: after any poll
        exception the job is reloaded, and only a job that provably
        reached DONE has a terminal outcome. A non-DONE (or
        unreloadable) job re-raises - the batch acks RETRYABLE and the
        next attempt re-attaches to the SAME id. A genuinely failed job
        committed nothing (load jobs are atomic): on a *destructive*
        path its id is burned even for a deterministically-classified
        error, because that error can be self-inflicted by a prior
        attempt's cleanup (the stage DROP after a polling timeout makes
        the abandoned job die with notFound) - the destructive
        preparation recreates the preconditions and the fresh
        submission re-classifies a genuine defect correctly, at the
        cost of one wasted upload. On the append path nothing destroys
        a job's preconditions, so a deterministic stored error is
        genuine and re-raises (fatal in the caller).
        """
        from google.api_core import exceptions as api_exceptions

        index = 0
        last_job: Any = None
        while True:
            if index >= _MAX_LOAD_JOB_CHAIN:
                raise RuntimeError(
                    f"BigQuery load-job chain {job_id_prefix} reached "
                    f"{_MAX_LOAD_JOB_CHAIN} ids without a terminal "
                    "success; refusing to extend it this attempt. "
                    "Investigate why this table's load jobs keep "
                    "failing transiently (all chain ids share this "
                    "prefix in the project's job history); the engine's "
                    "retry cap bounds further attempts"
                )
            try:
                job = client.get_job(
                    f"{job_id_prefix}_{index}", location=location
                )
            except api_exceptions.NotFound:
                break
            if last_job is not None and last_job.state != "DONE":
                # Violates the chain invariant; observable, not fatal -
                # the walk still only acts on the last job.
                logger.warning(
                    "BigQuery load-job chain %s has a non-terminal "
                    "non-last job %s (state %s); the chain invariant "
                    "expects only the last id to be in flight",
                    job_id_prefix,
                    last_job.job_id,
                    last_job.state,
                )
            last_job = job
            index += 1
        if last_job is None:
            return _ChainState(0, False)
        if last_job.state != "DONE":
            logger.info(
                "Attaching to in-flight BigQuery load job %s from a "
                "previous attempt (client-side polling was interrupted; "
                "the job kept running server-side)",
                last_job.job_id,
            )
        try:
            last_job.result()
        except Exception as exc:
            done, succeeded = self._job_terminal_state(last_job)
            if not done:
                # OUR polling (or the state reload) failed - the job
                # may still be running and may yet commit. Never
                # advance past a live job: re-raise so the engine backs
                # off and the next attempt re-attaches to this same id.
                raise
            if succeeded:
                # The poll error was ours; the job itself committed.
                return _ChainState(index, True)
            if not destructive and self._is_deterministic_google_error(
                exc
            ):
                raise
            logger.warning(
                "BigQuery load job %s reached a terminal failure on a "
                "previous attempt; its id is burned and the next id in "
                "the chain will be submitted (failed load jobs commit "
                "nothing)",
                last_job.job_id,
                exc_info=True,
            )
            return _ChainState(index, False)
        return _ChainState(index, True)

    @staticmethod
    def _job_terminal_state(job: Any) -> tuple[bool, bool]:
        """(reached DONE, succeeded) for *job*, refreshed best-effort.

        The reload targets the job's own stored project/location, so no
        location plumbing is needed. When the reload itself fails, the
        job's LOCALLY cached state still decides: ``result()`` refreshes
        the job before raising a stored job error, so a locally-DONE job
        with an ``error_result`` is provably terminal even without a
        fresh reload - without this fallback, a network blip on the
        reload would convert a burnable destructive-path failure into a
        spurious FATAL. A pure polling failure leaves the local state
        non-DONE, preserving the conservative "possibly live" re-raise
        exactly where it matters.
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

    def _submit_and_poll_load_job(
        self,
        client: bigquery.Client,
        buffer: io.BytesIO,
        destination: bigquery.TableReference,
        *,
        job_id: str | None,
        location: str | None,
        destructive: bool,
    ) -> None:
        """Submit one ``WRITE_APPEND`` Parquet load job and poll it done.

        With a deterministic *job_id*, an ``Already Exists: Job``
        conflict means a previous submission of this very id reached
        the server anyway - either this attempt's own insert response
        was lost and the HTTP layer retried past it (attach is safe:
        that job loaded this payload after any destructive step), or a
        PREVIOUS attempt's job that the chain walk's ``get_job`` did
        not see. The two are indistinguishable here, and on a
        *destructive* path the second case is lethal to attach to: the
        pre-existing job's rows either committed before the destructive
        step (just wiped - attaching would ack success for absent rows)
        or will commit after it under an id the chain considers spent.
        So: append path attaches and polls; destructive path raises
        RETRYABLE (a plain RuntimeError - the raw Conflict's
        ``duplicate`` reason would classify FATAL) so the next
        attempt's drain sees the now-visible job and handles it under
        the normal drain-then-destroy ordering, which is
        self-correcting. ``job_id=None`` submits under a
        client-generated id (the no-batch-context fallback), where a
        conflict is a genuine error.
        """
        from google.api_core import exceptions as api_exceptions
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            # The engine creates destination tables via DDL before the
            # first batch; a missing table is a defect that must fail
            # loud, never be re-created from a Parquet-inferred schema.
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
        except api_exceptions.Conflict as exc:
            if job_id is None:
                raise
            if destructive:
                raise RuntimeError(
                    f"BigQuery load job {job_id} already existed when "
                    "submitted after a destructive preparation step "
                    "(the chain walk raced its visibility); retrying "
                    "so the next attempt drains it before the "
                    "destructive step re-runs"
                ) from exc
            logger.info(
                "BigQuery load job %s already exists; attaching to the "
                "prior submission",
                job_id,
            )
            client.get_job(job_id, location=location).result()

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
