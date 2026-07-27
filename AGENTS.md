---
name: Google BigQuery
description: >
  Connect to Google BigQuery, Google Cloud's serverless data warehouse, over the ADBC BigQuery driver.
type: database
---
# Google BigQuery

Google BigQuery is Google Cloud's serverless, highly scalable data warehouse. This connector reads from and writes to a BigQuery project. Connect, SQL execution, discovery, and Arrow reads run over the engine's first-class ADBC BigQuery driver (`transport_type: adbc`, `driver: bigquery`); writes ride the CDK stage-then-apply cycle with a Parquet **load job** as the stage-fill mechanism (direct media upload — no GCS staging bucket). BigQuery is an HTTPS REST service — there is no host/port, username, or password; all connection state is passed as `adbc.bigquery.sql.*` driver options.

## Authentication

### Credential bundle (`auth.type: db`)
BigQuery does not use username/password and does not support API keys. Authentication is selected via the `auth_type` input:

- **`service` (default, recommended)** — a Google Cloud service-account JSON key. Paste the full JSON key contents into `auth_json_credential` (stored as a secret). The key embeds its own `project_id`, `client_email`, and `private_key`. Resolves to the driver's `json_credential_string` auth type.
- **`user`** — an OAuth user-credential flow. Supply `client_id`, `client_secret`, and `refresh_token`. Resolves to the driver's `user_authentication` auth type.

- Client app required: no
- Always-on TLS to `googleapis.com` (no TLS-mode options exposed by the driver)

## Post-Auth Steps

Resource discovery runs automatically on activation via the builtin `information_schema` strategy. BigQuery's metadata views are dataset-scoped and uppercase, so the dialect composes `` `project`.`dataset`.INFORMATION_SCHEMA.<VIEW> `` paths: datasets are enumerated from the project-scoped `INFORMATION_SCHEMA.SCHEMATA` (region-scoped through the `location` parameter), then tables/views and their column types are read per dataset and mapped to canonical Arrow types via `definition/type-map-read.json`.

## Connection Inputs

| Input | Storage | Required | Driver option | Notes |
|-------|---------|----------|---------------|-------|
| `project_id` | connection.parameters | no | `adbc.bigquery.sql.project_id` | GCP project to query; defaults to the project in the active credentials |
| `billing_project_id` | connection.parameters | no | `adbc.bigquery.sql.auth.quota_project` | Project charged for jobs/quota; defaults to `project_id` |
| `dataset_id` | connection.parameters | no | `adbc.bigquery.sql.dataset_id` | Default dataset (also scopes discovery) |
| `location` | connection.parameters | no | `adbc.bigquery.sql.location` | Dataset/region location (e.g. `US`, `EU`); also scopes which region's datasets discovery enumerates |
| `auth_type` | connection.parameters | yes | `adbc.bigquery.sql.auth_type` (via `lookup`) | `service` (default) \| `user` |
| `auth_json_credential` | secrets | conditional | `adbc.bigquery.sql.auth_credentials` | Full service-account JSON key contents (when `auth_type=service`) |
| `client_id` | connection.parameters | conditional | `adbc.bigquery.sql.auth.client_id` | OAuth client ID (when `auth_type=user`) |
| `client_secret` | secrets | conditional | `adbc.bigquery.sql.auth.client_secret` | OAuth client secret (when `auth_type=user`) |
| `refresh_token` | secrets | conditional | `adbc.bigquery.sql.auth.refresh_token` | OAuth refresh token (when `auth_type=user`) |

> Conditional inputs are all schema-optional (`required: false`); which ones are actually needed depends on the selected `auth_type`. Conditional requiredness is not expressible in the connection contract.

## Write Path

Writes ride the CDK's stage-then-apply write cycle (ADR `sql-write-path-v2`). The connector declares its write shape in `definition/connector.json` `sql_capabilities` (`bulk_load.adbc: load_job`, `stage.scope: real`, `stage.schema: target`, `stage.transactional_ddl: false`, `merge_form: merge`, `catalog: full`, `session_targeting: per_statement`); **without that block the engine rejects every write at `configure_schema` time.** For every write mode the engine runs one stepwise cycle: pre-flight `DROP` the batch's stage table → `CREATE TABLE … LIKE` the target (`BigQueryDialect.stage_table_sql`) → fill the stage (`bulk_land`) → on a truncate-insert's first batch, empty the target (`empty_table_sql` → `DELETE … WHERE TRUE`, because BigQuery's `DELETE` requires a `WHERE`) → apply the stage to the target (engine-rendered `INSERT … SELECT` for append / truncate-insert, `BigQueryDialect.merge_statement_sql` GoogleSQL `MERGE` on the declared conflict keys for upsert — BigQuery's `NOT ENFORCED` primary keys are sufficient) → `DROP` the stage.

The BigQuery-specific stage-fill is `BigQueryDialect.bulk_land`: the ADBC driver implements no `adbc.ingest.*` bulk path and DML `INSERT` is a BigQuery anti-pattern (quota-bound, slow, costly), so each Arrow batch is written to an in-memory Parquet buffer and shipped as a `WRITE_APPEND` load job (direct media upload — no GCS staging bucket, no extra inputs) into the engine-created stage.

- **Idempotent, orphan-drained load jobs** — the engine gives each batch a collision-proof stage table (its name embeds `sha256(run_id|stream_id|batch_seq|target)`) and drops+recreates it on every attempt. `bulk_land` submits under a deterministic job-id chain (`analitiq_<token>_0`, `_1`, …) hashing that stage identity and the exact Parquet payload. BigQuery load jobs are idempotent by job ID. **Within** a call, a re-submission that collides with an already-existing id (an `Already Exists: Job` conflict — an HTTP-layer insert retry) attaches to that job instead of duplicating it. **Across** engine retries, a prior attempt whose client-side polling was interrupted may have left its job running server-side; the chain walk finds and drains it to a terminal state — closing the window where it commits into the recreated same-named stage — then reuses it if it already landed this exact payload into the current stage incarnation (row count matches) or submits a fresh chain id. Row idempotency otherwise lives in the engine's mode statement (`MERGE` for upsert; the anti-join `INSERT … SELECT` for keyed insert; truncate-insert's append phase is at-least-once by design).
- **Credentials** — recovered per call from the same connection state (service-account key or OAuth user credentials) via `GetOption` on the live ADBC connection, including the billing project (`quota_project`); the load-job client is built inside the call and discarded on return (never cached), per the ADR's `load_job` mechanism contract. No second credential input exists.
- **Failure classification** — serialization defects and Google errors that cannot heal between retries raise `AdbcConfigurationError` (the batch fails FATAL); everything else acks RETRYABLE. `rateLimitExceeded`/`quotaExceeded` (HTTP 403), `backendError`, `internalError`, and `tableUnavailable` retry with backoff; `accessDenied`, `invalid`, `notFound`, and `duplicate` fail deterministic. OAuth credential-refresh failures retry unless the token endpoint names a deterministic error such as `invalid_grant` or `invalid_client`.

## Rate Limits

BigQuery enforces quotas and limits (concurrent interactive queries, query length, API request rates, load jobs per table per day, response size). Concrete numbers vary by project and edition — see https://cloud.google.com/bigquery/quotas.

## Type Mapping

Read direction (native → Arrow) is defined in `definition/type-map-read.json`; write direction (Arrow → native DDL) in `definition/type-map-write.json`. Scale-omitted decimal declarations (`NUMERIC(10)`) map with scale 0. The `NUMERIC`/`BIGNUMERIC` write render is handled by `BigQueryDialect.render_column_type` in `connector.py` (not the write map), because the NUMERIC-vs-BIGNUMERIC choice depends on precision, scale, **and** each type's integer-digit bound; invalid decimal shapes (precision < 1, scale > precision, or out of BIGNUMERIC range) fail loud at render time. `Duration`/`Interval` are deliberately unmapped in the write map — `render_column_type` takes over both families and rejects them with an actionable error (see Caveats).

## Caveats

- No TCP port — BigQuery is an HTTPS REST API; do not configure host/port.
- `auth_type` is abstracted to `service` | `user`; a raw access-token / Microsoft Entra (`aad`) path is not supported by the current ADBC driver's documented options.
- **Driver floor:** `adbc-driver-bigquery` / `adbc-driver-manager` >= 1.11.0 — the `quota_project` database option and the `GetOption` credential recovery the write path relies on are absent from older wheels.
- `BIGNUMERIC` maps to `Decimal256` (precision up to 76 exceeds `Decimal128`'s max of 38).
- `GEOGRAPHY` and `INTERVAL` are surfaced as `Utf8` (WKT / ISO-8601 text); `ARRAY`, `STRUCT`, and `RANGE` map to `Json`.
- Load jobs apply BigQuery column default value expressions for columns absent from the loaded Parquet file (engine-stamped metadata columns rely on this).
- `Json`-family canonicals (`Json`, `Object`, and the nested `List`/`Struct`/`Map` forms) render `STRING` DDL, not `JSON`: the CDK ships `Json` values as Parquet `STRING`, and BigQuery batch load jobs cannot populate `JSON` columns from Parquet (JSON ingest via batch load requires CSV, newline-delimited JSON, or Avro source formats). The JSON text lands intact in the `STRING` column and is queryable in place via `PARSE_JSON()` and the JSON functions. A BigQuery-native `JSON` source column therefore round-trips to a `STRING` destination column.
- Columns that materialize as genuine Arrow struct/list/map values (e.g. an endpoint column declared `Object` with a real `properties` sub-schema, or `List` with `items`) are serialized to compact JSON text before the Parquet write (`_jsonify_nested_columns` in `connector.py`), so they land in their `STRING` DDL columns like every other Json-family value. Inside that serialization: date/time/timestamp leaves become ISO-8601 text (aware timestamps keep their offset), decimals become strings (precision-preserving), binary becomes base64 text, non-finite float *values* become JSON `null`, and map keys are coerced to strings. Leaves JSON text would silently corrupt fail loud and deterministic at write time instead of degrading: nested `duration`/`interval`/union leaves and non-scalar map key types are rejected (cast upstream — mirroring the top-level `Duration`/`Interval` CREATE TABLE rejection), a map key that loses its identity as text (non-finite float keys; keys whose coerced text collides) fails the batch, and any other exotic leaf that degrades to its Python text form is WARNING-logged per column, never silent.
- `Duration`/`Interval`-typed source columns are rejected at `CREATE TABLE` time (`BigQueryDialect.render_column_type` raises; the two families are deliberately absent from `type-map-write.json`): BigQuery `INTERVAL` cannot be populated by a Parquet load job — Arrow interval values have no Parquet encoding at all, and Arrow duration serializes to Parquet `INT64`, which BigQuery refuses to load into `INTERVAL`. The stream fails loud at schema configuration, before an unloadable table exists; cast such columns upstream (e.g. to `Utf8`, or `Int64` for a raw duration count).
- The stage-table lifecycle (pre-flight `DROP IF EXISTS`, `CREATE … LIKE`, post-apply `DROP`) is the **engine's** now, not the connector's; the stage is co-located in the target's dataset (`sql_capabilities.stage.schema: target`). If the post-apply `DROP` fails, the engine orphan-logs the stage table (a WARNING names it); it must be dropped manually or expire via the dataset's default table expiration.
- `sql_capabilities.catalog: full` lets a destination stream write to a project other than the connection's default (three-level `project.dataset.table` addressing); it preserves the pre-rc17 `supports_catalog_addressing` behavior, now declared in `connector.json` rather than as a dialect flag.
