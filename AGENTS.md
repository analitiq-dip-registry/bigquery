---
name: Google BigQuery
description: >
  Connect to Google BigQuery, Google Cloud's serverless data warehouse, over the ADBC BigQuery driver.
type: database
---
# Google BigQuery

Google BigQuery is Google Cloud's serverless, highly scalable data warehouse. This connector reads from and writes to a BigQuery project. Connect, SQL execution, discovery, and Arrow reads run over the engine's first-class ADBC BigQuery driver (`transport_type: adbc`, `driver: bigquery`); writes ship as Parquet **load jobs** via the BigQuery API (direct media upload — no GCS staging bucket). BigQuery is an HTTPS REST service — there is no host/port, username, or password; all connection state is passed as `adbc.bigquery.sql.*` driver options.

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

The BigQuery ADBC driver implements no `adbc.ingest.*` bulk path, and DML `INSERT` writes are a BigQuery anti-pattern (quota-bound, slow, costly). Writes follow the industry-standard load-job pattern instead:

- **Append / truncate-insert** — each Arrow batch is written to an in-memory Parquet buffer and submitted as a `WRITE_APPEND` load job (direct media upload; no GCS staging bucket, no extra inputs).
- **Upsert / keyless-insert dedup** — **supported**: the batch loads into a stage table cloned with `CREATE TABLE … LIKE`, then `MERGE`s into the target on the declared conflict keys. BigQuery's `NOT ENFORCED` primary keys are sufficient — the `MERGE` needs only the `ON` clause.
- **Idempotent job IDs** — every load dispatched through `write_batch` submits under a deterministic job-id chain (`analitiq_<token>_0`, `_1`, …) hashing the batch identity (`run_id|stream_id|batch_seq` — the same inputs as the CDK's stage token), the exact Parquet payload, and (for truncate-insert append batches) a per-table truncate epoch rotated on every `TRUNCATE`. BigQuery load jobs are idempotent by job ID, so a retry after a client-side polling timeout **attaches** to the still-running job (or, on pure appends, resumes the committed one) instead of re-submitting — no duplicate appends, no double-filled `MERGE` stage. Destructive steps (`TRUNCATE`, stage `DROP`/`CREATE`) run only after **this batch's** chain is drained to a terminal state and never resume a prior success afterwards. Scope is per batch: absolute for the `MERGE` stage (its name embeds the batch identity); for `TRUNCATE`, an abandoned still-running job of a *different* batch or run remains a residual at-least-once window (as on main).
- **Credentials** — rebuilt from the same connection state (service-account key or OAuth user credentials), read back from the live ADBC connection; no second credential input exists.
- **Failure classification** — load-job failures are classified by BigQuery error reason: `rateLimitExceeded`/`quotaExceeded` (HTTP 403), `backendError`, and `internalError` are retried with backoff; `accessDenied`, `invalid`, `notFound`, and `duplicate` fail the batch as deterministic. OAuth credential-refresh failures are retried unless the token endpoint names a deterministic error such as `invalid_grant` or `invalid_client`.

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
- Columns that materialize as genuine Arrow struct/list/map values (an endpoint column declared `Object` with a real `properties` sub-schema, or `List` with `items`) are serialized to compact JSON text before the Parquet write (`_jsonify_nested_columns` in `connector.py`), so they land in their `STRING` DDL columns like every other Json-family value. Inside that serialization: temporal leaves become ISO-8601 text, decimals become strings (precision-preserving), binary becomes base64 text, non-finite floats become JSON `null`, and map keys are coerced to strings.
- `Duration`/`Interval`-typed source columns are rejected at `CREATE TABLE` time (`BigQueryDialect.render_column_type` raises; the two families are deliberately absent from `type-map-write.json`): BigQuery `INTERVAL` cannot be populated by a Parquet load job — Arrow interval values have no Parquet encoding at all, and Arrow duration serializes to Parquet `INT64`, which BigQuery refuses to load into `INTERVAL`. The stream fails loud at schema configuration, before an unloadable table exists; cast such columns upstream (e.g. to `Utf8`, or `Int64` for a raw duration count).
- If the post-`MERGE` stage-table `DROP` fails on the success path (two attempts), the stage table is orphaned — the batch is acked and never retried — and must be dropped manually or expire via the dataset's default table expiration; a WARNING log names the table.
