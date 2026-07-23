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
- **Credentials** — rebuilt from the same connection state (service-account key or OAuth user credentials), read back from the live ADBC connection; no second credential input exists.

## Rate Limits

BigQuery enforces quotas and limits (concurrent interactive queries, query length, API request rates, load jobs per table per day, response size). Concrete numbers vary by project and edition — see https://cloud.google.com/bigquery/quotas.

## Type Mapping

Read direction (native → Arrow) is defined in `definition/type-map-read.json`; write direction (Arrow → native DDL) in `definition/type-map-write.json`. Scale-omitted decimal declarations (`NUMERIC(10)`) map with scale 0. The `NUMERIC`/`BIGNUMERIC` write render is handled by `BigQueryDialect.render_column_type` in `connector.py` (not the write map), because the NUMERIC-vs-BIGNUMERIC choice depends on precision, scale, **and** each type's integer-digit bound; invalid decimal shapes (precision < 1, scale > precision, or out of BIGNUMERIC range) fail loud at render time.

## Caveats

- No TCP port — BigQuery is an HTTPS REST API; do not configure host/port.
- `auth_type` is abstracted to `service` | `user`; a raw access-token / Microsoft Entra (`aad`) path is not supported by the current ADBC driver's documented options.
- **Driver floor:** `adbc-driver-bigquery` / `adbc-driver-manager` >= 1.11.0 — the `quota_project` database option and the `GetOption` credential recovery the write path relies on are absent from older wheels.
- `BIGNUMERIC` maps to `Decimal256` (precision up to 76 exceeds `Decimal128`'s max of 38).
- `GEOGRAPHY` and `INTERVAL` are surfaced as `Utf8` (WKT / ISO-8601 text); `ARRAY`, `STRUCT`, and `RANGE` map to `Json`.
- Load jobs apply BigQuery column default value expressions for columns absent from the loaded Parquet file (engine-stamped metadata columns rely on this).
- `Json`-canonical columns are loaded from Parquet `STRING` into BigQuery `JSON` columns.
- `Duration`/`Interval`-typed source columns render `INTERVAL` DDL, but such columns are not representable in Parquet load jobs — loading one fails.
