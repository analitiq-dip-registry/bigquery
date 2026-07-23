# Google BigQuery

Google BigQuery is Google Cloud's serverless, highly scalable data warehouse. This connector reads from and writes to a BigQuery project over the engine's first-class ADBC BigQuery driver (`transport_type: adbc`, `driver: bigquery`), which hands Arrow buffers to BigQuery's Storage Write API. BigQuery is an HTTPS REST service — there is no host/port, username, or password; all connection state is passed as `adbc.bigquery.sql.*` driver options.

## Authentication

### Credential bundle (`auth.type: db`)
BigQuery does not use username/password and does not support API keys. Authentication is selected via the `auth_type` input:

- **`service` (default, recommended)** — a Google Cloud service-account JSON key. Paste the full JSON key contents into `auth_json_credential` (stored as a secret). The key embeds its own `project_id`, `client_email`, and `private_key`. Resolves to the driver's `json_credential_string` auth type.
- **`user`** — an OAuth user-credential flow. Supply `client_id`, `client_secret`, and `refresh_token`. Resolves to the driver's `user_authentication` auth type.

- Client app required: no
- Always-on TLS to `googleapis.com` (no TLS-mode options exposed by the driver)

## Post-Auth Steps

Resource discovery runs automatically on activation via the builtin `information_schema` strategy: tables, views, and their column types are read from BigQuery's `INFORMATION_SCHEMA` views and mapped to canonical Arrow types via `definition/type-map-read.json`.

## Connection Inputs

| Input | Storage | Required | Driver option | Notes |
|-------|---------|----------|---------------|-------|
| `project_id` | connection.parameters | no | `adbc.bigquery.sql.project_id` | GCP project to query; defaults to the project in the active credentials |
| `billing_project_id` | connection.parameters | no | `adbc.bigquery.sql.auth.quota_project` | Project charged for jobs/quota; defaults to `project_id` |
| `dataset_id` | connection.parameters | no | `adbc.bigquery.sql.dataset_id` | Default dataset (also scopes discovery) |
| `location` | connection.parameters | no | `adbc.bigquery.sql.location` | Dataset/region location (e.g. `US`, `EU`) |
| `auth_type` | connection.parameters | yes | `adbc.bigquery.sql.auth_type` (via `lookup`) | `service` (default) \| `user` |
| `auth_json_credential` | secrets | conditional | `adbc.bigquery.sql.auth_credentials` | Full service-account JSON key contents (when `auth_type=service`) |
| `client_id` | connection.parameters | conditional | `adbc.bigquery.sql.auth.client_id` | OAuth client ID (when `auth_type=user`) |
| `client_secret` | secrets | conditional | `adbc.bigquery.sql.auth.client_secret` | OAuth client secret (when `auth_type=user`) |
| `refresh_token` | secrets | conditional | `adbc.bigquery.sql.auth.refresh_token` | OAuth refresh token (when `auth_type=user`) |

> Conditional inputs are all schema-optional (`required: false`); which ones are actually needed depends on the selected `auth_type`. Conditional requiredness is not expressible in the connection contract.

## Rate Limits

BigQuery enforces quotas and limits (concurrent interactive queries, query length, API request rates, response size). Concrete numbers vary by project and edition — see https://cloud.google.com/bigquery/quotas.

## Type Mapping

Read direction (native → Arrow) is defined in `definition/type-map-read.json`; write direction (Arrow → native DDL) in `definition/type-map-write.json`. The `NUMERIC`/`BIGNUMERIC` write render is handled by `BigQueryDialect.render_column_type` in `connector.py` (not the write map), because the NUMERIC-vs-BIGNUMERIC choice depends on **both** a decimal's precision and scale.

## Caveats

- No TCP port — BigQuery is an HTTPS REST API; do not configure host/port.
- `auth_type` is abstracted to `service` | `user`; a raw access-token / Microsoft Entra (`aad`) path is not supported by the current ADBC driver's documented options.
- `BIGNUMERIC` maps to `Decimal256` (precision up to 76 exceeds `Decimal128`'s max of 38).
- `GEOGRAPHY` and `INTERVAL` are surfaced as `Utf8` (WKT / ISO-8601 text); `ARRAY`, `STRUCT`, and `RANGE` map to `Json`.
- ADBC upsert (MERGE) is not enabled — BigQuery primary keys are `NOT ENFORCED`; append and replace writes go through the base ADBC ingest path.
