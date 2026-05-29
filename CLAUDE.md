---
name: Google BigQuery
description: >
  Connect to Google BigQuery, Google Cloud's serverless data warehouse, over the ADBC BigQuery driver.
type: database
---

# Google BigQuery

Google BigQuery is Google Cloud's serverless, highly scalable data warehouse. This connector reads schemas, tables, and views from a BigQuery project over the engine's ADBC BigQuery driver (`transport_type: adbc`, `driver: bigquery`). BigQuery is an HTTPS REST service — there is no host/port, username, or password; all connection state is passed as `adbc.bigquery.*` driver options.

## Authentication

### Credential bundle (`auth.type: db`)
BigQuery does not use username/password and does not support API keys. Authentication is selected via the `auth_type` input:

- **`service` (default, recommended)** — a Google Cloud service-account JSON key. Paste the full JSON key contents into `auth_json_credential` (stored as a secret). The key embeds its own `project_id`, `client_email`, and `private_key`.
- **`user`** — an OAuth user-credential flow. Supply `client_id`, `client_secret`, and `refresh_token`.
- **`aad`** — Microsoft Entra ID. Supply `access_token` (and optionally `audience_uri`).

- Client app required: no
- Always-on TLS to `googleapis.com` (no TLS-mode options exposed by the driver)

## Post-Auth Steps

Resource discovery runs automatically on activation via the builtin `information_schema` strategy: tables, views, and their column types are read from BigQuery's `INFORMATION_SCHEMA` views and mapped to canonical types via `definition/type-map.json`.

## Connection Inputs

| Input | Storage | Required | Notes |
|-------|---------|----------|-------|
| `project_id` | connection.parameters | no | GCP project to query; defaults to the project in the active credentials |
| `billing_project_id` | connection.parameters | no | Project charged for jobs; defaults to `project_id` |
| `dataset_id` | connection.parameters | no | Default dataset for discovery scoping |
| `auth_type` | connection.parameters | yes | `service` (default) \| `user` \| `aad` |
| `auth_json_credential` | secrets | conditional | Full service-account JSON key contents (when `auth_type=service`) |
| `client_id` | connection.parameters | conditional | OAuth client ID (when `auth_type=user`) |
| `client_secret` | secrets | conditional | OAuth client secret (when `auth_type=user`) |
| `refresh_token` | secrets | conditional | OAuth refresh token (when `auth_type=user`) |
| `access_token` | secrets | conditional | Bearer token (primarily `auth_type=aad`) |
| `audience_uri` | connection.parameters | no | Token audience for Entra/OAuth |
| `scopes` | connection.parameters | no | Comma-separated OAuth scopes |
| `client_timeout` | connection.parameters | no | Client timeout in seconds |
| `query_results_timeout` | connection.parameters | no | Query-results fetch timeout in seconds (default 5 min) |

> Conditional inputs are all schema-optional (`required: false`); which ones are actually needed depends on the selected `auth_type`. Conditional requiredness is not expressible in the connection contract.

## Rate Limits

BigQuery enforces quotas and limits (concurrent interactive queries, query length, API request rates, response size). Concrete numbers vary by project and edition — see https://cloud.google.com/bigquery/quotas.

## Caveats

- No TCP port — BigQuery is an HTTPS REST API; do not configure host/port.
- `dataset_id` scopes discovery but has no `adbc.bigquery.*` option key, so it is not passed as a driver option.
- `BIGNUMERIC` maps to `Decimal256` (precision up to 76 exceeds `Decimal128`'s max of 38).
- `GEOGRAPHY` and `INTERVAL` are surfaced as `Utf8` (WKT / ISO-8601 text); `ARRAY`, `STRUCT`, and `RANGE` map to `Json`.
