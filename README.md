# Google BigQuery

[![Status: unverified](https://img.shields.io/badge/status-unverified-orange)](https://github.com/analitiq-dip-registry)
[![Latest release](https://img.shields.io/github/v/release/analitiq-dip-registry/bigquery)](https://github.com/analitiq-dip-registry/bigquery/releases)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Read from and write to Google BigQuery — Google Cloud's serverless, highly scalable data warehouse — over the ADBC BigQuery driver (reads) and Parquet load jobs (writes).

## What is this?

This is a **connector** — a configuration that defines how to authenticate with Google BigQuery and what data is available for reading and writing. It does not move data by itself. Instead, it is used by the [Analitiq](https://analitiq-app.com) data integration platform or the open-source `analitiq-dip-registry` engine to set up data pipelines.

This is a **database** connector: it connects over the engine's ADBC BigQuery driver (`transport_type: adbc`, `driver: bigquery`) and discovers tables and column types at runtime from BigQuery's `INFORMATION_SCHEMA` — so there are no per-endpoint definition files. Writes follow the industry-standard load-job pattern: each batch ships as an in-memory Parquet buffer submitted as a BigQuery load job (no GCS staging bucket needed), with upserts staged and `MERGE`d on the declared keys.

## How to use this connector

There are two ways to use this connector:

### Option 1 — Analitiq Cloud (no setup required)

All connectors from this registry are automatically available on [analitiq-app.com](https://analitiq-app.com). Simply log in, select the connector, and follow the on-screen instructions to connect your account.

### Option 2 — Open Source (self-hosted)

All connectors are open source and free to use. To get started:

1. Clone the [analitiq-dip-registry](https://github.com/analitiq-dip-registry) repository
2. Install the Claude plugin `analitiq-plugin-dataflow`
3. Launch Claude in the root directory of `analitiq-dip-registry`
4. Tell it: *"I need to move data from X to Y"*

The `analitiq-plugin-dataflow` plugin will automatically fetch the required connectors from the [Analitiq DIP Registry](https://github.com/analitiq-dip-registry) and set up the data flow pipeline for you.

## Prerequisites

Before you can connect, you need:

- A **Google Cloud project** with the BigQuery API enabled.
- Credentials for one of the supported authentication methods:
  - **Service account (recommended):** a service-account JSON key file with at least the `BigQuery Data Viewer` and `BigQuery Job User` roles (or broader) on the project you want to read.
  - **OAuth user credentials:** an OAuth client ID, client secret, and a refresh token.
- The **project ID** you want to query (optional — it can be inferred from the credentials).

## Authentication

BigQuery does **not** use a username and password and does **not** support API keys. Pick an authentication method via the `auth_type` setting:

- **`service` (default)** — paste the full contents of a service-account JSON key into the **Service Account JSON Key** field. This is the recommended path for automated pipelines.
- **`user`** — supply an OAuth **client ID**, **client secret**, and **refresh token**.

All traffic goes to `googleapis.com` over TLS, which is always on.

### How to get your credentials

For a service-account key (recommended):

1. Open the [Google Cloud Console](https://console.cloud.google.com) and select your project.
2. Go to **IAM & Admin > Service Accounts** and create (or pick) a service account.
3. Grant it the BigQuery roles it needs (e.g. **BigQuery Data Viewer** + **BigQuery Job User**).
4. Open the service account, go to **Keys > Add Key > Create new key**, choose **JSON**, and download the file.
5. Paste the **entire contents** of that JSON file into the connector's **Service Account JSON Key** field.

## Available Data

This is a database connector — it does not ship a fixed list of endpoints. Instead it discovers what is available at connection time:

| Resource | How it's discovered | Description |
|----------|---------------------|-------------|
| Datasets / tables / views | `INFORMATION_SCHEMA` (builtin discovery) | Tables and views in the project (optionally scoped to a default dataset) are listed on activation; column types are mapped to canonical Analitiq types via `definition/type-map-read.json` (write direction: `definition/type-map-write.json`). |

## Limitations

- **No TCP port** — BigQuery is an HTTPS REST API; there is no host or port to configure.
- **Quotas & limits** — BigQuery enforces per-project quotas (concurrent queries, query length, API request rates, response size). Concrete values vary by project and edition; see the [BigQuery quotas documentation](https://cloud.google.com/bigquery/quotas).
- **Type mapping** — `BIGNUMERIC` maps to `Decimal256`; `GEOGRAPHY` and `INTERVAL` are surfaced as text (`Utf8`); `ARRAY`, `STRUCT`, and `RANGE` are surfaced as `Json`.
- **Default dataset** — `dataset_id` scopes discovery and is passed to the driver as `adbc.bigquery.sql.dataset_id`.

## For AI agents

This connector includes `CLAUDE.md` and `AGENTS.md` files — machine-readable references used by AI agents and agentic frameworks. They document authentication types, available data, post-auth steps, and any caveats for programmatic use. Both files are kept identical — `CLAUDE.md` is for Claude Code, `AGENTS.md` is for other agent frameworks.

## Create a connector to any system

You can create a new connector to any API or database using Claude and the Analitiq connector builder plugin:

1. Install [Claude Code](https://claude.ai/code)
2. Install the connector builder plugin:
   ```
   claude plugin add analitiq-dip-registry/analitiq-plugin-connector-builder
   ```
3. Launch Claude and say: *"I want to create a connector for [system name]"*
4. The plugin will interview you about the system, research its API documentation, and generate the full connector with all required files

No coding required — the plugin handles authentication research, endpoint schema generation, and file creation automatically.

![Example of Claude building a connector](media/example_1.png)

## Contributing

All connectors in this registry are community-maintained and live at [github.com/analitiq-dip-registry](https://github.com/analitiq-dip-registry). To add new endpoints or improve an existing connector, install the [connector builder plugin](https://github.com/analitiq-dip-registry/analitiq-plugin-connector-builder) and follow its instructions.

## Links

- [BigQuery documentation](https://cloud.google.com/bigquery/docs)
- [BigQuery data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
- [ADBC BigQuery driver](https://arrow.apache.org/adbc/current/driver/bigquery.html)
- [Analitiq Cloud](https://analitiq-app.com)
- [Analitiq Engine (open source)](https://github.com/analitiq-ai/analitiq-engine)
- [Analitiq DIP Registry (open source)](https://github.com/analitiq-dip-registry)
