# REPLACE_CONNECTOR_NAME

REPLACE with a short description of what this connector does and what system it integrates with.

## What is this?

This is a **connector** — a configuration that defines how to authenticate with REPLACE_SYSTEM_NAME and what data endpoints are available for reading and writing. It does not move data by itself. Instead, it is used by the [Analitiq](https://analitiq-app.com) data integration platform or the open-source `analitiq-dip-registry` engine to set up data pipelines.

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

REPLACE with what the user needs before they can connect. Be specific:

- e.g., "A registered OAuth2 application with client ID and client secret"
- e.g., "An API key generated from your account settings"
- e.g., "Admin access to your organisation's account"

## Authentication

REPLACE with a plain-language explanation of how to authenticate. If the system supports multiple authentication methods, explain when to use each one.

### How to get your credentials

REPLACE with step-by-step instructions:

1. e.g., "Log in to your account at https://app.example.com"
2. e.g., "Navigate to Settings > API Keys"
3. e.g., "Click 'Generate New Key' and copy the key"

## Available Endpoints

The table below lists all data endpoints defined by this connector. Each endpoint represents a resource you can read from or write to.

| Endpoint | Method | Description |
|----------|--------|-------------|
|          |        |             |

## Limitations

REPLACE with any important limitations users should know about:

- **Rate limits** — e.g., "The API allows 60 requests per minute"
- **Data freshness** — e.g., "Data may be delayed by up to 15 minutes"
- **Sandbox vs Production** — e.g., "Sandbox and production use different API keys"

## For AI agents

This connector includes `CLAUDE.md` and `AGENTS.md` files — machine-readable references used by AI agents and agentic frameworks. They document authentication types, available endpoints, post-auth steps, and any caveats for programmatic use. Both files are kept identical — `CLAUDE.md` is for Claude Code, `AGENTS.md` is for other agent frameworks.

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

- [API Documentation](REPLACE with URL)
- [Analitiq Cloud](https://analitiq-app.com)
- [Analitiq Engine (open source)](https://github.com/analitiq-ai/analitiq-engine)
- [Analitiq DIP Registry (open source)](https://github.com/analitiq-dip-registry)