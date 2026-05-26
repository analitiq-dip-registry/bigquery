# Contributing

Thank you for your interest in contributing to the Analitiq connector registry. This guide explains how to add endpoints, improve existing connectors, and create new connectors for systems that don't have one yet.

## Before you start

- Install [Claude Code](https://claude.ai/code)
- Install the connector builder plugin:
  ```
  claude plugin add analitiq-dip-registry/analitiq-plugin-connector-builder
  ```
- Fork the connector repo you want to contribute to (or the [connector-template](https://github.com/analitiq-dip-registry/connector-template) for new connectors)

## Adding endpoints to an existing connector

1. Fork and clone the connector repo
2. Launch Claude in the repo directory
3. Tell it: *"Add an endpoint for [resource name]"*
4. The plugin will research the API documentation and generate the endpoint definition
5. Create a PR with the label `version:minor`

## Fixing or improving an existing connector or endpoint

1. Fork and clone the connector repo
2. Make your changes (fix auth details, correct schema fields, update documentation, etc.)
3. Create a PR with the label `version:patch`

For breaking changes to authentication or connector structure, use the label `version:major`.

## Creating a new connector

Before creating a new connector, check if one already exists at [github.com/analitiq-dip-registry](https://github.com/analitiq-dip-registry). Connectors are named `connector-{system-name}`.

If the connector doesn't exist:

1. Launch Claude anywhere
2. Tell it: *"I want to create a connector for [system name]"*
3. The plugin will interview you about the system, research its API documentation, and generate the full connector with all required files
4. Push the new repo to the `analitiq-dip-registry` GitHub org (or request access to do so)

No coding required — the plugin handles authentication research, endpoint schema generation, and file creation automatically.

## Pull request guidelines

- **All PRs must target the default branch** and pass CI checks before merging
- **Apply a version label** to every PR:
  - `version:minor` — new endpoints added
  - `version:patch` — fixes to existing definitions or documentation
  - `version:major` — breaking changes to authentication or connector structure
- **Do not manually edit** `definition/manifest.json` version or `CHANGELOG.md` version entries — a GitHub Action updates these automatically when the PR is merged
- **One connector per repo** — do not add multiple connectors to a single repository

## What the automated pipeline does

When a PR is merged into `main` with a version label, the `Version Bump on PR Merge` workflow runs and:

1. Bumps the version in `definition/manifest.json` according to the label
2. Appends a new entry to `CHANGELOG.md` with the PR title and date
3. Commits and pushes the bump to `main`
4. Creates a git tag `vX.Y.Z` and a matching GitHub Release targeting `main`
5. POSTs a notification to the Analitiq registry webhook with the release metadata (`slug`, `name`, `type`, `repo`, `version`, `tag`, `release_url`, `published_at`, `bump`)

### When a release is NOT created

- **PRs merged without a version label** — the workflow exits before step 1. Nothing is bumped, tagged, released, or notified.
- **PRs closed without merging** — workflow does not run.
- **Direct pushes to `main`** — workflow is `pull_request`-gated, so direct pushes (including the bot's own bump commit) do not trigger it.

### Creating a release when you're ready

The intended path is: **apply a version label to the PR before merging**. That is the only "release button" — merging a labeled PR runs the full pipeline end-to-end.

If you already merged a PR without a label and want a release, open a small follow-up PR (for example a docs tweak) with the desired `version:*` label and merge it. Do not create tags or GitHub Releases by hand — doing so bypasses the manifest bump, the CHANGELOG entry, and the webhook notification, and leaves the registry out of sync.

## Repository structure

```
connector-{name}/
├── CLAUDE.md               # Agent reference for Claude Code
├── AGENTS.md               # Agent reference for other frameworks (identical to CLAUDE.md)
├── README.md               # Human documentation
├── CHANGELOG.md            # Version history (auto-updated on merge)
├── CONTRIBUTING.md         # This file
└── definition/             # Connector definition files
    ├── connector.json      # Authentication and connector config
    ├── manifest.json       # Endpoint manifest with version (auto-bumped on merge)
    └── endpoints/          # Individual endpoint JSON definitions
        └── {name}.json
```

## Questions?

If you're unsure about anything, open an issue on the connector repo or on the [connector builder plugin](https://github.com/analitiq-dip-registry/analitiq-plugin-connector-builder) repo.