"""BigQuery connector — dialect + connector class for the Analitiq CDK.

Everything BigQuery-specific lives here, in the connector package: the
INFORMATION_SCHEMA exclusion for discovery and the BigQuery dialect wired
to the CDK's GenericSQLConnector base.

The connector runs on the ADBC BigQuery driver (transport_type ``adbc``),
which exchanges Arrow buffers natively over the BigQuery Storage Read API;
there is no SQLAlchemy transport, so the SQLAlchemy hooks stay on the
neutral base. BigQuery is HTTPS/TLS-only to ``googleapis.com`` with no
selectable TLS mode, so there is no ``ssl_mode`` input and no
``build_tls_connect_arg`` hook.

"No SQLAlchemy transport" covers only the connect/write path: no SQLAlchemy
``Engine`` is ever constructed here. The engine's shared read path still
compiles paged ``SELECT``s through SQLAlchemy Core and resolves this dialect
via ``sqlalchemy.dialects.registry.load("bigquery")``, so
``sqlalchemy-bigquery`` is a required runtime dependency (see
``requirements.txt``).

The write direction is fully declarative: ``definition/type-map.json``
owns every column-type render, so this dialect ships no Python
type-rendering table and needs no ``render_column_type`` override.

Registered under connector_id ``bigquery`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector


class BigQueryDialect(SqlDialect):
    """BigQuery SQL strategy: backtick-quoted identifiers and
    INFORMATION_SCHEMA discovery scoped to each dataset."""

    name = "bigquery"
    # INFORMATION_SCHEMA is a virtual schema that exists inside every
    # dataset; it is not a user dataset and must be excluded from discovery.
    system_schemas = ("INFORMATION_SCHEMA",)
    supports_upsert_adbc = True


class BigQueryConnector(GenericSQLConnector):
    """BigQuery connector: the CDK SQL base wired to the BigQuery dialect."""

    dialect_class = BigQueryDialect
