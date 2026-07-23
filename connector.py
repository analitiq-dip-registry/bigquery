"""Google BigQuery connector - dialect + connector class for the Analitiq CDK.

BigQuery runs on the first-class ADBC BigQuery driver (``transport_type:
adbc``), which hands Arrow buffers to BigQuery's Storage Write API with no
row-by-row path. BigQuery is an HTTPS REST service - there is no host/port,
username, or password - and traffic to ``googleapis.com`` is always TLS with
no selectable mode, so there is no ``ssl_mode`` input and no
``build_tls_connect_arg`` hook. All connection state is carried in the
transport's ``adbc.bigquery.sql.*`` ``db_kwargs``.

Everything BigQuery-specific lives here:

* **Backtick quoting** - BigQuery reads ``"..."`` as a string literal, so
  identifiers are quoted with backticks (``quote_char``).
* **Three-level addressing** - project -> dataset -> table; the project is a
  per-statement addressable catalog (``supports_catalog_addressing``).
* **NOT ENFORCED primary keys** - BigQuery's parser requires the
  ``NOT ENFORCED`` qualifier on a PRIMARY KEY clause and never enforces
  uniqueness (``pk_not_enforced``).
* **NUMERIC/BIGNUMERIC render arithmetic** - the single write-direction rule
  ``type-map-write.json`` cannot express. NUMERIC (precision <= 38, scale <= 9)
  vs BIGNUMERIC (precision <= 76, scale <= 38) is chosen from BOTH a Decimal's
  precision and scale, so a plain regex rule would emit an out-of-range
  ``NUMERIC(30, 15)``. Everything else delegates back to the write map via
  ``super().render_column_type``.

The read path still compiles paged ``SELECT``s through SQLAlchemy Core and
resolves this dialect by name via
``sqlalchemy.dialects.registry.load("bigquery")``, so ``sqlalchemy-bigquery``
is a required runtime dependency (see ``requirements.txt``) even though the
data/write transport is ADBC.

Registered under connector_id ``bigquery`` via the package entry points
(``analitiq.source_connectors`` / ``analitiq.destination_connectors``).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from cdk.sql.dialects import SqlDialect
from cdk.sql.generic import GenericSQLConnector

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cdk.type_map.mapper import TypeMapper


class BigQueryDialect(SqlDialect):
    """BigQuery SQL strategy: backtick quoting, three-level (project ->
    dataset -> table) addressing, NOT ENFORCED primary keys, and the
    NUMERIC/BIGNUMERIC precision-range render override."""

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

    #: Decimal128/Decimal256 canonical, tolerant of author whitespace.
    _DECIMAL_RE = re.compile(
        r"^Decimal(?:128|256)\(\s*(?P<p>\d+)\s*,\s*(?P<s>\d+)\s*\)$"
    )

    def render_column_type(
        self,
        canonical: str,
        type_mapper: "TypeMapper",
        *,
        params: "Mapping[str, Any] | None" = None,
    ) -> str:
        """Render a canonical Arrow type to BigQuery DDL.

        Only the Decimal family needs code: NUMERIC vs BIGNUMERIC is chosen
        from BOTH precision and scale, which the declarative write map
        cannot express. A Decimal that fits neither native range fails loud
        rather than emitting invalid DDL. Every other canonical delegates to
        ``type-map-write.json`` through the base implementation.
        """
        match = self._DECIMAL_RE.match(canonical)
        if match is not None:
            precision = int(match.group("p"))
            scale = int(match.group("s"))
            # BigQuery NUMERIC/DECIMAL: precision <= 38, scale <= 9,
            # integer digits (precision - scale) <= 29.
            if precision <= 38 and scale <= 9 and (precision - scale) <= 29:
                return f"NUMERIC({precision}, {scale})"
            # BigQuery BIGNUMERIC/BIGDECIMAL: precision <= 76, scale <= 38.
            if precision <= 76 and scale <= 38 and (precision - scale) <= 38:
                return f"BIGNUMERIC({precision}, {scale})"
            raise ValueError(
                f"{self.name}: Decimal(precision={precision}, scale={scale}) "
                "exceeds BigQuery's BIGNUMERIC range (precision <= 76, "
                "scale <= 38); no lossless native decimal type exists"
            )
        return super().render_column_type(canonical, type_mapper, params=params)


class BigQueryConnector(GenericSQLConnector):
    """Google BigQuery connector: the CDK SQL base wired to the BigQuery dialect."""

    dialect_class = BigQueryDialect
