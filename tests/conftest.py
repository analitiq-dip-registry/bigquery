"""Test bootstrap for the flat-layout BigQuery connector package.

Two jobs, both required before any test module imports:

1. Stub the engine-private ``cdk`` modules so ``connector.py`` imports
   without the engine installed. Only the surface ``connector.py`` touches
   is mirrored; ``AdbcConfigurationError`` is a real class so tests can
   raise/catch it, and the stub ``SqlDialect`` provides the real base's
   identifier-quoting surface (``quote_ident`` / ``quote_table`` with
   BigQuery-style three-level ``catalog.schema.table`` addressing) so the
   render tests exercise real composition. ``pyarrow`` /
   ``pyarrow.parquet`` are real dependencies and are NOT stubbed.

2. Import the package under its real distribution name. The repo root IS
   the wheel package directory (``package-dir`` maps
   ``analitiq_connector_bigquery`` to ``.``), so the root ``__init__.py``
   holds a relative import that only works inside a package; pre-importing
   the real package here and aliasing the collector's ``__init__`` module
   name to it turns pytest's bare-module import into a cache hit.
"""

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

# ---- 1. engine-private stubs (before the package import below) ----------


@dataclass(frozen=True)
class TableAddress:
    """Mirror of cdk.sql.dialects.TableAddress (table, schema, catalog)."""

    table: str
    schema: str = ""
    catalog: str = ""


class AdbcConfigurationError(Exception):
    """Mirror of cdk.adbc_registry.AdbcConfigurationError for test isolation."""


class _SqlDialect:
    """Minimal base mirroring the CDK ``SqlDialect`` quoting surface.

    ``quote_table`` renders BigQuery's three-level ``catalog.schema.table``
    (dropping empty parts), matching the real base so the write-path render
    tests exercise real composition logic. ``_check_catalog`` is a no-op
    (the real base gates on the declared ``sql_capabilities.catalog``, which
    is out of scope for a unit test), and ``render_column_type`` returns a
    marker so tests can assert the dialect delegates non-decimal canonicals
    to the base.
    """

    name = ""
    quote_char = '"'

    def quote_ident(self, name: str) -> str:
        return f"{self.quote_char}{name}{self.quote_char}"

    def quote_table(self, address: TableAddress) -> str:
        parts = [
            p for p in (address.catalog, address.schema, address.table) if p
        ]
        return ".".join(self.quote_ident(p) for p in parts)

    def _check_catalog(self, catalog: str) -> None:
        return None

    def render_column_type(self, canonical, type_mapper, *, params=None) -> str:
        return f"<delegated:{canonical}>"


class _GenericSQLConnector:
    pass


def _normalize_canonical_type(canonical: str) -> str:
    """Identity stub: tests pass already-canonical spellings."""
    return canonical


_cdk_adbc_registry = MagicMock()
_cdk_adbc_registry.AdbcConfigurationError = AdbcConfigurationError

_cdk_sql_dialects = MagicMock()
_cdk_sql_dialects.SqlDialect = _SqlDialect
_cdk_sql_dialects.TableAddress = TableAddress

_cdk_sql_generic = MagicMock()
_cdk_sql_generic.GenericSQLConnector = _GenericSQLConnector

_cdk_type_map = MagicMock()
_cdk_type_map.normalize_canonical_type = _normalize_canonical_type

sys.modules.setdefault("cdk", MagicMock())
sys.modules.setdefault("cdk.adbc_registry", _cdk_adbc_registry)
sys.modules.setdefault("cdk.sql", MagicMock())
sys.modules.setdefault("cdk.sql.dialects", _cdk_sql_dialects)
sys.modules.setdefault("cdk.sql.generic", _cdk_sql_generic)
sys.modules.setdefault("cdk.type_map", _cdk_type_map)
sys.modules.setdefault("cdk.type_map.mapper", MagicMock())

# ---- 2. import the flat-layout package properly -------------------------

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))  # `from connector import ...` in tests

_spec = importlib.util.spec_from_file_location(
    "analitiq_connector_bigquery",
    _root / "__init__.py",
    submodule_search_locations=[str(_root)],
)
_pkg = importlib.util.module_from_spec(_spec)
sys.modules["analitiq_connector_bigquery"] = _pkg
_spec.loader.exec_module(_pkg)
# pytest's Package collector resolves the root __init__.py to a module
# literally named "__init__"; satisfy it from the cache.
sys.modules.setdefault("__init__", _pkg)
