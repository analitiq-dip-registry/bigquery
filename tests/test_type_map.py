"""Pins the write section of ``definition/type-map.json``: rule order and the
canonicals deliberately kept out of it.

Read-map regex correctness (e.g. decimal precision/scale bounds) is not
duplicated here: ``analitiq-validate`` (RULE-TMAP-010) is the single gate over
a capture's range against its Arrow parameter position, run on every PR by the
org's pinned validator. A repo-local test re-declaring that check is a second
gate over the same shape (schema-contracts.md, "One gate per document").
"""

import json
import re
from pathlib import Path

TYPE_MAP_PATH = Path(__file__).resolve().parent.parent / "definition" / "type-map.json"

TYPE_MAP = json.loads(TYPE_MAP_PATH.read_text(encoding="utf-8"))


class TestWriteSection:
    EXPECTED = [
        ("exact", "BOOL", "Boolean"),
        ("exact", "INT64", "Int8"),
        ("exact", "INT64", "Int16"),
        ("exact", "INT64", "Int32"),
        ("exact", "INT64", "Int64"),
        ("exact", "INT64", "UInt8"),
        ("exact", "INT64", "UInt16"),
        ("exact", "INT64", "UInt32"),
        ("exact", "INT64", "UInt64"),
        ("exact", "FLOAT64", "Float16"),
        ("exact", "FLOAT64", "Float32"),
        ("exact", "FLOAT64", "Float64"),
        ("exact", "STRING", "Utf8"),
        ("exact", "STRING", "LargeUtf8"),
        ("exact", "BYTES", "Binary"),
        ("exact", "BYTES", "LargeBinary"),
        ("exact", "DATE", "Date32"),
        ("exact", "DATE", "Date64"),
        ("exact", "STRING", "Json"),
        ("exact", "STRING", "Object"),
        ("regex", "BYTES", r"^FixedSizeBinary\(\d+\)$"),
        ("regex", "TIME", r"^Time(32|64)\(.*\)$"),
        ("regex", "TIMESTAMP", r"^Timestamp\([^,)]+,[^)]+\)$"),
        ("regex", "DATETIME", r"^Timestamp\([^,)]+\)$"),
        ("regex", "STRING", r"^(List|LargeList|FixedSizeList)<.*>$"),
        ("regex", "STRING", r"^Struct<.*>$"),
        ("regex", "STRING", r"^Map<.*>$"),
    ]

    def test_rules_and_order_are_unchanged(self):
        actual = [
            (r["match"], r["native_type"], r["arrow_type"]) for r in TYPE_MAP["write"]
        ]
        assert actual == self.EXPECTED

    def test_duration_and_interval_stay_unmapped(self):
        # render_column_type rejects both families before the map is consulted;
        # a write rule for either would let an unloadable table be created.
        for rule in TYPE_MAP["write"]:
            assert not re.search(r"Duration|Interval", rule["arrow_type"])
            assert not re.search(r"INTERVAL", rule["native_type"].upper())
