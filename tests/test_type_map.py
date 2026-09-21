"""Pins ``definition/type-map.json``: decimal read bounds and the write section.

The type-map contract matches regex rules in RE2 with a full match, on the
probed native type after ``normalize_native`` (strip, collapse whitespace,
uppercase). Python ``re`` stands in for RE2 here: the read patterns use only
literals, ``\\s``, ``\\d``, character classes, alternation, groups and anchors,
which mean the same in both dialects. The one syntactic difference is the named
group spelling, RE2 accepts ``(?<p>...)`` and Python needs ``(?P<p>...)``, so it
is translated before compiling. ``fullmatch`` mirrors the contract's whole-string
match.
"""

import json
import re
from pathlib import Path

import pytest

TYPE_MAP_PATH = Path(__file__).resolve().parent.parent / "definition" / "type-map.json"

TYPE_MAP = json.loads(TYPE_MAP_PATH.read_text(encoding="utf-8"))


def _normalize_native(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).upper()


def _read_arrow_type(native: str):
    """First-match-wins read lookup; None when no rule matches."""
    subject = _normalize_native(native)
    for rule in TYPE_MAP["read"]:
        if rule["match"] == "exact":
            if _normalize_native(rule["native_type"]) == subject:
                return rule["arrow_type"]
            continue
        pattern = rule["native_type"].replace("(?<", "(?P<")
        match = re.fullmatch(pattern, subject)
        if match:
            return re.sub(r"\$\{(\w+)\}", lambda m: match.group(m.group(1)), rule["arrow_type"])
    return None


class TestDecimalReadBounds:
    @pytest.mark.parametrize(
        "native, arrow",
        [
            ("NUMERIC(38,9)", "Decimal128(38, 9)"),
            ("NUMERIC(1,0)", "Decimal128(1, 0)"),
            ("NUMERIC( 10 , 2 )", "Decimal128(10, 2)"),
            ("NUMERIC(10)", "Decimal128(10, 0)"),
            ("DECIMAL(38,38)", "Decimal128(38, 38)"),
            ("BIGNUMERIC(76,38)", "Decimal256(76, 38)"),
            ("BIGNUMERIC(76,76)", "Decimal256(76, 76)"),
            ("BIGDECIMAL(76)", "Decimal256(76, 0)"),
        ],
    )
    def test_in_range_declarations_map(self, native, arrow):
        assert _read_arrow_type(native) == arrow

    @pytest.mark.parametrize(
        "native",
        [
            "NUMERIC(39,0)",
            "NUMERIC(0,0)",
            "NUMERIC(10,39)",
            "NUMERIC(39)",
            "DECIMAL(0)",
            "BIGNUMERIC(77,0)",
            "BIGNUMERIC(10,77)",
            "BIGDECIMAL(77)",
        ],
    )
    def test_out_of_range_declarations_match_no_rule(self, native):
        assert _read_arrow_type(native) is None


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
