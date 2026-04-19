"""
Tests for the TopDrawerSoccer commitments extractor.

Extraction is a pure function over fixture HTML; no network, no DB.
"""

from __future__ import annotations

import os
import sys
from datetime import date as _date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.topdrawer_commitments import (  # noqa: E402
    parse_commitment_date,
    parse_topdrawer_commitments_html,
)


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "topdrawer_commitments_sample.html"
)


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# date parsing
# ---------------------------------------------------------------------------


def test_parse_commitment_date_month_name():
    assert parse_commitment_date("August 5, 2025") == _date(2025, 8, 5)
    assert parse_commitment_date("Aug 5 2025") == _date(2025, 8, 5)


def test_parse_commitment_date_numeric():
    assert parse_commitment_date("7/12/25") == _date(2025, 7, 12)
    assert parse_commitment_date("07/12/2025") == _date(2025, 7, 12)


def test_parse_commitment_date_month_year_only_falls_back_to_day_one():
    assert parse_commitment_date("October 2025") == _date(2025, 10, 1)


def test_parse_commitment_date_unparseable_returns_none():
    assert parse_commitment_date("soon") is None
    assert parse_commitment_date("") is None


# ---------------------------------------------------------------------------
# page-level parse — core acceptance test
# ---------------------------------------------------------------------------


def test_parse_fixture_extracts_all_expected_rows():
    html = _read_fixture()
    rows = parse_topdrawer_commitments_html(
        html,
        source_url="https://www.topdrawersoccer.com/college-soccer-commitments/girls/2026",
    )
    # Fixture has 7 <tr> rows; the last is malformed (no player name)
    # and must be dropped → 6 valid commitments.
    assert len(rows) >= 5, f"expected >=5 parsed rows, got {len(rows)}"
    assert len(rows) == 6, f"expected exactly 6 parsed rows, got {len(rows)}"

    # Every row must have the required contract fields.
    for r in rows:
        assert r["player_name"], f"missing player_name in row: {r}"
        assert r["college_name_raw"], f"missing college_name_raw in row: {r}"
        assert r["source_url"].startswith("https://www.topdrawersoccer.com"), r

    # Spot-check the first parsed row end-to-end.
    first = rows[0]
    assert first["player_name"] == "Ava Brennan"
    assert first["position"] == "Forward"
    assert first["graduation_year"] == 2026
    assert first["club_name_raw"] == "Slammers FC HB Koge"
    assert first["college_name_raw"] == "Stanford University"
    assert first["commitment_date"] == _date(2025, 8, 5)


def test_parse_fixture_bad_row_is_dropped():
    """Row with empty <td> for player name must not be emitted."""
    html = _read_fixture()
    rows = parse_topdrawer_commitments_html(
        html,
        source_url="https://www.topdrawersoccer.com/test",
    )
    for r in rows:
        assert r["player_name"] not in (None, "", " "), r


def test_parse_fixture_handles_month_year_only_date():
    """Row with 'October 2025' (no day) must parse as 2025-10-01."""
    html = _read_fixture()
    rows = parse_topdrawer_commitments_html(
        html,
        source_url="https://www.topdrawersoccer.com/test",
    )
    maya = next((r for r in rows if r["player_name"] == "Maya Sorensen"), None)
    assert maya is not None
    assert maya["commitment_date"] == _date(2025, 10, 1)
    assert maya["graduation_year"] == 2027


def test_parse_fixture_handles_numeric_date():
    html = _read_fixture()
    rows = parse_topdrawer_commitments_html(
        html,
        source_url="https://www.topdrawersoccer.com/test",
    )
    lila = next((r for r in rows if r["player_name"] == "Lila Okonkwo"), None)
    assert lila is not None
    assert lila["commitment_date"] == _date(2025, 7, 12)


def test_parse_empty_html_returns_empty_list():
    assert parse_topdrawer_commitments_html("", source_url="https://x") == []
    # Plain page with no tables + no cards parses to empty list.
    assert (
        parse_topdrawer_commitments_html(
            "<html><body><p>nothing here</p></body></html>",
            source_url="https://x",
        )
        == []
    )
