"""
Tests for the TopDrawerSoccer transfer-portal extractor.

Extraction is a pure function over fixture HTML; no network, no DB.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.topdrawer_transfer_portal import (  # noqa: E402
    parse_topdrawer_transfer_portal_html,
    split_position_prefix,
)


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "topdrawer_transfer_portal_sample.html"
)


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# position-prefix splitter
# ---------------------------------------------------------------------------


def test_split_position_prefix_single_token():
    assert split_position_prefix("D Hadley Hendrickson") == ("D", "Hadley Hendrickson")
    assert split_position_prefix("M Reece Paget") == ("M", "Reece Paget")
    assert split_position_prefix("F Josephine Vance") == ("F", "Josephine Vance")
    assert split_position_prefix("GK Mira Okafor") == ("GK", "Mira Okafor")


def test_split_position_prefix_slash_combined():
    assert split_position_prefix("D/F Chloe Bryant") == ("D/F", "Chloe Bryant")
    assert split_position_prefix("M/F Reece Paget") == ("M/F", "Reece Paget")


def test_split_position_prefix_none_when_no_prefix():
    # Names that don't start with a known token → position=None
    assert split_position_prefix("Priya Ambedkar") == (None, "Priya Ambedkar")
    # A name that STARTS with a letter that is coincidentally a token but
    # without a space delimiter must NOT be treated as a prefix.
    assert split_position_prefix("Dominic Something") == (None, "Dominic Something")


def test_split_position_prefix_empty_input():
    assert split_position_prefix("") == (None, "")
    assert split_position_prefix("   ") == (None, "")


# ---------------------------------------------------------------------------
# page-level parse — core acceptance test
# ---------------------------------------------------------------------------


def test_parse_fixture_extracts_all_valid_rows():
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(
        html,
        source_url=(
            "https://www.topdrawersoccer.com/college-soccer-articles/"
            "2026-womens-division-i-transfer-tracker_aid55352"
        ),
    )
    # Fixture has 9 <tr> rows; 2 are malformed (missing player name or
    # missing outgoing college) and must be dropped. Remaining 7 are
    # valid — 6 with position prefixes + 1 without.
    assert len(rows) == 7, f"expected exactly 7 parsed rows, got {len(rows)}"

    # Every row has the contract fields.
    for r in rows:
        assert r["player_name"], f"missing player_name in row: {r}"
        assert r["from_college_name_raw"], f"missing outgoing in row: {r}"
        assert r["to_college_name_raw"], f"missing incoming in row: {r}"
        assert r["source_url"].startswith(
            "https://www.topdrawersoccer.com/college-soccer-articles/"
        ), r


def test_parse_fixture_position_prefix_split():
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(
        html, source_url="https://x",
    )

    by_name = {r["player_name"]: r for r in rows}

    assert by_name["Chloe Bryant"]["position"] == "D/F"
    assert by_name["Chloe Bryant"]["from_college_name_raw"] == "Grambling State"
    assert by_name["Chloe Bryant"]["to_college_name_raw"] == "Akron"

    assert by_name["Mira Okafor"]["position"] == "GK"
    assert by_name["Hadley Hendrickson"]["position"] == "D"
    assert by_name["Reece Paget"]["position"] == "M/F"
    assert by_name["Josephine Vance"]["position"] == "F"
    assert by_name["Ana Castellanos"]["position"] == "M"


def test_parse_fixture_row_without_position_prefix():
    """A row whose Name cell has no known position token parses with
    position=None and the full name preserved."""
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    priya = next((r for r in rows if r["player_name"] == "Priya Ambedkar"), None)
    assert priya is not None
    assert priya["position"] is None
    assert priya["from_college_name_raw"] == "Oregon State"
    assert priya["to_college_name_raw"] == "Stanford"


def test_parse_fixture_drops_rows_missing_required_fields():
    """Rows with empty player name OR empty outgoing college must be
    dropped — the natural key and schema both require both."""
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    for r in rows:
        assert r["player_name"] not in (None, "", " "), r
        assert r["from_college_name_raw"] not in (None, "", " "), r
        assert r["to_college_name_raw"] not in (None, "", " "), r


def test_parse_empty_html_returns_empty_list():
    assert parse_topdrawer_transfer_portal_html("", source_url="https://x") == []
    # Plain page with no tables parses to empty list.
    assert (
        parse_topdrawer_transfer_portal_html(
            "<html><body><p>nothing here</p></body></html>",
            source_url="https://x",
        )
        == []
    )


def test_parse_ignores_unrelated_tables():
    """A page with a non-transfer-tracker table (e.g. a standings
    table) must be ignored — the header-mapping requires all three
    required field keys."""
    html = """
    <html><body>
      <table>
        <thead><tr><th>Rank</th><th>Team</th><th>Points</th></tr></thead>
        <tbody><tr><td>1</td><td>Stanford</td><td>42</td></tr></tbody>
      </table>
    </body></html>
    """
    assert parse_topdrawer_transfer_portal_html(html, source_url="https://x") == []
