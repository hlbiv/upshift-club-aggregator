"""
Tests for the TopDrawerSoccer transfer-portal extractor.

Extraction is a pure function over fixture HTML; no network, no DB.

The fixture is a trimmed REAL capture of a TDS transfer-tracker article
(see ``scraper/tests/fixtures/topdrawer_transfer_portal_sample.html``).
The original shipped fixture was synthesized from a WebFetch summary
and didn't match live reality — specifically, live TDS tables have no
<thead>/<th>; the header row is a <tr> inside <tbody> with <td> cells
wrapped in <strong>. The test suite now pins that shape.
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


def test_split_position_prefix_nbsp_between_token_and_name():
    """Live TDS inserts &nbsp; between the position token and the
    player name (e.g. "D/F&nbsp;Chloe Bryant"). BeautifulSoup decodes
    &nbsp; to U+00A0; the splitter must still fire because _clean
    normalizes U+00A0 to a regular space."""
    assert split_position_prefix("D/F\u00a0Chloe Bryant\u00a0") == (
        "D/F", "Chloe Bryant",
    )
    assert split_position_prefix("GK\u00a0Kennedy Zorn") == ("GK", "Kennedy Zorn")


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
    # Fixture has 12 data <tr> rows below the td-header row; 2 are
    # malformed (empty player name / empty outgoing college) and must
    # be dropped. Remaining 10 are valid.
    assert len(rows) == 10, f"expected exactly 10 parsed rows, got {len(rows)}"

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

    assert by_name["Olivia Herrera"]["position"] == "GK"
    assert by_name["Hadley Hendrickson"]["position"] == "D"
    assert by_name["Reece Paget"]["position"] == "M/F"
    assert by_name["Aurora Gaines"]["position"] == "F"
    assert by_name["Kelly Gordon"]["position"] == "M"
    assert by_name["Kennedy Zorn"]["position"] == "GK"


def test_parse_fixture_name_cell_with_anchor():
    """Live TDS puts some player names inside an <a> tag (e.g.
    ``<td>D <a>Leah Klurman</a></td>``). get_text() must still yield
    "D Leah Klurman" and the splitter must extract ("D", "Leah Klurman").
    """
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    by_name = {r["player_name"]: r for r in rows}
    assert "Leah Klurman" in by_name
    assert by_name["Leah Klurman"]["position"] == "D"
    assert by_name["Leah Klurman"]["from_college_name_raw"] == "Tennessee"
    assert by_name["Leah Klurman"]["to_college_name_raw"] == "Alabama"

    assert "Madden McDonald" in by_name
    assert by_name["Madden McDonald"]["position"] == "D"


def test_parse_fixture_drops_rows_missing_required_fields():
    """Rows with empty player name OR empty outgoing college must be
    dropped — the natural key and schema both require both."""
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    player_names = {r["player_name"] for r in rows}
    # The two malformed rows in the fixture have names that should NOT
    # appear in the output.
    assert "Someone Else" not in player_names
    for r in rows:
        assert r["player_name"] not in (None, "", " "), r
        assert r["from_college_name_raw"] not in (None, "", " "), r
        assert r["to_college_name_raw"] not in (None, "", " "), r


def test_parse_td_header_row_is_not_emitted_as_data():
    """The td-based header row ("Name" / "Outgoing College" / "Incoming
    College") must be consumed as headers, NOT re-emitted as a data
    row with player_name="Name" etc."""
    html = _read_fixture()
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    for r in rows:
        assert r["player_name"] != "Name", r
        assert r["from_college_name_raw"] != "Outgoing College", r
        assert r["to_college_name_raw"] != "Incoming College", r


def test_parse_table_without_thead_or_th():
    """Regression test for the live-HTML shape: a <table> with no
    <thead> and no <th>, where the header row is <tr><td><strong>Name...
    inside <tbody>. This is the exact shape TDS ships and the shape
    that yielded 0 rows before the fix."""
    html = """
    <html><body>
      <table style="width: 600px;" border="5">
        <tbody>
          <tr>
            <td><strong>Name</strong></td>
            <td><strong>Outgoing College</strong></td>
            <td><strong>Incoming College</strong></td>
          </tr>
          <tr>
            <td>D/F&nbsp;Chloe Bryant</td>
            <td>Grambling State</td>
            <td>Akron</td>
          </tr>
          <tr>
            <td>GK Mira Okafor</td>
            <td>Towson</td>
            <td>Boston College</td>
          </tr>
        </tbody>
      </table>
    </body></html>
    """
    rows = parse_topdrawer_transfer_portal_html(html, source_url="https://x")
    assert len(rows) == 2
    by_name = {r["player_name"]: r for r in rows}
    assert by_name["Chloe Bryant"]["position"] == "D/F"
    assert by_name["Mira Okafor"]["position"] == "GK"


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
    required field keys. This guard is even more important now that
    the parser accepts <td>-based header rows as a fallback: a random
    table whose first <td> row doesn't match the aliases must still
    be skipped."""
    html = """
    <html><body>
      <table>
        <thead><tr><th>Rank</th><th>Team</th><th>Points</th></tr></thead>
        <tbody><tr><td>1</td><td>Stanford</td><td>42</td></tr></tbody>
      </table>
      <table>
        <tbody>
          <tr><td><strong>Rank</strong></td><td><strong>Team</strong></td><td><strong>Points</strong></td></tr>
          <tr><td>1</td><td>Stanford</td><td>42</td></tr>
        </tbody>
      </table>
    </body></html>
    """
    assert parse_topdrawer_transfer_portal_html(html, source_url="https://x") == []
