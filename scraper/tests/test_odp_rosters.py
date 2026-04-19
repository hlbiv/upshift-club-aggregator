"""
Tests for the ODP state-association roster extractor.

These tests exercise the pure parsing layer — no network, no DB. A
Cal South fixture is treated as the canonical shape; additional
state fixtures can be added as they're captured.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.odp_rosters import (  # noqa: E402
    PARSERS,
    parse_odp_page,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "odp"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Registry shape
# ---------------------------------------------------------------------------


def test_parsers_registry_ships_top_five_states():
    """Every top-5 state slug referenced in the YAML must be in PARSERS."""
    expected = {"calsouth", "ntxsoccer", "fysa", "enysoccer", "epysa"}
    assert expected.issubset(PARSERS.keys()), (
        f"Missing parsers for: {expected - set(PARSERS.keys())}"
    )


def test_unknown_parser_returns_empty_list():
    """An unknown parser key must degrade gracefully, not raise."""
    rows = parse_odp_page("does-not-exist", "<html></html>")
    assert rows == []


# ---------------------------------------------------------------------------
# CA parser — the canonical fixture
# ---------------------------------------------------------------------------


def test_calsouth_parser_extracts_at_least_five_players():
    html = _read("odp_ca_sample.html")
    rows = parse_odp_page("calsouth", html)
    assert len(rows) >= 5, f"expected >=5 players, got {len(rows)}"


def test_calsouth_parser_populates_required_fields():
    html = _read("odp_ca_sample.html")
    rows = parse_odp_page("calsouth", html)
    for r in rows:
        assert r["player_name"], "player_name must be populated"
        # Optional fields can be None but the keys must exist so the
        # runner can assume the shape.
        assert "graduation_year" in r
        assert "position" in r
        assert "club_name_raw" in r


def test_calsouth_parser_captures_club_and_position():
    html = _read("odp_ca_sample.html")
    rows = parse_odp_page("calsouth", html)
    # At least one row should have a club and a position populated —
    # otherwise the table-column detection broke.
    clubbed = [r for r in rows if r.get("club_name_raw")]
    positioned = [r for r in rows if r.get("position")]
    assert len(clubbed) >= 3, f"expected clubs populated for >=3 rows, got {len(clubbed)}"
    assert len(positioned) >= 3, f"expected positions populated for >=3 rows, got {len(positioned)}"


def test_calsouth_parser_rejects_non_player_rows():
    """Rows like 'TBD' or 'Subtotal: 6 players' must not be emitted
    as player entries — the name heuristic should filter them."""
    html = _read("odp_ca_sample.html")
    rows = parse_odp_page("calsouth", html)
    names = {r["player_name"] for r in rows}
    assert "TBD" not in names
    assert not any("Subtotal" in n for n in names)


def test_calsouth_parser_on_empty_html_returns_empty():
    rows = parse_odp_page("calsouth", "<html><body><h1>No pools yet</h1></body></html>")
    assert rows == []


# ---------------------------------------------------------------------------
# Generic parsers — fixtures inline so we don't need per-state HTML
# files to demonstrate the helper functions work.
# ---------------------------------------------------------------------------


def test_ntxsoccer_parses_dash_separated_lists():
    html = """
    <html><body>
      <h3>Boys 2010</h3>
      <ul>
        <li>Sam Gupta - FC Dallas Academy</li>
        <li>Liam Chen - Solar SC</li>
        <li>Jordan Adeyemi - Dallas Texans</li>
      </ul>
    </body></html>
    """
    rows = parse_odp_page("ntxsoccer", html)
    assert len(rows) == 3
    assert rows[0]["player_name"] == "Sam Gupta"
    assert rows[0]["club_name_raw"] == "FC Dallas Academy"


def test_epysa_parses_paren_club_lists():
    html = """
    <html><body>
      <h2>U15 Boys</h2>
      <ul>
        <li>Oscar Gasga (Philadelphia Union Academy)</li>
        <li>Rockford Martin (FC Delco)</li>
        <li>Iker Lucas-Avila (PA Classics)</li>
      </ul>
    </body></html>
    """
    rows = parse_odp_page("epysa", html)
    assert len(rows) == 3
    assert rows[0]["club_name_raw"] == "Philadelphia Union Academy"
    # Hyphenated last name must survive the name heuristic.
    assert any(r["player_name"] == "Iker Lucas-Avila" for r in rows)


def test_enysoccer_parses_comma_separated_lists():
    html = """
    <html><body>
      <h3>Girls 2008</h3>
      <ul>
        <li>Abigail Gruner, Hudson Valley Premier</li>
        <li>Viviana Haley, World Class FC</li>
        <li>Rose Poller, East Meadow SC</li>
      </ul>
    </body></html>
    """
    rows = parse_odp_page("enysoccer", html)
    assert len(rows) == 3
    assert rows[0]["player_name"] == "Abigail Gruner"
    assert rows[0]["club_name_raw"] == "Hudson Valley Premier"


def test_fysa_parses_html_table_with_grad_year():
    html = """
    <html><body>
      <table>
        <thead><tr><th>Player</th><th>Club</th><th>Position</th><th>Class</th></tr></thead>
        <tbody>
          <tr><td>Ava Martinez</td><td>West Florida Flames</td><td>F</td><td>2008</td></tr>
          <tr><td>Mia Rodriguez</td><td>Weston FC</td><td>M</td><td>2009</td></tr>
          <tr><td>Sophia Patel</td><td>CFC Academy</td><td>D</td><td>'10</td></tr>
        </tbody>
      </table>
    </body></html>
    """
    rows = parse_odp_page("fysa", html)
    assert len(rows) == 3
    grads = {r["player_name"]: r["graduation_year"] for r in rows}
    assert grads["Ava Martinez"] == 2008
    assert grads["Sophia Patel"] == 2010
