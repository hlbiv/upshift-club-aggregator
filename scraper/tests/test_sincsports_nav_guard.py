"""
Tests for the SincSports legacy extractor's nav-string guard.

Regression coverage for the ``canonical_clubs`` pollution incident
(ids 15479-15483) where the parser walked every <table> on
``TTTeamList.aspx`` and pulled UI strings like "SINC Content Manager",
"Merge Tourneys", "USYS", and a 494-character Display Settings blob into
the canonical clubs table. The hardened parser must:

  1. Only walk <table> elements whose first row contains the headers
     Team / Club / State.
  2. Reject any club name that looks like a nav/UI label even if it
     somehow appears in a Team/Club/State row.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.sincsports import (  # noqa: E402
    _is_nav_string,
    _is_team_table,
    _parse_clubs_from_html,
)
from bs4 import BeautifulSoup  # noqa: E402


def test_is_nav_string_rejects_known_offenders():
    for bad in [
        "SINC Content Manager",
        "Merge Tourneys",
        "USYS",
        "US Club",
        "Us",
        "Display Settings",
        "Default Division",
        "Sort by Team Name",
    ]:
        assert _is_nav_string(bad), f"expected {bad!r} to be flagged"


def test_is_nav_string_rejects_long_blobs():
    blob = "Display Settings" + "x" * 500
    assert _is_nav_string(blob)


def test_is_nav_string_accepts_real_club_names():
    for good in [
        "FC Dallas",
        "Solar SC",
        "Hoover Soccer Club",
        "Mississippi Rush",
        "Real Salt Lake AZ",
    ]:
        assert not _is_nav_string(good), f"expected {good!r} to pass"


def test_is_team_table_requires_team_club_state_headers():
    team_html = """
    <table>
      <tr><th>Team</th><th>Club</th><th>State</th><th>Division</th></tr>
      <tr><td>Solar 12B</td><td>Solar SC</td><td>TX</td><td>Premier</td></tr>
    </table>
    """
    settings_html = """
    <table>
      <tr><td>Default Division:</td><td>Adult</td><td>Sort by Team</td></tr>
      <tr><td>U19</td><td>Boys</td><td>USYS</td></tr>
    </table>
    """
    team_soup = BeautifulSoup(team_html, "lxml").find("table")
    settings_soup = BeautifulSoup(settings_html, "lxml").find("table")
    assert _is_team_table(team_soup) is True
    assert _is_team_table(settings_soup) is False


def test_parser_skips_settings_tables_entirely():
    """Mixed page with one real team table and one settings panel.
    Only the real club must be returned."""
    html = """
    <html><body>
      <table>
        <tr><td>Display Settings</td></tr>
        <tr><td>SINC Content Manager</td><td>Merge Tourneys</td><td>USYS</td></tr>
        <tr><td>Default Division:</td><td>Adult</td><td>US Club</td></tr>
      </table>
      <h2>2017 (U9) Girls Gold</h2>
      <table>
        <tr><th>Team</th><th>Club</th><th>State</th></tr>
        <tr><td>Solar 17G Premier</td><td>Solar SC</td><td>TX</td></tr>
        <tr><td>FC Dallas 17G</td><td>FC Dallas</td><td>TX</td></tr>
      </table>
    </body></html>
    """
    records = _parse_clubs_from_html(html, "https://example/x", "Test Event")
    names = sorted(r["club_name"] for r in records)
    assert names == ["FC Dallas", "Solar SC"]
    # No nav strings made it through.
    for bad in ("SINC Content Manager", "Merge Tourneys", "USYS", "US Club"):
        assert all(bad not in n for n in names)


def test_parser_skips_nav_string_in_team_table_row():
    """Even if a settings string appears inside a Team/Club/State table,
    the secondary nav guard must drop it."""
    html = """
    <table>
      <tr><th>Team</th><th>Club</th><th>State</th></tr>
      <tr><td>Solar 17G</td><td>Solar SC</td><td>TX</td></tr>
      <tr><td>weirdrow</td><td>SINC Content Manager</td><td>AL</td></tr>
    </table>
    """
    records = _parse_clubs_from_html(html, "https://example/x", "Test")
    names = [r["club_name"] for r in records]
    assert names == ["Solar SC"]
