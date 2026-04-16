"""
Tests for the GotSport roster extractor + runner.

Run:
    python -m pytest scraper/tests/test_gotsport_rosters.py -v
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.gotsport_rosters import (  # noqa: E402
    parse_roster_page,
    parse_division_code,
    extract_division_codes,
    _extract_team_roster_links,
)


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "gotsport")


def _read_fixture(name: str) -> str:
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Roster page parsing
# ---------------------------------------------------------------------------

class TestParseRosterPage:

    def test_extracts_players_from_fixture(self):
        html = _read_fixture("sample_roster_page.html")
        players = parse_roster_page(html)
        assert len(players) == 5

    def test_player_fields(self):
        html = _read_fixture("sample_roster_page.html")
        players = parse_roster_page(html)
        first = players[0]
        assert first["player_name"] == "Adriann Agaton"
        assert first["jersey_number"] == "1"
        assert first["position"] == "GK"

    def test_html_entity_decoding(self):
        html = _read_fixture("sample_roster_page.html")
        players = parse_roster_page(html)
        # Ryan O'Brien has &#39; in the fixture.
        obrien = [p for p in players if "Brien" in p["player_name"]]
        assert len(obrien) == 1
        assert obrien[0]["player_name"] == "Ryan O'Brien"

    def test_skips_placeholder_names(self):
        html = """
        <table>
          <tr><th>Name</th><th>#</th></tr>
          <tr><td>TBD</td><td>0</td></tr>
          <tr><td>N/A</td><td>0</td></tr>
          <tr><td>Real Player</td><td>5</td></tr>
        </table>
        """
        players = parse_roster_page(html)
        assert len(players) == 1
        assert players[0]["player_name"] == "Real Player"

    def test_empty_table_returns_empty(self):
        html = "<html><body><table></table></body></html>"
        players = parse_roster_page(html)
        assert players == []

    def test_no_table_returns_empty(self):
        html = "<html><body><p>No roster available</p></body></html>"
        players = parse_roster_page(html)
        assert players == []

    def test_handles_missing_position_column(self):
        html = """
        <table>
          <tr><th>#</th><th>Name</th></tr>
          <tr><td>10</td><td>Solo Player</td></tr>
        </table>
        """
        players = parse_roster_page(html)
        assert len(players) == 1
        assert players[0]["player_name"] == "Solo Player"
        assert players[0]["jersey_number"] == "10"
        assert players[0]["position"] is None


# ---------------------------------------------------------------------------
# Division code parsing
# ---------------------------------------------------------------------------

class TestParseDivisionCode:

    def test_male_u12(self):
        assert parse_division_code("m_12") == ("M", "U12")

    def test_female_u15(self):
        assert parse_division_code("f_15") == ("F", "U15")

    def test_uppercase(self):
        assert parse_division_code("M_14") == ("M", "U14")

    def test_invalid_returns_none(self):
        assert parse_division_code("xyz") == (None, None)
        assert parse_division_code("") == (None, None)


class TestExtractDivisionCodes:

    def test_extracts_from_options(self):
        html = '''
        <select>
          <option value="m_12">Male U12</option>
          <option value="f_15">Female U15</option>
          <option value="m_14">Male U14</option>
        </select>
        '''
        codes = extract_division_codes(html)
        assert codes == ["f_15", "m_12", "m_14"]

    def test_deduplicates(self):
        html = '''
        <select>
          <option value="m_12">Male U12</option>
          <option value="m_12">Male U12 again</option>
        </select>
        '''
        codes = extract_division_codes(html)
        assert codes == ["m_12"]

    def test_no_options_returns_empty(self):
        codes = extract_division_codes("<html><body>no select</body></html>")
        assert codes == []


# ---------------------------------------------------------------------------
# Team roster link extraction
# ---------------------------------------------------------------------------

class TestExtractTeamRosterLinks:

    def test_extracts_links_from_fixture(self):
        html = _read_fixture("sample_division_teams_with_roster_links.html")
        entries = _extract_team_roster_links(html, "99999", "m_15")
        assert len(entries) == 3

        # First two have roster links, third does not.
        club1, team1, url1 = entries[0]
        assert club1 == "Concorde Fire SC"
        assert team1 == "Concorde Fire SC 09 Premier"
        assert url1 == "https://system.gotsport.com/org_event/teams/12345/roster"

        club2, team2, url2 = entries[1]
        assert club2 == "NTH Tophat"
        assert url2 is not None
        assert "/12346/roster" in url2

        club3, team3, url3 = entries[2]
        assert club3 == "Atlanta United"
        assert url3 is None  # no link

    def test_skips_placeholder_teams(self):
        html = """
        <table>
          <tr><th>Club</th><th>Team</th><th>State</th></tr>
          <tr><td>Club A</td><td>TBD</td><td>GA</td></tr>
          <tr><td>Club B</td><td>Real Team</td><td>FL</td></tr>
        </table>
        """
        entries = _extract_team_roster_links(html, "99999", "m_12")
        assert len(entries) == 1
        assert entries[0][1] == "Real Team"


# ---------------------------------------------------------------------------
# Dry-run mode (runner level)
# ---------------------------------------------------------------------------

class TestRunnerDryRun:

    @patch("gotsport_rosters_runner._get_connection")
    @patch("gotsport_rosters_runner.scrape_gotsport_rosters")
    @patch("gotsport_rosters_runner.insert_roster_snapshots")
    def test_dry_run_passes_flag(self, mock_insert, mock_scrape, mock_conn):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from gotsport_rosters_runner import run_gotsport_rosters

        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            (1, "Ev", "55555", "2025-26", None)
        ]
        mock_cur.description = [
            ("id",), ("name",), ("platform_event_id",), ("season",), ("league_name",),
        ]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        conn_obj = MagicMock()
        conn_obj.cursor.return_value = mock_cur
        mock_conn.return_value = conn_obj

        mock_scrape.return_value = [
            {
                "club_name_raw": "Test FC",
                "player_name": "Test Player",
                "snapshot_date": "2026-04-15",
                "event_id": None,
            },
        ]
        mock_insert.return_value = {"inserted": 0, "updated": 0, "skipped": 0, "diffs_written": 0}

        run_gotsport_rosters(dry_run=True)

        mock_insert.assert_called_once()
        assert mock_insert.call_args[1]["dry_run"] is True

    @patch("gotsport_rosters_runner._get_connection")
    @patch("gotsport_rosters_runner.scrape_gotsport_rosters")
    @patch("gotsport_rosters_runner.insert_roster_snapshots")
    def test_stamps_event_db_id(self, mock_insert, mock_scrape, mock_conn):
        """Runner should stamp events.id (DB FK) onto each roster row."""
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from gotsport_rosters_runner import run_gotsport_rosters

        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            (77, "Big Event", "88888", "2025-26", "GA Premier")
        ]
        mock_cur.description = [
            ("id",), ("name",), ("platform_event_id",), ("season",), ("league_name",),
        ]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        conn_obj = MagicMock()
        conn_obj.cursor.return_value = mock_cur
        mock_conn.return_value = conn_obj

        mock_scrape.return_value = [
            {
                "club_name_raw": "Club X",
                "player_name": "Player A",
                "snapshot_date": "2026-04-15",
                "event_id": None,
            },
        ]
        mock_insert.return_value = {"inserted": 1, "updated": 0, "skipped": 0, "diffs_written": 0}

        run_gotsport_rosters(dry_run=False)

        rows_passed = mock_insert.call_args[0][0]
        assert all(r["event_id"] == 77 for r in rows_passed)
