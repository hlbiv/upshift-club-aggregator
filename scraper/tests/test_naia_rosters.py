"""
Tests for the NAIA roster scraper.

Extraction tests run against fixture HTML files. DB-write tests stub
psycopg2 to verify dry_run behaviour without Postgres.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

# Ensure scraper package is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_rosters import (  # noqa: E402
    normalize_year,
    parse_roster_html,
)
from extractors.naia_rosters import (  # noqa: E402
    scrape_naia_rosters,
    _discover_sport_shortname,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "naia"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_roster_html — NAIA pages use the same HTML strategies as NCAA
# ---------------------------------------------------------------------------


class TestParseNaiaRoster:
    """Verify NAIA fixture extracts correctly via the shared parser."""

    def test_extracts_all_players(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 4

    def test_player_names(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        names = [p.player_name for p in players]
        assert "Taylor Brooks" in names
        assert "Jordan Rivera" in names
        assert "Morgan Okafor" in names
        assert "Casey Nguyen" in names

    def test_jersey_numbers(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Taylor Brooks"].jersey_number == "5"
        assert by_name["Jordan Rivera"].jersey_number == "11"
        assert by_name["Casey Nguyen"].jersey_number == "1"

    def test_positions(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Taylor Brooks"].position == "MF"
        assert by_name["Jordan Rivera"].position == "FW"
        assert by_name["Morgan Okafor"].position == "DF"
        assert by_name["Casey Nguyen"].position == "GK"

    def test_year_normalized(self):
        """Year normalization is shared with NCAA — verify it works on
        NAIA fixture data too."""
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Taylor Brooks"].year == "junior"
        assert by_name["Jordan Rivera"].year == "freshman"
        assert by_name["Morgan Okafor"].year == "senior"
        assert by_name["Casey Nguyen"].year == "sophomore"

    def test_hometown(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Taylor Brooks"].hometown == "Nashville, Tenn."
        assert by_name["Morgan Okafor"].hometown == "Lagos, Nigeria"

    def test_prev_club(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Taylor Brooks"].prev_club == "Nashville SC Academy"
        assert by_name["Jordan Rivera"].prev_club == "Solar SC"


# ---------------------------------------------------------------------------
# Year normalization — reuse from ncaa_rosters (not duplicated)
# ---------------------------------------------------------------------------


class TestYearNormalizationReuse:
    """Confirm we import normalize_year from ncaa_rosters, not duplicate it."""

    def test_basic_mapping(self):
        assert normalize_year("Fr") == "freshman"
        assert normalize_year("Sr.") == "senior"
        assert normalize_year("Gr") == "grad"

    def test_redshirt(self):
        assert normalize_year("RS-Fr") == "freshman"
        assert normalize_year("R-So") == "sophomore"

    def test_none_passthrough(self):
        assert normalize_year(None) is None
        assert normalize_year("") is None


# ---------------------------------------------------------------------------
# dry_run — no DB writes
# ---------------------------------------------------------------------------


class TestDryRunNoWrites:
    """Verify dry_run=True never calls psycopg2."""

    @mock.patch("extractors.naia_rosters.psycopg2", None)
    @mock.patch("extractors.ncaa_rosters.psycopg2", None)
    def test_dry_run_without_db_returns_zero(self):
        """With no DB available, dry_run returns zeros without error."""
        result = scrape_naia_rosters(dry_run=True)
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0

    @mock.patch("extractors.naia_rosters._get_connection")
    @mock.patch("extractors.naia_rosters.fetch_with_retry")
    @mock.patch("extractors.naia_rosters._fetch_naia_colleges")
    @mock.patch("extractors.naia_rosters.time.sleep")
    def test_dry_run_parses_but_skips_writes(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """With a mock DB for college list, dry_run parses but never upserts."""
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_fetch_colleges.return_value = [
            {
                "id": 1,
                "name": "Campbellsville University",
                "slug": "campbellsville-naia-w",
                "division": "NAIA",
                "conference": "Mid-South",
                "state": "KY",
                "city": "Campbellsville",
                "website": "https://campbellsvilletigers.com",
                "soccer_program_url": "https://campbellsvilletigers.com/sports/womens-soccer/roster",
                "gender_program": "womens",
                "last_scraped_at": None,
            }
        ]

        fixture_html = _read("sample_roster.html")
        mock_fetch_retry.return_value = fixture_html

        result = scrape_naia_rosters(gender="womens", limit=1, dry_run=True)

        assert result["scraped"] == 1
        assert result["rows_inserted"] == 0  # dry_run: no writes
        assert result["rows_updated"] == 0
        # Verify no cursor execute calls were made for upserts
        mock_conn.cursor.return_value.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Sport-name discovery
# ---------------------------------------------------------------------------


class TestSportNameDiscovery:
    """Verify Sidearm sport-name API parsing."""

    def test_finds_womens_soccer(self):
        session = mock.MagicMock()
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.text = '{"sports":[{"sport":"wsoc","sportInfo":{"sport_title":"Women\'s Soccer","sport_abbrev":"WSOC","sport_shortname":"wsoc","roster_id":"1","schedule_id":"1","season_id":"1","global_sport_name":"wsoc"}}]}'
        session.get.return_value = resp

        result = _discover_sport_shortname(session, "https://example.com", "womens")
        assert result == "wsoc"

    def test_finds_mens_soccer(self):
        session = mock.MagicMock()
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.text = '{"sports":[{"sport":"msoc","sportInfo":{"sport_title":"Men\'s Soccer","sport_abbrev":"MSOC","sport_shortname":"msoc","roster_id":"1","schedule_id":"1","season_id":"1","global_sport_name":"msoc"}}]}'
        session.get.return_value = resp

        result = _discover_sport_shortname(session, "https://example.com", "mens")
        assert result == "msoc"

    def test_returns_none_on_404(self):
        session = mock.MagicMock()
        resp = mock.MagicMock()
        resp.status_code = 404
        session.get.return_value = resp

        result = _discover_sport_shortname(session, "https://example.com", "mens")
        assert result is None

    def test_returns_none_on_html_response(self):
        session = mock.MagicMock()
        resp = mock.MagicMock()
        resp.status_code = 200
        resp.text = "<html><body>Not JSON</body></html>"
        session.get.return_value = resp

        result = _discover_sport_shortname(session, "https://example.com", "mens")
        assert result is None
