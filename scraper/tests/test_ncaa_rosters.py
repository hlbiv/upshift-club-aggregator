"""
Tests for the NCAA roster scraper.

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
    build_column_index,
    ColumnIndex,
    current_academic_year,
    scrape_college_rosters,
    _fetch_colleges,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_roster_html — table strategy
# ---------------------------------------------------------------------------


class TestParseRosterTable:
    """Strategy 2: header-aware <table> parsing."""

    def test_extracts_all_players(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 6

    def test_player_names(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        names = [p.player_name for p in players]
        assert "Emma Johnson" in names
        assert "Sofia Martinez" in names
        assert "Aisha Williams" in names
        assert "Riley O'Connor" in names
        assert "Mei Chen" in names
        assert "Daniela Ruiz-Fernandez" in names

    def test_jersey_numbers(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].jersey_number == "1"
        assert by_name["Sofia Martinez"].jersey_number == "7"
        assert by_name["Mei Chen"].jersey_number == "22"

    def test_positions(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].position == "GK"
        assert by_name["Sofia Martinez"].position == "MF"
        assert by_name["Aisha Williams"].position == "FW"
        assert by_name["Riley O'Connor"].position == "DF"

    def test_year_normalized(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].year == "senior"
        assert by_name["Sofia Martinez"].year == "junior"
        assert by_name["Aisha Williams"].year == "freshman"
        assert by_name["Riley O'Connor"].year == "freshman"  # RS-Fr → freshman
        assert by_name["Mei Chen"].year == "grad"
        assert by_name["Daniela Ruiz-Fernandez"].year == "sophomore"

    def test_hometown(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].hometown == "Alpharetta, Ga."
        assert by_name["Aisha Williams"].hometown == "Manchester, England"

    def test_prev_club(self):
        html = _read("sample_roster.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        assert by_name["Emma Johnson"].prev_club == "Concorde Fire"
        assert by_name["Sofia Martinez"].prev_club == "Orlando City YS"


# ---------------------------------------------------------------------------
# parse_roster_html — Sidearm strategy
# ---------------------------------------------------------------------------


class TestParseSidearmRoster:
    """Strategy 1: Sidearm roster elements."""

    def test_extracts_sidearm_players(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 2

    def test_sidearm_fields(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        marcus = players[0]
        assert marcus.player_name == "Marcus Thompson"
        assert marcus.jersey_number == "9"
        assert marcus.position == "Forward"
        assert marcus.year == "junior"
        assert marcus.hometown == "Atlanta, Ga"
        assert marcus.prev_club == "Atlanta United Academy"

    def test_sidearm_redshirt(self):
        html = _read("sidearm_roster.html")
        players = parse_roster_html(html)
        javier = players[1]
        assert javier.player_name == "Javier Lopez"
        assert javier.year == "sophomore"  # R-So → sophomore


# ---------------------------------------------------------------------------
# parse_roster_html — card strategy
# ---------------------------------------------------------------------------


class TestParseCardRoster:
    """Strategy 3: card/div layout."""

    def test_extracts_card_players(self):
        html = _read("card_roster.html")
        players = parse_roster_html(html)
        assert len(players) == 2

    def test_card_fields(self):
        html = _read("card_roster.html")
        players = parse_roster_html(html)
        kwame = players[0]
        assert kwame.player_name == "Kwame Asante"
        assert kwame.jersey_number == "11"
        assert kwame.position == "MF"
        assert kwame.year == "grad"  # 5th → grad
        assert kwame.hometown == "Accra, Ghana"


# ---------------------------------------------------------------------------
# parse_roster_html — Sidearm Vue-embedded JSON strategy
# ---------------------------------------------------------------------------


class TestParseSidearmVueEmbeddedJson:
    """Strategy 5: Sidearm classic sites whose roster <li> elements are
    never rendered server-side, but ship the full player list inline
    inside a ``new Vue({ data: () => ({ roster: {...} }) })`` block.

    Fixture is a trimmed real capture from gomason.com (George Mason
    men's soccer) — one of the D1 programs where both the static
    SIDEARM-card parser AND the Playwright fallback returned 0 players
    in production before this strategy existed.
    """

    def test_extracts_at_least_ten_players(self):
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        assert len(players) >= 10, (
            f"expected >=10 players from Vue-embedded JSON, got {len(players)}"
        )

    def test_first_player_fields(self):
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # John Balkey is the first player in the George Mason roster JSON
        assert "John Balkey" in by_name
        balkey = by_name["John Balkey"]
        assert balkey.jersey_number == "12"
        assert balkey.position == "M"
        assert balkey.year == "sophomore"  # "So." normalizes to sophomore
        assert balkey.hometown == "Leesburg, Va"

    def test_prev_club_falls_back_to_highschool(self):
        """When ``previous_school`` is blank, the parser uses ``highschool``."""
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # Balkey has previous_school="" and highschool="Riverside High School"
        # — the parser should prefer previous_school but fall through when empty.
        assert by_name["John Balkey"].prev_club == "Riverside High School"

    def test_redshirt_year_normalized(self):
        """RS-year formats in ``academic_year_short`` map to the base enum."""
        html = _read("sidearm_vue_embedded_sample.html")
        players = parse_roster_html(html)
        by_name = {p.player_name: p for p in players}
        # Jack Desroches is R-Sr. in the source JSON — should normalize to "senior"
        assert by_name["Jack Desroches"].year == "senior"


# ---------------------------------------------------------------------------
# normalize_year — exhaustive edge cases
# ---------------------------------------------------------------------------


class TestYearNormalization:
    """Verify all year/class variants map correctly."""

    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("Fr", "freshman"),
            ("Fr.", "freshman"),
            ("Freshman", "freshman"),
            ("fr", "freshman"),
            ("So", "sophomore"),
            ("So.", "sophomore"),
            ("Sophomore", "sophomore"),
            ("Jr", "junior"),
            ("Jr.", "junior"),
            ("Junior", "junior"),
            ("Sr", "senior"),
            ("Sr.", "senior"),
            ("Senior", "senior"),
            ("Gr", "grad"),
            ("Gr.", "grad"),
            ("Grad", "grad"),
            ("Graduate", "grad"),
            ("5th", "grad"),
            # Redshirt variants
            ("RS-Fr", "freshman"),
            ("RS-Fr.", "freshman"),
            ("R-Fr", "freshman"),
            ("RS Fr", "freshman"),
            ("R-So", "sophomore"),
            ("RS-So", "sophomore"),
            ("R-Jr", "junior"),
            ("RS-Jr", "junior"),
            ("R-Sr", "senior"),
            ("RS-Sr", "senior"),
            # Edge cases
            (None, None),
            ("", None),
            ("  Fr.  ", "freshman"),
            ("SENIOR", "senior"),  # lowercased to "senior" which is a known key
        ],
    )
    def test_normalize(self, raw, expected):
        assert normalize_year(raw) == expected


# ---------------------------------------------------------------------------
# build_column_index
# ---------------------------------------------------------------------------


class TestBuildColumnIndex:
    def test_standard_headers(self):
        headers = ["No.", "Name", "Pos.", "Yr.", "Ht.", "Hometown / High School", "Previous School"]
        idx = build_column_index(headers)
        assert idx.jersey_number == 0
        assert idx.player_name == 1
        assert idx.position == 2
        assert idx.class_year == 3
        assert idx.height == 4
        assert idx.hometown == 5
        assert idx.high_school == 6

    def test_alternate_headers(self):
        headers = ["#", "Player", "Position", "Class", "Height", "From", "HS"]
        idx = build_column_index(headers)
        assert idx.jersey_number == 0
        assert idx.player_name == 1
        assert idx.position == 2
        assert idx.class_year == 3
        assert idx.hometown == 5
        assert idx.high_school == 6

    def test_missing_columns(self):
        headers = ["Name", "Position"]
        idx = build_column_index(headers)
        assert idx.player_name == 0
        assert idx.position == 1
        assert idx.jersey_number is None
        assert idx.class_year is None
        assert idx.hometown is None


# ---------------------------------------------------------------------------
# current_academic_year
# ---------------------------------------------------------------------------


class TestCurrentAcademicYear:
    def test_format(self):
        year = current_academic_year()
        # Should be like "2025-26"
        assert len(year) == 7
        assert year[4] == "-"
        assert year[:4].isdigit()
        assert year[5:].isdigit()


# ---------------------------------------------------------------------------
# dry_run — no DB writes
# ---------------------------------------------------------------------------


class TestDryRunNoWrites:
    """Verify dry_run=True never calls psycopg2."""

    @mock.patch("extractors.ncaa_rosters.psycopg2", None)
    def test_dry_run_without_db_returns_zero(self):
        """With no DB available, dry_run returns zeros without error."""
        result = scrape_college_rosters(division="D1", dry_run=True)
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0

    @mock.patch("extractors.ncaa_rosters._get_connection")
    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    @mock.patch("extractors.ncaa_rosters._fetch_colleges")
    @mock.patch("extractors.ncaa_rosters.time.sleep")
    def test_dry_run_parses_but_skips_writes(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """With a mock DB for college list, dry_run parses but never upserts."""
        # Provide a fake college list
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_fetch_colleges.return_value = [
            {
                "id": 1,
                "name": "Test University",
                "slug": "test-university",
                "division": "D2",
                "conference": "Test Conf",
                "state": "GA",
                "city": "Atlanta",
                "website": "https://testathletics.com",
                "soccer_program_url": "https://testathletics.com/sports/womens-soccer/roster",
                "gender_program": "womens",
                "last_scraped_at": None,
            }
        ]

        # Return fixture HTML when roster URL is fetched
        fixture_html = _read("sample_roster.html")
        mock_fetch_retry.return_value = fixture_html

        result = scrape_college_rosters(division="D2", gender="womens", limit=1, dry_run=True)

        assert result["scraped"] == 1
        assert result["rows_inserted"] == 0  # dry_run: no writes
        assert result["rows_updated"] == 0
        # Verify no cursor execute calls were made for upserts
        mock_conn.cursor.return_value.execute.assert_not_called()


# ---------------------------------------------------------------------------
# skip_unresolved — don't fetch rosters for schools with no URL for the
# requested gender.
# ---------------------------------------------------------------------------


class TestSkipUnresolvedColleges:
    """Regression guard: a ``colleges`` row with NULL ``soccer_program_url``
    means the PR-2 resolver probed every SIDEARM candidate path and found
    none responding — i.e. the school doesn't field that sport. The runner
    must NEVER issue an HTTP fetch for those rows; doing so burns ~6s per
    school on static-fetch + Playwright fallback before SKIPping with
    "no players parsed", which both wastes time and inflates error counts
    into looking like parser bugs.

    Schools hit by this in production: Minnesota, Oregon, USC (all women's
    only; seeded as mens+womens in ``seed_colleges.py`` because they're
    Big Ten members). See PR description for full list.
    """

    def test_fetch_colleges_sql_includes_not_null_clause(self):
        """Default query must filter out rows with NULL soccer_program_url."""
        fake_cursor = mock.MagicMock()
        fake_cursor.description = [
            ("id",), ("name",), ("slug",), ("division",), ("conference",),
            ("state",), ("city",), ("website",), ("soccer_program_url",),
            ("gender_program",), ("last_scraped_at",),
        ]
        fake_cursor.fetchall.return_value = []
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

        _fetch_colleges(fake_conn, division="D1", gender="mens")

        # Inspect the SQL that was executed
        sql = fake_cursor.execute.call_args[0][0]
        assert "soccer_program_url IS NOT NULL" in sql
        # Keep the other filters intact
        assert "division = %s" in sql
        assert "gender_program = %s" in sql

    def test_fetch_colleges_opt_out_drops_the_clause(self):
        """Passing skip_unresolved=False must omit the IS NOT NULL filter
        so debug / audit callers still see every seeded row.
        """
        fake_cursor = mock.MagicMock()
        fake_cursor.description = [
            ("id",), ("name",), ("slug",), ("division",), ("conference",),
            ("state",), ("city",), ("website",), ("soccer_program_url",),
            ("gender_program",), ("last_scraped_at",),
        ]
        fake_cursor.fetchall.return_value = []
        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

        _fetch_colleges(fake_conn, division="D1", gender="mens",
                        skip_unresolved=False)

        sql = fake_cursor.execute.call_args[0][0]
        assert "soccer_program_url IS NOT NULL" not in sql

    @mock.patch("extractors.ncaa_rosters._get_connection")
    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    @mock.patch("extractors.ncaa_rosters._fetch_colleges")
    @mock.patch("extractors.ncaa_rosters.time.sleep")
    def test_fetch_never_called_for_unresolved_college(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """End-to-end: if ``_fetch_colleges`` returns only rows with real
        URLs (which is what skip_unresolved=True guarantees), no HTTP
        fetch ever targets the ``website`` base of a NULL-URL school.

        The critical assertion: ``fetch_with_retry`` is NEVER called with
        any URL under the 'no-mens-program' school's hostname. This is
        the regression-guard contract for the skip-missing-gender fix.
        """
        # Simulate the post-filter list: only the resolved school is
        # present. Minnesota (no men's program) is absent because its
        # soccer_program_url is NULL → filtered out by _fetch_colleges.
        mock_conn = mock.MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_fetch_colleges.return_value = [
            {
                "id": 1,
                "name": "Georgetown University",
                "slug": "georgetown-d1-m",
                "division": "D1",
                "conference": "Big East",
                "state": "DC",
                "city": "Washington",
                "website": "https://guhoyas.com",
                "soccer_program_url": "https://guhoyas.com/sports/mens-soccer/roster",
                "gender_program": "mens",
                "last_scraped_at": None,
            },
        ]
        # Return a trivial 'no players' HTML so the parser path executes
        # but produces 0 — the call-tracking assertion is what matters.
        mock_fetch_retry.return_value = "<html><body></body></html>"

        scrape_college_rosters(division="D1", gender="mens", dry_run=True)

        # Verify _fetch_colleges was called with skip_unresolved=True
        mock_fetch_colleges.assert_called_once()
        call_kwargs = mock_fetch_colleges.call_args.kwargs
        assert call_kwargs.get("skip_unresolved") is True, (
            "scrape_college_rosters must default to skip_unresolved=True"
        )

        # Collect every URL that fetch_with_retry was asked to fetch.
        fetched_urls = [
            call.args[1] if len(call.args) >= 2 else call.kwargs.get("url")
            for call in mock_fetch_retry.call_args_list
        ]
        # The KEY assertion: no fetch ever targeted a school that wasn't
        # in the post-filter list. Minnesota's base host must not appear.
        minnesota_hosts = ("gophersports.com",)
        oregon_hosts = ("goducks.com",)
        usc_hosts = ("usctrojans.com",)
        for url in fetched_urls:
            if url is None:
                continue
            for host in minnesota_hosts + oregon_hosts + usc_hosts:
                assert host not in url, (
                    f"fetch_with_retry was called for {host!r} "
                    f"(URL: {url!r}) — this host has no men's soccer "
                    f"program and should have been filtered out at "
                    f"enumeration time by skip_unresolved=True"
                )


# ---------------------------------------------------------------------------
# Empty / malformed HTML
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_html_returns_no_players(self):
        players = parse_roster_html("<html><body></body></html>")
        assert players == []

    def test_table_without_name_header_skipped(self):
        html = """
        <table>
          <thead><tr><th>Col A</th><th>Col B</th></tr></thead>
          <tbody><tr><td>foo</td><td>bar</td></tr></tbody>
        </table>
        """
        players = parse_roster_html(html)
        assert players == []

    def test_short_names_filtered(self):
        html = """
        <table>
          <thead><tr><th>Name</th><th>Pos</th></tr></thead>
          <tbody>
            <tr><td>A</td><td>GK</td></tr>
            <tr><td>Jo Smith</td><td>MF</td></tr>
          </tbody>
        </table>
        """
        players = parse_roster_html(html)
        assert len(players) == 1
        assert players[0].player_name == "Jo Smith"
