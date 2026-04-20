"""
Tests for the single-school NCAA D1 roster MVP:

- ``extract_head_coach_from_html`` — SIDEARM staff-block head-coach extractor
- ``scrape_school_url``           — end-to-end parse wrapper
- ``ingest.ncaa_roster_writer``   — upsert SQL shape against a mocked cursor

The SIDEARM fixture used here is modeled on UCLA Men's Soccer
(uclabruins.com/sports/mens-soccer/roster — a real D1 program hosted
on SIDEARM). Player personal details are fictional; structural
markup (class names, element nesting) matches the live site.

Run::

    python -m pytest scraper/tests/test_ncaa_roster_single_school.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_rosters import (  # noqa: E402
    extract_head_coach_from_html,
    parse_roster_html,
    scrape_school_url,
)
from ingest import ncaa_roster_writer  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
UCLA_FIXTURE = FIXTURE_DIR / "ucla_mens_soccer_roster.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# extract_head_coach_from_html
# ---------------------------------------------------------------------------


class TestExtractHeadCoach:
    def test_sidearm_head_coach_extracted(self):
        html = _read(UCLA_FIXTURE)
        coach = extract_head_coach_from_html(html)
        assert coach is not None
        assert coach["name"] == "Ryan Jorden"
        assert coach["title"] == "Head Men's Soccer Coach"
        assert coach["is_head_coach"] is True

    def test_email_pulled_from_mailto(self):
        html = _read(UCLA_FIXTURE)
        coach = extract_head_coach_from_html(html)
        assert coach["email"] == "rjorden@athletics.ucla.edu"

    def test_phone_parsed(self):
        html = _read(UCLA_FIXTURE)
        coach = extract_head_coach_from_html(html)
        # Accept either "(310) 555-0142" or the same trimmed/reformatted.
        assert coach["phone"] is not None
        assert "555" in coach["phone"]
        assert "0142" in coach["phone"]

    def test_no_coach_returns_none(self):
        html = "<html><body><p>no staff here</p></body></html>"
        assert extract_head_coach_from_html(html) is None

    def test_assistant_coach_not_returned(self):
        """Staff blocks without 'Head Coach' in the title are skipped."""
        html = """
        <div class="sidearm-staff-member">
          <div class="sidearm-staff-member-name"><h3>Asst Person</h3></div>
          <div class="sidearm-staff-member-title">Assistant Coach</div>
        </div>
        """
        assert extract_head_coach_from_html(html) is None


# ---------------------------------------------------------------------------
# scrape_school_url — end-to-end (HTTP mocked)
# ---------------------------------------------------------------------------


class TestScrapeSchoolUrl:
    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    def test_parses_fixture_and_returns_structured_dict(self, mock_fetch):
        mock_fetch.return_value = _read(UCLA_FIXTURE)

        result = scrape_school_url(
            "https://uclabruins.com/sports/mens-soccer/roster",
            name="UCLA",
            division="D1",
            gender_program="mens",
            state="CA",
        )

        # Top-level shape
        assert set(result.keys()) >= {
            "college", "players", "coaches", "academic_year", "source_url", "sidearm",
        }
        assert result["sidearm"] is True
        assert result["source_url"] == "https://uclabruins.com/sports/mens-soccer/roster"
        # Academic year looks like "YYYY-YY"
        assert len(result["academic_year"]) == 7 and result["academic_year"][4] == "-"

        # College row ready for upsert
        college = result["college"]
        assert college["name"] == "UCLA"
        assert college["division"] == "D1"
        assert college["gender_program"] == "mens"
        assert college["state"] == "CA"
        # Strips /roster for soccer_program_url
        assert college["soccer_program_url"] == "https://uclabruins.com/sports/mens-soccer"
        assert college["scrape_confidence"] == 0.95  # sidearm-detected

    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    def test_players_extracted(self, mock_fetch):
        mock_fetch.return_value = _read(UCLA_FIXTURE)

        result = scrape_school_url(
            "https://uclabruins.com/sports/mens-soccer/roster",
            name="UCLA",
        )
        players = result["players"]
        assert len(players) == 4
        names = {p.player_name for p in players}
        assert names == {
            "Justin Garces", "Kevin Dilanchyan",
            "Joey Skinner", "Daniel Kuzemka",
        }
        by_name = {p.player_name: p for p in players}
        assert by_name["Justin Garces"].year == "senior"
        assert by_name["Daniel Kuzemka"].year == "freshman"  # RS-Fr → freshman
        assert by_name["Justin Garces"].jersey_number == "1"
        assert by_name["Kevin Dilanchyan"].position == "Midfielder"

    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    def test_head_coach_included(self, mock_fetch):
        mock_fetch.return_value = _read(UCLA_FIXTURE)
        result = scrape_school_url(
            "https://uclabruins.com/sports/mens-soccer/roster",
            name="UCLA",
        )
        assert len(result["coaches"]) == 1
        assert result["coaches"][0]["name"] == "Ryan Jorden"
        assert result["coaches"][0]["is_head_coach"] is True
        assert result["coaches"][0]["source_url"].endswith("/roster")

    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    def test_fetch_failure_raises(self, mock_fetch):
        mock_fetch.return_value = None
        with pytest.raises(RuntimeError, match="failed to fetch"):
            scrape_school_url(
                "https://example.edu/mens-soccer/roster",
                name="Example",
            )

    @mock.patch("extractors.ncaa_rosters.fetch_with_retry")
    def test_zero_players_raises(self, mock_fetch):
        mock_fetch.return_value = "<html><body><p>placeholder</p></body></html>"
        with pytest.raises(RuntimeError, match="parsed 0 players"):
            scrape_school_url(
                "https://example.edu/mens-soccer/roster",
                name="Example",
            )

    def test_invalid_division_rejected(self):
        with pytest.raises(ValueError):
            scrape_school_url(
                "https://example.edu/roster",
                name="Example",
                division="D4",
            )

    def test_invalid_gender_rejected(self):
        with pytest.raises(ValueError):
            scrape_school_url(
                "https://example.edu/roster",
                name="Example",
                gender_program="coed",
            )


# ---------------------------------------------------------------------------
# ingest.ncaa_roster_writer — upsert SQL shape via mocked cursor
# ---------------------------------------------------------------------------


class _FakeCursor:
    """Captures the SQL + params of each `execute` call."""

    def __init__(self, returns_inserted: bool = True, returns_id: int = 42):
        self.calls = []
        self._returns_inserted = returns_inserted
        self._returns_id = returns_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        # The three upserts all RETURN either `(id, inserted)` or `(inserted,)`.
        # Inspect the last SQL to decide which shape to emit.
        last_sql = self.calls[-1][0] if self.calls else ""
        if "RETURNING id" in last_sql:
            return (self._returns_id, self._returns_inserted)
        return (self._returns_inserted,)


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.committed = 0
        self.rolled_back = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed += 1

    def rollback(self):
        self.rolled_back += 1

    def close(self):
        pass


class TestWriterUpsertShape:
    def test_upsert_college_targets_named_constraint(self):
        cur = _FakeCursor(returns_inserted=True, returns_id=42)
        conn = _FakeConn(cur)
        college = {
            "name": "UCLA",
            "division": "D1",
            "gender_program": "mens",
            "state": "CA",
            "website": "https://uclabruins.com",
            "soccer_program_url": "https://uclabruins.com/sports/mens-soccer",
        }
        college_id, inserted = ncaa_roster_writer.upsert_college(
            college, conn=conn, dry_run=False,
        )
        assert college_id == 42
        assert inserted is True
        assert len(cur.calls) == 1
        sql, params = cur.calls[0]
        assert "INSERT INTO colleges" in sql
        assert "ON CONFLICT ON CONSTRAINT colleges_name_division_gender_uq" in sql
        assert "RETURNING id, (xmax = 0) AS inserted" in sql
        # Slug auto-generated when not supplied.
        assert params["slug"] == "ucla-d1-mens"
        assert params["scrape_confidence"] == 0.9  # default

    def test_upsert_college_rejects_bad_division(self):
        conn = _FakeConn(_FakeCursor())
        with pytest.raises(ValueError, match="invalid division"):
            ncaa_roster_writer.upsert_college(
                {"name": "X", "division": "D4", "gender_program": "mens"},
                conn=conn,
            )

    def test_upsert_college_rejects_bad_gender(self):
        conn = _FakeConn(_FakeCursor())
        with pytest.raises(ValueError, match="invalid gender_program"):
            ncaa_roster_writer.upsert_college(
                {"name": "X", "division": "D1", "gender_program": "coed"},
                conn=conn,
            )

    def test_upsert_coaches_uses_named_constraint(self):
        cur = _FakeCursor(returns_inserted=True)
        conn = _FakeConn(cur)
        counts = ncaa_roster_writer.upsert_coaches(
            [
                {"name": "Ryan Jorden", "title": "Head Men's Soccer Coach",
                 "is_head_coach": True, "source_url": "https://uclabruins.com/r"},
                {"name": "Matthew Taylor", "title": "Assistant Coach"},
            ],
            college_id=42,
            conn=conn,
            dry_run=False,
        )
        assert counts == {"inserted": 2, "updated": 0, "skipped": 0}
        assert len(cur.calls) == 2
        for sql, _ in cur.calls:
            assert "INSERT INTO college_coaches" in sql
            assert "ON CONFLICT ON CONSTRAINT college_coaches_college_name_title_uq" in sql

    def test_upsert_roster_players_uses_named_constraint(self):
        cur = _FakeCursor(returns_inserted=True)
        conn = _FakeConn(cur)
        players = [
            {"player_name": "Justin Garces", "position": "Goalkeeper",
             "year": "senior", "jersey_number": "1", "hometown": "Cerritos, Calif."},
            {"player_name": "Daniel Kuzemka", "position": "Defender",
             "year": "freshman", "jersey_number": "22"},
        ]
        counts = ncaa_roster_writer.upsert_roster_players(
            players,
            college_id=42,
            academic_year="2025-26",
            conn=conn,
            dry_run=False,
        )
        assert counts == {"inserted": 2, "updated": 0, "skipped": 0}
        assert len(cur.calls) == 2
        for sql, params in cur.calls:
            assert "INSERT INTO college_roster_history" in sql
            assert "ON CONFLICT ON CONSTRAINT college_roster_history_college_player_year_uq" in sql
            assert params["college_id"] == 42
            assert params["academic_year"] == "2025-26"

    def test_upsert_roster_skips_rows_missing_player_name(self):
        cur = _FakeCursor(returns_inserted=True)
        conn = _FakeConn(cur)
        counts = ncaa_roster_writer.upsert_roster_players(
            [
                {"player_name": "Justin Garces"},
                {"player_name": ""},  # skipped
                {"position": "MF"},   # skipped — no name
            ],
            college_id=42,
            academic_year="2025-26",
            conn=conn,
            dry_run=False,
        )
        assert counts["skipped"] == 2
        assert counts["inserted"] == 1

    def test_dry_run_skips_all_sql(self):
        cur = _FakeCursor()
        conn = _FakeConn(cur)
        # college
        college_id, inserted = ncaa_roster_writer.upsert_college(
            {"name": "X", "division": "D1", "gender_program": "mens"},
            conn=conn, dry_run=True,
        )
        assert college_id is None and inserted is False
        # coaches
        counts = ncaa_roster_writer.upsert_coaches(
            [{"name": "Y"}], college_id=1, conn=conn, dry_run=True,
        )
        assert counts == {"inserted": 0, "updated": 0, "skipped": 0}
        # players
        counts = ncaa_roster_writer.upsert_roster_players(
            [{"player_name": "Z"}], college_id=1, academic_year="2025-26",
            conn=conn, dry_run=True,
        )
        assert counts == {"inserted": 0, "updated": 0, "skipped": 0}
        assert cur.calls == []  # nothing touched the DB


# ---------------------------------------------------------------------------
# slugify helper
# ---------------------------------------------------------------------------


class TestSlugify:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("UCLA", "ucla"),
            ("North Carolina", "north-carolina"),
            ("Saint Mary's (CA)", "saint-mary-s-ca"),
            ("  UC Santa Barbara  ", "uc-santa-barbara"),
        ],
    )
    def test_slugify(self, raw, expected):
        assert ncaa_roster_writer.slugify(raw) == expected
