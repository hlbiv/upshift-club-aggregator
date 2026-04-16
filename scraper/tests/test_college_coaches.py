"""
Tests for the NCAA coaching staff scraper.

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

from extractors.college_coaches import (  # noqa: E402
    parse_staff_html,
    looks_like_name,
    is_blocked_title,
    detect_head_coach,
    extract_email,
    scrape_college_coaches,
    CoachEntry,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_staff_html — Sidearm strategy
# ---------------------------------------------------------------------------


class TestParseSidearmStaff:
    """Strategy 1: Sidearm staff cards."""

    def test_extracts_coaches_only(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        # 4 coaches (Smith, Garcia, Chen, Johnson); 3 non-coaches filtered
        assert len(coaches) == 4

    def test_coach_names(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        names = [c.name for c in coaches]
        assert "John Smith" in names
        assert "Maria Garcia" in names
        assert "Alex Chen" in names
        assert "Sarah Johnson" in names

    def test_blocklist_filtered(self):
        """Non-coach titles (Athletic Trainer, Equipment Manager, Video Coordinator)
        should be excluded."""
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        names = [c.name for c in coaches]
        assert "Mike Wilson" not in names
        assert "Lisa Brown" not in names
        assert "Tom Davis" not in names

    def test_head_coach_detection(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        by_name = {c.name: c for c in coaches}
        assert by_name["John Smith"].is_head_coach is True
        assert by_name["Maria Garcia"].is_head_coach is True  # "Associate Head Coach" contains "Head Coach"
        assert by_name["Alex Chen"].is_head_coach is False
        assert by_name["Sarah Johnson"].is_head_coach is False

    def test_email_extraction(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        by_name = {c.name: c for c in coaches}
        assert by_name["John Smith"].email == "jsmith@testuniv.edu"
        assert by_name["Maria Garcia"].email == "mgarcia@testuniv.edu"
        assert by_name["Alex Chen"].email is None
        assert by_name["Sarah Johnson"].email == "sjohnson@testuniv.edu"

    def test_phone_extraction(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        by_name = {c.name: c for c in coaches}
        assert by_name["John Smith"].phone == "(404) 555-1234"
        assert by_name["Maria Garcia"].phone is None

    def test_titles(self):
        html = _read("sample_staff_sidearm.html")
        coaches = parse_staff_html(html, school_name="Test University")
        by_name = {c.name: c for c in coaches}
        assert by_name["John Smith"].title == "Head Coach"
        assert by_name["Maria Garcia"].title == "Associate Head Coach"
        assert by_name["Alex Chen"].title == "Assistant Coach"
        assert by_name["Sarah Johnson"].title == "Goalkeeper Coach"


# ---------------------------------------------------------------------------
# parse_staff_html — generic table strategy
# ---------------------------------------------------------------------------


class TestParseGenericStaff:
    """Strategy 2: table rows."""

    def test_extracts_coaches_only(self):
        html = _read("sample_staff_generic.html")
        coaches = parse_staff_html(html, school_name="Generic University")
        # 3 coaches (Rodriguez, Kim, O'Brien); 2 non-coaches filtered
        assert len(coaches) == 3

    def test_coach_names(self):
        html = _read("sample_staff_generic.html")
        coaches = parse_staff_html(html, school_name="Generic University")
        names = [c.name for c in coaches]
        assert "Emily Rodriguez" in names
        assert "David Kim" in names
        assert "Rachel O'Brien" in names

    def test_blocklist_filtered(self):
        html = _read("sample_staff_generic.html")
        coaches = parse_staff_html(html, school_name="Generic University")
        names = [c.name for c in coaches]
        assert "James Taylor" not in names  # Strength and Conditioning
        assert "Amy Scott" not in names  # Sports Information Director

    def test_head_coach_detection(self):
        html = _read("sample_staff_generic.html")
        coaches = parse_staff_html(html, school_name="Generic University")
        by_name = {c.name: c for c in coaches}
        assert by_name["Emily Rodriguez"].is_head_coach is True
        assert by_name["David Kim"].is_head_coach is False

    def test_email_extraction(self):
        html = _read("sample_staff_generic.html")
        coaches = parse_staff_html(html, school_name="Generic University")
        by_name = {c.name: c for c in coaches}
        assert by_name["Emily Rodriguez"].email == "erodriguez@generic.edu"
        assert by_name["David Kim"].email == "dkim@generic.edu"
        assert by_name["Rachel O'Brien"].email is None


# ---------------------------------------------------------------------------
# Title blocklist
# ---------------------------------------------------------------------------


class TestTitleBlocklist:
    """Verify non-coach titles are detected by the blocklist."""

    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Head Coach", False),
            ("Assistant Coach", False),
            ("Associate Head Coach", False),
            ("Goalkeeper Coach", False),
            ("Volunteer Assistant Coach", False),
            ("Athletic Trainer", True),
            ("Equipment Manager", True),
            ("Strength and Conditioning Coach", True),
            ("Director of Operations", True),
            ("Video Coordinator", True),
            ("Team Manager", True),
            ("Sports Information Director", True),
            ("Academic Advisor", True),
            ("Compliance Officer", True),
            ("Athletic Director", True),
            (None, False),
            ("", False),
        ],
    )
    def test_blocklist(self, title, expected):
        assert is_blocked_title(title) == expected


# ---------------------------------------------------------------------------
# Head coach detection
# ---------------------------------------------------------------------------


class TestHeadCoachDetection:
    """Verify head coach title detection."""

    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Head Coach", True),
            ("Women's Head Coach", True),
            ("Men's Head Soccer Coach", True),
            ("Head Soccer Coach", True),
            ("Director of Soccer", True),
            ("Assistant Coach", False),
            ("Associate Head Coach", True),  # contains "Head Coach"
            ("Goalkeeper Coach", False),
            (None, False),
            ("", False),
        ],
    )
    def test_detection(self, title, expected):
        assert detect_head_coach(title) == expected


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------


class TestEmailExtraction:
    """Verify mailto: link parsing."""

    def test_mailto_link(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="mailto:coach@example.edu">Email</a></div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) == "coach@example.edu"

    def test_mailto_with_params(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="mailto:coach@example.edu?subject=Hello">Email</a></div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) == "coach@example.edu"

    def test_text_email(self):
        from bs4 import BeautifulSoup
        html = '<div>Contact: coach@example.edu for info</div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) == "coach@example.edu"

    def test_no_email(self):
        from bs4 import BeautifulSoup
        html = '<div>No contact info here</div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) is None


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------


class TestLooksLikeName:
    """Verify the name validator filters non-names."""

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("John Smith", True),
            ("Maria Garcia-Lopez", True),
            ("Alex O'Brien", True),
            ("Jane Ann Smith", True),
            # Too short
            ("Jo", False),
            # Single word
            ("Coach", False),
            # All caps
            ("JOHN SMITH", False),
            # Contains digit
            ("John Smith3", False),
            # Blocklisted phrase
            ("Head Coach", False),
            ("Coaching Staff", False),
            # Blocklisted token
            ("Soccer Director", False),
        ],
    )
    def test_name_validation(self, text, expected):
        assert looks_like_name(text) == expected


# ---------------------------------------------------------------------------
# dry_run — no DB writes
# ---------------------------------------------------------------------------


class TestDryRunNoWrites:
    """Verify dry_run=True never calls psycopg2."""

    @mock.patch("extractors.college_coaches.psycopg2", None)
    def test_dry_run_without_db_returns_zero(self):
        """With no DB available, dry_run returns zeros without error."""
        result = scrape_college_coaches(division="D1", dry_run=True)
        assert result["scraped"] == 0
        assert result["rows_inserted"] == 0
        assert result["rows_updated"] == 0

    @mock.patch("extractors.college_coaches._get_connection")
    @mock.patch("extractors.college_coaches.fetch_with_retry")
    @mock.patch("extractors.college_coaches._fetch_colleges")
    @mock.patch("extractors.college_coaches.time.sleep")
    def test_dry_run_parses_but_skips_writes(
        self, mock_sleep, mock_fetch_colleges, mock_fetch_retry, mock_get_conn,
    ):
        """With a mock DB for college list, dry_run parses but never upserts."""
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
                "soccer_program_url": "https://testathletics.com/sports/womens-soccer",
                "gender_program": "womens",
                "last_scraped_at": None,
            }
        ]

        fixture_html = _read("sample_staff_sidearm.html")
        mock_fetch_retry.return_value = fixture_html

        result = scrape_college_coaches(division="D2", gender="womens", limit=1, dry_run=True)

        assert result["scraped"] == 1
        assert result["rows_inserted"] == 0  # dry_run: no writes
        assert result["rows_updated"] == 0
        # Verify no cursor execute calls were made for upserts
        mock_conn.cursor.return_value.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Empty / malformed HTML
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_empty_html_returns_no_coaches(self):
        coaches = parse_staff_html("<html><body></body></html>")
        assert coaches == []

    def test_table_without_names_returns_empty(self):
        html = """
        <table>
          <thead><tr><th>Col A</th><th>Col B</th></tr></thead>
          <tbody><tr><td>foo</td><td>bar</td></tr></tbody>
        </table>
        """
        coaches = parse_staff_html(html)
        assert coaches == []

    def test_count_guard_rejects_large_result_set(self):
        """A table with >15 rows of valid-looking names should be rejected
        as a false positive."""
        rows = ""
        for i in range(20):
            rows += f'<tr><td>Coach Number{i} Person</td><td>Assistant Coach</td></tr>\n'
        html = f"""
        <table>
          <tbody>
            {rows}
          </tbody>
        </table>
        """
        coaches = parse_staff_html(html, school_name="Big Table School")
        assert len(coaches) == 0
