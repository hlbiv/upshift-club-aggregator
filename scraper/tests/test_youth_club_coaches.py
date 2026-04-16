"""
Tests for the youth club coaching staff scraper.

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

from extractors.youth_club_coaches import (  # noqa: E402
    parse_staff_html,
    looks_like_name,
    is_blocked_title,
    detect_head_coach,
    extract_email,
    extract_phone,
    detect_platform,
    get_staff_paths,
    discover_staff_url,
    CoachEntry,
    PARSE_COUNT_GUARD,
    SPORTSENGINE_PATHS,
    GENERIC_STAFF_PATHS,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "youth"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# parse_staff_html — card strategy
# ---------------------------------------------------------------------------


class TestParseStaffCards:
    """Strategy 1: Card-style staff members."""

    def test_extracts_coaches_only(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        # 4 coaches: Bertolini, Vasquez, O'Brien, Moreno-Cruz
        # 5 non-coaches filtered: Patterson, Rivera, Foster, Nguyen, Williams
        assert len(coaches) == 4

    def test_coach_names(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        names = [c.name for c in coaches]
        assert "Marco Bertolini" in names
        assert "Elena Vasquez" in names
        assert "James O'Brien" in names
        assert "Daniela Moreno-Cruz" in names

    def test_blocklist_filtered(self):
        """Non-coach titles should be excluded."""
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        names = [c.name for c in coaches]
        assert "Chris Patterson" not in names   # Athletic Trainer
        assert "Angela Rivera" not in names      # Equipment Manager
        assert "Brian Foster" not in names       # Video Coordinator
        assert "David Nguyen" not in names       # Groundskeeper
        assert "Sarah Williams" not in names     # Team Manager

    def test_head_coach_detection(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        by_name = {c.name: c for c in coaches}
        assert by_name["Marco Bertolini"].is_head_coach is True   # "Director of Soccer"
        assert by_name["Elena Vasquez"].is_head_coach is False
        assert by_name["James O'Brien"].is_head_coach is False
        assert by_name["Daniela Moreno-Cruz"].is_head_coach is False

    def test_email_extraction(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        by_name = {c.name: c for c in coaches}
        assert by_name["Marco Bertolini"].email == "marco@concordefire.com"
        assert by_name["Elena Vasquez"].email == "elena@concordefire.com"
        assert by_name["James O'Brien"].email is None
        assert by_name["Daniela Moreno-Cruz"].email == "daniela@concordefire.com"

    def test_phone_extraction(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        by_name = {c.name: c for c in coaches}
        assert by_name["Marco Bertolini"].phone is not None
        assert "404" in by_name["Marco Bertolini"].phone
        assert by_name["Daniela Moreno-Cruz"].phone is not None
        assert "678" in by_name["Daniela Moreno-Cruz"].phone
        assert by_name["Elena Vasquez"].phone is None

    def test_titles_extracted(self):
        html = _read("sample_staff_page.html")
        coaches = parse_staff_html(html, club_name="Concorde Fire SC")
        by_name = {c.name: c for c in coaches}
        assert by_name["Marco Bertolini"].title == "Head Coach / Director of Soccer"
        assert by_name["Elena Vasquez"].title == "Assistant Coach"
        assert by_name["James O'Brien"].title == "Goalkeeper Coach"
        assert by_name["Daniela Moreno-Cruz"].title == "Technical Director"


# ---------------------------------------------------------------------------
# parse_staff_html — table strategy
# ---------------------------------------------------------------------------


class TestParseStaffTable:
    """Strategy 2: Table-based staff page."""

    def test_table_extraction(self):
        html = """
        <html><body>
        <table>
          <tr><td>Sarah Mitchell</td><td>Head Coach</td><td>sarah@club.com</td></tr>
          <tr><td>David Lee</td><td>Assistant Coach</td><td></td></tr>
          <tr><td>Tom Baker</td><td>Athletic Trainer</td><td></td></tr>
        </table>
        </body></html>
        """
        coaches = parse_staff_html(html, club_name="Test Club")
        assert len(coaches) == 2
        names = [c.name for c in coaches]
        assert "Sarah Mitchell" in names
        assert "David Lee" in names
        assert "Tom Baker" not in names  # Athletic Trainer blocked

    def test_table_head_coach_detected(self):
        html = """
        <html><body>
        <table>
          <tr><td>Sarah Mitchell</td><td>Head Coach</td></tr>
          <tr><td>David Lee</td><td>Assistant Coach</td></tr>
        </table>
        </body></html>
        """
        coaches = parse_staff_html(html, club_name="Test Club")
        by_name = {c.name: c for c in coaches}
        assert by_name["Sarah Mitchell"].is_head_coach is True
        assert by_name["David Lee"].is_head_coach is False


# ---------------------------------------------------------------------------
# Name validation
# ---------------------------------------------------------------------------


class TestLooksLikeName:
    def test_valid_names(self):
        assert looks_like_name("John Smith") is True
        assert looks_like_name("Maria Garcia-Lopez") is True
        assert looks_like_name("James O'Brien") is True
        assert looks_like_name("Anna Marie Chen") is True
        assert looks_like_name("Li Wei Zhang") is True

    def test_too_short(self):
        assert looks_like_name("Jo") is False
        assert looks_like_name("") is False

    def test_single_word(self):
        assert looks_like_name("Madonna") is False

    def test_too_many_words(self):
        assert looks_like_name("One Two Three Four Five") is False

    def test_digits(self):
        assert looks_like_name("John Smith3") is False
        assert looks_like_name("Player 123") is False

    def test_all_caps(self):
        assert looks_like_name("JOHN SMITH") is False

    def test_starts_lowercase(self):
        assert looks_like_name("john Smith") is False

    def test_blocklist_phrases(self):
        assert looks_like_name("About Us") is False
        assert looks_like_name("Our Team") is False
        assert looks_like_name("Head Coach") is False

    def test_blocklist_tokens(self):
        assert looks_like_name("Soccer Coach") is False
        assert looks_like_name("Staff Director") is False
        assert looks_like_name("Club Director") is False


# ---------------------------------------------------------------------------
# Title blocklist
# ---------------------------------------------------------------------------


class TestTitleBlocklist:
    def test_blocked_titles(self):
        assert is_blocked_title("Athletic Trainer") is True
        assert is_blocked_title("Equipment Manager") is True
        assert is_blocked_title("Video Coordinator") is True
        assert is_blocked_title("Team Manager") is True
        assert is_blocked_title("Sports Information Director") is True
        assert is_blocked_title("Photographer") is True
        assert is_blocked_title("Groundskeeper") is True
        assert is_blocked_title("Strength & Conditioning Coach") is True
        assert is_blocked_title("Facilities Manager") is True

    def test_allowed_titles(self):
        assert is_blocked_title("Head Coach") is False
        assert is_blocked_title("Assistant Coach") is False
        assert is_blocked_title("Goalkeeper Coach") is False
        assert is_blocked_title("Technical Director") is False
        assert is_blocked_title("Director of Soccer") is False
        assert is_blocked_title("U14 Head Coach") is False
        assert is_blocked_title(None) is False


# ---------------------------------------------------------------------------
# Head coach detection
# ---------------------------------------------------------------------------


class TestHeadCoachDetection:
    def test_head_coach_variants(self):
        assert detect_head_coach("Head Coach") is True
        assert detect_head_coach("Head Soccer Coach") is True
        assert detect_head_coach("Director of Soccer") is True
        assert detect_head_coach("head coach / boys program") is True

    def test_not_head_coach(self):
        assert detect_head_coach("Assistant Coach") is False
        assert detect_head_coach("Goalkeeper Coach") is False
        assert detect_head_coach("Technical Director") is False
        assert detect_head_coach(None) is False
        assert detect_head_coach("") is False


# ---------------------------------------------------------------------------
# Parse count guard
# ---------------------------------------------------------------------------


class TestParseCountGuard:
    def test_count_guard_skips_large_results(self):
        """Pages with >15 entries per selector are treated as false positives."""
        # Build an HTML page with 20 staff-member cards
        cards = []
        for i in range(20):
            first = f"Person{chr(65 + i % 26)}"
            last = f"Lastname{i}"
            cards.append(f"""
            <div class="staff-member">
                <h3>{first} {last}</h3>
                <div class="staff-title">Coach</div>
            </div>
            """)
        html = f"<html><body>{''.join(cards)}</body></html>"
        coaches = parse_staff_html(html, club_name="Overflow Club")
        # Count guard should reject all 20 from the card strategy.
        # The names contain blocklist token "Coach" in "Coach" title — but
        # the guard fires before individual filtering, so the net result
        # depends on the strategy. With 20 matching the selector, strategy 1
        # skips. Name validation will filter some in subsequent strategies.
        # What matters: we do NOT get 20 results back.
        assert len(coaches) <= PARSE_COUNT_GUARD


# ---------------------------------------------------------------------------
# Platform detection + URL paths
# ---------------------------------------------------------------------------


class TestPlatformDetection:
    def test_sportsengine_url(self):
        assert detect_platform("https://myclub.sportsengine.com") == "sportsengine"
        assert detect_platform("https://club.sportngin.com/page") == "sportsengine"

    def test_leagueapps_url(self):
        assert detect_platform("https://myclub.leagueapps.com") == "leagueapps"

    def test_wordpress_html(self):
        assert detect_platform("https://myclub.com", "<html>wp-content themes</html>") == "wordpress"

    def test_unknown(self):
        assert detect_platform("https://myclub.com") == "unknown"

    def test_sportsengine_paths(self):
        paths = get_staff_paths("sportsengine")
        assert paths == SPORTSENGINE_PATHS

    def test_generic_paths(self):
        paths = get_staff_paths("unknown")
        assert paths == GENERIC_STAFF_PATHS
        paths2 = get_staff_paths("wordpress")
        assert paths2 == GENERIC_STAFF_PATHS


# ---------------------------------------------------------------------------
# Confidence assignment
# ---------------------------------------------------------------------------


class TestConfidence:
    def test_platform_detected_confidence(self):
        """Platform-detected pages should get 0.90 confidence."""
        # We test this via discover_staff_url which returns (url, confidence)
        # Use a mock session that returns HTML for the first path
        session = mock.MagicMock()
        response = mock.MagicMock()
        response.status_code = 200
        response.text = "<html>" + "x" * 600 + "</html>"
        response.headers = {"content-type": "text/html"}
        session.get.return_value = response

        result = discover_staff_url(session, "https://myclub.sportsengine.com", "sportsengine")
        assert result is not None
        url, confidence = result
        assert confidence == 0.90

    def test_generic_confidence(self):
        """Generic/unknown platform pages should get 0.75 confidence."""
        session = mock.MagicMock()
        response = mock.MagicMock()
        response.status_code = 200
        response.text = "<html>" + "x" * 600 + "</html>"
        response.headers = {"content-type": "text/html"}
        session.get.return_value = response

        result = discover_staff_url(session, "https://myclub.com", "unknown")
        assert result is not None
        url, confidence = result
        assert confidence == 0.75


# ---------------------------------------------------------------------------
# Email + phone extraction from HTML elements
# ---------------------------------------------------------------------------


class TestEmailPhoneExtraction:
    def test_extract_email_mailto(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="mailto:coach@club.com">Email</a></div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) == "coach@club.com"

    def test_extract_email_text(self):
        from bs4 import BeautifulSoup
        html = "<div>Contact: coach@example.org for info</div>"
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) == "coach@example.org"

    def test_extract_email_none(self):
        from bs4 import BeautifulSoup
        html = "<div>No email here</div>"
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_email(el) is None

    def test_extract_phone_tel(self):
        from bs4 import BeautifulSoup
        html = '<div><a href="tel:4045551234">Call us</a></div>'
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_phone(el) is not None
        assert "404" in extract_phone(el)

    def test_extract_phone_text(self):
        from bs4 import BeautifulSoup
        html = "<div>Phone: (678) 555-9876</div>"
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_phone(el) is not None
        assert "678" in extract_phone(el)

    def test_extract_phone_none(self):
        from bs4 import BeautifulSoup
        html = "<div>No phone here</div>"
        soup = BeautifulSoup(html, "html.parser")
        el = soup.find("div")
        assert extract_phone(el) is None


# ---------------------------------------------------------------------------
# Dry-run mode (no DB needed)
# ---------------------------------------------------------------------------


class TestDryRun:
    @mock.patch.dict(os.environ, {}, clear=True)
    def test_dry_run_no_db(self):
        """Dry-run with no DATABASE_URL should return zeros, not crash."""
        from extractors.youth_club_coaches import scrape_youth_club_coaches
        result = scrape_youth_club_coaches(dry_run=True)
        assert result["scraped"] == 0
        assert result["errors"] == 0

    @mock.patch.dict(os.environ, {}, clear=True)
    def test_non_dry_run_no_db_errors(self):
        """Non-dry-run with no DATABASE_URL should report 1 error."""
        from extractors.youth_club_coaches import scrape_youth_club_coaches
        result = scrape_youth_club_coaches(dry_run=False)
        assert result["errors"] == 1


# ---------------------------------------------------------------------------
# Deduplication within a page
# ---------------------------------------------------------------------------


class TestDeduplication:
    def test_duplicate_names_deduplicated(self):
        """Same name appearing twice in different cards should only yield one entry."""
        html = """
        <html><body>
        <div class="staff-member">
            <h3>Marco Bertolini</h3>
            <div class="staff-title">Head Coach</div>
        </div>
        <div class="staff-member">
            <h3>Marco Bertolini</h3>
            <div class="staff-title">Director of Soccer</div>
        </div>
        <div class="staff-member">
            <h3>Elena Vasquez</h3>
            <div class="staff-title">Assistant Coach</div>
        </div>
        </body></html>
        """
        coaches = parse_staff_html(html, club_name="Test Club")
        names = [c.name for c in coaches]
        assert names.count("Marco Bertolini") == 1
        assert len(coaches) == 2
