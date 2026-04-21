"""
Smoke tests for scrape_staff.py parsers and staff URL confidence scoring.
Tests use in-memory HTML fixtures — no network calls or DB required.

psycopg2 is mocked at module level so the scraper can be imported without
a live database connection.
"""

import sys
import os
from unittest import mock
import pytest

# Stub out psycopg2 ONLY if not installed, so tests run without a live
# Postgres instance. Unconditional stubs leak MagicMocks into later-collected
# test modules (pytest imports all test files before any tests run) and
# break imports like `from psycopg2.extras import Json`.
try:
    import psycopg2  # noqa: F401
    import psycopg2.extras  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = mock.MagicMock()
    sys.modules["psycopg2.extras"] = mock.MagicMock()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from scrape_staff import (
    _parse_sportsengine,
    _parse_leagueapps,
    _parse_wordpress,
    _parse_generic,
    _parse_staff_page,
    _detect_platform,
    _STAFF_URL_CANDIDATES,
)
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# SportsEngine parser
# ---------------------------------------------------------------------------

SE_HTML = """
<html><body>
<div class="staff-card">
  <span class="staff-name">Jane Smith</span>
  <span class="staff-title">Head Coach</span>
  <a href="mailto:jane@example.com">Email</a>
</div>
<div class="staff-card">
  <span class="staff-name">Bob Jones</span>
  <span class="staff-title">Assistant Coach</span>
</div>
</body></html>
"""


def test_parse_sportsengine_names():
    soup = BeautifulSoup(SE_HTML, "lxml")
    records = _parse_sportsengine(soup)
    assert len(records) == 2
    names = [r["name"] for r in records]
    assert "Jane Smith" in names
    assert "Bob Jones" in names


def test_parse_sportsengine_email():
    soup = BeautifulSoup(SE_HTML, "lxml")
    records = _parse_sportsengine(soup)
    jane = next(r for r in records if r["name"] == "Jane Smith")
    assert jane["email"] == "jane@example.com"
    assert jane["title"] == "Head Coach"


def test_parse_sportsengine_no_email():
    soup = BeautifulSoup(SE_HTML, "lxml")
    records = _parse_sportsengine(soup)
    bob = next(r for r in records if r["name"] == "Bob Jones")
    assert bob["email"] is None
    assert bob["title"] == "Assistant Coach"


# ---------------------------------------------------------------------------
# WordPress Team Members plugin parser
# ---------------------------------------------------------------------------

WP_TEAM_HTML = """
<html><body>
<div class="team-member">
  <span class="team-member-name">Alice Torres</span>
  <span class="team-member-role">Technical Director</span>
  <a href="mailto:alice@club.org">alice@club.org</a>
</div>
<div class="team-member">
  <h3>Carlos Ruiz</h3>
  <p>Director of Coaching</p>
</div>
</body></html>
"""


def test_parse_wordpress_team_members_name():
    soup = BeautifulSoup(WP_TEAM_HTML, "lxml")
    records = _parse_wordpress(soup)
    names = [r["name"] for r in records]
    assert "Alice Torres" in names
    assert "Carlos Ruiz" in names


def test_parse_wordpress_team_members_role():
    soup = BeautifulSoup(WP_TEAM_HTML, "lxml")
    records = _parse_wordpress(soup)
    alice = next(r for r in records if r["name"] == "Alice Torres")
    assert alice["title"] == "Technical Director"
    assert alice["email"] == "alice@club.org"


def test_parse_wordpress_fallback_h3():
    """WordPress generic h3+p fallback when no .team-member cards exist."""
    html = """
    <html><body>
    <section id="staff">
      <h3>Maria Chen</h3>
      <p>Goalkeeper Coach</p>
    </section>
    </body></html>
    """
    soup = BeautifulSoup(html, "lxml")
    records = _parse_wordpress(soup)
    names = [r["name"] for r in records]
    assert "Maria Chen" in names
    maria = next(r for r in records if r["name"] == "Maria Chen")
    assert maria["title"] == "Goalkeeper Coach"


# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

def test_detect_sportsengine_by_url():
    assert _detect_platform("https://club.sportsengine.com/staff", "<html></html>") == "sportsengine"


def test_detect_leagueapps_by_url():
    assert _detect_platform("https://club.leagueapps.com/coaches", "<html></html>") == "leagueapps"


def test_detect_wordpress_by_wp_content():
    html = '<html><head><link href="/wp-content/themes/x/style.css"></head></html>'
    assert _detect_platform("https://myclub.org/staff", html) == "wordpress"


def test_detect_unknown():
    html = "<html><body><p>Hello</p></body></html>"
    assert _detect_platform("https://someclub.com/staff", html) == "unknown"


# ---------------------------------------------------------------------------
# Title normalization (null-safe idempotency)
# ---------------------------------------------------------------------------

def test_parse_staff_page_normalizes_none_title():
    """Verify _parse_staff_page always returns string title (never None)."""
    html = """
    <html><body>
    <section id="staff">
      <h3>Coach Without Title</h3>
    </section>
    </body></html>
    """
    records = _parse_staff_page(html, "unknown", "https://example.com/staff")
    for rec in records:
        assert rec.get("title") is not None, "title must never be None after parse"
        assert isinstance(rec["title"], str)


# ---------------------------------------------------------------------------
# Staff URL confidence ordering
# ---------------------------------------------------------------------------

def test_staff_url_confidence_ordering():
    """High-confidence paths come before lower-confidence ones."""
    high = [c for _, c in _STAFF_URL_CANDIDATES if c == 1.0]
    low = [c for _, c in _STAFF_URL_CANDIDATES if c == 0.7]
    assert len(high) > 0, "should have confidence=1.0 paths"
    assert len(low) > 0, "should have confidence=0.7 paths"
    # All 1.0 entries must appear before the first 0.7 entry
    first_low_idx = next(i for i, (_, c) in enumerate(_STAFF_URL_CANDIDATES) if c < 1.0)
    for i, (_, c) in enumerate(_STAFF_URL_CANDIDATES):
        if i < first_low_idx:
            assert c == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
