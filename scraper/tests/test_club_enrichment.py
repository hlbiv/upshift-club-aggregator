"""
Tests for the D1 club enrichment extractor, writer, and dedup utility.

Extractor and dedup tests are pure-logic (no HTTP, no DB).
Writer tests use a stubbed psycopg2 cursor.

Run:
    python -m pytest scraper/tests/test_club_enrichment.py -v
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.club_website import (  # noqa: E402
    _check_website_status,
    _compute_confidence,
    _discover_logo,
    _extract_socials,
    _find_staff_page,
    ClubEnrichmentResult,
    extract_club_enrichment,
)
from ingest.club_enrichment_writer import (  # noqa: E402
    _normalize_row,
    update_club_enrichment,
)
from dedup.club_dedup import (  # noqa: E402
    combined_similarity,
    find_duplicate_pairs,
    levenshtein_similarity,
    token_set_similarity,
)


# ===========================================================================
# Extractor tests
# ===========================================================================


class TestCheckWebsiteStatus:
    def _make_response(self, status_code=200, url="https://club.com", history=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.url = url
        resp.history = history or []
        return resp

    def test_active_site(self):
        resp = self._make_response(200)
        assert _check_website_status(resp, "<html>Normal page</html>") == "active"

    def test_dead_site_4xx(self):
        resp = self._make_response(404)
        assert _check_website_status(resp, "") == "dead"

    def test_dead_site_5xx(self):
        resp = self._make_response(500)
        assert _check_website_status(resp, "") == "dead"

    def test_parked_page_detection(self):
        resp = self._make_response(200)
        text = "This domain is for sale. Buy this domain at GoDaddy."
        assert _check_website_status(resp, text) == "dead"

    def test_redirect_to_different_domain(self):
        history_entry = MagicMock()
        history_entry.url = "https://oldclub.com/page"
        resp = self._make_response(200, url="https://newhost.org/landing", history=[history_entry])
        assert _check_website_status(resp, "Welcome") == "redirected"


class TestDiscoverLogo:
    def test_og_image(self):
        from bs4 import BeautifulSoup
        html = '<html><head><meta property="og:image" content="/images/logo.png"></head></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = _discover_logo(soup, "https://club.com")
        assert result == "https://club.com/images/logo.png"

    def test_img_with_logo_in_src(self):
        from bs4 import BeautifulSoup
        html = '<html><body><img src="/assets/club-logo.png" alt="Club"></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = _discover_logo(soup, "https://club.com")
        assert result == "https://club.com/assets/club-logo.png"

    def test_no_logo_found(self):
        from bs4 import BeautifulSoup
        html = '<html><body><p>Hello world</p></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = _discover_logo(soup, "https://club.com")
        assert result is None


class TestExtractSocials:
    def test_finds_instagram_and_facebook(self):
        from bs4 import BeautifulSoup
        html = """
        <html><body>
            <a href="https://www.instagram.com/myclub">IG</a>
            <a href="https://www.facebook.com/myclubfc">FB</a>
            <a href="https://twitter.com/myclub_sc">Twitter</a>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        socials = _extract_socials(soup)
        assert socials["instagram"] == "myclub"
        assert socials["facebook"] == "myclubfc"
        assert socials["twitter"] == "myclub_sc"

    def test_x_dot_com_maps_to_twitter(self):
        from bs4 import BeautifulSoup
        html = '<html><body><a href="https://x.com/clubhandle">X</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        socials = _extract_socials(soup)
        assert socials["twitter"] == "clubhandle"

    def test_skips_generic_paths(self):
        from bs4 import BeautifulSoup
        html = '<html><body><a href="https://www.instagram.com/share">Share</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        socials = _extract_socials(soup)
        assert "instagram" not in socials


class TestFindStaffPage:
    def test_finds_staff_link(self):
        from bs4 import BeautifulSoup
        html = '<html><body><a href="/our-coaches">Coaching Staff</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = _find_staff_page(soup, "https://club.com")
        assert result == "https://club.com/our-coaches"

    def test_ignores_external_link(self):
        from bs4 import BeautifulSoup
        html = '<html><body><a href="https://other.com/staff">Staff</a></body></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = _find_staff_page(soup, "https://club.com")
        assert result is None


class TestComputeConfidence:
    def test_dead_site_low_confidence(self):
        result = ClubEnrichmentResult(club_id=1, website_url="x", website_status="dead")
        assert _compute_confidence(result) == 10.0

    def test_active_with_logo_and_socials(self):
        result = ClubEnrichmentResult(
            club_id=1, website_url="x", website_status="active",
            logo_url="https://club.com/logo.png",
            instagram="myclub",
            facebook="myclub",
            staff_page_url="https://club.com/staff",
        )
        conf = _compute_confidence(result)
        # base(20) + logo(25) + 2 socials(20) + staff(15) = 80
        assert conf == 80.0

    def test_active_bare_minimum(self):
        result = ClubEnrichmentResult(club_id=1, website_url="x", website_status="active")
        assert _compute_confidence(result) == 20.0


class TestExtractClubEnrichment:
    @patch("extractors.club_website.requests.get")
    def test_timeout_returns_dead(self, mock_get):
        import requests as _requests
        mock_get.side_effect = _requests.exceptions.Timeout("timed out")
        result = extract_club_enrichment(1, "https://club.com")
        assert result.website_status == "dead"
        assert result.error == "timeout"

    @patch("extractors.club_website.requests.get")
    def test_connection_error_returns_dead(self, mock_get):
        import requests as _requests
        mock_get.side_effect = _requests.exceptions.ConnectionError("refused")
        result = extract_club_enrichment(1, "https://club.com")
        assert result.website_status == "dead"
        assert "connection_error" in result.error

    @patch("extractors.club_website.requests.get")
    def test_successful_extraction(self, mock_get):
        html = """
        <html>
        <head>
            <meta property="og:image" content="/logo.png">
        </head>
        <body>
            <a href="https://www.instagram.com/testclub">IG</a>
            <a href="/staff">Our Staff</a>
        </body>
        </html>
        """
        resp = MagicMock()
        resp.status_code = 200
        resp.url = "https://club.com"
        resp.history = []
        resp.text = html
        mock_get.return_value = resp

        result = extract_club_enrichment(1, "https://club.com")
        assert result.website_status == "active"
        assert result.logo_url == "https://club.com/logo.png"
        assert result.instagram == "testclub"
        assert result.staff_page_url == "https://club.com/staff"
        assert result.scrape_confidence > 50


# ===========================================================================
# Writer tests
# ===========================================================================


class TestNormalizeRow:
    def test_missing_club_id_raises(self):
        with pytest.raises(ValueError, match="club_id"):
            _normalize_row({})

    def test_valid_row(self):
        row = _normalize_row({
            "club_id": 42,
            "logo_url": "https://example.com/logo.png",
            "instagram": "myclub",
        })
        assert row["club_id"] == 42
        assert row["logo_url"] == "https://example.com/logo.png"
        assert row["instagram"] == "myclub"
        assert row["facebook"] is None
        assert row["twitter"] is None


class TestUpdateClubEnrichment:
    def test_dry_run_returns_zeros(self):
        counts = update_club_enrichment(
            [{"club_id": 1, "logo_url": "x"}],
            dry_run=True,
        )
        assert counts["updated"] == 0
        assert counts["skipped"] == 0

    def test_empty_rows(self):
        counts = update_club_enrichment([])
        assert counts == {"updated": 0, "skipped": 0}


# ===========================================================================
# Dedup tests
# ===========================================================================


class TestLevenshteinSimilarity:
    def test_identical_strings(self):
        assert levenshtein_similarity("abc", "abc") == 1.0

    def test_empty_strings(self):
        assert levenshtein_similarity("", "") == 1.0

    def test_completely_different(self):
        sim = levenshtein_similarity("abc", "xyz")
        assert sim == 0.0

    def test_one_edit_away(self):
        sim = levenshtein_similarity("kitten", "sitten")
        assert 0.8 <= sim <= 0.9


class TestTokenSetSimilarity:
    def test_identical(self):
        assert token_set_similarity("Concorde Fire SC", "Concorde Fire SC") == 1.0

    def test_abbreviation_expansion(self):
        # "FC" expands to "football club", "SC" expands to "soccer club"
        sim = token_set_similarity("Atlanta FC", "Atlanta Football Club")
        assert sim >= 0.8

    def test_noise_words_stripped(self):
        sim = token_set_similarity("The Atlanta Youth Soccer Club", "Atlanta")
        assert sim >= 0.8

    def test_completely_different(self):
        sim = token_set_similarity("Concorde Fire", "Georgia United")
        assert sim < 0.3


class TestCombinedSimilarity:
    def test_near_duplicates(self):
        sim = combined_similarity(
            "Concorde Fire SC",
            "Concorde Fire Soccer Club",
        )
        assert sim >= 0.8

    def test_different_clubs(self):
        sim = combined_similarity("Concorde Fire", "Georgia United")
        assert sim < 0.5


class TestFindDuplicatePairs:
    def test_finds_similar_pair_in_same_state(self):
        clubs = [
            {"id": 1, "name": "Concorde Fire SC", "state": "GA"},
            {"id": 2, "name": "Concorde Fire Soccer Club", "state": "GA"},
            {"id": 3, "name": "Georgia United FC", "state": "GA"},
        ]
        pairs = find_duplicate_pairs(clubs, threshold=0.7)
        assert len(pairs) >= 1
        # The Fire pair should be found
        fire_pair = [p for p in pairs if {p.club_a_id, p.club_b_id} == {1, 2}]
        assert len(fire_pair) == 1
        assert fire_pair[0].similarity >= 0.7

    def test_cross_state_not_flagged(self):
        clubs = [
            {"id": 1, "name": "Concorde Fire SC", "state": "GA"},
            {"id": 2, "name": "Concorde Fire SC", "state": "NC"},
        ]
        pairs = find_duplicate_pairs(clubs, threshold=0.85)
        assert len(pairs) == 0

    def test_pairs_ordered_by_id(self):
        clubs = [
            {"id": 5, "name": "ABC United FC", "state": "TX"},
            {"id": 2, "name": "ABC United Football Club", "state": "TX"},
        ]
        pairs = find_duplicate_pairs(clubs, threshold=0.7)
        assert len(pairs) >= 1
        # Lower id should be club_a
        assert pairs[0].club_a_id == 2
        assert pairs[0].club_b_id == 5

    def test_empty_input(self):
        pairs = find_duplicate_pairs([], threshold=0.85)
        assert pairs == []

    def test_single_state_single_club(self):
        clubs = [{"id": 1, "name": "Alone FC", "state": "WY"}]
        pairs = find_duplicate_pairs(clubs, threshold=0.85)
        assert pairs == []
