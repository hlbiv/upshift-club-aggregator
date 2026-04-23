"""
Tests for ncaa_discover_urls_google.py — Google CSE URL auto-resolver.

Covers:
  - classify_url: soccer_program_url / website / None (skip) classification
  - _pass1_query / _pass2_query: query string builders
  - discover_soccer_url: happy-path pass-1 hit, pass-1 miss → pass-2 hit,
    full miss, quota exhaustion propagation

Run::

    python -m pytest scraper/tests/test_ncaa_discover_urls_google.py -v
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_discover_urls_google import (  # noqa: E402
    classify_url,
    discover_soccer_url,
    _pass1_query,
    _pass2_query,
    _QuotaExhausted,
)


# ---------------------------------------------------------------------------
# classify_url
# ---------------------------------------------------------------------------


class TestClassifyUrl:
    def test_direct_soccer_path_mens(self):
        assert classify_url(
            "https://gostanford.com/sports/mens-soccer/roster", "mens"
        ) == "soccer_program_url"

    def test_direct_soccer_path_womens(self):
        assert classify_url(
            "https://goduke.com/sports/womens-soccer/roster", "womens"
        ) == "soccer_program_url"

    def test_msoc_abbreviation(self):
        assert classify_url(
            "https://ukathletics.com/sports/msoc/roster", "mens"
        ) == "soccer_program_url"

    def test_wsoc_abbreviation(self):
        assert classify_url(
            "https://goheels.com/sports/wsoc/roster", "womens"
        ) == "soccer_program_url"

    def test_combined_gender_soccer_path(self):
        assert classify_url(
            "https://gopsusports.com/sports/soccer/roster", "mens"
        ) == "soccer_program_url"

    def test_edu_root_is_website(self):
        assert classify_url("https://athletics.purdue.edu/", "mens") == "website"

    def test_edu_no_path_is_website(self):
        assert classify_url("https://athletics.stanford.edu", "mens") == "website"

    def test_edu_athletics_path_is_website(self):
        assert classify_url("https://unc.edu/athletics/", "womens") == "website"

    def test_edu_deep_path_is_skipped(self):
        # Deep .edu path that isn't an athletics homepage
        assert classify_url("https://unc.edu/news/soccer-team-wins", "mens") is None

    def test_wikipedia_is_skipped(self):
        assert classify_url(
            "https://en.wikipedia.org/wiki/Stanford_Cardinal_soccer", "mens"
        ) is None

    def test_topdrawersoccer_is_skipped(self):
        assert classify_url(
            "https://www.topdrawersoccer.com/college-soccer/", "womens"
        ) is None

    def test_empty_url_is_none(self):
        assert classify_url("", "mens") is None

    def test_social_media_is_skipped(self):
        assert classify_url("https://twitter.com/StanfordMSoccer", "mens") is None


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------


class TestQueryBuilders:
    def test_pass1_mens_with_state(self):
        q = _pass1_query("Purdue Fort Wayne", "IN", "mens")
        assert "Purdue Fort Wayne" in q
        assert "IN" in q
        assert "mens" in q
        assert "soccer" in q
        assert "roster" in q

    def test_pass1_womens(self):
        q = _pass1_query("Duke University", "NC", "womens")
        assert "womens" in q
        assert "soccer" in q

    def test_pass1_no_state(self):
        q = _pass1_query("Gonzaga University", None, "mens")
        assert "Gonzaga University" in q
        assert "None" not in q

    def test_pass2_includes_athletics(self):
        q = _pass2_query("Purdue Fort Wayne", "IN")
        assert "Purdue Fort Wayne" in q
        assert "athletics" in q

    def test_pass2_no_state(self):
        q = _pass2_query("Pepperdine University", None)
        assert "None" not in q
        assert "athletics" in q


# ---------------------------------------------------------------------------
# discover_soccer_url
# ---------------------------------------------------------------------------


class TestDiscoverSoccerUrl:
    """All network calls are mocked — no real HTTP requests."""

    def _make_mock_search(self, pass1_items=None, pass2_items=None):
        """Return a mock for extractors.ncaa_discover_urls_google._search."""
        calls = []

        def _search(query, *, api_key, cx, num=3, session=None):
            calls.append(query)
            if len(calls) == 1:
                return pass1_items or []
            return pass2_items or []

        return _search, calls

    def test_pass1_direct_soccer_url_hit(self):
        mock_fn, calls = self._make_mock_search(
            pass1_items=[{"link": "https://goduke.com/sports/mens-soccer/roster"}]
        )
        with mock.patch(
            "extractors.ncaa_discover_urls_google._search", side_effect=mock_fn
        ):
            result = discover_soccer_url(
                "Duke University", "NC", "mens", api_key="k", cx="c"
            )
        assert result == ("https://goduke.com/sports/mens-soccer/roster", "soccer_program_url")
        assert len(calls) == 1  # pass 1 hit → no pass 2

    def test_pass1_miss_falls_to_pass2_website(self):
        mock_fn, calls = self._make_mock_search(
            pass1_items=[{"link": "https://topdrawersoccer.com/duke"}],  # skip
            pass2_items=[{"link": "https://athletics.duke.edu/"}],        # website
        )
        with mock.patch(
            "extractors.ncaa_discover_urls_google._search", side_effect=mock_fn
        ):
            result = discover_soccer_url(
                "Duke University", "NC", "mens", api_key="k", cx="c"
            )
        assert result == ("https://athletics.duke.edu/", "website")
        assert len(calls) == 2

    def test_full_miss_returns_none(self):
        mock_fn, _ = self._make_mock_search(pass1_items=[], pass2_items=[])
        with mock.patch(
            "extractors.ncaa_discover_urls_google._search", side_effect=mock_fn
        ):
            result = discover_soccer_url(
                "Unknown University", "XX", "mens", api_key="k", cx="c"
            )
        assert result is None

    def test_quota_exhausted_propagates(self):
        def _raise(*args, **kwargs):
            raise _QuotaExhausted()

        with mock.patch(
            "extractors.ncaa_discover_urls_google._search", side_effect=_raise
        ):
            with pytest.raises(_QuotaExhausted):
                discover_soccer_url(
                    "Stanford", "CA", "womens", api_key="k", cx="c"
                )

    def test_empty_items_list_handled(self):
        mock_fn, _ = self._make_mock_search(
            pass1_items=[{"link": None}],  # malformed item — link is None
            pass2_items=[],
        )
        with mock.patch(
            "extractors.ncaa_discover_urls_google._search", side_effect=mock_fn
        ):
            result = discover_soccer_url(
                "Some College", "TX", "mens", api_key="k", cx="c"
            )
        assert result is None
