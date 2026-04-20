"""
Tests for the Playwright fallback in ``ncaa_rosters.scrape_school_url``.

Covers:
- ``_playwright_fallback_enabled`` — env-flag gating
- ``_fetch_and_parse_with_fallback`` — behavior matrix:
    * static HTML parses ≥1 player → no fallback attempted
    * static HTML returns 0 players + flag off → no fallback attempted
    * static HTML returns 0 players + flag on + render succeeds with
      players → rendered DOM + players replace shell
    * static HTML returns 0 players + flag on + render fails → returns
      shell HTML + empty list (caller handles SKIP)
    * static fetch itself fails → None + empty (caller handles FAIL)

Playwright itself is mocked — we can't run headless Chromium in CI.

Run::

    python -m pytest scraper/tests/test_ncaa_playwright_fallback.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import ncaa_rosters  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
UCLA_FIXTURE = FIXTURE_DIR / "ucla_mens_soccer_roster.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# _playwright_fallback_enabled — env-flag gating
# ---------------------------------------------------------------------------


class TestPlaywrightFallbackEnabled:
    @pytest.mark.parametrize("value", ["1", "true", "True", "TRUE", "yes", "on"])
    def test_truthy_values_enable(self, value, monkeypatch):
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", value)
        assert ncaa_rosters._playwright_fallback_enabled() is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
    def test_falsy_values_disable(self, value, monkeypatch):
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", value)
        assert ncaa_rosters._playwright_fallback_enabled() is False

    def test_unset_disables(self, monkeypatch):
        monkeypatch.delenv("NCAA_PLAYWRIGHT_FALLBACK", raising=False)
        assert ncaa_rosters._playwright_fallback_enabled() is False


# ---------------------------------------------------------------------------
# _fetch_and_parse_with_fallback — behavior matrix
# ---------------------------------------------------------------------------


class TestFetchAndParseWithFallback:
    def test_static_hit_returns_players_without_rendering(self, monkeypatch):
        """Happy path: static HTML has players → Playwright never invoked."""
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        ucla_html = _read(UCLA_FIXTURE)

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=ucla_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright") as mock_render:
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://uclabruins.com/sports/mens-soccer/roster"
            )

        assert html == ucla_html
        assert len(players) > 0
        mock_render.assert_not_called()

    def test_static_miss_fallback_disabled_no_render(self, monkeypatch):
        """Shell HTML + flag off → caller still gets empty list; no render."""
        monkeypatch.delenv("NCAA_PLAYWRIGHT_FALLBACK", raising=False)
        shell_html = "<html><body><div id='roster-root'></div></body></html>"

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright") as mock_render:
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://example.edu/sports/mens-soccer/roster"
            )

        assert html == shell_html
        assert players == []
        mock_render.assert_not_called()

    def test_static_miss_fallback_on_render_hits(self, monkeypatch):
        """Shell → Playwright renders → rendered DOM re-parses to players."""
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell_html = "<html><body><div id='roster-root'></div></body></html>"
        rendered_html = _read(UCLA_FIXTURE)

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright", return_value=rendered_html) as mock_render:
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://gostanford.com/sports/mens-soccer/roster"
            )

        assert html == rendered_html
        assert len(players) > 0
        mock_render.assert_called_once_with(
            "https://gostanford.com/sports/mens-soccer/roster"
        )

    def test_static_miss_fallback_on_render_fails(self, monkeypatch):
        """Shell → Playwright render returns None → shell is returned for caller to SKIP."""
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell_html = "<html><body><div id='roster-root'></div></body></html>"

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright", return_value=None) as mock_render:
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://example.edu/sports/mens-soccer/roster"
            )

        assert html == shell_html
        assert players == []
        mock_render.assert_called_once()

    def test_static_miss_fallback_on_render_still_yields_zero(self, monkeypatch):
        """Shell → render succeeds but rendered DOM also has no players (dead page).

        Caller gets the rendered HTML back + empty list; it's up to the
        caller to SKIP. We still return the rendered HTML rather than the
        shell because it's the more useful artifact for diagnostics.
        """
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell_html = "<html><body><div id='roster-root'></div></body></html>"
        rendered_empty = "<html><body><div>Roster coming soon.</div></body></html>"

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright", return_value=rendered_empty):
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://example.edu/sports/mens-soccer/roster"
            )

        assert html == rendered_empty
        assert players == []

    def test_static_fetch_fails_returns_none_empty(self, monkeypatch):
        """Network failure: no fallback attempted; return None + empty."""
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")

        fake_session = mock.Mock()
        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=None), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright") as mock_render:
            html, players = ncaa_rosters._fetch_and_parse_with_fallback(
                fake_session, "https://example.edu/sports/mens-soccer/roster"
            )

        assert html is None
        assert players == []
        mock_render.assert_not_called()


# ---------------------------------------------------------------------------
# scrape_school_url — end-to-end fallback integration
# ---------------------------------------------------------------------------


class TestScrapeSchoolUrlFallback:
    def test_rendered_html_flows_through_to_college_dict(self, monkeypatch):
        """With the fallback on and rendering succeeding, the 'college' dict
        comes back populated just like the static-hit case. Proves the
        fallback path is fully wired end-to-end."""
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")
        shell_html = "<html><body><div id='roster-root'></div></body></html>"
        rendered_html = _read(UCLA_FIXTURE)

        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html), \
             mock.patch.object(ncaa_rosters, "_render_with_playwright", return_value=rendered_html):
            parsed = ncaa_rosters.scrape_school_url(
                "https://uclabruins.com/sports/mens-soccer/roster",
                name="UCLA",
                division="D1",
                gender_program="mens",
                state="CA",
            )

        assert parsed["college"]["name"] == "UCLA"
        assert len(parsed["players"]) > 0
        assert parsed["sidearm"] is True

    def test_static_miss_flag_off_still_raises_zero_players(self, monkeypatch):
        """Backward-compat: flag off + static miss → the pre-PR-4 RuntimeError."""
        monkeypatch.delenv("NCAA_PLAYWRIGHT_FALLBACK", raising=False)
        shell_html = "<html><body></body></html>"

        with mock.patch.object(ncaa_rosters, "fetch_with_retry", return_value=shell_html):
            with pytest.raises(RuntimeError, match="parsed 0 players"):
                ncaa_rosters.scrape_school_url(
                    "https://example.edu/sports/mens-soccer/roster",
                    name="Example",
                    division="D1",
                    gender_program="mens",
                )
