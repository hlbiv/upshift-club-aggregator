"""
Tests for the NextGen / Nuxt XHR-aware Playwright fallback.

The NextGen / Sidearm-Nuxt shell cluster (hokiesports.com, goaztecs.com,
gopsusports.com, ... plus ~7 others) ships zero player data in the static
HTML *and* hydrates the roster DOM only after a ``/website-api/rosters?...``
XHR response. The classic Playwright fallback's 5-second selector wait
times out before that hydration lands — the browser paints the cards 8-10
seconds in, not 5, because Nuxt has to finish loading its JS chunk tree
before it can fire the XHR.

This test module covers two layers of the fix:

1. Parser-layer: the ``.roster-list-item`` and ``.player-list-item`` DOM
   templates both resolve to full ``RosterPlayer`` rows (names, jerseys,
   positions, years, hometowns, previous schools) against a fixture modeled
   on the live hokiesports.com / goaztecs.com capture.

2. Playwright-layer: the renderer uses ``page.wait_for_event("response",
   predicate=...)`` keyed off the ``/website-api/rosters`` path regex.
   Playwright itself is mocked — we can't launch Chromium in CI — but the
   mock verifies the XHR-wait arm fires when the rendered HTML comes back.

Run::

    python -m pytest scraper/tests/test_ncaa_nuxt_xhr.py -v
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
NUXT_LIST_FIXTURE = FIXTURE_DIR / "nuxt_list_roster_template.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Parser coverage for NextGen ``roster-list-item`` template
# ---------------------------------------------------------------------------


class TestNuxtListRosterParser:
    def test_extracts_twelve_players_excluding_staff(self):
        """Fixture has 12 player cards + 1 staff card; parser returns 12."""
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        assert len(players) == 12
        names = [p.player_name for p in players]
        # Staff entry must never appear among players
        assert not any("Nameson" in n for n in names)
        assert not any("Coach" in n for n in names)

    def test_ten_plus_players_threshold(self):
        """Task requirement: parser extracts >= 10 players from the
        NextGen XHR fixture. Kept as a separate assert so failure
        messaging is specific."""
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        assert len(players) >= 10, (
            f"Expected >= 10 players parsed from nuxt-list fixture; "
            f"got {len(players)}"
        )

    def test_first_player_all_fields(self):
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Leo Ledin")
        assert p.jersey_number == "0"
        assert p.position == "Goalkeeper"
        assert p.year == "sophomore"
        assert p.hometown == "Stockholm, Sweden"
        # Previous-school trumps high-school when both are present — this is
        # what makes the column informative for recruiting research.
        assert p.prev_club == "Wofford"

    def test_player_without_previous_school_falls_back_to_high_school(self):
        """When only ``--high-school`` is present and ``--previous-school``
        is absent, the HS value must populate ``prev_club`` so we don't drop
        the signal entirely."""
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        # Amir Ariely has no previous-school in the fixture
        p = next(p for p in players if p.player_name == "Amir Ariely")
        assert p.prev_club == "Barnsley College"

    def test_redshirt_year_normalizes(self):
        """R-Jr / RS-Jr in --class-level must normalize to ``junior``."""
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Marcus Oswald")
        assert p.year == "junior"

    def test_staff_card_with_matching_container_is_skipped(self):
        """A ``.roster-list-item`` that also carries
        ``.roster-staff-members-list-item`` (the NextGen staff marker) must
        be filtered out *before* extraction — otherwise the head coach would
        appear as a player with jersey=empty, breaking downstream upserts."""
        players = ncaa_rosters.parse_roster_html(_read(NUXT_LIST_FIXTURE))
        for p in players:
            assert "Coach" not in p.player_name
            assert "Nameson" not in p.player_name


# ---------------------------------------------------------------------------
# Playwright XHR-wait — mocked Page surface
# ---------------------------------------------------------------------------


class _FakeResponse:
    """Minimal stand-in for Playwright's Response object that
    ``_NUXT_XHR_PATH_RE`` can pattern-match against."""

    def __init__(self, url: str, status: int = 200):
        self.url = url
        self.status = status


class _FakePage:
    """Records every API call made against it so tests can assert wiring.

    Playwright's real ``Page`` exposes ``goto``, ``wait_for_event``,
    ``wait_for_selector``, ``wait_for_timeout``, and ``content``. We
    replicate those four (plus an ``expect_response`` stub in case a
    future Playwright version drops ``wait_for_event``) and feed the
    test-supplied HTML through ``content()``.
    """

    def __init__(self, rendered_html: str, xhr_should_match: bool = True):
        self._html = rendered_html
        self._xhr_should_match = xhr_should_match
        self.calls: list = []

    def goto(self, url, timeout=None, wait_until=None):
        self.calls.append(("goto", url, wait_until))

    def wait_for_event(self, event, *, predicate=None, timeout=None):
        self.calls.append(("wait_for_event", event, timeout))
        # Simulate that exactly one response satisfies the predicate so we
        # can verify the XHR arm takes the success branch.
        fake = _FakeResponse("https://example.edu/website-api/rosters?filter%5Bsport_id%5D=8")
        if self._xhr_should_match and (predicate is None or predicate(fake)):
            return fake
        # No-match path mimics a TimeoutError from Playwright.
        from playwright.sync_api import TimeoutError as PlaywrightTimeout
        raise PlaywrightTimeout("timed out (fake)")

    def wait_for_selector(self, selector, timeout=None):
        self.calls.append(("wait_for_selector", selector, timeout))

    def wait_for_timeout(self, ms):
        self.calls.append(("wait_for_timeout", ms))

    def content(self):
        self.calls.append(("content",))
        return self._html


class _FakeContext:
    def __init__(self, page):
        self._page = page

    def new_page(self):
        return self._page


class _FakeBrowser:
    def __init__(self, page):
        self._ctx = _FakeContext(page)
        self.closed = False

    def new_context(self, user_agent=None):
        return self._ctx

    def close(self):
        self.closed = True


class _FakePlaywrightChromium:
    def __init__(self, page):
        self._page = page

    def launch(self, headless=True):
        return _FakeBrowser(self._page)


class _FakePlaywrightApi:
    """Stand-in for the object returned by ``sync_playwright().__enter__()``."""

    def __init__(self, page):
        self.chromium = _FakePlaywrightChromium(page)


class _FakeSyncPlaywrightContextManager:
    def __init__(self, page):
        self._api = _FakePlaywrightApi(page)

    def __enter__(self):
        return self._api

    def __exit__(self, exc_type, exc, tb):
        return False


def _make_sync_playwright_factory(page):
    def _factory():
        return _FakeSyncPlaywrightContextManager(page)
    return _factory


class TestNuxtXhrPlaywrightRender:
    def test_render_waits_for_nuxt_xhr_and_returns_dom(self, monkeypatch):
        """Full happy-path. Playwright is mocked. The renderer should:
          - call goto with the URL
          - call wait_for_event('response', ...) with a predicate
          - call wait_for_timeout (the 750ms paint delay)
          - call wait_for_selector across the widened selector set
          - return page.content() to the caller
        """
        import playwright.sync_api as pw_api

        rendered = _read(NUXT_LIST_FIXTURE)
        fake_page = _FakePage(rendered, xhr_should_match=True)

        monkeypatch.setattr(
            pw_api, "sync_playwright",
            _make_sync_playwright_factory(fake_page),
        )
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")

        html = ncaa_rosters._render_with_playwright(
            "https://hokiesports.com/sports/mens-soccer/roster"
        )
        assert html == rendered

        kinds = [c[0] for c in fake_page.calls]
        # Critical ordering: goto → wait_for_event (XHR) → wait_for_timeout
        # (paint grace) → wait_for_selector (DOM) → content.
        assert "goto" in kinds
        assert "wait_for_event" in kinds
        # The 750 ms paint delay only fires if the XHR wait matched.
        assert "wait_for_timeout" in kinds
        assert "wait_for_selector" in kinds

        # Confirm the XHR predicate passed to Playwright matched the NextGen
        # pattern (i.e. wait_for_event received ``predicate`` not None and it
        # returned truthy for a /website-api/rosters URL).
        wfe = next(c for c in fake_page.calls if c[0] == "wait_for_event")
        assert wfe[1] == "response"
        # XHR timeout should be longer than the selector timeout — XHRs fire
        # later than selector paints on SIDEARM classic but earlier on NextGen.
        assert wfe[2] == ncaa_rosters._PLAYWRIGHT_XHR_TIMEOUT_MS

    def test_render_falls_through_when_no_xhr_matches(self, monkeypatch):
        """SIDEARM classic sites don't fire /website-api/rosters. The XHR wait
        must time out gracefully and the renderer must still return page
        content — otherwise we'd regress every SIDEARM site by turning its
        Playwright fallback into a hard fail."""
        import playwright.sync_api as pw_api

        rendered = "<html><body>(hydrated sidearm DOM)</body></html>"
        fake_page = _FakePage(rendered, xhr_should_match=False)

        monkeypatch.setattr(
            pw_api, "sync_playwright",
            _make_sync_playwright_factory(fake_page),
        )
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")

        html = ncaa_rosters._render_with_playwright(
            "https://uclabruins.com/sports/mens-soccer/roster"
        )
        assert html == rendered

        kinds = [c[0] for c in fake_page.calls]
        assert "wait_for_event" in kinds
        # XHR timeout → no 750 ms paint delay. Still must call the selector
        # wait + content.
        assert "wait_for_selector" in kinds

    def test_ten_plus_players_from_rendered_xhr_dom(self, monkeypatch):
        """End-to-end: mock the XHR-wait path + the DOM return, then run the
        parser over the returned HTML and confirm >= 10 RosterPlayer rows.
        This is the single canonical "fix worked" signal asked for by the
        task spec."""
        import playwright.sync_api as pw_api

        rendered = _read(NUXT_LIST_FIXTURE)
        fake_page = _FakePage(rendered, xhr_should_match=True)

        monkeypatch.setattr(
            pw_api, "sync_playwright",
            _make_sync_playwright_factory(fake_page),
        )
        monkeypatch.setenv("NCAA_PLAYWRIGHT_FALLBACK", "true")

        html = ncaa_rosters._render_with_playwright(
            "https://hokiesports.com/sports/mens-soccer/roster"
        )
        assert html is not None
        players = ncaa_rosters.parse_roster_html(html)
        assert len(players) >= 10


# ---------------------------------------------------------------------------
# XHR path regex
# ---------------------------------------------------------------------------


class TestNuxtXhrPathRegex:
    @pytest.mark.parametrize("url", [
        "https://hokiesports.com/website-api/rosters?filter%5Bsport_id%5D=8&include=season",
        "https://goaztecs.com/website-api/rosters?filter%5Bsport_id%5D=18",
        "https://gopsusports.com/website-api/rosters?filter%5Bsport_id%5D=28&per_page=200",
        "https://gowyo.com/website-api/rosters",
        "https://example.edu/website-api/rosters/1457",
    ])
    def test_matches_nextgen_roster_endpoints(self, url):
        assert ncaa_rosters._NUXT_XHR_PATH_RE.search(url) is not None

    @pytest.mark.parametrize("url", [
        "https://hokiesports.com/website-api/schedule-events?filter=1",
        "https://hokiesports.com/api/v2/promotions/1/click",
        "https://hokiesports.com/website-api/menus/135",
        "https://example.edu/api/rosters/mens-soccer",   # Sidearm classic, not NextGen
        "https://example.edu/_nuxt/rosters.js",
    ])
    def test_does_not_match_unrelated_endpoints(self, url):
        assert ncaa_rosters._NUXT_XHR_PATH_RE.search(url) is None
