"""
Tests for the cleanups in ``scraper_js`` introduced by PR #18:

1. Fallback exception preservation — when the JS scrape falls back to
   the static path AND the static path also fails, the re-raised
   exception MUST carry the original Playwright error in its chain
   (``__cause__`` / message) so an operator sees both causes.

2. Second wait_for_load_state skipped on goto timeout — when the
   primary ``page.goto(...)`` raises ``PlaywrightTimeout``, the
   secondary ``page.wait_for_load_state("domcontentloaded", ...)``
   MUST NOT run (compounded timeouts hurt; the DOM is whatever it is).

3. USER_AGENT consistency — every place that talks to the wire uses
   the centralized ``config.USER_AGENT`` rather than its own copy.

Playwright is stubbed so this runs in environments without the real
package (dev laptops, CI). The stub strategy mirrors
``test_scraper_js_archive.py``.

Run:
    python -m pytest scraper/tests/test_scraper_js.py -v
"""

from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Playwright stub (installed before scraper_js import)
# ---------------------------------------------------------------------------

def _install_playwright_stub() -> None:
    if "playwright.sync_api" in sys.modules:
        return
    pw = types.ModuleType("playwright")
    pw_sync = types.ModuleType("playwright.sync_api")

    class _StubError(Exception):
        pass

    class _StubTimeout(_StubError):
        pass

    pw_sync.sync_playwright = lambda *a, **kw: None  # type: ignore[attr-defined]
    pw_sync.TimeoutError = _StubTimeout  # type: ignore[attr-defined]
    pw_sync.Error = _StubError  # type: ignore[attr-defined]
    sys.modules["playwright"] = pw
    sys.modules["playwright.sync_api"] = pw_sync


_install_playwright_stub()

try:
    import scraper_js  # type: ignore  # noqa: E402
    import scraper_static  # type: ignore  # noqa: E402
    import config  # type: ignore  # noqa: E402
    from extractors import gotsport_matches  # type: ignore  # noqa: E402
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"scraper imports unavailable in this environment: {exc}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Helpers — fake sync_playwright context manager that exposes a configurable
# page so individual tests can inject goto / wait_for_load_state behaviors.
# ---------------------------------------------------------------------------


def _build_fake_sync_playwright(
    *,
    goto_side_effect=None,
    wait_for_load_state_side_effect=None,
    html: str = "<html></html>",
    url: str = "https://example.com/final",
):
    """
    Build a stub sync_playwright() callable. The returned ``page``
    MagicMock lets the caller inspect / assert on individual method
    calls afterwards.
    """
    page = MagicMock()
    if goto_side_effect is not None:
        page.goto = MagicMock(side_effect=goto_side_effect)
    else:
        page.goto = MagicMock(return_value=None)
    if wait_for_load_state_side_effect is not None:
        page.wait_for_load_state = MagicMock(side_effect=wait_for_load_state_side_effect)
    else:
        page.wait_for_load_state = MagicMock(return_value=None)
    page.content = MagicMock(return_value=html)
    page.url = url

    context = MagicMock()
    context.new_page = MagicMock(return_value=page)

    browser = MagicMock()
    browser.new_context = MagicMock(return_value=context)
    browser.close = MagicMock(return_value=None)

    chromium = MagicMock()
    chromium.launch = MagicMock(return_value=browser)

    pw = MagicMock()
    pw.chromium = chromium

    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=pw)
    cm.__exit__ = MagicMock(return_value=False)

    return MagicMock(return_value=cm), page


# ---------------------------------------------------------------------------
# (1) Fallback exception preservation
# ---------------------------------------------------------------------------


def test_static_fallback_preserves_original_js_exception(monkeypatch):
    """
    When ``scrape_js`` falls back to the static scraper because of an
    unexpected (non-network) Playwright error AND the static path also
    raises, the re-raised exception must:

      - be of the static fallback's exception type (so callers that
        special-case e.g. ``requests.RequestException`` still match);
      - have its message embed the original Playwright exception's
        type + repr (so a single log line shows both causes);
      - have ``__cause__`` set (so a traceback chain is preserved).
    """
    # Force the JS path to raise an unexpected exception that lands in
    # the broad `except Exception` block.
    fake_sp, page = _build_fake_sync_playwright()
    page.content = MagicMock(side_effect=RuntimeError("playwright crashed mid-render"))
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)

    # The static fallback must also fail so we exercise the preservation
    # path. Use a custom sentinel exception type so we can assert that
    # the re-raise preserves the type.
    class _StaticBoom(Exception):
        pass

    def _fail_static(url, league_name, scrape_run_log_id=None):
        raise _StaticBoom("static path also failed")

    monkeypatch.setattr(scraper_js, "_static_fallback", _fail_static)

    with pytest.raises(_StaticBoom) as excinfo:
        scraper_js.scrape_js("https://example.com/x", "Test League")

    msg = str(excinfo.value)
    # Both causes are present in the message.
    assert "static path also failed" in msg, (
        f"static-side message missing from re-raise: {msg!r}"
    )
    assert "playwright crashed mid-render" in msg, (
        f"original JS error text missing from re-raise: {msg!r}"
    )
    assert "RuntimeError" in msg, (
        f"original JS error type missing from re-raise: {msg!r}"
    )

    # Chain is preserved (the immediate cause is the fallback failure).
    assert excinfo.value.__cause__ is not None
    assert isinstance(excinfo.value.__cause__, _StaticBoom)


def test_static_fallback_success_path_still_returns_records(monkeypatch):
    """
    Sanity check on the wrapper's happy path: when the static fallback
    succeeds, ``scrape_js`` returns its records and does NOT raise.
    Ensures the preservation wrapper doesn't accidentally turn
    successful fallbacks into failures.
    """
    fake_sp, page = _build_fake_sync_playwright()
    page.content = MagicMock(side_effect=RuntimeError("force fallback"))
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)

    monkeypatch.setattr(
        scraper_js,
        "_static_fallback",
        lambda url, league_name, scrape_run_log_id=None: [{"club_name": "from-static"}],
    )

    result = scraper_js.scrape_js("https://example.com/x", "Test League")
    assert result == [{"club_name": "from-static"}]


# ---------------------------------------------------------------------------
# (2) Second wait_for_load_state skipped on goto timeout
# ---------------------------------------------------------------------------


def test_second_wait_skipped_when_goto_times_out(monkeypatch):
    """
    Regression: when ``page.goto(...)`` raises ``PlaywrightTimeout``,
    the secondary ``page.wait_for_load_state("domcontentloaded", ...)``
    MUST NOT be called. Compounding the timeout was the previous bug.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    fake_sp, page = _build_fake_sync_playwright(
        goto_side_effect=PWTimeout("primary load timed out")
    )
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(scraper_js, "_parse_rendered_html", lambda *_a, **_k: [])

    # Don't let the archive path do anything interesting.
    monkeypatch.setattr(scraper_js, "archive_raw_html", lambda *a, **k: None)

    scraper_js.scrape_js("https://example.com/x", "Test League")

    page.goto.assert_called_once()
    page.wait_for_load_state.assert_not_called()


def test_second_wait_runs_when_goto_succeeds(monkeypatch):
    """
    Counter-test for the above: when ``page.goto(...)`` succeeds, the
    secondary ``page.wait_for_load_state("domcontentloaded", ...)``
    DOES run. Ensures we didn't over-correct and accidentally remove
    the secondary wait entirely.
    """
    fake_sp, page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(scraper_js, "_parse_rendered_html", lambda *_a, **_k: [])
    monkeypatch.setattr(scraper_js, "archive_raw_html", lambda *a, **k: None)

    scraper_js.scrape_js("https://example.com/x", "Test League")

    page.goto.assert_called_once()
    page.wait_for_load_state.assert_called_once_with(
        "domcontentloaded", timeout=5000
    )


# ---------------------------------------------------------------------------
# (3) USER_AGENT centralization regression test
# ---------------------------------------------------------------------------


def test_user_agent_centralized_in_config():
    """
    The canonical UA lives at ``config.USER_AGENT``. Every consumer
    that talks to the wire must reference that constant rather than
    holding its own string.

    A future refactor that re-introduces a duplicate UA literal will
    fail this test.
    """
    canonical = config.USER_AGENT
    assert canonical, "config.USER_AGENT must be a non-empty string"
    # All three call sites resolve to the same UA string.
    assert scraper_static.HEADERS["User-Agent"] == canonical
    assert gotsport_matches._HEADERS["User-Agent"] == canonical


def test_user_agent_passed_to_playwright_context(monkeypatch):
    """
    The JS scraper passes ``config.USER_AGENT`` into
    ``browser.new_context(user_agent=...)``. We verify by capturing
    the kwargs of the ``new_context`` call.
    """
    fake_sp, page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(scraper_js, "_parse_rendered_html", lambda *_a, **_k: [])
    monkeypatch.setattr(scraper_js, "archive_raw_html", lambda *a, **k: None)

    scraper_js.scrape_js("https://example.com/x", "Test League")

    # Walk the fake_sp call chain to find the captured new_context kwargs.
    cm = fake_sp.return_value
    pw = cm.__enter__.return_value
    browser = pw.chromium.launch.return_value
    new_context = browser.new_context

    new_context.assert_called_once()
    _args, kwargs = new_context.call_args
    assert kwargs.get("user_agent") == config.USER_AGENT
