"""
Tests for the raw-HTML archival hook in ``scraper_js.scrape_js``.

This mirrors the hook that was already wired into ``scraper_static.py``
in PR #73. The three scenarios we care about:

1. With ``ARCHIVE_RAW_HTML_ENABLED=true``, a successful JS scrape
   triggers exactly one ``archive_raw_html`` call with the
   post-render DOM returned by ``page.content()`` and the page's
   final URL.
2. If ``archive_raw_html`` raises, the scrape continues — the
   caller of ``scrape_js`` never sees the archival exception, and
   extraction still runs.
3. With the flag unset, ``archive_raw_html`` short-circuits before
   it would touch Replit Object Storage. We verify this indirectly:
   the module-level client init path is gated on
   ``_is_enabled()``, so no SDK / bucket interaction occurs.

Playwright itself is mocked — we never launch a real browser from
pytest. The style matches ``test_html_archive.py`` + the Playwright
stubbing pattern in ``test_run_py_dispatch.py``.

Run:
    python -m pytest scraper/tests/test_scraper_js_archive.py -v
"""

from __future__ import annotations

import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Playwright stub (shared across tests; installed before import of scraper_js)
# ---------------------------------------------------------------------------

def _install_playwright_stub() -> None:
    """
    Pre-install a stub for ``playwright.sync_api`` so ``scraper_js`` is
    importable in environments (dev laptops, CI) that don't have the
    playwright package installed. Real scraping still requires the real
    dependency on Replit; this stub only makes the module importable
    and its ``sync_playwright`` symbol mockable.
    """
    if "playwright.sync_api" in sys.modules:
        return
    pw = types.ModuleType("playwright")
    pw_sync = types.ModuleType("playwright.sync_api")

    class _StubError(Exception):
        pass

    pw_sync.sync_playwright = lambda *a, **kw: None  # type: ignore[attr-defined]
    pw_sync.TimeoutError = _StubError  # type: ignore[attr-defined]
    pw_sync.Error = _StubError  # type: ignore[attr-defined]
    sys.modules["playwright"] = pw
    sys.modules["playwright.sync_api"] = pw_sync


_install_playwright_stub()

try:
    import scraper_js  # type: ignore  # noqa: E402
    from utils import html_archive  # type: ignore  # noqa: E402
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"scraper_js imports unavailable in this environment: {exc}",
        allow_module_level=True,
    )


# ---------------------------------------------------------------------------
# Fixtures — build a fake sync_playwright context manager that yields a
# page whose .content() returns a known HTML string.
# ---------------------------------------------------------------------------

_POST_RENDER_HTML = "<html><body><main>rendered by JS</main></body></html>"
_FINAL_URL = "https://example.com/club-directory"


def _build_fake_sync_playwright(html: str = _POST_RENDER_HTML, url: str = _FINAL_URL):
    """
    Return a zero-arg callable matching ``sync_playwright()``'s API:
    a context manager whose ``__enter__`` yields a ``pw`` object with
    ``.chromium.launch(...)`` → browser → new_context → new_page.

    The page's ``.goto``, ``.wait_for_load_state`` are no-ops; its
    ``.content()`` returns the canned HTML and ``.url`` returns the
    canned final URL.
    """
    page = MagicMock()
    page.goto = MagicMock(return_value=None)
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


@pytest.fixture(autouse=True)
def _reset_archive_state(monkeypatch):
    """Clear html_archive module caches + env before/after each test."""
    monkeypatch.delenv("ARCHIVE_RAW_HTML_ENABLED", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    html_archive._reset_for_tests()
    yield
    html_archive._reset_for_tests()


# ---------------------------------------------------------------------------
# Test 1 — happy path: flag on → archive_raw_html called once with
# post-render content and the page's final URL
# ---------------------------------------------------------------------------

def test_archive_called_once_with_post_render_content(monkeypatch):
    """
    When ``ARCHIVE_RAW_HTML_ENABLED=true`` and the Playwright pipeline
    produces HTML, ``scrape_js`` hands that HTML to
    ``archive_raw_html`` exactly once, with the page's final URL.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    fake_sp, page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)

    # Stub out the extractor so the test doesn't depend on any
    # particular DOM shape / scraper_static implementation.
    monkeypatch.setattr(
        scraper_js,
        "_parse_rendered_html",
        lambda html, url, league_name: [{"name": "fake club"}],
    )

    archive_mock = MagicMock(return_value={"sha256": "abc", "bucket_path": "x", "content_bytes": 1})
    monkeypatch.setattr(scraper_js, "archive_raw_html", archive_mock)

    result = scraper_js.scrape_js("https://example.com/start", "Test League")

    # Normal extraction result propagates.
    assert result == [{"name": "fake club"}]

    # Post-render DOM was captured.
    page.content.assert_called_once()

    # archive_raw_html called exactly once with the post-render HTML
    # and the page's final URL.
    archive_mock.assert_called_once()
    args, kwargs = archive_mock.call_args
    # Positional signature is (source_url, html, scrape_run_log_id=None).
    assert args[0] == _FINAL_URL
    assert args[1] == _POST_RENDER_HTML
    # Caller didn't pass scrape_run_log_id → defaults to None.
    assert kwargs.get("scrape_run_log_id") is None


def test_archive_receives_threaded_scrape_run_log_id(monkeypatch):
    """
    When ``scrape_js`` is called with an explicit ``scrape_run_log_id``,
    that id is forwarded to ``archive_raw_html`` as a kwarg so the
    archive row gets FK'd to the owning scrape run.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    fake_sp, _page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(
        scraper_js,
        "_parse_rendered_html",
        lambda html, url, league_name: [],
    )

    archive_mock = MagicMock(return_value=None)
    monkeypatch.setattr(scraper_js, "archive_raw_html", archive_mock)

    scraper_js.scrape_js(
        "https://example.com/start",
        "Test League",
        scrape_run_log_id=12345,
    )

    archive_mock.assert_called_once()
    _args, kwargs = archive_mock.call_args
    assert kwargs.get("scrape_run_log_id") == 12345, (
        "scrape_js must forward its scrape_run_log_id kwarg to "
        "archive_raw_html so the archive row is tied to the owning run"
    )


def test_archive_uses_original_url_when_page_url_falsy(monkeypatch):
    """
    Defensive fallback: if ``page.url`` is falsy (empty string, None),
    the original ``url`` argument is used as the archive source_url.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    fake_sp, page = _build_fake_sync_playwright(url="")
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(scraper_js, "_parse_rendered_html", lambda *_a, **_k: [])

    archive_mock = MagicMock(return_value=None)
    monkeypatch.setattr(scraper_js, "archive_raw_html", archive_mock)

    scraper_js.scrape_js("https://example.com/start", "Test League")

    archive_mock.assert_called_once()
    args, _kwargs = archive_mock.call_args
    assert args[0] == "https://example.com/start"


# ---------------------------------------------------------------------------
# Test 2 — archive_raw_html raising must not break the scrape
# ---------------------------------------------------------------------------

def test_archive_exception_does_not_break_scrape(monkeypatch, caplog):
    """
    If ``archive_raw_html`` raises (e.g. transient Object Storage
    glitch slips past its own defensive guards), ``scrape_js`` logs a
    warning and returns the extractor's result as if archival never
    happened. The caller MUST NOT see the exception.
    """
    import logging as py_logging

    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    fake_sp, _page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(
        scraper_js,
        "_parse_rendered_html",
        lambda html, url, league_name: [{"name": "survived"}],
    )

    def _boom(*_a, **_kw):
        raise RuntimeError("object storage hiccup")

    monkeypatch.setattr(scraper_js, "archive_raw_html", _boom)

    caplog.set_level(py_logging.WARNING, logger=scraper_js.logger.name)

    # The scrape must return the extractor's result; no RuntimeError
    # escapes. (scrape_js has a broad ``except Exception`` fallback to
    # the static scraper; we don't want that path to fire here — the
    # archival try/except should catch it in place.)
    result = scraper_js.scrape_js("https://example.com/start", "Test League")

    assert result == [{"name": "survived"}], (
        "archival failure must not divert the scrape into the static "
        "fallback path"
    )

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("raw-html archival skipped" in m for m in messages), (
        f"expected a warning log for archival failure, got: {messages}"
    )


# ---------------------------------------------------------------------------
# Test 3 — flag unset: archive_raw_html short-circuits before hitting
# Replit Object Storage
# ---------------------------------------------------------------------------

def test_flag_unset_skips_object_storage(monkeypatch):
    """
    When ``ARCHIVE_RAW_HTML_ENABLED`` is unset, ``archive_raw_html``
    returns None immediately without attempting to import / init the
    Replit Object Storage SDK. We verify by asserting the real
    ``archive_raw_html`` is a no-op (returns None) under this config,
    and that no Object Storage client is constructed.

    This test intentionally does NOT monkeypatch
    ``scraper_js.archive_raw_html`` — we want to exercise the real
    gating check end-to-end.
    """
    # Ensure the env var is NOT set (fixture already cleared it, but
    # be explicit for readers of this test).
    assert "ARCHIVE_RAW_HTML_ENABLED" not in os.environ

    fake_sp, _page = _build_fake_sync_playwright()
    monkeypatch.setattr(scraper_js, "sync_playwright", fake_sp)
    monkeypatch.setattr(scraper_js, "_parse_rendered_html", lambda *_a, **_k: [])

    # Patch the private initializer so we can assert it was never
    # called. If the gating check works, _init_client never runs.
    init_spy = MagicMock(return_value=True)
    monkeypatch.setattr(html_archive, "_init_client", init_spy)

    # Patch the upload path too — double-checking no Object Storage
    # bucket call is attempted even if the gate somehow fell through.
    upload_spy = MagicMock(return_value=True)
    monkeypatch.setattr(html_archive, "_upload_blob", upload_spy)

    result = scraper_js.scrape_js("https://example.com/start", "Test League")

    assert result == []
    # The real archive_raw_html returned None immediately — no client
    # init, no upload, no DB work.
    init_spy.assert_not_called()
    upload_spy.assert_not_called()
