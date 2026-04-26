"""
Tests for the raw-HTML archival hook in ``scraper_static.scrape_static``.

The static path was the first scraper to wire archival in; this file
covers the ``scrape_run_log_id`` plumbing so the archive row can be
tied to the owning ``scrape_run_logs`` entry.

We mock ``archive_raw_html`` and the network ``http_get`` call so no
external service / live HTTP is touched.

Run:
    python -m pytest scraper/tests/test_scraper_static_archive.py -v
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import scraper_static  # type: ignore  # noqa: E402
    from utils import html_archive  # type: ignore  # noqa: E402
except Exception as exc:  # pragma: no cover
    pytest.skip(
        f"scraper_static imports unavailable in this environment: {exc}",
        allow_module_level=True,
    )


_PAGE_HTML = "<html><body><table><tr><th>Club</th></tr><tr><td>FC Test</td></tr></table></body></html>"
_FINAL_URL = "https://example.com/clubs"


def _build_fake_response(html: str = _PAGE_HTML, url: str = _FINAL_URL) -> MagicMock:
    """Return a MagicMock that quacks like a requests.Response."""
    r = MagicMock()
    r.text = html
    r.url = url
    r.raise_for_status = MagicMock(return_value=None)
    r.status_code = 200
    return r


@pytest.fixture(autouse=True)
def _reset_archive_state(monkeypatch):
    """Clear html_archive caches + env before/after each test."""
    monkeypatch.delenv("ARCHIVE_RAW_HTML_ENABLED", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    html_archive._reset_for_tests()
    yield
    html_archive._reset_for_tests()


def test_archive_receives_threaded_scrape_run_log_id(monkeypatch):
    """
    When ``scrape_static`` is called with an explicit
    ``scrape_run_log_id``, that id is forwarded to ``archive_raw_html``
    so the archive row gets FK'd to the owning scrape run.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    monkeypatch.setattr(
        scraper_static,
        "http_get",
        lambda *_a, **_kw: _build_fake_response(),
    )

    archive_mock = MagicMock(return_value=None)
    monkeypatch.setattr(scraper_static, "archive_raw_html", archive_mock)

    scraper_static.scrape_static(
        "https://example.com/start",
        "Test League",
        scrape_run_log_id=98765,
    )

    archive_mock.assert_called_once()
    _args, kwargs = archive_mock.call_args
    assert kwargs.get("scrape_run_log_id") == 98765, (
        "scrape_static must forward its scrape_run_log_id kwarg to "
        "archive_raw_html so the archive row is tied to the owning run"
    )


def test_archive_default_scrape_run_log_id_is_none(monkeypatch):
    """
    Backwards-compat: callers that don't pass ``scrape_run_log_id`` get
    None forwarded — the archive row simply has no run FK.
    """
    monkeypatch.setenv("ARCHIVE_RAW_HTML_ENABLED", "true")

    monkeypatch.setattr(
        scraper_static,
        "http_get",
        lambda *_a, **_kw: _build_fake_response(),
    )

    archive_mock = MagicMock(return_value=None)
    monkeypatch.setattr(scraper_static, "archive_raw_html", archive_mock)

    scraper_static.scrape_static(
        "https://example.com/start",
        "Test League",
    )

    archive_mock.assert_called_once()
    _args, kwargs = archive_mock.call_args
    assert kwargs.get("scrape_run_log_id") is None
