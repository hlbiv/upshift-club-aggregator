"""
JavaScript-rendered page scraper — uses Playwright (headless Chromium).
Used for pages that require JS to load their club directory content.

Falls back to the static scraper automatically when:
  - Playwright can't resolve DNS (sandbox/network restriction)
  - The browser crashes or times out on launch
  - Any other unrecoverable browser error occurs

Raw HTML archival
-----------------
After ``page.content()`` captures the post-render DOM, the HTML is
handed to :func:`utils.html_archive.archive_raw_html` (same
``ARCHIVE_RAW_HTML_ENABLED`` env gate as the static path). We archive
the *post-render* DOM rather than the pre-JS source because every
extractor parses the hydrated DOM — archival exists so we can re-parse
later, and the wire source isn't what we re-parse. Capturing the
pre-JS source is a separate follow-up if we ever need it.

Memory: the HTML reference is held on a single local variable and
dropped as soon as the extractor returns, so archival does not double
the per-page memory footprint. The archive call is wrapped in a
try/except so a bucket outage never breaks a scrape.
"""

from __future__ import annotations

import logging
from typing import List, Dict, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Error as PlaywrightError

from config import PLAYWRIGHT_TIMEOUT, PLAYWRIGHT_WAIT_FOR, USER_AGENT
from utils.html_archive import archive_raw_html
from utils.http import pick_proxy_server

logger = logging.getLogger(__name__)


def _playwright_proxy_for(url: str) -> Optional[Dict[str, str]]:
    """
    Return a Playwright proxy kwarg for ``url``, or ``None`` if no
    proxy is configured for the host.

    The returned dict is shaped for ``browser.new_context(proxy=...)``.
    On an empty pool we return ``None`` so the existing behaviour
    (direct connection) is preserved bit-for-bit.

    TODO(proxy-cooldown): This helper asks the shared config for a
    single non-cooldown proxy and passes it to the browser context.
    It does NOT (yet) implement the 429-driven rotation loop that the
    ``requests`` path in ``utils.http`` has — Playwright contexts
    can't swap proxies mid-flight without recreating the browser, so
    the cooldown loop needs a different architecture (likely: launch
    per proxy, rotate on failure, reuse the static-scraper fallback).
    Tracked for a follow-up PR; out of scope for the initial
    abstraction.
    """
    hostname = urlparse(url).hostname or ""
    proxy_url = pick_proxy_server(hostname)
    if proxy_url is None:
        return None
    return {"server": proxy_url}

# Chromium flags needed for sandboxed/container environments (Replit, Docker, CI)
_CHROMIUM_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--single-process",
]

# Network error substrings that indicate the browser can't reach external URLs
_NETWORK_ERRORS = (
    "ERR_NAME_NOT_RESOLVED",
    "ERR_CONNECTION_REFUSED",
    "ERR_INTERNET_DISCONNECTED",
    "ERR_NETWORK_CHANGED",
    "net::ERR_",
)


def _is_network_error(exc: PlaywrightError) -> bool:
    msg = str(exc)
    return any(tag in msg for tag in _NETWORK_ERRORS)


def _parse_rendered_html(html: str, url: str, league_name: str) -> List[Dict]:
    """Parse a fully-rendered DOM using the same extraction logic as the static scraper."""
    from scraper_static import (
        _extract_clubs_from_table,
        _extract_clubs_from_lists,
        _extract_clubs_from_links,
    )

    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()

    records = _extract_clubs_from_table(soup, url, league_name)
    if records:
        return records

    records = _extract_clubs_from_lists(soup, url, league_name)
    if records:
        return records

    return _extract_clubs_from_links(soup, url, league_name)


def scrape_js(
    url: str,
    league_name: str,
    scrape_run_log_id: Optional[int] = None,
) -> List[Dict]:
    """
    Launch a headless browser, wait for JS to settle, extract clubs from the DOM.

    Automatically falls back to the static (requests + BeautifulSoup) scraper if:
      - The browser can't resolve DNS (sandboxed environment like Replit)
      - Any unrecoverable browser/network error occurs

    ``scrape_run_log_id`` is the FK to ``scrape_run_logs.id`` for the owning
    scrape run; threaded down so the post-render raw-HTML archive row can be
    tied back to the run for post-mortem replay. Pass ``None`` from ad-hoc
    / test callers. The static-fallback path also receives the id.

    Returns a list of raw club dicts (pre-normalization).
    """
    logger.info("JS scrape: %s", url)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=_CHROMIUM_ARGS,
            )
            context_kwargs: Dict = {"user_agent": USER_AGENT}
            proxy = _playwright_proxy_for(url)
            if proxy is not None:
                context_kwargs["proxy"] = proxy
                logger.info("JS scrape via proxy %s", proxy["server"])
            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            goto_timed_out = False
            try:
                page.goto(url, wait_until=PLAYWRIGHT_WAIT_FOR, timeout=PLAYWRIGHT_TIMEOUT)
            except PlaywrightTimeout:
                logger.warning("Timeout on %s; extracting current DOM.", url)
                goto_timed_out = True
            except PlaywrightError as exc:
                if _is_network_error(exc):
                    raise  # re-raise to outer handler for static fallback
                logger.warning("Page navigation error on %s: %s", url, exc)

            # Only do the secondary "domcontentloaded" wait if the first
            # wait actually finished. If page.goto() already timed out
            # waiting for the primary load state, calling wait_for_load_state
            # again just compounds the timeout for no benefit — the DOM is
            # whatever it is, and we'll snapshot it as-is below.
            if not goto_timed_out:
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=5000)
                except PlaywrightTimeout:
                    pass

            html = page.content()
            final_url = page.url or url
            browser.close()

        # Archive the post-render DOM before extractor dispatch. If
        # extraction later raises, we still have the snapshot on disk
        # for a re-parse. The archive path is strictly defensive —
        # any failure is logged and swallowed so scraping proceeds.
        # ``scrape_run_log_id`` is threaded down from run.py's
        # scrape_league() so the archive row is FK'd to the owning run.
        try:
            archive_raw_html(
                final_url,
                html,
                scrape_run_log_id=scrape_run_log_id,
            )
        except Exception as exc:  # pragma: no cover — strictly defensive
            logger.warning("raw-html archival skipped (%s): %s", final_url, exc)

        records = _parse_rendered_html(html, url, league_name)
        logger.info("JS extraction yielded %d clubs from %s", len(records), url)
        return records

    except PlaywrightError as exc:
        if _is_network_error(exc):
            logger.warning(
                "Playwright network error on %s (%s). "
                "Falling back to static scraper — JS-rendered content may be incomplete.",
                url, type(exc).__name__,
            )
            return _static_fallback_preserving(
                url, league_name, exc, scrape_run_log_id
            )
        logger.error("Playwright error on %s: %s", url, exc)
        return []

    except Exception as exc:
        logger.error("Unexpected error in JS scraper for %s: %s", url, exc)
        return _static_fallback_preserving(
            url, league_name, exc, scrape_run_log_id
        )


def _static_fallback_preserving(
    url: str,
    league_name: str,
    original_exc: BaseException,
    scrape_run_log_id: Optional[int] = None,
) -> List[Dict]:
    """
    Run the static fallback for ``url`` while preserving the original
    Playwright exception in any chained traceback.

    On the happy path this is a thin wrapper around
    :func:`_static_fallback`. If the static fallback ALSO raises, we
    re-raise an exception of the fallback's type whose message embeds
    both errors and whose ``__cause__`` chain points to the fallback
    failure. The original Playwright exception's repr is included in
    the message text so an operator reading a single log line sees
    BOTH causes — the JS error that triggered the fallback and the
    static error that ended the scrape.

    Without this wrapper a bare ``except`` in the caller would lose
    the JS-side exception entirely and we'd only see the static-side
    failure, which has historically been the source of "static
    scraper failed" tickets where the real culprit was a Playwright
    timeout three layers up.
    """
    try:
        return _static_fallback(url, league_name, scrape_run_log_id)
    except Exception as fallback_exc:
        raise type(fallback_exc)(
            f"static fallback failed: {fallback_exc} "
            f"(original JS error: {type(original_exc).__name__}: {original_exc})"
        ) from fallback_exc


def _static_fallback(
    url: str,
    league_name: str,
    scrape_run_log_id: Optional[int] = None,
) -> List[Dict]:
    """Attempt a plain requests + BeautifulSoup scrape as a fallback."""
    from scraper_static import scrape_static
    logger.info("Static fallback scrape: %s", url)
    return scrape_static(url, league_name, scrape_run_log_id=scrape_run_log_id)
