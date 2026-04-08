"""
JavaScript-rendered page scraper — uses Playwright (headless Chromium).
Used for pages that require JS to load their club directory content.

Falls back to the static scraper automatically when:
  - Playwright can't resolve DNS (sandbox/network restriction)
  - The browser crashes or times out on launch
  - Any other unrecoverable browser error occurs
"""

from __future__ import annotations

import logging
from typing import List, Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Error as PlaywrightError

from config import PLAYWRIGHT_TIMEOUT, PLAYWRIGHT_WAIT_FOR

logger = logging.getLogger(__name__)

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


def scrape_js(url: str, league_name: str) -> List[Dict]:
    """
    Launch a headless browser, wait for JS to settle, extract clubs from the DOM.

    Automatically falls back to the static (requests + BeautifulSoup) scraper if:
      - The browser can't resolve DNS (sandboxed environment like Replit)
      - Any unrecoverable browser/network error occurs

    Returns a list of raw club dicts (pre-normalization).
    """
    logger.info("JS scrape: %s", url)

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=_CHROMIUM_ARGS,
            )
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
                )
            )
            page = context.new_page()

            try:
                page.goto(url, wait_until=PLAYWRIGHT_WAIT_FOR, timeout=PLAYWRIGHT_TIMEOUT)
            except PlaywrightTimeout:
                logger.warning("Timeout on %s; extracting current DOM.", url)
            except PlaywrightError as exc:
                if _is_network_error(exc):
                    raise  # re-raise to outer handler for static fallback
                logger.warning("Page navigation error on %s: %s", url, exc)

            try:
                page.wait_for_load_state("domcontentloaded", timeout=5000)
            except PlaywrightTimeout:
                pass

            html = page.content()
            browser.close()

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
            return _static_fallback(url, league_name)
        logger.error("Playwright error on %s: %s", url, exc)
        return []

    except Exception as exc:
        logger.error("Unexpected error in JS scraper for %s: %s", url, exc)
        return _static_fallback(url, league_name)


def _static_fallback(url: str, league_name: str) -> List[Dict]:
    """Attempt a plain requests + BeautifulSoup scrape as a fallback."""
    from scraper_static import scrape_static
    logger.info("Static fallback scrape: %s", url)
    return scrape_static(url, league_name)
