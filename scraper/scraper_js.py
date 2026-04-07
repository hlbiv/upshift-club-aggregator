"""
JavaScript-rendered page scraper — uses Playwright (headless Chromium).
Used for pages that require JS to load their club directory content.
"""

from __future__ import annotations

import logging
from typing import List, Dict

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from config import PLAYWRIGHT_TIMEOUT, PLAYWRIGHT_WAIT_FOR

logger = logging.getLogger(__name__)


def _parse_rendered_html(html: str, url: str, league_name: str) -> List[Dict]:
    """
    Parse the fully-rendered DOM the same way the static scraper does.
    Import inline to avoid circular dependency.
    """
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
    Launch a headless browser, navigate to `url`, wait for content to settle,
    then extract clubs from the rendered DOM.

    Returns a list of raw club dicts (pre-normalization).
    """
    logger.info("JS scrape: %s", url)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
            )
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until=PLAYWRIGHT_WAIT_FOR, timeout=PLAYWRIGHT_TIMEOUT)
        except PlaywrightTimeout:
            logger.warning("Timeout waiting for networkidle on %s; using current DOM.", url)

        # Extra wait to let any lazy-loaded content render
        try:
            page.wait_for_load_state("domcontentloaded", timeout=5000)
        except PlaywrightTimeout:
            pass

        html = page.content()
        browser.close()

    records = _parse_rendered_html(html, url, league_name)
    logger.info("JS extraction yielded %d clubs from %s", len(records), url)
    return records
