"""
Custom extractor for ECNL (Elite Club National League).

The ECNL directory and sub-league pages (theecnl.com) use the Sidearm Sports
CMS platform. Club lists are rendered via JavaScript after page load.

Strategy:
  1. Use Playwright to fully render the page.
  2. Try Sidearm-specific CSS selectors to find the club list.
  3. If selectors miss, fall back to all <li> items in the main content area.
  4. Also covers ECNL RL Boys and Girls sub-league pages.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register
from extractors.playwright_helper import render_page

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}

# Ordered list of CSS selectors to try on the rendered Sidearm DOM
_SIDEARM_SELECTORS = [
    ".s-directory-list-item",
    ".s-directory-item",
    "[class*='directory'] li",
    "[class*='directory'] td",
    ".ec-directory li",
    ".club-list li",
    ".sport-directory li",
    ".s-text-paragraph-body li",
    ".s-main-section li",
    "#sidearm-page-content li",
    # Very broad fallback: all <li> in main content
    "main li",
    "article li",
    "#page-content li",
]

# Minimum chars for a string to be a club name (filters out nav noise)
_MIN_LEN = 4
_MAX_LEN = 80


def _clubs_from_html(html: str, url: str, league_name: str) -> List[Dict]:
    """Parse rendered HTML and return club records using Sidearm selectors."""
    soup = BeautifulSoup(html, "lxml")
    # Remove chrome (nav, header, footer, ads)
    for tag in soup.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()

    seen: set = set()
    records: List[Dict] = []

    for sel in _SIDEARM_SELECTORS:
        items = soup.select(sel)
        if not items:
            continue
        for item in items:
            text = item.get_text(strip=True)
            if _MIN_LEN < len(text) <= _MAX_LEN and text not in seen:
                seen.add(text)
                records.append({
                    "club_name": text,
                    "league_name": league_name,
                    "city": "",
                    "state": "",
                    "source_url": url,
                })
        if records:
            logger.info("[ECNL] Selector '%s' -> %d clubs from %s", sel, len(records), url)
            return records

    logger.warning("[ECNL] No clubs via selectors; raw li count=%d", len(soup.find_all("li")))
    return records


def _scrape_url(url: str, league_name: str) -> List[Dict]:
    # 1. Try Playwright for JS-rendered content
    html = render_page(url, wait_until="networkidle", timeout_ms=35_000)
    if html:
        records = _clubs_from_html(html, url, league_name)
        if records:
            return records

    # 2. Fall back to static request
    logger.info("[ECNL] Playwright empty; trying static for %s", url)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        return _clubs_from_html(r.text, url, league_name)
    except requests.RequestException as exc:
        logger.error("[ECNL] Static fetch failed: %s", exc)
        return []


@register(r"theecnl\.com/sports/directory")
def scrape_ecnl(url: str, league_name: str) -> List[Dict]:
    logger.info("[ECNL custom] %s", url)
    records = _scrape_url(url, league_name)
    logger.info("[ECNL custom] %d clubs total", len(records))
    return records


@register(r"theecnl\.com/sports/ecnl-regional-league")
def scrape_ecnl_rl(url: str, league_name: str) -> List[Dict]:
    logger.info("[ECNL RL custom] %s", url)
    records = _scrape_url(url, league_name)
    logger.info("[ECNL RL custom] %d clubs total", len(records))
    return records
