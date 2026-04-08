"""
Custom extractor for EDP Soccer (edpsoccer.com).

EDP is built on Wix (parastorage CDN), which makes traditional scraping
difficult. Strategy:
  1. Attempt requests-based scrape of the homepage and any linked club pages.
  2. Look for club name patterns in the rendered HTML.
  3. EDP populates team data dynamically; if static fails, log a clear warning
     so a Playwright-based run can be used when network access allows.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_CLUB_PAGE_HINTS = ("clubs", "members", "teams", "organizations", "directory")


@register(r"edpsoccer\.com")
def scrape_edp(url: str, league_name: str) -> List[Dict]:
    logger.info("[EDP custom] Scraping %s", url)
    records: List[Dict] = []
    seen: set = set()

    # Fetch homepage to discover club-related sub-pages
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("EDP fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")

    # Look for links to club/member pages
    candidate_urls = [url]
    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        if any(k in href for k in _CLUB_PAGE_HINTS):
            full = a["href"] if a["href"].startswith("http") else f"https://www.edpsoccer.com{a['href']}"
            if full not in candidate_urls:
                candidate_urls.append(full)
                logger.info("[EDP custom] Candidate page: %s", full)

    for page_url in candidate_urls[:5]:  # limit to avoid crawling entire site
        try:
            resp = requests.get(page_url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception:
            continue

        psoup = BeautifulSoup(resp.text, "lxml")
        for tag in psoup.find_all(["nav", "footer", "script", "style"]):
            tag.decompose()

        # Wix renders club names as text nodes within span/p/div inside sections
        for el in psoup.find_all(["li", "h3", "h4", "td", "span"]):
            text = el.get_text(strip=True)
            if (text and 4 < len(text) < 70 and text not in seen
                    and any(c.isupper() for c in text)
                    and not re.match(r"^(Home|About|Contact|Register|Login|News|Events)", text)):
                seen.add(text)
                records.append({
                    "club_name": text,
                    "league_name": league_name,
                    "city": "",
                    "state": "",
                    "source_url": page_url,
                })

    if not records:
        logger.warning(
            "[EDP custom] No clubs extracted — EDP is Wix-based and may require "
            "a full JS render. Run with Playwright in a non-sandboxed environment."
        )

    logger.info("[EDP custom] Found %d clubs", len(records))
    return records
