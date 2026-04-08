"""
Custom extractor for DPL (Development Player League).

DPL is a WordPress site with no standalone club directory. Clubs appear as
team names inside tournament bracket and standings pages. These pages are
JS-rendered (Wix-style widgets embedded).

Strategy:
  1. Fetch the WordPress pages API to discover recent event/bracket pages.
  2. Use Playwright to render each event page and extract team names from
     table cells (bracket grids).
  3. Fall back to static request if Playwright is unavailable.
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
_BASE = "https://dpleague.org"

# Slugs of known DPL event pages that contain bracket/team data (newest first)
_KNOWN_EVENT_SLUGS = [
    "richmond26",   # 2026 Eastern Regional U13/U14
    "tampa2026",    # 2026 Eastern Regional U15-U19
    "summit2026",   # 2026 Summit U13-U19
    "finals25",     # DPL Finals 2025
    "summit25",     # Summit 2025
    "phoenix25",    # 2025 Western Regional U15-19
    "provo25",      # 2025 Western Regional U13/U14
]

_EVENT_KEYWORDS = ("regional", "finals", "summit", "eastern", "western", "bracket", "standings")

_TEAM_MIN = 4
_TEAM_MAX = 60

# Words that are definitely NOT club names (headers, labels, boilerplate)
_NOISE = {
    "home", "away", "date", "time", "score", "pts", "results", "schedule",
    "standings", "pool", "bracket", "group", "field", "division", "age",
    "boys", "girls", "accept", "decline", "register", "contact", "news",
    "close modal window", "close", "submit", "load more", "follow on instagram",
    "back to top", "privacy policy", "terms", "careers", "events",
}


def _is_team_name(text: str) -> bool:
    low = text.lower().strip()
    return (
        _TEAM_MIN <= len(text) <= _TEAM_MAX
        and any(c.isalpha() for c in text)
        and low not in _NOISE
        and not low.startswith("#")
        and not re.match(r"^\d+[\s:/-]", text)       # starts with number
        and not re.match(r"^(https?://|www\.)", low)  # URL
    )


def _extract_from_html(html: str, source_url: str, league_name: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()

    seen: set = set()
    records: List[Dict] = []

    # Primary: table cells (bracket grids)
    for td in soup.find_all("td"):
        text = td.get_text(strip=True)
        if text and text not in seen and _is_team_name(text):
            seen.add(text)
            records.append({"club_name": text, "league_name": league_name,
                            "city": "", "state": "", "source_url": source_url})

    # Secondary: heading-level elements that may list team names
    for tag in soup.find_all(["h3", "h4"]):
        text = tag.get_text(strip=True)
        if text and text not in seen and _is_team_name(text):
            seen.add(text)
            records.append({"club_name": text, "league_name": league_name,
                            "city": "", "state": "", "source_url": source_url})

    return records


@register(r"dpleague\.org")
def scrape_dpl(url: str, league_name: str) -> List[Dict]:
    logger.info("[DPL custom] Starting scrape")
    all_records: List[Dict] = []
    seen_clubs: set = set()

    # Build list of event pages to scrape
    target_urls: List[str] = [f"{_BASE}/{slug}/" for slug in _KNOWN_EVENT_SLUGS]

    # Also discover any new event pages via WordPress API
    try:
        r = requests.get(f"{_BASE}/wp-json/wp/v2/pages?per_page=100", headers=_HEADERS, timeout=15)
        if r.status_code == 200:
            for page in r.json():
                slug = page.get("slug", "").lower()
                title = page.get("title", {}).get("rendered", "").lower()
                link = page.get("link", "")
                if any(k in slug or k in title for k in _EVENT_KEYWORDS):
                    if link not in target_urls:
                        target_urls.append(link)
    except Exception as exc:
        logger.warning("[DPL custom] WP API error: %s", exc)

    logger.info("[DPL custom] Event pages to scrape: %d", len(target_urls))

    for event_url in target_urls:
        # Try Playwright first (JS-rendered brackets)
        html = render_page(event_url, wait_until="networkidle", timeout_ms=30_000)
        if not html:
            # Fall back to static
            try:
                resp = requests.get(event_url, headers=_HEADERS, timeout=15)
                resp.raise_for_status()
                html = resp.text
            except Exception as exc:
                logger.warning("[DPL] Skipping %s: %s", event_url, exc)
                continue

        records = _extract_from_html(html, event_url, league_name)
        new = [rec for rec in records if rec["club_name"] not in seen_clubs]
        seen_clubs.update(rec["club_name"] for rec in new)
        all_records.extend(new)
        logger.info("[DPL] %s -> %d teams (%d new)", event_url, len(records), len(new))

    logger.info("[DPL custom] Total clubs: %d", len(all_records))
    return all_records
