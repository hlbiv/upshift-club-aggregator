"""
Custom extractor for Girls Academy and GA Aspire.

Both pages share the same structure:
  <article>
    <h3>Conference Name</h3>
    <ul>
      <li>Club Name (City, State)</li>
      ...
    </ul>
  </article>

City and state are extracted from the trailing parenthetical.
"""

from __future__ import annotations

import re
import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}

# Matches "(City, ST)" or "(City, State Name)" at end of li text
_LOCATION_RE = re.compile(r"\(([^,)]+),\s*([^)]+)\)\s*$")


def _parse_location(text: str) -> tuple[str, str, str]:
    """Return (club_name, city, state) parsed from 'Club Name (City, ST)'."""
    m = _LOCATION_RE.search(text)
    if m:
        club = text[: m.start()].strip()
        city = m.group(1).strip()
        state = m.group(2).strip()
    else:
        club = text.strip()
        city = ""
        state = ""
    return club, city, state


@register(r"girlsacademyleague\.com/(members|aspire-membership)")
def scrape_girls_academy(url: str, league_name: str) -> List[Dict]:
    logger.info("[GA custom] Scraping %s", url)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("GA fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    article = soup.find("article")
    if not article:
        logger.warning("GA: no <article> found on %s", url)
        return []

    records: List[Dict] = []
    current_conf = ""

    for el in article.find_all(["h3", "h4", "li"]):
        if el.name in ("h3", "h4"):
            current_conf = el.get_text(strip=True)
            continue
        # Capture any direct website link in the <li> before stripping tags
        a_tag = el.find("a", href=True)
        website = ""
        if a_tag:
            href = a_tag["href"].strip()
            if href.startswith("http"):
                website = href
        text = el.get_text(strip=True)
        if not text or len(text) < 3:
            continue
        club_name, city, state = _parse_location(text)
        if not club_name:
            continue
        records.append({
            "club_name": club_name,
            "league_name": league_name,
            "city": city,
            "state": state,
            "source_url": url,
            "conference": current_conf,
            "website": website,
        })

    logger.info("[GA custom] Found %d clubs on %s", len(records), url)
    return records
