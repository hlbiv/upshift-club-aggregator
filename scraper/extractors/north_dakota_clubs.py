"""
North Dakota Soccer Association club extractor.

Source: https://www.northdakotasoccer.org/club-info/
Structure: Each club is a block with:
  - <p><strong>Club Name</strong></p>
  - <a href="...">Visit website</a>

The primary site (ndsoccer.org/find-a-club/) uses a SportsEngine widget
that requires JavaScript rendering. This extractor uses the secondary site
(northdakotasoccer.org/club-info/) which has static HTML.

8 clubs as of April 2026.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0)"}
_SOURCE_URL = "https://www.northdakotasoccer.org/club-info/"

# Skip phrases for non-club entries (regional orgs, admin sections)
_SKIP_LOWER = {
    "ndsa soccer league",
    "dakota olympic development",
    "midwest regional league",
    "region ii",
    "north dakota soccer association",
    "club info",
    "area clubs",
    "regional organizations",
    "contact",
    "email",
    "phone",
}


def scrape_nd_clubs(league_name: str) -> List[Dict]:
    """Scrape the NDSA club-info page for all member clubs."""
    try:
        r = requests.get(_SOURCE_URL, headers=_HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("[ND] club-info page returned %s", r.status_code)
            return []
    except Exception as exc:
        logger.warning("[ND] Failed to fetch club-info page: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    content = (
        soup.find("div", class_="entry-content")
        or soup.find("main")
        or soup.find("article")
        or soup.body
    )
    if not content:
        logger.warning("[ND] No content area found on %s", _SOURCE_URL)
        return []

    records: List[Dict] = []
    seen_names: set = set()

    # Strategy 1: Find <strong> tags containing club names
    for strong in content.find_all("strong"):
        name = strong.get_text(strip=True)
        if not name or len(name) < 3 or len(name) > 100:
            continue
        if name.lower() in _SKIP_LOWER:
            continue
        if any(skip in name.lower() for skip in _SKIP_LOWER):
            continue

        # Skip if it looks like a label, not a club name
        if ":" in name or name.startswith("(") or name.isnumeric():
            continue

        canon = name.lower().strip()
        if canon in seen_names:
            continue
        seen_names.add(canon)

        # Look for a nearby "Visit website" link
        website = ""
        parent = strong.parent
        if parent:
            # Search siblings and following elements for a website link
            for sibling in parent.find_all_next("a", href=True, limit=5):
                link_text = sibling.get_text(strip=True).lower()
                href = sibling["href"]
                if ("visit" in link_text or "website" in link_text or
                        href.startswith("http") and "northdakotasoccer" not in href):
                    website = href.strip()
                    break

        record = {
            "club_name": name,
            "league_name": league_name,
            "city": "",
            "state": "North Dakota",
            "source_url": _SOURCE_URL,
        }
        if website:
            record["website"] = website

        records.append(record)

    logger.info("[ND] Scraped %d clubs from %s", len(records), _SOURCE_URL)
    return records
