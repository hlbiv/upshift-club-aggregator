"""
Custom extractor for NorCal Premier Soccer (/clubs/).

The clubs page has a table with columns: Club | Location | Region
Each row is a member club. Region gives a sub-geography within NorCal.
"""

from __future__ import annotations

import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}


@register(r"norcalpremier\.com")
def scrape_norcal(url: str, league_name: str) -> List[Dict]:
    # Always use the /clubs/ directory page regardless of the configured URL
    clubs_url = "https://norcalpremier.com/clubs/"
    logger.info("[NorCal custom] Scraping %s", clubs_url)

    try:
        r = requests.get(clubs_url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("NorCal fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records: List[Dict] = []

    # The page has a table with th headers: Club, Location, Region
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not any("club" in h for h in headers):
            continue

        col_club = next((i for i, h in enumerate(headers) if "club" in h), 0)
        col_location = next((i for i, h in enumerate(headers) if "location" in h or "city" in h), None)
        col_region = next((i for i, h in enumerate(headers) if "region" in h), None)

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells or len(cells) <= col_club:
                continue
            club_name = cells[col_club].get_text(strip=True)
            if not club_name or len(club_name) < 2:
                continue

            location = cells[col_location].get_text(strip=True) if col_location and len(cells) > col_location else ""
            region = cells[col_region].get_text(strip=True) if col_region and len(cells) > col_region else ""

            records.append({
                "club_name": club_name,
                "league_name": league_name,
                "city": location,
                "state": "CA",      # NorCal is California-only
                "source_url": clubs_url,
                "region": region,
            })

    logger.info("[NorCal custom] Found %d clubs", len(records))
    return records
