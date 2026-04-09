"""
Custom extractor for Heartland Soccer Association.

Heartland is the largest soccer league/tournament host in the US (Kansas/Missouri).
They use a proprietary registration system (registration.heartlandsoccer.net) and
do NOT expose a GotSport club list.

The /member-clubs/ page lists their 5 founding member organizations, each of
which is itself a large multi-team club:
  - Kansas Rush Soccer Club
  - Overland Park Soccer Club
  - Northeast United Soccer Club
  - Kansas Premier Soccer League
  - Sporting Blue Valley

All clubs are in the Kansas City metro area (KS/MO).
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}
_MEMBER_CLUBS_URL = "https://www.heartlandsoccer.net/member-clubs/"


@register(r"heartlandsoccer\.net")
def scrape_heartland(url: str, league_name: str) -> List[Dict]:
    logger.info("[Heartland custom] Scraping member clubs from %s", _MEMBER_CLUBS_URL)

    try:
        r = requests.get(_MEMBER_CLUBS_URL, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("[Heartland] Fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records: List[Dict] = []

    for h3 in soup.find_all("h3"):
        name = h3.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        city = ""
        state = "KS"   # all founding members are in the Overland Park / Kansas City KS area

        # Look for a mailing address in the next sibling blocks
        container = h3.parent
        addr_text = container.get_text(" ", strip=True) if container else ""
        m = re.search(r"([A-Za-z ]+),\s*([A-Z]{2})\s+\d{5}", addr_text)
        if m:
            city = m.group(1).strip()
            state = m.group(2)

        records.append({
            "club_name":   name,
            "league_name": league_name,
            "city":        city,
            "state":       state,
            "source_url":  _MEMBER_CLUBS_URL,
        })

    logger.info("[Heartland custom] %d member clubs found", len(records))
    return records
