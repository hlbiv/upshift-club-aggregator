"""
Shared helper for scraping GotSport club roster pages.

GotSport is used by several leagues (SOCAL, MSPSP, etc.) to manage
league events. Each event has a clubs tab at:
  https://system.gotsport.com/org_event/events/{event_id}/clubs

The page renders plain HTML (no JS required). Rows starting with "ZZ-"
are internal admin/SRA placeholder entries and are filtered out.
"""

from __future__ import annotations

import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}


def scrape_gotsport_event(event_id: int | str, league_name: str, state: str = "") -> List[Dict]:
    """
    Fetch all clubs from a GotSport event clubs page.

    Args:
        event_id:    The numeric event ID from the GotSport URL.
        league_name: League name to tag on each record.
        state:       Two-letter state code to inject (empty if multi-state).

    Returns:
        List of club dicts ready for normalizer.
    """
    url = f"https://system.gotsport.com/org_event/events/{event_id}/clubs"
    logger.info("[GotSport] Fetching event %s: %s", event_id, url)

    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("GotSport fetch failed (event_id=%s): %s", event_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records: List[Dict] = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        raw = tds[0].get_text(strip=True)
        club_name = raw.replace("Schedule", "").strip()
        if not club_name or len(club_name) < 2:
            continue
        if club_name.startswith("ZZ-"):
            continue

        records.append({
            "club_name": club_name,
            "league_name": league_name,
            "city": "",
            "state": state,
            "source_url": url,
        })

    logger.info("[GotSport] event %s → %d clubs", event_id, len(records))
    return records
