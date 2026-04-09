"""
Custom extractor for ECNL (Elite Club National League) and ECNL Regional League.

Data source: AthleteOne standings API (api.athleteone.com), which backs the TGS
widget embedded in theecnl.com.

Discovery mechanism (2025-26 season):
  The API endpoint /get-conference-standings/0/{org_id}/{org_season_id}/0/0
  returns an HTML page that includes a full <select id="event-select"> dropdown
  listing EVERY conference for that org_season (with their event_ids). We parse
  that dropdown to get all event_ids, then fetch each conference's standings to
  collect team names.

Org season IDs (org_id=12, current season):
  69 → Girls ECNL    (10 conferences: Mid-Atlantic, Midwest, New England, ...)
  70 → Boys ECNL     (16 conferences: Far West, Florida, Heartland, ...)
  71 → Girls RL      (24 conferences: Carolinas, Florida, Frontier, ...)
  72 → Boys RL       (26 conferences: Carolinas, Chicago Metro, Far West, ...)

Team name format in standings: "Oregon Premier ECNL B13Qualification:..."
Club name extraction: strip " ECNL [BG]YY..." suffix.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://theecnl.com/",
    "Accept": "*/*",
}

_BASE = "https://api.athleteone.com/api/Script/get-conference-standings"
_ORG_ID = 12

# Strip " ECNL B13Qualification:..." or " ECNL RL G12..." suffix
_CLUB_RE = re.compile(
    r"^(.+?)\s+(?:Pre-)?ECNL(?:\s+RL)?\s+[BG]\d+",
    re.IGNORECASE,
)

_MIN = 3
_MAX = 80


def _api_url(org_season_id: int | str, event_id: int | str = 0) -> str:
    # Correct order: /{event_id}/{org_id}/{org_season_id}/0/0
    # event_id=0 returns default conference + full dropdown listing all conference event_ids
    return f"{_BASE}/{event_id}/{_ORG_ID}/{org_season_id}/0/0"


def _get_conference_event_ids(org_season_id: int | str) -> List[Tuple[str, str]]:
    """
    Call the API with event_id=0 to get the full list of conference event IDs.
    Returns list of (event_id, conference_name) tuples.
    """
    url = _api_url(org_season_id, 0)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200 or len(r.text) < 100:
            logger.warning("Conference list fetch failed for org_season=%s: status=%d",
                           org_season_id, r.status_code)
            return []
    except Exception as exc:
        logger.error("Conference list fetch exception (org_season=%s): %s", org_season_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    event_sel = soup.find("select", id="event-select")
    if not event_sel:
        logger.warning("No event-select found in response for org_season=%s", org_season_id)
        return []

    events = []
    for opt in event_sel.find_all("option"):
        val = opt.get("value", "").strip()
        txt = opt.get_text(strip=True)
        if val and val != "0":
            events.append((val, txt))

    logger.info("org_season=%s → %d conferences discovered", org_season_id, len(events))
    return events


def _fetch_clubs_for_event(org_season_id: int | str, event_id: str) -> List[str]:
    """Fetch one conference's standings and return unique base club names."""
    url = _api_url(org_season_id, event_id)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 100:
            return []
    except Exception as exc:
        logger.debug("Fetch failed (org_season=%s event=%s): %s", org_season_id, event_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    clubs: set[str] = set()
    for td in soup.find_all("td"):
        text = td.get_text(separator=" ", strip=True)
        m = _CLUB_RE.match(text)
        if m:
            club = m.group(1).strip()
            if _MIN < len(club) <= _MAX:
                clubs.add(club)
    return list(clubs)


def _scrape_org_seasons(
    org_season_ids: List[int],
    league_name: str,
    source_url: str,
) -> List[Dict]:
    """
    Scrape one or more org_seasons, collecting all clubs across every conference.
    Uses dynamic conference discovery and concurrent fetches.
    """
    # Step 1: discover all conference event_ids for each org_season
    all_events: List[Tuple[str, str, str]] = []  # (org_season_id, event_id, conf_name)
    for org_season_id in org_season_ids:
        for event_id, conf_name in _get_conference_event_ids(org_season_id):
            all_events.append((str(org_season_id), event_id, conf_name))

    if not all_events:
        logger.error("No conferences discovered for org_seasons=%s", org_season_ids)
        return []

    logger.info("[ECNL API] Fetching %d conferences across %d org_seasons",
                len(all_events), len(org_season_ids))

    # Step 2: fetch all conferences concurrently
    all_clubs: set[str] = set()
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {
            ex.submit(_fetch_clubs_for_event, org_season_id, event_id): (org_season_id, event_id, conf_name)
            for org_season_id, event_id, conf_name in all_events
        }
        for f in as_completed(futs):
            org_season_id, event_id, conf_name = futs[f]
            clubs = f.result()
            logger.debug("  %s (event=%s) → %d clubs", conf_name, event_id, len(clubs))
            all_clubs.update(clubs)

    logger.info("[ECNL API] Total unique clubs: %d", len(all_clubs))

    return [
        {
            "club_name": c,
            "league_name": league_name,
            "city": "",
            "state": "",
            "source_url": source_url,
        }
        for c in sorted(all_clubs)
    ]


# ---------------------------------------------------------------------------
# Registered extractors
# ---------------------------------------------------------------------------

@register(r"theecnl\.com/sports/directory")
def scrape_ecnl(url: str, league_name: str) -> List[Dict]:
    """ECNL (Boys + Girls) — all 16+10=26 regional conferences."""
    logger.info("[ECNL custom] Scraping Boys + Girls ECNL via AthleteOne API")
    # org_season 70 = Boys ECNL, 69 = Girls ECNL
    return _scrape_org_seasons([70, 69], league_name, url)


@register(r"theecnl\.com/sports/ecnl-regional-league")
def scrape_ecnl_rl(url: str, league_name: str) -> List[Dict]:
    """ECNL Regional League — Boys RL (72) or Girls RL (71)."""
    logger.info("[ECNL RL custom] Scraping ECNL RL via AthleteOne API")
    # org_season 72 = Boys RL, 71 = Girls RL
    if "boys" in url.lower():
        org_seasons = [72]
    elif "girls" in url.lower():
        org_seasons = [71]
    else:
        org_seasons = [72, 71]
    return _scrape_org_seasons(org_seasons, league_name, url)
