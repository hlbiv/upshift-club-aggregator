"""
State Association extractor for all 54 USYS tier-4 state associations.

Strategy (in priority order):
1. GotSport event clubs endpoint   — system.gotsport.com/org_event/events/{id}/clubs
2. Google My Maps KML feed         — google.com/maps/d/kml?forcekml=1&mid={id}
3. unknown / no data               — returns [] with a warning

All data sources are declared in data/state_assoc_config.json, keyed by the
canonical URL listed in leagues_master.csv (no trailing slash).

Coverage (as of research pass):
  GotSport    (19 states): AL, CA-North, DE, FL, GA, ID, KS, KY, ME, MI, NH,
                           NM, NY-West, OH, OK, VT, VA, KY, KS
  Google Maps ( 6 states): CT, Eastern PA, IN, MO, TN, TX-South
  Unknown     (29 states): AK, AZ, AR, CA-South, CO, E-NY, HI, IL, IA, LA,
                           MD, MA, MN, MS, MT, NE, NV, NJ, NC, ND, NT, OR,
                           PA-West, RI, SC, SD, UT, WA, WV, WI, WY
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from config import FUZZY_THRESHOLD
from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event
from normalizer import _canonical
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0)"}

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "state_assoc_config.json")
with open(_CONFIG_PATH) as _f:
    _STATE_CONFIG: dict = json.load(_f)

_CONFIG_BY_DOMAIN: dict[str, dict] = {}
for _url, _cfg in _STATE_CONFIG.items():
    _domain = _url.rstrip("/").replace("https://", "").replace("http://", "")
    _CONFIG_BY_DOMAIN[_domain] = {**_cfg, "_url": _url}


def _lookup_config(url: str) -> dict | None:
    url_clean = url.rstrip("/")
    if url_clean in _STATE_CONFIG:
        return _STATE_CONFIG[url_clean]
    domain = url_clean.replace("https://", "").replace("http://", "").lstrip("www.")
    for key, cfg in _STATE_CONFIG.items():
        if key.rstrip("/").replace("https://", "").replace("http://", "").lstrip("www.") == domain:
            return cfg
    return None


def _multi_event_dedup(clubs_list: List[Dict]) -> List[Dict]:
    """Deduplicate across multiple GotSport events using fuzzy matching."""
    seen_canonical: list[str] = []
    out: List[Dict] = []
    for club in clubs_list:
        canon = _canonical(club["club_name"])
        is_dup = False
        for seen in seen_canonical:
            if fuzz.token_sort_ratio(canon, seen) >= FUZZY_THRESHOLD:
                is_dup = True
                break
        if not is_dup:
            seen_canonical.append(canon)
            out.append(club)
    return out


def _scrape_gotsport(event_ids: List[int], league_name: str, state: str) -> List[Dict]:
    raw: List[Dict] = []
    for eid in event_ids:
        clubs = scrape_gotsport_event(eid, league_name, state=state)
        raw.extend(clubs)
        logger.info("  GotSport event %s: %d clubs", eid, len(clubs))
    return _multi_event_dedup(raw)


def _scrape_google_maps(map_ids: List[str], league_name: str, state: str) -> List[Dict]:
    """Fetch Google My Maps KML and extract place names as club names."""
    raw: List[Dict] = []
    SKIP_PHRASES = {"layer", "sheet", "csv", "directory", "map", "find a place", "member"}

    for mid in map_ids:
        url = f"https://www.google.com/maps/d/kml?forcekml=1&mid={mid}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                logger.warning("Google Maps KML %s returned %s", mid, r.status_code)
                continue

            names = re.findall(r"<name>([^<]+)</name>", r.text)
            for raw_name in names:
                name = raw_name.strip()
                if len(name) < 4 or len(name) > 100:
                    continue
                lower = name.lower()
                if any(phrase in lower for phrase in SKIP_PHRASES):
                    continue
                raw.append({
                    "club_name": name,
                    "league_name": league_name,
                    "city": "",
                    "state": state,
                    "source_url": url,
                })
            logger.info("  Google Maps KML %s: %d raw places", mid, len(raw))
        except Exception as exc:
            logger.warning("Google Maps KML %s error: %s", mid, exc)

    return _multi_event_dedup(raw)


def _scrape_state(url: str, league_name: str) -> List[Dict]:
    cfg = _lookup_config(url)
    if not cfg:
        logger.warning("No state_assoc_config entry for URL: %s — skipping", url)
        return []

    state = cfg.get("state", "")
    src_type = cfg.get("type", "unknown")
    logger.info("State: %s | source: %s", state, src_type)

    if src_type == "gotsport":
        event_ids = [int(e) for e in cfg.get("events", [])]
        return _scrape_gotsport(event_ids, league_name, state)

    if src_type == "google_maps":
        map_ids = cfg.get("map_ids", [])
        return _scrape_google_maps(map_ids, league_name, state)

    logger.info("No automated source for %s (%s) — skipping", state, url)
    return []


_STATE_PATTERN = "|".join(
    re.escape(url.replace("https://", "").replace("http://", ""))
    for url in _STATE_CONFIG
)


@register(_STATE_PATTERN)
def scrape_state_association(url: str, league_name: str) -> List[Dict]:
    """Dispatch to the appropriate sub-scraper based on state_assoc_config.json."""
    return _scrape_state(url, league_name)
