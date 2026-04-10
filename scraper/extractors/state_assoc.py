"""
State Association extractor for all 54 USYS tier-4 state associations.

Strategy (in priority order):
1. GotSport event clubs endpoint   — system.gotsport.com/org_event/events/{id}/clubs
2. Google My Maps KML feed         — google.com/maps/d/kml?forcekml=1&mid={id}
3. js_club_list                    — JS variable `const clubs=[{n:'...'}]` on assoc page
4. html_club_list                  — plain-text club list scraped from assoc page HTML
5. no_source_found / unknown       — returns [] with a warning

All data sources are declared in data/state_assoc_config.json, keyed by the
canonical URL listed in leagues_master.csv (no trailing slash).

Coverage (Task #12 complete — April 2026; 5 more states added):
  GotSport    (34 states): AL, AK, AR, AZ, CA-North, CA-South, CO, DE, E-NY,
                           FL, GA, ID, IL, IA, KS, KY, ME, MD, MI, MN, MT,
                           NV, NH, NJ, NM, NT, NY-West, OH, OK, VT, VA,
                           WA, WV, WY
  Google Maps ( 6 states): CT, Eastern PA, IN, MO, TN, TX-South
  JS club list ( 1 state):  NC  (ncsoccer.org/find-my-club/ JS variable)
  HTML club list (2 states): OR (oregonyouthsoccer.org/find-a-club/),
                             PA-West (pawest-soccer.org/club-list/)
  No source   (11 states): HI, LA, MA, MS, NE, ND, RI, SC, SD, UT, WI
    — HI/LA/MA/MS/NE/ND/RI/SC/SD/UT/WI: no public event or Maps found.
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


def _scrape_gotsport(
    event_ids: List[int],
    league_name: str,
    state: str,
    multi_state: bool = False,
) -> List[Dict]:
    raw: List[Dict] = []
    for eid in event_ids:
        clubs = scrape_gotsport_event(eid, league_name, state=state, multi_state=multi_state)
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


def _scrape_js_club_list(page_url: str, js_var: str, league_name: str, state: str) -> List[Dict]:
    """Extract club names from a JavaScript array variable embedded in a webpage.

    Handles the pattern used by NCYSA's find-my-club page:
        const clubs=[{n:'Club Name', lat:..., ...}, ...]

    Args:
        page_url: Full URL of the page containing the JS variable.
        js_var:   Name of the JavaScript variable (e.g. "clubs").
        league_name: League name to tag on each record.
        state:    State name to tag on each record.
    """
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("JS club list page %s returned %s", page_url, r.status_code)
            return []
    except Exception as exc:
        logger.warning("JS club list page %s error: %s", page_url, exc)
        return []

    pattern = rf"const\s+{re.escape(js_var)}\s*=\s*\[(.*?)\];"
    m = re.search(pattern, r.text, re.DOTALL)
    if not m:
        logger.warning("JS variable '%s' not found on %s", js_var, page_url)
        return []

    clubs_js = m.group(0)
    names = re.findall(r"\{n:'([^']+)'", clubs_js)
    if not names:
        logger.warning("No club names found in JS variable '%s' on %s", js_var, page_url)
        return []

    records = []
    for name in names:
        name = name.strip()
        if not name or len(name) < 2:
            continue
        records.append({
            "club_name": name,
            "league_name": league_name,
            "city": "",
            "state": state,
            "source_url": page_url,
        })
    logger.info("  JS club list %s ('%s'): %d clubs", page_url, js_var, len(records))
    return _multi_event_dedup(records)


def _scrape_html_club_list(
    page_url: str,
    skip_phrases: List[str],
    league_name: str,
    state: str,
) -> List[Dict]:
    """Extract club names from plain-text paragraphs/lines in a static HTML page.

    Handles the pattern used by OYSA's find-a-club page and PA West's club-list
    page, where club names appear as plain text within the page's content area.
    Lines that are all-uppercase section headers, very short, or match skip
    phrases are filtered out.

    Args:
        page_url:     Full URL of the HTML page.
        skip_phrases: Lower-cased substrings that disqualify a line as a club name.
        league_name:  League name to tag on each record.
        state:        State name to tag on each record.
    """
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("HTML club list page %s returned %s", page_url, r.status_code)
            return []
    except Exception as exc:
        logger.warning("HTML club list page %s error: %s", page_url, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    content = (
        soup.find("div", class_="entry-content")
        or soup.find("main")
        or soup.find("article")
        or soup.body
    )
    if not content:
        logger.warning("No content area found on %s", page_url)
        return []

    raw_lines = [
        line.strip()
        for line in content.get_text(separator="\n").splitlines()
        if line.strip()
    ]

    _CLUB_KEYWORDS = {
        "fc", "sc", "soccer", "club", "united", "academy", "futbol", "athletic",
        "youth", "sports", "association", "assoc", "football", "warriors", "force",
        "rush", "storm", "elite", "premier", "lightning", "select", "heat", "fire",
        "rangers", "eagles", "hawks", "stars", "united", "knights", "tigers",
        "wolves", "falcons", "thunder", "impact", "fusion", "cosmos", "dynamo",
    }

    records = []
    for line in raw_lines:
        if len(line) < 3 or len(line) > 120:
            continue
        lower = line.lower()
        if any(phrase in lower for phrase in skip_phrases):
            continue
        if line.startswith("–") or line.startswith("-"):
            continue
        if line.isupper():
            lower_line = line.lower()
            if not any(kw in lower_line for kw in _CLUB_KEYWORDS):
                continue
        records.append({
            "club_name": line,
            "league_name": league_name,
            "city": "",
            "state": state,
            "source_url": page_url,
        })

    logger.info("  HTML club list %s: %d raw lines, %d kept", page_url, len(raw_lines), len(records))
    return _multi_event_dedup(records)


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
        multi_state = cfg.get("multi_state", False)
        return _scrape_gotsport(event_ids, league_name, state, multi_state=multi_state)

    if src_type == "google_maps":
        map_ids = cfg.get("map_ids", [])
        return _scrape_google_maps(map_ids, league_name, state)

    if src_type == "js_club_list":
        page_url = cfg.get("page_url", "")
        js_var = cfg.get("js_var", "clubs")
        return _scrape_js_club_list(page_url, js_var, league_name, state)

    if src_type == "html_club_list":
        page_url = cfg.get("page_url", "")
        skip_phrases = cfg.get("skip_phrases", [])
        return _scrape_html_club_list(page_url, skip_phrases, league_name, state)

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
