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

# Strip " ECNL B13Qualification:..." or " ECNL RL G12..." or " - PRE ECNL B13..." suffix.
# Handles all observed formats:
#   "Oregon Premier ECNL B13"
#   "Bloomingdale Lightning FC Pre-ECNL B13"
#   "Chicago Magic  - PRE ECNL B13"   (space-dash-space before PRE)
#   "Fort Lauderdale United FC ECNL RL G13"
_CLUB_RE = re.compile(
    r"^(.+?)\s+(?:-\s+)?(?:Pre[-\s]+)?ECNL(?:\s+RL)?\s+[BG]\d+",
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


_TEAM_AGE_RE = re.compile(r"[BG](\d{2})\b")
_GENDER_MAP = {"B": "Male", "G": "Female"}

# Columns present in the standings table (no header row rendered)
# Order: rank, team_name, gp, w, l, d, gf, ga, gd, ppg, pts
_STANDING_COLS = ["rank", "team_name_raw", "gp", "w", "l", "d", "gf", "ga", "gd", "ppg", "pts"]


def _parse_team_name(raw: str) -> Tuple[str, str, str, str]:
    """
    Parse a raw cell like 'Florida Kraze ECNL B13 Qualification: Champions League 28'.

    Returns (club_name, age_group, gender_letter, qualification).
    """
    qual_split = re.split(r"\s*Qualification:\s*", raw, maxsplit=1)
    base = qual_split[0].strip()
    qualification = qual_split[1].strip() if len(qual_split) > 1 else ""

    m = _CLUB_RE.match(base)
    if not m:
        return "", "", "", qualification

    club_name = m.group(1).strip()
    suffix = base[len(club_name):].strip()
    age_m = _TEAM_AGE_RE.search(suffix)
    age_group = age_m.group(1) if age_m else ""
    gender_letter = "B" if re.search(r"\bB\d{2}\b", suffix) else ("G" if re.search(r"\bG\d{2}\b", suffix) else "")

    return club_name, age_group, gender_letter, qualification


def _fetch_clubs_for_event(
    org_season_id: int | str,
    event_id: str,
    conf_name: str = "",
) -> Tuple[List[str], List[Dict]]:
    """
    Fetch one conference's standings.

    Returns:
        (club_names, team_records)
        club_names  — unique base club names (for the existing club-level pipeline)
        team_records — one record per team row with full standings data
    """
    url = _api_url(org_season_id, event_id)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 100:
            return [], []
    except Exception as exc:
        logger.debug("Fetch failed (org_season=%s event=%s): %s", org_season_id, event_id, exc)
        return [], []

    soup = BeautifulSoup(r.text, "lxml")
    clubs: set[str] = set()
    team_records: List[Dict] = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue

        raw_name = tds[1].get_text(separator=" ", strip=True)
        m = _CLUB_RE.match(raw_name)
        if not m:
            continue

        club_name = m.group(1).strip()
        if not (_MIN < len(club_name) <= _MAX):
            continue

        clubs.add(club_name)

        club, age_group, gender_letter, qualification = _parse_team_name(raw_name)
        gender = _GENDER_MAP.get(gender_letter, "")

        # Extract AthleteOne IDs from the individual-team-item span
        span = tds[1].find("span", class_="individual-team-item")
        club_id = span.get("data-club-id", "") if span else ""
        team_id = span.get("data-team-id", "") if span else ""
        # data-event-id on the span equals the conference event_id we already have

        def _td(i: int) -> str:
            return tds[i].get_text(strip=True) if i < len(tds) else ""

        team_records.append({
            "club_name":     club or club_name,
            "team_name_raw": raw_name,
            "age_group":     age_group,
            "gender":        gender,
            "conference":    conf_name,
            "org_season_id": str(org_season_id),
            "event_id":      event_id,
            "club_id":       club_id,
            "team_id":       team_id,
            "qualification": qualification,
            "rank":  _td(0),
            "gp":    _td(2),
            "w":     _td(3),
            "l":     _td(4),
            "d":     _td(5),
            "gf":    _td(6),
            "ga":    _td(7),
            "gd":    _td(8),
            "ppg":   _td(9),
            "pts":   _td(10),
            "source_url": url,
        })

    return list(clubs), team_records


def _scrape_org_seasons(
    org_season_ids: List[int],
    league_name: str,
    source_url: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Scrape one or more org_seasons, collecting all clubs and team standings
    across every conference. Uses dynamic conference discovery and concurrent fetches.

    Returns:
        (club_records, team_records)
    """
    all_events: List[Tuple[str, str, str]] = []
    for org_season_id in org_season_ids:
        for event_id, conf_name in _get_conference_event_ids(org_season_id):
            all_events.append((str(org_season_id), event_id, conf_name))

    if not all_events:
        logger.error("No conferences discovered for org_seasons=%s", org_season_ids)
        return [], []

    logger.info("[ECNL API] Fetching %d conferences across %d org_seasons",
                len(all_events), len(org_season_ids))

    all_clubs: set[str] = set()
    all_team_records: List[Dict] = []

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {
            ex.submit(_fetch_clubs_for_event, org_season_id, event_id, conf_name):
                (org_season_id, event_id, conf_name)
            for org_season_id, event_id, conf_name in all_events
        }
        for f in as_completed(futs):
            org_season_id, event_id, conf_name = futs[f]
            clubs, team_records = f.result()
            logger.debug("  %s (event=%s) → %d clubs, %d teams",
                         conf_name, event_id, len(clubs), len(team_records))
            all_clubs.update(clubs)
            for rec in team_records:
                rec["league_name"] = league_name
            all_team_records.extend(team_records)

    logger.info("[ECNL API] Total unique clubs: %d  |  total team rows: %d",
                len(all_clubs), len(all_team_records))

    club_records = [
        {
            "club_name": c,
            "league_name": league_name,
            "city": "",
            "state": "",
            "source_url": source_url,
        }
        for c in sorted(all_clubs)
    ]
    return club_records, all_team_records


# ---------------------------------------------------------------------------
# Registered extractors
# ---------------------------------------------------------------------------

def _scrape_and_save_teams(org_season_ids: List[int], league_name: str, url: str) -> List[Dict]:
    """
    Shared wrapper: scrapes clubs + team standings, saves teams CSV as a side
    effect (no extra HTTP requests needed), returns club records for the main
    pipeline.
    """
    from storage import save_teams_csv  # imported here to avoid circular issues at module load

    club_records, team_records = _scrape_org_seasons(org_season_ids, league_name, url)

    if team_records:
        save_teams_csv(team_records, league_name)

    return club_records


@register(r"theecnl\.com/sports/directory")
def scrape_ecnl(url: str, league_name: str) -> List[Dict]:
    """ECNL (Boys + Girls) — all 16+10=26 regional conferences."""
    logger.info("[ECNL custom] Scraping Boys + Girls ECNL via AthleteOne API")
    return _scrape_and_save_teams([70, 69], league_name, url)


@register(r"theecnl\.com/sports/ecnl-regional-league")
def scrape_ecnl_rl(url: str, league_name: str) -> List[Dict]:
    """ECNL Regional League — Boys RL (72) or Girls RL (71)."""
    logger.info("[ECNL RL custom] Scraping ECNL RL via AthleteOne API")
    if "boys" in url.lower():
        org_seasons = [72]
    elif "girls" in url.lower():
        org_seasons = [71]
    else:
        org_seasons = [72, 71]
    return _scrape_and_save_teams(org_seasons, league_name, url)


@register(r"theecnl\.com/sports/2024/11/12/Directory")
def scrape_pre_ecnl(url: str, league_name: str) -> List[Dict]:
    """
    Pre-ECNL and Pre-ECNL North — development league one tier below ECNL.

    Org season IDs (org_id=12):
      67 = Pre-ECNL Boys     (11 conferences, older age groups)
      66 = Pre-ECNL Girls    ( 9 conferences, older age groups)
      76 = Pre-ECNL North Boys  (16 conferences, younger B2015)
      75 = Pre-ECNL North Girls (13 conferences, younger G2015)

    Both "Pre-ECNL Boys/Girls" and "Pre-ECNL North Boys/Girls" share the
    same directory URL, so we dispatch based on league_name.

    Team name formats observed:
      "Bloomingdale Lightning FC Pre-ECNL B13"
      "Chicago Magic  - PRE ECNL B13"
    Both are handled by the updated _CLUB_RE.
    """
    name_lower = league_name.lower()
    logger.info("[Pre-ECNL custom] Scraping %s via AthleteOne API", league_name)

    if "north" in name_lower and "boys" in name_lower:
        org_seasons = [76]
    elif "north" in name_lower and "girls" in name_lower:
        org_seasons = [75]
    elif "boys" in name_lower:
        org_seasons = [67]
    elif "girls" in name_lower:
        org_seasons = [66]
    else:
        # Scrape all four Pre-ECNL org_seasons
        org_seasons = [67, 66, 76, 75]

    return _scrape_and_save_teams(org_seasons, league_name, url)
