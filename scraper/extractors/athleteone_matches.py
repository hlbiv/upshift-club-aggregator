"""
AthleteOne match/schedule extractor — ECNL family.

Fetches match results from the AthleteOne API that backs ECNL, ECNL RL,
and Pre-ECNL league scheduling.

DISCOVERY FLOW:
  1. Call /get-conference-standings/0/{org_id}/{org_season_id}/0/0 to get all
     conference event_ids (same endpoint used by ecnl.py for club discovery).
  2. For each conference event_id, call the schedule endpoint to get match rows.

SCHEDULE ENDPOINT (speculative — validate on first run):
  https://api.athleteone.com/api/Script/get-event-schedule/{event_id}/{org_id}/{org_season_id}/0/0

  If this endpoint 404s or returns no data, check the DevTools network panel
  on theecnl.com while navigating to a conference schedule page to identify
  the actual endpoint path.

ORG_ID = 12 for all ECNL org_seasons.

ORG_SEASON_IDS (current 2025-26 season):
  69 = ECNL Girls           (Tier 1)
  70 = ECNL Boys            (Tier 1)
  71 = ECNL RL Girls        (Tier 2)
  72 = ECNL RL Boys         (Tier 2)
  66 = Pre-ECNL Girls       (Tier 2)
  67 = Pre-ECNL Boys        (Tier 2)
  75 = Pre-ECNL North Girls (Tier 2)
  76 = Pre-ECNL North Boys  (Tier 2)

OUTPUT:
  League conference matches → ``matches`` table (via matches_writer.insert_matches)
  Showcase / national event matches → ``tournament_matches`` table
  (distinction made by inspecting event_type field in API response if present,
   otherwise defaults to league for all ECNL regular-season conferences)

NOTE: This extractor is a first-pass implementation based on the AthleteOne
API patterns from ecnl.py. Validate the schedule endpoint URL on first run:
  python3 run.py --source athleteone-matches --dry-run
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://theecnl.com/",
    "Accept": "*/*",
}

_BASE = "https://api.athleteone.com/api/Script"
_ORG_ID = 12

# org_season_id → (league_name, gender, is_tournament_league)
ORG_SEASON_MAP: Dict[int, Tuple[str, str, bool]] = {
    70: ("ECNL Boys", "M", False),
    69: ("ECNL Girls", "G", False),
    72: ("ECNL Regional League Boys", "M", False),
    71: ("ECNL Regional League Girls", "G", False),
    67: ("Pre-ECNL Boys", "M", False),
    66: ("Pre-ECNL Girls", "G", False),
    76: ("Pre-ECNL North Boys", "M", False),
    75: ("Pre-ECNL North Girls", "G", False),
}

# All org_season_ids to scrape by default.
ALL_ORG_SEASONS = list(ORG_SEASON_MAP.keys())

# Conference name hints that indicate a showcase/national event vs regular season.
_SHOWCASE_KEYWORDS = re.compile(
    r"\b(national event|showcase|cup|fest|invitational|classic)\b",
    re.IGNORECASE,
)

_AGE_RE = re.compile(r"\b[BG](\d{2})\b")
_GENDER_LETTER = re.compile(r"\b([BG])\d{2}\b")
_SCORE_RE = re.compile(r"^(\d+)\s*-\s*(\d+)$")

_DATE_FORMATS = [
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]


def _conference_standings_url(org_season_id: int, event_id: int = 0) -> str:
    return f"{_BASE}/get-conference-standings/{event_id}/{_ORG_ID}/{org_season_id}/0/0"


def _schedule_url(org_season_id: int, event_id: int) -> str:
    return f"{_BASE}/get-event-schedule/{event_id}/{_ORG_ID}/{org_season_id}/0/0"


def _get_conference_event_ids(org_season_id: int) -> List[Tuple[str, str]]:
    """Return list of (event_id, conference_name) for an org_season."""
    url = _conference_standings_url(org_season_id, 0)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200 or len(r.text) < 100:
            logger.warning("[AthleteOne matches] conference list failed org_season=%s status=%d",
                           org_season_id, r.status_code)
            return []
    except Exception as exc:
        logger.error("[AthleteOne matches] conference list exception org_season=%s: %s",
                     org_season_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    event_sel = soup.find("select", id="event-select")
    if not event_sel:
        logger.warning("[AthleteOne matches] no event-select in response org_season=%s", org_season_id)
        return []

    events: List[Tuple[str, str]] = []
    for opt in event_sel.find_all("option"):
        val = opt.get("value", "").strip()
        txt = opt.get_text(strip=True)
        if val and val != "0":
            events.append((val, txt))

    logger.info("[AthleteOne matches] org_season=%s → %d conferences", org_season_id, len(events))
    return events


def _parse_score(text: str) -> Tuple[Optional[int], Optional[int]]:
    m = _SCORE_RE.match(text.strip())
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _parse_date(text: str) -> Optional[datetime]:
    text = text.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_age_group(raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract age group (e.g. 'U13') and gender letter from ECNL team name."""
    age_m = _AGE_RE.search(raw_name)
    gender_m = _GENDER_LETTER.search(raw_name)
    age_group = f"U{age_m.group(1)}" if age_m else None
    gender = "M" if (gender_m and gender_m.group(1) == "B") else (
        "G" if (gender_m and gender_m.group(1) == "G") else None
    )
    return age_group, gender


def _fetch_schedule_for_conference(
    org_season_id: int,
    event_id: str,
    conf_name: str,
    league_name: str,
    season: Optional[str],
) -> List[Dict]:
    """
    Fetch match rows for one conference event.

    The AthleteOne schedule endpoint returns HTML. We attempt to parse it as
    a table of match rows. If the endpoint returns empty or unexpected HTML,
    we log a warning and return [] — the caller continues to the next conference.
    """
    url = _schedule_url(org_season_id, int(event_id))
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 50:
            logger.debug("[AthleteOne matches] schedule empty org_season=%s event=%s status=%d",
                         org_season_id, event_id, r.status_code)
            return []
    except Exception as exc:
        logger.debug("[AthleteOne matches] schedule fetch failed event=%s: %s", event_id, exc)
        return []

    return _parse_schedule_html(
        r.text,
        source_url=url,
        league_name=league_name,
        conf_name=conf_name,
        org_season_id=org_season_id,
        event_id=event_id,
        season=season,
    )


def _parse_schedule_html(
    html: str,
    source_url: str,
    league_name: str,
    conf_name: str,
    org_season_id: int,
    event_id: str,
    season: Optional[str],
) -> List[Dict]:
    """
    Parse AthleteOne schedule HTML into match row dicts.

    AthleteOne schedule pages vary by season — this parser tries the most
    common table structure. If it returns 0 rows on first run, inspect the
    raw HTML via --dry-run and adjust the column mapping below.

    Expected columns (subject to validation):
      td[0] = Match date/time
      td[1] = Home team name (with ECNL suffix, e.g. "Concorde Fire ECNL B15")
      td[2] = Score or "vs"
      td[3] = Away team name
      td[4] = Venue/field (optional)
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []

    is_showcase = bool(_SHOWCASE_KEYWORDS.search(conf_name))

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            col = [td.get_text(separator=" ", strip=True) for td in tds]

            # Skip header rows.
            if any(kw in col[0].lower() for kw in ("date", "time", "home", "match")):
                continue

            date_text = col[0]
            home_raw = col[1] if len(col) > 1 else ""
            score_text = col[2] if len(col) > 2 else ""
            away_raw = col[3] if len(col) > 3 else ""

            if not home_raw or not away_raw:
                continue

            score_lower = score_text.lower().strip()
            is_played = score_lower not in ("vs", "v", "", "-", "tbd")
            home_score, away_score = _parse_score(score_text) if is_played else (None, None)
            status = "final" if home_score is not None else "scheduled"

            match_date = _parse_date(date_text)
            age_group, gender = _parse_age_group(home_raw)

            # Strip ECNL suffix from team name for storage.
            # e.g. "Concorde Fire ECNL B15" → "Concorde Fire"
            home_team = _strip_ecnl_suffix(home_raw)
            away_team = _strip_ecnl_suffix(away_raw)

            # platform_match_id from data attribute on the row (if present).
            platform_match_id = tr.get("data-match-id") or tr.get("data-game-id")

            row: Dict = {
                "home_team_name":   home_team or home_raw,
                "away_team_name":   away_team or away_raw,
                "home_score":       home_score,
                "away_score":       away_score,
                "match_date":       match_date,
                "age_group":        age_group,
                "gender":           gender,
                "division":         conf_name,
                "season":           season,
                "status":           status,
                "source":           "athleteone",
                "source_url":       source_url,
                "platform_match_id": platform_match_id,
                "league":           league_name,
            }
            if is_showcase:
                row["tournament_name"] = conf_name
                row["match_type"] = "group"

            rows.append(row)

    return rows


_ECNL_SUFFIX_RE = re.compile(
    r"\s+(?:-\s+)?(?:Pre[-\s]+)?ECNL(?:\s+RL)?\s+[BG]\d+.*$",
    re.IGNORECASE,
)


def _strip_ecnl_suffix(raw: str) -> str:
    return _ECNL_SUFFIX_RE.sub("", raw).strip()


def scrape_athleteone_matches(
    org_season_ids: Optional[List[int]] = None,
    season: Optional[str] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Scrape all match rows across ECNL org_seasons.

    Args:
        org_season_ids: org_season_ids to scrape (default: ALL_ORG_SEASONS)
        season:         Season tag e.g. "2025-26"

    Returns:
        (league_rows, tournament_rows)
        league_rows      — for matches_writer.insert_matches()
        tournament_rows  — for tournament_matches_writer.insert_tournament_matches()
    """
    if org_season_ids is None:
        org_season_ids = ALL_ORG_SEASONS

    # Collect all (org_season_id, event_id, conf_name, league_name) work items.
    work: List[Tuple[int, str, str, str]] = []
    for org_season_id in org_season_ids:
        league_name, _gender, _is_tourn = ORG_SEASON_MAP.get(
            org_season_id, (f"ECNL org_season={org_season_id}", "", False)
        )
        for event_id, conf_name in _get_conference_event_ids(org_season_id):
            work.append((org_season_id, event_id, conf_name, league_name))

    if not work:
        logger.error("[AthleteOne matches] no conferences discovered for org_seasons=%s",
                     org_season_ids)
        return [], []

    logger.info("[AthleteOne matches] fetching schedules for %d conferences", len(work))

    league_rows: List[Dict] = []
    tournament_rows: List[Dict] = []

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {
            ex.submit(
                _fetch_schedule_for_conference,
                org_season_id, event_id, conf_name, league_name, season
            ): (org_season_id, event_id, conf_name)
            for org_season_id, event_id, conf_name, league_name in work
        }
        for f in as_completed(futs):
            org_season_id, event_id, conf_name = futs[f]
            rows = f.result()
            for row in rows:
                if row.get("tournament_name"):
                    tournament_rows.append(row)
                else:
                    league_rows.append(row)
            if rows:
                logger.debug("[AthleteOne matches] %s (event=%s) → %d rows",
                             conf_name, event_id, len(rows))

    logger.info(
        "[AthleteOne matches] total league=%d tournament=%d",
        len(league_rows), len(tournament_rows),
    )
    return league_rows, tournament_rows
