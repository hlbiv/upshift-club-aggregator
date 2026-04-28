"""
AthleteOne match/schedule extractor — ECNL family.

Fetches match results from the AthleteOne API that backs ECNL, ECNL RL,
and Pre-ECNL league scheduling.

DISCOVERY FLOW:
  1. get-conference-standings/0/{org_id}/{org_season_id}/0/0
       → conference event_ids + names (select#event-select options)
  2. get-division-list-by-event-id/{org_id}/{event_id}/0/0
       → division_ids for each conference (select#division-select options)
  3. get-conference-schedules/{org_id}/{org_season_id}/{event_id}/{division_id}/0
       → HTML table of match rows for that conference + division

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

ALL_ORG_SEASONS = list(ORG_SEASON_MAP.keys())

_SHOWCASE_KEYWORDS = re.compile(
    r"\b(national event|showcase|cup|fest|invitational|classic)\b",
    re.IGNORECASE,
)

_AGE_RE = re.compile(r"\b[BG](\d{2})\b")
_GENDER_LETTER = re.compile(r"\b([BG])\d{2}\b")
_SCORE_RE = re.compile(r"^(\d+)$")

_DATE_FORMATS = [
    "%b %d, %Y %I:%M %p",   # "Aug 17, 2025 11:00 AM"
    "%b %d, %Y",             # "Aug 17, 2025"
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
]


def _get_conference_event_ids(org_season_id: int) -> List[Tuple[str, str]]:
    """Return list of (event_id, conference_name) for an org_season."""
    url = f"{_BASE}/get-conference-standings/0/{_ORG_ID}/{org_season_id}/0/0"
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


def _get_division_ids(event_id: str) -> List[Tuple[str, str]]:
    """Return list of (division_id, division_label) for a conference event."""
    url = f"{_BASE}/get-division-list-by-event-id/{_ORG_ID}/{event_id}/0/0"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 10:
            return []
    except Exception as exc:
        logger.debug("[AthleteOne matches] division list failed event=%s: %s", event_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    # Try select#division-select first, then any select with division options.
    sel = soup.find("select", id="division-select") or soup.find("select")
    if not sel:
        return []

    divisions: List[Tuple[str, str]] = []
    for opt in sel.find_all("option"):
        val = opt.get("value", "").strip()
        txt = opt.get_text(strip=True)
        if val and val != "0":
            divisions.append((val, txt))

    return divisions


def _parse_score(text: str) -> Optional[int]:
    m = _SCORE_RE.match(text.strip())
    return int(m.group(1)) if m else None


def _parse_date(text: str) -> Optional[datetime]:
    text = re.sub(r"\s+", " ", text.strip())
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_age_group(raw_name: str) -> Tuple[Optional[str], Optional[str]]:
    age_m = _AGE_RE.search(raw_name)
    gender_m = _GENDER_LETTER.search(raw_name)
    age_group = f"U{age_m.group(1)}" if age_m else None
    gender = "M" if (gender_m and gender_m.group(1) == "B") else (
        "G" if (gender_m and gender_m.group(1) == "G") else None
    )
    return age_group, gender


_ECNL_SUFFIX_RE = re.compile(
    r"\s+(?:-\s+)?(?:Pre[-\s]+)?ECNL(?:\s+RL)?\s+[BG]\d+.*$",
    re.IGNORECASE,
)


def _strip_ecnl_suffix(raw: str) -> str:
    return _ECNL_SUFFIX_RE.sub("", raw).strip()


def _fetch_schedule(
    org_season_id: int,
    event_id: str,
    division_id: str,
    division_label: str,
    conf_name: str,
    league_name: str,
    season: Optional[str],
) -> List[Dict]:
    url = f"{_BASE}/get-conference-schedules/{_ORG_ID}/{org_season_id}/{event_id}/{division_id}/0"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200 or len(r.text) < 50:
            logger.debug("[AthleteOne matches] empty response event=%s div=%s status=%d",
                         event_id, division_id, r.status_code)
            return []
    except Exception as exc:
        logger.debug("[AthleteOne matches] fetch failed event=%s div=%s: %s",
                     event_id, division_id, exc)
        return []

    return _parse_schedule_html(
        r.text,
        source_url=url,
        league_name=league_name,
        conf_name=conf_name,
        division_label=division_label,
        season=season,
    )


def _parse_schedule_html(
    html: str,
    source_url: str,
    league_name: str,
    conf_name: str,
    division_label: str,
    season: Optional[str],
) -> List[Dict]:
    """
    Parse AthleteOne get-conference-schedules HTML.

    Observed table structure (theecnl.com, April 2026):
      td[0] = GM# (game number = platform_match_id)
      td[1] = GAME INFO: date + time text, division label (multiline)
      td[2] = TEAM & VENUE: two team name elements stacked, then venue
      td[3] = DETAILS: home score, away score (separate elements), "Box Score" link

    Age group / gender are extracted from team names in td[2].
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []
    is_showcase = bool(_SHOWCASE_KEYWORDS.search(conf_name))

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            # td[0]: GM# / game number
            gm_text = tds[0].get_text(strip=True)
            if not gm_text or not gm_text.isdigit():
                continue
            platform_match_id = gm_text

            # td[1]: GAME INFO — date/time on first line(s), division label after
            info_lines = [s.strip() for s in tds[1].get_text(separator="\n").splitlines() if s.strip()]
            date_text = ""
            for line in info_lines:
                # Stop at the division label line (contains "ECNL", "RL", etc.)
                if re.search(r"ECNL|Pre-ECNL|RL", line, re.IGNORECASE):
                    break
                if date_text:
                    date_text += " " + line
                else:
                    date_text = line
            match_date = _parse_date(date_text) if date_text else None

            # td[2]: TEAM & VENUE — team names always contain "ECNL" (e.g. "Eugene Metro FC ECNL B11")
            # venue links do not (e.g. "Bob Keefer - Field 1"), so filter by that.
            team_cell = tds[2]
            cell_lines = [s.strip() for s in team_cell.get_text(separator="\n").splitlines()
                          if s.strip()]
            ecnl_lines = [l for l in cell_lines if re.search(r"ECNL|Pre.?ECNL", l, re.IGNORECASE)]
            if len(ecnl_lines) >= 2:
                home_raw = ecnl_lines[0]
                away_raw = ecnl_lines[1]
            elif len(cell_lines) >= 2:
                home_raw = cell_lines[0]
                away_raw = cell_lines[1]
            else:
                continue

            if not home_raw or not away_raw:
                continue

            # td[3]: DETAILS — scores
            home_score: Optional[int] = None
            away_score: Optional[int] = None
            if len(tds) >= 4:
                score_lines = [s.strip() for s in tds[3].get_text(separator="\n").splitlines()
                               if s.strip() and s.strip().isdigit()]
                if len(score_lines) >= 2:
                    home_score = _parse_score(score_lines[0])
                    away_score = _parse_score(score_lines[1])

            status = "final" if (home_score is not None and away_score is not None) else "scheduled"
            age_group, gender = _parse_age_group(home_raw)
            home_team = _strip_ecnl_suffix(home_raw) or home_raw
            away_team = _strip_ecnl_suffix(away_raw) or away_raw

            row: Dict = {
                "home_team_name":    home_team,
                "away_team_name":    away_team,
                "home_score":        home_score,
                "away_score":        away_score,
                "match_date":        match_date,
                "age_group":         age_group or division_label,
                "gender":            gender,
                "division":          conf_name,
                "season":            season,
                "status":            status,
                "source":            "athleteone",
                "source_url":        source_url,
                "platform_match_id": platform_match_id,
                "league":            league_name,
            }
            if is_showcase:
                row["tournament_name"] = conf_name
                row["match_type"] = "group"

            rows.append(row)

    return rows


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
    """
    if org_season_ids is None:
        org_season_ids = ALL_ORG_SEASONS

    # Build work items: (org_season_id, event_id, division_id, division_label, conf_name, league_name)
    work: List[Tuple[int, str, str, str, str, str]] = []
    for org_season_id in org_season_ids:
        league_name, _gender, _is_tourn = ORG_SEASON_MAP.get(
            org_season_id, (f"ECNL org_season={org_season_id}", "", False)
        )
        for event_id, conf_name in _get_conference_event_ids(org_season_id):
            divisions = _get_division_ids(event_id)
            if not divisions:
                # Fall back: try with division_id=0 (returns all divisions together)
                work.append((org_season_id, event_id, "0", "", conf_name, league_name))
            else:
                for div_id, div_label in divisions:
                    work.append((org_season_id, event_id, div_id, div_label, conf_name, league_name))

    if not work:
        logger.error("[AthleteOne matches] no work items for org_seasons=%s", org_season_ids)
        return [], []

    logger.info("[AthleteOne matches] fetching %d conference×division schedules", len(work))

    league_rows: List[Dict] = []
    tournament_rows: List[Dict] = []

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {
            ex.submit(
                _fetch_schedule,
                org_season_id, event_id, div_id, div_label, conf_name, league_name, season
            ): (conf_name, div_label)
            for org_season_id, event_id, div_id, div_label, conf_name, league_name in work
        }
        for f in as_completed(futs):
            conf_name, div_label = futs[f]
            rows = f.result()
            for row in rows:
                if row.get("tournament_name"):
                    tournament_rows.append(row)
                else:
                    league_rows.append(row)
            if rows:
                logger.debug("[AthleteOne matches] %s %s → %d rows", conf_name, div_label, len(rows))

    logger.info(
        "[AthleteOne matches] total league=%d tournament=%d",
        len(league_rows), len(tournament_rows),
    )
    return league_rows, tournament_rows
