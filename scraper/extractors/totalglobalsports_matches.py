"""
TotalGlobalSports (TGS) match schedule extractor.

Fetches game schedules from the AthleteOne-backed TGS API at
``api.athleteone.com/api/Event`` and returns rows shaped for
``matches_writer.insert_matches``.

DISCOVERY FLOW
--------------
1. ``/api/Event/get-event-schedule-or-standings/{eventID}``
       → girlsDivAndFlightList + boysDivAndFlightList; each div has a
         flightList of {flightID, flightName, teamsCount, hasActiveSchedule}.

2. For each flightID:
   ``/api/Event/get-team-list-by-flight/{flightID}``
       → list of {clubID, name, teamID, headCoach, ...}

3. Collect unique clubIDs across all flights, then for each:
   ``/api/Event/get-club-schedules-by-eventID-and-clubID/{eventID}/{clubID}``
       → array of game objects; deduplicate across clubs by matchID.

NOTE: The ``get-schedule-list/{eventID}/{divisionID}`` endpoint does not
exist in the TGS API — it 404s. The ``get-schedules-by-flight`` variant
exists but returns empty arrays for league-play events; it appears reserved
for bracket/playoff data. The per-club approach above is the confirmed
production path.

KNOWN TGS EVENT IDs (STXCL NPL, 2025-26):
    3979 — ECNL RL STXCL (current season A)
    3973 — ECNL RL STXCL (current season B)

SCHEDULE RESPONSE FIELD NAMES (confirmed against event 3780, Surf Cup NW):
    matchID / scheduleID / gamenumber  → platform_match_id
    gameDate (ISO "2025-07-18T13:00:00") → match_date
    homeTeam / awayTeam               → team names
    hometeamscore / awayteamscore     → scores (null if unplayed)
    division                          → division label
    status                            → e.g. "On Time"

OUTPUT
------
League play → ``matches`` table via ``matches_writer.insert_matches``.
``home_club_id`` / ``away_club_id`` stay NULL at scrape time (linker resolves
them in a separate pass, same as GotSport).
"""

from __future__ import annotations

import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.retry import retry_with_backoff, TransientError  # noqa: E402

logger = logging.getLogger(__name__)

_API_BASE = "https://api.athleteone.com/api/Event"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
    ),
    "Accept": "application/json",
    "Origin": "https://public.totalglobalsports.com",
    "Referer": "https://public.totalglobalsports.com/",
}
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

MAX_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 2.0
try:
    from config import (  # type: ignore
        MAX_RETRIES as _mr,
        RETRY_BASE_DELAY_SECONDS as _rbd,
    )
    MAX_RETRIES = _mr
    RETRY_BASE_DELAY_SECONDS = _rbd
except ImportError:
    pass

# TGS event IDs for STXCL NPL (South Texas Champions League).
KNOWN_EVENT_IDS = ["3979", "3973"]

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%Y-%m-%d",
]

_SCORE_RE = re.compile(r"^\d+$")
_AGE_RE = re.compile(r"\b[BG](\d{2}|\d{4})\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b([BG])\d{2}", re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS_CODES
    if isinstance(exc, TransientError):
        return True
    return False


def _fetch_json(url: str) -> object:
    def _do() -> object:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        if r.status_code in _RETRYABLE_STATUS_CODES:
            raise TransientError(f"HTTP {r.status_code} from {url}")
        r.raise_for_status()
        return r.json()

    return retry_with_backoff(
        _do,
        retryable_check=_is_retryable,
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
        label=f"tgs-fetch {url}",
    )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_date(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    text = re.sub(r"\s+", " ", str(raw).strip())
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _parse_score(val: object) -> Optional[int]:
    if val is None:
        return None
    s = str(val).strip()
    return int(s) if _SCORE_RE.match(s) else None


def _parse_age_gender(
    division_name: str,
    season: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Infer U-age and gender ('M'/'F') from a TGS division name like 'B2009'."""
    gender_m = _GENDER_RE.search(division_name)
    age_m = _AGE_RE.search(division_name)

    gender: Optional[str] = None
    if gender_m:
        gender = "M" if gender_m.group(1).upper() == "B" else "F"

    age_group: Optional[str] = None
    if age_m:
        token = age_m.group(1)
        birth_year = int(token) if len(token) == 4 else 2000 + int(token)
        if season:
            m = re.match(r"(\d{4})", season)
            if m:
                season_start = int(m.group(1))
                age_group = f"U{max(1, season_start - birth_year + 1)}"

    return age_group, gender


def _strip_brand(team_name: str) -> str:
    """Remove ECNL RL STXCL / STXCL tournament suffix from a team name."""
    cleaned = re.sub(
        r"\s+(ECNL\s+RL\s+STXCL|ECNL\s+RL|STXCL)\b.*$",
        "",
        team_name,
        flags=re.IGNORECASE,
    ).strip()
    return cleaned or team_name


# ---------------------------------------------------------------------------
# Discovery: flights + clubs
# ---------------------------------------------------------------------------

def _get_flights(event_id: str) -> List[Dict]:
    """Fetch flight list via get-event-schedule-or-standings.

    Returns list of dicts with keys: flightID, flightName, divisionName,
    divisionID.
    """
    url = f"{_API_BASE}/get-event-schedule-or-standings/{event_id}"
    try:
        resp = _fetch_json(url)
    except Exception as exc:
        logger.error("[tgs-matches] flights fetch failed event=%s: %s", event_id, exc)
        return []

    if not isinstance(resp, dict):
        logger.warning("[tgs-matches] event=%s: unexpected flights response type=%s",
                       event_id, type(resp).__name__)
        return []

    data = resp.get("data") or {}
    if not isinstance(data, dict):
        data = {}

    flights: List[Dict] = []
    for gender_key in ("girlsDivAndFlightList", "boysDivAndFlightList"):
        for div in (data.get(gender_key) or []):
            if not isinstance(div, dict):
                continue
            div_name = str(div.get("divisionName") or "").strip()
            div_id = str(div.get("divisionID") or "").strip()
            for flight in (div.get("flightList") or []):
                if not isinstance(flight, dict):
                    continue
                flight_id = str(flight.get("flightID") or "").strip()
                if flight_id and flight_id != "0":
                    flights.append({
                        "flightID": flight_id,
                        "flightName": str(flight.get("flightName") or "").strip(),
                        "divisionName": div_name,
                        "divisionID": div_id,
                    })

    logger.info("[tgs-matches] event=%s → %d flight(s)", event_id, len(flights))
    return flights


def _get_club_ids_for_flight(flight_id: str) -> List[str]:
    """Return distinct clubIDs for a flight via get-team-list-by-flight."""
    url = f"{_API_BASE}/get-team-list-by-flight/{flight_id}"
    try:
        resp = _fetch_json(url)
    except Exception as exc:
        logger.debug("[tgs-matches] team list failed flight=%s: %s", flight_id, exc)
        return []

    data = (resp.get("data") or []) if isinstance(resp, dict) else []
    if not isinstance(data, list):
        return []

    seen: set = set()
    club_ids: List[str] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        cid = str(item.get("clubID") or "").strip()
        if cid and cid not in seen:
            seen.add(cid)
            club_ids.append(cid)
    return club_ids


# ---------------------------------------------------------------------------
# Schedule fetch + parse (per club)
# ---------------------------------------------------------------------------

def _fetch_club_schedule(
    event_id: str,
    club_id: str,
    *,
    league_name: Optional[str],
    season: Optional[str],
) -> List[Dict]:
    """Fetch raw game list for one club via get-club-schedules-by-eventID-and-clubID."""
    url = f"{_API_BASE}/get-club-schedules-by-eventID-and-clubID/{event_id}/{club_id}"
    try:
        resp = _fetch_json(url)
    except Exception as exc:
        logger.debug("[tgs-matches] club schedule failed event=%s club=%s: %s",
                     event_id, club_id, exc)
        return []

    data = (resp.get("data") or []) if isinstance(resp, dict) else []
    if not isinstance(data, list):
        logger.debug("[tgs-matches] event=%s club=%s: unexpected data type=%s",
                     event_id, club_id, type(data).__name__)
        return []

    return data


def _parse_game_item(
    item: Dict,
    *,
    source_url: str,
    league_name: Optional[str],
    season: Optional[str],
) -> Optional[Dict]:
    """Parse one raw game dict from get-club-schedules response."""
    if not isinstance(item, dict):
        return None

    home_raw = str(item.get("homeTeam") or "").strip()
    away_raw = str(item.get("awayTeam") or "").strip()

    if not home_raw or not away_raw:
        return None
    if home_raw.lower() in {"bye", "tbd", "tba"} or away_raw.lower() in {"bye", "tbd", "tba"}:
        return None

    home_team = _strip_brand(home_raw)
    away_team = _strip_brand(away_raw)

    home_score = _parse_score(item.get("hometeamscore"))
    away_score = _parse_score(item.get("awayteamscore"))
    status = "final" if (home_score is not None and away_score is not None) else "scheduled"

    date_str = str(item.get("gameDate") or "").strip()
    match_date = _parse_date(date_str) if date_str else None

    div_name = str(item.get("division") or "").strip() or None
    age_group, gender = _parse_age_gender(div_name or "", season)

    platform_match_id = (
        str(item.get("matchID") or item.get("scheduleID") or item.get("gamenumber") or "").strip()
        or None
    )

    return {
        "home_team_name":    home_team,
        "away_team_name":    away_team,
        "home_score":        home_score,
        "away_score":        away_score,
        "match_date":        match_date,
        "age_group":         age_group,
        "gender":            gender,
        "division":          div_name,
        "season":            season,
        "league":            league_name,
        "status":            status,
        "source":            "totalglobalsports",
        "source_url":        source_url,
        "platform_match_id": platform_match_id,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_totalglobalsports_matches(
    event_id: str,
    *,
    league_name: Optional[str] = None,
    season: Optional[str] = None,
) -> List[Dict]:
    """Scrape all match schedules for one TGS event.

    Discovery flow:
    1. Fetch flights from get-event-schedule-or-standings.
    2. Collect unique clubIDs from get-team-list-by-flight for each flight.
    3. Fetch per-club schedules from get-club-schedules-by-eventID-and-clubID.
    4. Deduplicate across clubs by matchID.

    Returns a list of match dicts shaped for ``insert_matches()``. Never
    writes to the DB — pure extraction.

    ``home_club_id`` / ``away_club_id`` are always absent from the output;
    the canonical-club linker resolves them in a separate pass.
    """
    event_id = str(event_id)

    # Step 1: flights
    flights = _get_flights(event_id)
    if not flights:
        logger.warning("[tgs-matches] event=%s: no flights found — nothing to scrape", event_id)
        return []

    # Step 2: unique club IDs across all flights
    all_club_ids: set = set()
    for flight in flights:
        club_ids = _get_club_ids_for_flight(flight["flightID"])
        all_club_ids.update(club_ids)

    if not all_club_ids:
        logger.warning(
            "[tgs-matches] event=%s: no clubs found across %d flight(s)",
            event_id, len(flights),
        )
        return []

    logger.info(
        "[tgs-matches] event=%s → %d unique club(s) across %d flight(s)",
        event_id, len(all_club_ids), len(flights),
    )

    # Step 3: fetch per-club schedules, deduplicate by matchID
    seen_match_ids: set = set()
    all_rows: List[Dict] = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(
                _fetch_club_schedule,
                event_id, club_id,
                league_name=league_name,
                season=season,
            ): club_id
            for club_id in all_club_ids
        }
        for f in as_completed(futs):
            club_id = futs[f]
            try:
                raw_games = f.result()
            except Exception as exc:
                logger.error("[tgs-matches] event=%s club=%s failed: %s",
                             event_id, club_id, exc)
                continue

            source_url = (
                f"{_API_BASE}/get-club-schedules-by-eventID-and-clubID"
                f"/{event_id}/{club_id}"
            )
            for item in raw_games:
                match_id = str(
                    item.get("matchID") or item.get("scheduleID") or ""
                ).strip()
                if match_id and match_id in seen_match_ids:
                    continue
                if match_id:
                    seen_match_ids.add(match_id)

                parsed = _parse_game_item(
                    item,
                    source_url=source_url,
                    league_name=league_name,
                    season=season,
                )
                if parsed:
                    all_rows.append(parsed)

    logger.info(
        "[tgs-matches] event=%s: %d total match rows across %d club(s)",
        event_id, len(all_rows), len(all_club_ids),
    )
    return all_rows
