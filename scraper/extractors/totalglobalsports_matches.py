"""
TotalGlobalSports (TGS) match schedule extractor.

Fetches game schedules from the AthleteOne-backed TGS API at
``api.athleteone.com/api/Event`` and returns rows shaped for
``matches_writer.insert_matches``.

DISCOVERY FLOW
--------------
1. ``/api/Event/get-event-details-by-eventID/{eventID}``
       → event name + metadata (reuses logic from totalglobalsports_events.py)
2. ``/api/Event/get-division-list-by-event/{eventID}``
       → divisions with divisionID, divisionName, divGender
3. For each division:
   ``/api/Event/get-schedule-list/{eventID}/{divisionID}``
       → JSON array of game objects with home/away team, score, date fields

KNOWN TGS EVENT IDs (STXCL NPL, 2025-26):
    3979 — ECNL RL STXCL (current season A)
    3973 — ECNL RL STXCL (current season B)

OUTPUT
------
League play → ``matches`` table via ``matches_writer.insert_matches``.
``home_club_id`` / ``away_club_id`` stay NULL at scrape time (linker resolves
them in a separate pass, same as GotSport).

NOTE ON ENDPOINT DISCOVERY
---------------------------
The ``/api/Event/get-schedule-list/{eventID}/{divisionID}`` endpoint is the
best candidate based on TGS's AthleteOne API naming convention. If the
response structure differs from the parser below, run with ``--dry-run`` and
check INFO/DEBUG logs — the raw JSON keys are logged at DEBUG level so the
parser can be adjusted with a targeted edit.
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
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%m/%d/%Y",
    "%Y-%m-%d",
]

_SCORE_RE = re.compile(r"^\d+$")
_AGE_RE = re.compile(r"\b[BG](\d{2}|\d{4})\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b([BG])\d{2}", re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP helpers (mirrors totalglobalsports_events.py)
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
        if len(token) == 4:
            birth_year = int(token)
        else:
            birth_year = 2000 + int(token)
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
# Division discovery
# ---------------------------------------------------------------------------

def _get_divisions(event_id: str) -> List[Dict]:
    """Return list of division dicts from the TGS division endpoint.

    Each dict has at minimum: divisionID (str), divisionName (str),
    divGender (str or None).
    """
    url = f"{_API_BASE}/get-division-list-by-event/{event_id}"
    try:
        resp = _fetch_json(url)
    except Exception as exc:
        logger.error("[tgs-matches] division fetch failed event=%s: %s", event_id, exc)
        return []

    data = resp.get("data") if isinstance(resp, dict) else resp
    if not isinstance(data, list):
        logger.warning(
            "[tgs-matches] unexpected division payload type=%s event=%s",
            type(data).__name__, event_id,
        )
        return []

    divisions = []
    for item in data:
        if not isinstance(item, dict):
            continue
        div_id = str(item.get("divisionID") or item.get("divisionId") or "").strip()
        div_name = str(item.get("divisionName") or "").strip()
        if div_id and div_id != "0":
            divisions.append({
                "divisionID": div_id,
                "divisionName": div_name,
                "divGender": item.get("divGender"),
            })

    logger.info("[tgs-matches] event=%s → %d division(s)", event_id, len(divisions))
    return divisions


# ---------------------------------------------------------------------------
# Schedule fetch + parse (per division)
# ---------------------------------------------------------------------------

def _fetch_division_schedule(
    event_id: str,
    division: Dict,
    *,
    league_name: Optional[str],
    season: Optional[str],
) -> List[Dict]:
    """Fetch match schedule for one division and return list of match dicts."""
    div_id = division["divisionID"]
    div_name = division.get("divisionName", "")

    # Primary endpoint — best-guess based on AthleteOne /api/Event naming.
    url = f"{_API_BASE}/get-schedule-list/{event_id}/{div_id}"
    source_url = url

    try:
        resp = _fetch_json(url)
    except Exception as exc:
        logger.debug(
            "[tgs-matches] schedule fetch failed event=%s div=%s: %s",
            event_id, div_id, exc,
        )
        return []

    data = resp.get("data") if isinstance(resp, dict) else resp
    if not isinstance(data, list):
        # Log keys so operators can see the actual structure on Replit.
        if isinstance(resp, dict):
            logger.debug(
                "[tgs-matches] event=%s div=%s: top-level keys=%s",
                event_id, div_id, list(resp.keys()),
            )
        logger.debug(
            "[tgs-matches] event=%s div=%s: no data array in response",
            event_id, div_id,
        )
        return []

    age_group, gender = _parse_age_gender(div_name, season)

    rows: List[Dict] = []
    seen: set = set()

    for item in data:
        if not isinstance(item, dict):
            continue

        # Log first item's keys so we can verify the field names on Replit.
        if not rows and not seen:
            logger.debug(
                "[tgs-matches] event=%s div=%s: first game keys=%s",
                event_id, div_id, sorted(item.keys()),
            )

        # TGS JSON field names (best-guess from AthleteOne convention).
        # If these are wrong the debug log above shows the actual keys.
        home_raw = (
            item.get("homeTeamName") or item.get("home_team_name") or
            item.get("homeTeam") or item.get("homeName") or ""
        ).strip()
        away_raw = (
            item.get("awayTeamName") or item.get("away_team_name") or
            item.get("awayTeam") or item.get("awayName") or ""
        ).strip()

        if not home_raw or not away_raw:
            continue
        if home_raw.lower() in {"bye", "tbd", "tba"}:
            continue

        home_team = _strip_brand(home_raw)
        away_team = _strip_brand(away_raw)

        # Scores.
        home_score = _parse_score(
            item.get("homeScore") or item.get("home_score") or
            item.get("homeGoals")
        )
        away_score = _parse_score(
            item.get("awayScore") or item.get("away_score") or
            item.get("awayGoals")
        )
        status = "final" if (home_score is not None and away_score is not None) else "scheduled"

        # Date/time — TGS sometimes combines date + time as "gameDate" +
        # "gameTime", sometimes as a single ISO field.
        date_str = (
            item.get("gameDate") or item.get("game_date") or
            item.get("matchDate") or item.get("date") or ""
        )
        time_str = item.get("gameTime") or item.get("game_time") or ""
        if date_str and time_str:
            combined = f"{date_str} {time_str}".strip()
        else:
            combined = str(date_str).strip()
        match_date = _parse_date(combined) if combined else None

        # Platform match ID.
        platform_match_id = (
            str(item.get("gameID") or item.get("gameId") or
                item.get("matchID") or item.get("scheduleID") or "").strip()
            or None
        )

        # Division / age group from the division header when not in item.
        item_div = (item.get("divisionName") or div_name or "").strip() or None

        # Dedup within a single division fetch.
        dedup_key = (
            home_team.lower(), away_team.lower(),
            (match_date.isoformat() if match_date else ""),
            (age_group or ""), (gender or ""),
        )
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        rows.append({
            "home_team_name":    home_team,
            "away_team_name":    away_team,
            "home_score":        home_score,
            "away_score":        away_score,
            "match_date":        match_date,
            "age_group":         age_group,
            "gender":            gender,
            "division":          item_div,
            "season":            season,
            "league":            league_name,
            "status":            status,
            "source":            "totalglobalsports",
            "source_url":        source_url,
            "platform_match_id": platform_match_id,
        })

    logger.debug(
        "[tgs-matches] event=%s div=%s (%s) → %d rows",
        event_id, div_id, div_name, len(rows),
    )
    return rows


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

    Returns a list of match dicts shaped for ``insert_matches()``. Never
    writes to the DB — pure extraction.

    ``home_club_id`` / ``away_club_id`` are always absent from the output;
    the canonical-club linker resolves them in a separate pass.

    Args:
        event_id:    TGS numeric event ID string (e.g. "3979").
        league_name: Human-readable name stamped on each row.
        season:      Season string (e.g. "2025-26") for age-group inference.
    """
    event_id = str(event_id)
    divisions = _get_divisions(event_id)

    if not divisions:
        logger.warning(
            "[tgs-matches] event=%s: no divisions found — nothing to scrape",
            event_id,
        )
        return []

    all_rows: List[Dict] = []

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(
                _fetch_division_schedule,
                event_id, div,
                league_name=league_name,
                season=season,
            ): div
            for div in divisions
        }
        for f in as_completed(futs):
            div = futs[f]
            try:
                rows = f.result()
            except Exception as exc:
                logger.error(
                    "[tgs-matches] event=%s div=%s failed: %s",
                    event_id, div.get("divisionID"), exc,
                )
                rows = []
            all_rows.extend(rows)

    logger.info(
        "[tgs-matches] event=%s: %d total match rows across %d divisions",
        event_id, len(all_rows), len(divisions),
    )
    return all_rows
