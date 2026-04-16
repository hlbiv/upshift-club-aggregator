"""
TotalGlobalSports (TGS) events extractor — produces rows for the Path A
``events`` + ``event_teams`` tables.

Complements ``gotsport_events.py`` and ``sincsports_events.py``. Covers
leagues that publish team lists on TGS's Angular SPA at
``public.totalglobalsports.com`` (notably STXCL NPL — South Texas Champions
League — events 3973 and 3979 as of April 2026).

STRATEGY
--------
The TGS public site is an Angular SPA. Data is loaded after hydration from
``api.athleteone.com/api/Event/...`` (AthleteOne is TGS's backend). We
bypass the SPA and hit the JSON endpoints directly:

    GET /api/Event/get-event-details-by-eventID/{eventID}
        → event metadata (name, city, location, startDate, endDate)

    GET /api/Event/get-division-list-by-event/{eventID}
        → divisions with divisionID, divisionName (e.g. "G2008/2007"),
          divGender ("Male" | "Female"), and team count (flights field)

    GET /api/Event/get-team-list-by-eventID/{eventID}
        → flat team list: [{teamID, teamName}, ...]

Team names embed the club name plus the tournament suffix, e.g.
``"210 FC ECNL RL STXCL B09 Black"``. We split on the first occurrence
of the tournament branding ("ECNL RL STXCL" / "STXCL" / "ECNL RL")
to derive the club_name; what remains after is the age/gender/squad
bracket. Division age/gender is inferred from the division list
(``B2009`` → age=U17, gender=M for a 2025-26 season).

PUBLIC API
----------
``scrape_totalglobalsports_event(event_id, league_name, season)`` returns
a ``(EventMeta, List[TeamRow])`` tuple.

``parse_team_name(team_name_raw, season)`` is the pure-string helper used
by tests to split a team name into (club_name, age_group, gender, squad).
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.retry import retry_with_backoff, TransientError  # noqa: E402

# Re-use the same dataclasses from sincsports_events so the writer accepts
# all three sources (gotsport, sincsports, totalglobalsports) without an
# adapter.
from extractors.sincsports_events import EventMeta, TeamRow  # noqa: E402


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

# Tournament-branding prefixes to strip when deriving club_name from a
# team name. Ordered longest-first so "ECNL RL STXCL" is tried before
# "STXCL" alone.
_BRAND_PATTERNS: List[re.Pattern] = [
    re.compile(r"\s+(ECNL\s+RL\s+STXCL|ECNL\s+RL|STXCL)\s+.*$", re.IGNORECASE),
]

# Age-group inference — for a 2025-26 competition year, a player born in
# YYYY plays in age group U{2025 - YYYY + 1} (lower bound convention).
# Division names: "G2008/2007" / "B2009" / "B2008/2007" — we take the
# earliest birth year as the age anchor.
_DIV_YEAR_RE = re.compile(r"(?P<gender>[BG])(?P<year>\d{4})", re.IGNORECASE)

_SKIP_TEAM_NAMES = frozenset({"", "tbd", "tba", "bye", "n/a"})


# ---------------------------------------------------------------------------
# HTTP
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


def _fetch_json(url: str) -> dict:
    """GET a JSON endpoint with retry + backoff. Raises on permanent fail."""

    def _do() -> dict:
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
# Pure parsing helpers
# ---------------------------------------------------------------------------

def parse_team_name(
    team_name_raw: str,
    season: Optional[str] = None,
) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    """Split a TGS team name into ``(club_name, age_group, gender, squad)``.

    Best-effort: if the branding prefix isn't found, the entire team name
    is returned as the club name with age/gender/squad = None.

    Examples
    --------
    >>> parse_team_name("210 FC ECNL RL STXCL G07/08", "2025-26")
    ('210 FC', 'U18', 'F', 'G07/08')
    >>> parse_team_name("AHFC CENTRAL ECNL RL STXCL B09 Red", "2025-26")
    ('AHFC CENTRAL', 'U17', 'M', 'B09 Red')
    >>> parse_team_name("No Brand Club", "2025-26")
    ('No Brand Club', None, None, None)
    """
    raw = team_name_raw.strip()

    club = raw
    squad = None
    for pat in _BRAND_PATTERNS:
        m = pat.search(raw)
        if m:
            club = raw[: m.start()].strip(" -")
            # Everything after the brand is the squad descriptor.
            squad = raw[m.end():].strip() or None
            # The brand-matched portion also contains the squad; capture it.
            brand_and_squad = raw[m.start():].strip()
            # brand_and_squad is like "ECNL RL STXCL B09 Red" — split off
            # the brand words.
            squad = re.sub(
                r"^\s*(ECNL\s+RL\s+STXCL|ECNL\s+RL|STXCL)\s*",
                "",
                brand_and_squad,
                flags=re.IGNORECASE,
            ).strip()
            if not squad:
                squad = None
            break

    age_group: Optional[str] = None
    gender: Optional[str] = None
    if squad:
        m = re.search(r"\b([BG])(\d{2}|\d{4})\b", squad, flags=re.IGNORECASE)
        if m:
            gchar = m.group(1).upper()
            gender = "M" if gchar == "B" else "F"
            year_token = m.group(2)
            if len(year_token) == 2:
                yr = 2000 + int(year_token)
            else:
                yr = int(year_token)
            if season:
                # Infer U{age} from season's starting year. A 2025-26 season
                # → players aged 1 Jan 2025 = 2025 - birth_year.
                season_start = _season_start_year(season)
                if season_start:
                    age_group = f"U{max(1, season_start - yr + 1)}"

    return club, age_group, gender, squad


def _season_start_year(season: str) -> Optional[int]:
    """Extract the starting year from a season string like ``"2025-26"``."""
    m = re.match(r"(\d{4})", season)
    return int(m.group(1)) if m else None


def _age_from_division(division_name: str, season: Optional[str]) -> Optional[str]:
    """Map ``"G2008/2007"`` / ``"B2009"`` → ``"U17"`` or similar."""
    if not division_name:
        return None
    m = _DIV_YEAR_RE.search(division_name)
    if not m:
        return None
    year = int(m.group("year"))
    if season:
        season_start = _season_start_year(season)
        if season_start:
            return f"U{max(1, season_start - year + 1)}"
    return None


def _gender_from_division(division_name: str) -> Optional[str]:
    m = _DIV_YEAR_RE.search(division_name or "")
    if not m:
        return None
    return "M" if m.group("gender").upper() == "B" else "F"


# ---------------------------------------------------------------------------
# Public scrape function
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug[:100] or "tgs-event"


def scrape_totalglobalsports_event(
    event_id: str,
    *,
    league_name: Optional[str] = None,
    season: str = "2025-26",
) -> Tuple[EventMeta, List[TeamRow]]:
    """Scrape one TGS event into ``(EventMeta, List[TeamRow])``.

    Never writes to the DB — pure extraction. The caller (runner) handles
    persistence via ``events_writer.upsert_event_and_teams``.

    Raises on permanent HTTP failure. Returns an empty team list (with a
    populated meta) if the event has no registered teams yet.
    """
    event_id = str(event_id)

    details = _fetch_json(f"{_API_BASE}/get-event-details-by-eventID/{event_id}")
    detail_data = details.get("data") if isinstance(details, dict) else None
    if not isinstance(detail_data, dict):
        raise RuntimeError(
            f"TGS event {event_id}: unexpected details payload: {details!r}"
        )

    name = detail_data.get("name") or f"TGS Event {event_id}"
    city = detail_data.get("city")
    # The "location" field is a free-text display like "Tomball, Texas".
    # Prefer explicit city; leave state null unless we can derive it.
    location = detail_data.get("location") or ""
    state = None
    if "," in location:
        state = location.split(",")[-1].strip() or None

    start_date = detail_data.get("startDate")
    end_date = detail_data.get("endDate")
    # TGS gives dates as "09/05/25" — leave as string; DB accepts TIMESTAMP
    # cast or NULL. Null out if non-parseable.
    start_date = _normalize_date(start_date)
    end_date = _normalize_date(end_date)

    meta = EventMeta(
        tid=f"tgs-{event_id}",
        name=name,
        slug=f"tgs-{event_id}-{_slugify(name)}",
        # events_source_enum CHECK constraint only allows
        # gotsport/sincsports/manual/other/NULL. Use "other" and prefix
        # platform_event_id to preserve source identity.
        source="other",
        platform_event_id=f"tgs-{event_id}",
        league_name=league_name,
        source_url=f"https://public.totalglobalsports.com/events/{event_id}",
        location_city=city,
        location_state=state,
        start_date=start_date,
        end_date=end_date,
        season=season,
    )

    # Divisions give us age/gender per divisionID. We need team→division
    # mapping from the flight-level endpoint, but the flat team list
    # already carries the division embedded in the team name — the
    # TGS naming convention is consistent ("B09", "G07/08"), so we parse
    # from the name first and fall back to division scan when ambiguous.
    divisions = _fetch_json(
        f"{_API_BASE}/get-division-list-by-event/{event_id}"
    )
    div_data = divisions.get("data") if isinstance(divisions, dict) else []
    division_by_name: Dict[str, dict] = {}
    if isinstance(div_data, list):
        for dv in div_data:
            if isinstance(dv, dict) and dv.get("divisionName"):
                division_by_name[dv["divisionName"]] = dv

    teams_resp = _fetch_json(
        f"{_API_BASE}/get-team-list-by-eventID/{event_id}"
    )
    team_data = teams_resp.get("data") if isinstance(teams_resp, dict) else []
    if not isinstance(team_data, list):
        return meta, []

    rows: List[TeamRow] = []
    for t in team_data:
        if not isinstance(t, dict):
            continue
        raw_name = (t.get("teamName") or "").strip()
        if not raw_name or raw_name.lower() in _SKIP_TEAM_NAMES:
            continue

        club_name, age_group, gender, squad = parse_team_name(raw_name, season)

        # Division-code: keep the "B09" / "G07/08" token from the name as
        # a stable division identifier; fall back to divisionName match.
        division_code = None
        if squad:
            m = re.search(r"\b[BG]\d{2}(?:/\d{2})?\b", squad, re.IGNORECASE)
            if m:
                division_code = m.group(0).upper()

        rows.append(TeamRow(
            team_name_raw=raw_name,
            club_name=club_name,
            state=state,
            age_group=age_group,
            gender=gender,
            division_code=division_code,
            birth_year=None,
        ))

    logger.info(
        "[tgs-events] event %s: %d teams extracted",
        event_id, len(rows),
    )
    return meta, rows


def _normalize_date(raw: Optional[str]) -> Optional[str]:
    """Convert ``"09/05/25"`` → ``"2025-09-05"`` (ISO) or return None."""
    if not raw or not isinstance(raw, str):
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})$", raw.strip())
    if not m:
        return None
    mm, dd, yy = m.group(1), m.group(2), m.group(3)
    if len(yy) == 2:
        yy = "20" + yy
    return f"{yy}-{int(mm):02d}-{int(dd):02d}"
