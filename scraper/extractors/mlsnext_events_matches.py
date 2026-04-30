"""
MLS NEXT events match extractor — Fest (event 75) and Flex (event 88).

These are annual showcase/tournament events hosted on the same Modular11
platform as the MLS NEXT regular season, but they use a different API path
and require a bracket ID (resolved by fetching the event iframe page).

API ENDPOINT
------------
    GET https://www.modular11.com/events/league/get_matches
    Params:
        tournament    = <event_id>    (75=Fest, 88=Flex)
        age           = <age_uid>     (see AGE_GROUPS below)
        gender        = 1             (1=male; events are male-only)
        brackets[]    = <bracket_id>  (per age+schedule_type, parsed from iframe)
        match_type    = 1             (group play)
        open_page     = <N>           (1-indexed; stop when response has 0 rows)
        start_date    = "YYYY-MM-DD HH:mm:ss"
        end_date      = "YYYY-MM-DD HH:mm:ss"
        schedule      = 0
        academy       = 0
        status        = "scheduled"   (returns all — past + upcoming)
    Headers: X-Requested-With: XMLHttpRequest  (signals AJAX, avoids full HTML)
    Response: HTML fragment (same structure as public_schedule endpoint)

IFRAME URL PATTERN (used for bracket discovery)
------------------------------------------------
    https://www.modular11.com/events/event/iframe/schedule/{schedule_type}/{event_id}/{age_uid}/1

    The server embeds a ``scheduleConfig`` JS block in the response:
        scheduleConfig.filter.tournament = 88;
        scheduleConfig.filter.age        = 33;
        scheduleConfig.filter.brackets   = [39];
        scheduleConfig.nickName          = 'groupplay';
    This block is parsed to extract the bracket ID for each age group.

PAGINATION
----------
    Pages are 1-indexed (unlike the public_schedule endpoint which is 0-indexed).
    Stop condition: page returns 0 ``table-content-row`` divs.

SCORE FORMAT
------------
    Regular:  "1 : 4"
    Penalty:  "1 : 1\n(4 : 3)"  — we keep the regulation score; penalty
              result is captured in ``bracket_round`` as a note.

HTML STRUCTURE (confirmed April 2026 against live endpoint)
-----------------------------------------------------------
    div.row.table-content-row.hidden-xs (desktop row; ignores mobile duplicate)
        col-sm-1.pad-0              → match ID + gender  ("22525\nMALE")
        col-sm-2 (first)            → date + field       ("04/25/26 05:00pm\n2 - Toyota …")
        col-sm-1.pad-0 (second)     → age label          ("U15")
        col-sm-2 (second)           → competition + division  ("Group Play\nGroup M")
        col-sm-6.pad-0              → teams + score
            div.container-first-team p  → home team name
            div.container-second-team p → away team name
            div.container-score         → score text

EVENTS COVERED
--------------
    MLS NEXT Fest  (event_id=75)  — U13/U14, HD and AD divisions
        hdgroupplay  bracket=76
        adgroupplay  bracket=75
        hdshowcase   bracket=79    (may return 0 rows if not yet played)
        adshowcase   bracket=78
        bestof       bracket=62

    MLS NEXT Flex  (event_id=88)  — U15–U19
        groupplay    bracket=39    (single bracket for all ages)

    Approximate row counts per event (April 2026):
        Fest: ~732 matches  |  Flex: ~456 matches

AGE GROUPS (shared with mlsnext_matches.py)
-----------
    U13 → 21 | U14 → 22 | U15 → 33 | U16 → 14 | U17 → 15 | U19 → 26
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_EVENTS_API_URL = "https://www.modular11.com/events/league/get_matches"
_IFRAME_BASE = "https://www.modular11.com/events/event/iframe/schedule"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.modular11.com/",
    "Origin": "https://www.modular11.com",
}

# Shared with mlsnext_matches.py
AGE_GROUPS: Dict[int, str] = {
    21: "U13",
    22: "U14",
    33: "U15",
    14: "U16",
    15: "U17",
    26: "U19",
}

_GENDER_MAP = {"MALE": "male", "FEMALE": "female"}

_DATE_FORMATS = [
    "%m/%d/%y %I:%M%p",
    "%m/%d/%y %I:%M %p",
    "%m/%d/%Y %I:%M%p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%y",
    "%m/%d/%Y",
]

# Matches "1 : 4" and also captures only the regulation score from "1 : 1\n(4 : 3)".
# The first occurrence of "D : D" is always the regulation score.
_SCORE_RE = re.compile(r"(\d+)\s*:\s*(\d+)")

# Matches the penalty-shootout note "(4 : 3)" to detect shootout wins.
_PENALTY_RE = re.compile(r"\((\d+)\s*:\s*(\d+)\)")

# Regex to pull bracket from the server-side scheduleConfig JS block.
_BRACKETS_RE = re.compile(r"scheduleConfig\.filter\.brackets\s*=\s*\[(\d+)\]")


# ---------------------------------------------------------------------------
# Static event configs — bracket IDs confirmed April 2026.
# ---------------------------------------------------------------------------

@dataclass
class _ScheduleEntry:
    """One (age_uid, bracket_id, schedule_type) combination to scrape."""
    age_uid: int
    bracket_id: int
    schedule_type: str


@dataclass
class EventConfig:
    """All scrape parameters for one Modular11 event."""
    event_id: int
    tournament_name: str
    # Wide-window date range covering the full event (past + future re-runs).
    start_date: str
    end_date: str
    schedules: List[_ScheduleEntry] = field(default_factory=list)


# MLS NEXT Fest 2025-26 — U13/U14, HD and AD division split.
_FEST_2026 = EventConfig(
    event_id=75,
    tournament_name="MLS NEXT Fest",
    start_date="2025-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
    schedules=[
        # HD Group Play — bracket 76
        _ScheduleEntry(age_uid=21, bracket_id=76, schedule_type="hdgroupplay"),
        _ScheduleEntry(age_uid=22, bracket_id=76, schedule_type="hdgroupplay"),
        # AD Group Play — bracket 75
        _ScheduleEntry(age_uid=21, bracket_id=75, schedule_type="adgroupplay"),
        _ScheduleEntry(age_uid=22, bracket_id=75, schedule_type="adgroupplay"),
        # HD Showcase — bracket 79 (may be empty before event)
        _ScheduleEntry(age_uid=21, bracket_id=79, schedule_type="hdshowcase"),
        _ScheduleEntry(age_uid=22, bracket_id=79, schedule_type="hdshowcase"),
        # AD Showcase — bracket 78
        _ScheduleEntry(age_uid=21, bracket_id=78, schedule_type="adshowcase"),
        _ScheduleEntry(age_uid=22, bracket_id=78, schedule_type="adshowcase"),
        # Best Of — bracket 62
        _ScheduleEntry(age_uid=21, bracket_id=62, schedule_type="bestof"),
        _ScheduleEntry(age_uid=22, bracket_id=62, schedule_type="bestof"),
    ],
)

# MLS NEXT Flex 2025-26 — U15–U19, single Group Play bracket.
_FLEX_2026 = EventConfig(
    event_id=88,
    tournament_name="MLS NEXT Flex",
    start_date="2025-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
    schedules=[
        _ScheduleEntry(age_uid=33, bracket_id=39, schedule_type="groupplay"),
        _ScheduleEntry(age_uid=14, bracket_id=39, schedule_type="groupplay"),
        _ScheduleEntry(age_uid=15, bracket_id=39, schedule_type="groupplay"),
        _ScheduleEntry(age_uid=26, bracket_id=39, schedule_type="groupplay"),
    ],
)

# MLS NEXT Cup Qualifiers — feed into the Cup playoffs.
# event_id=74 confirmed April 2026.  Single Group Play bracket for all ages.
_CUP_QUALIFIERS = EventConfig(
    event_id=74,
    tournament_name="MLS NEXT Cup Qualifiers",
    start_date="2024-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
    schedules=[
        _ScheduleEntry(age_uid=21, bracket_id=39, schedule_type="groupplay"),  # U13
        _ScheduleEntry(age_uid=22, bracket_id=39, schedule_type="groupplay"),  # U14
        _ScheduleEntry(age_uid=33, bracket_id=39, schedule_type="groupplay"),  # U15
        _ScheduleEntry(age_uid=14, bracket_id=39, schedule_type="groupplay"),  # U16
        _ScheduleEntry(age_uid=15, bracket_id=39, schedule_type="groupplay"),  # U17
        _ScheduleEntry(age_uid=26, bracket_id=39, schedule_type="groupplay"),  # U19
    ],
)

# Generation adidas Cup — multi-division showcase tournament, U15/U16.
# event_id=80 confirmed April 2026.  U17 used event_id=53 in an earlier year
# but currently returns 0 rows; omitted until data reappears.
_GA_CUP = EventConfig(
    event_id=80,
    tournament_name="Generation adidas Cup",
    start_date="2024-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
    schedules=[
        # U15 — Group Play, Championship, Showcase
        _ScheduleEntry(age_uid=33, bracket_id=39, schedule_type="groupplay"),
        _ScheduleEntry(age_uid=33, bracket_id=5,  schedule_type="championship"),
        _ScheduleEntry(age_uid=33, bracket_id=16, schedule_type="showcase"),
        # U16 — Group Play, Championship, Premier, Consolation, Showcase
        _ScheduleEntry(age_uid=14, bracket_id=39, schedule_type="groupplay"),
        _ScheduleEntry(age_uid=14, bracket_id=5,  schedule_type="championship"),
        _ScheduleEntry(age_uid=14, bracket_id=1,  schedule_type="premier"),
        _ScheduleEntry(age_uid=14, bracket_id=55, schedule_type="consolationgroups"),
        _ScheduleEntry(age_uid=14, bracket_id=16, schedule_type="showcase"),
    ],
)

# MLS NEXT Cup — playoffs + showcase, all age groups.
# event_id=72 confirmed April 2026 (event held June 2025, June 2026 upcoming).
# Wide date range captures both past (2025) and future (2026+) running of this event.
_CUP = EventConfig(
    event_id=72,
    tournament_name="MLS NEXT Cup",
    start_date="2024-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
    schedules=[
        # Playoffs — U13/U14 split into Premier (bracket=1) and Championship (bracket=5)
        _ScheduleEntry(age_uid=21, bracket_id=1,  schedule_type="premier"),
        _ScheduleEntry(age_uid=21, bracket_id=5,  schedule_type="championship"),
        _ScheduleEntry(age_uid=22, bracket_id=1,  schedule_type="premier"),
        _ScheduleEntry(age_uid=22, bracket_id=5,  schedule_type="championship"),
        # Playoffs — U15/U16/U17 single bracket
        _ScheduleEntry(age_uid=33, bracket_id=34, schedule_type="playoffs"),
        _ScheduleEntry(age_uid=14, bracket_id=34, schedule_type="playoffs"),
        _ScheduleEntry(age_uid=15, bracket_id=34, schedule_type="playoffs"),
        # Playoffs — U19 Championship bracket
        _ScheduleEntry(age_uid=26, bracket_id=5,  schedule_type="championship"),
        # Showcase — U13/U14/U15 (bracket=16; may be empty before event)
        _ScheduleEntry(age_uid=21, bracket_id=16, schedule_type="showcase"),
        _ScheduleEntry(age_uid=22, bracket_id=16, schedule_type="showcase"),
        _ScheduleEntry(age_uid=33, bracket_id=16, schedule_type="showcase"),
    ],
)

# Registry: event_id → EventConfig.  Extend here when new events are identified.
EVENT_REGISTRY: Dict[int, EventConfig] = {
    72: _CUP,
    74: _CUP_QUALIFIERS,
    75: _FEST_2026,
    80: _GA_CUP,
    88: _FLEX_2026,
}


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch_page(
    event_id: int,
    age_uid: int,
    bracket_id: int,
    page: int,
    start_date: str,
    end_date: str,
    *,
    timeout: int = 30,
) -> str:
    """Fetch one page of match HTML from the events API."""
    params: Dict[str, object] = {
        "tournament":    event_id,
        "age":           age_uid,
        "gender":        1,
        "brackets[]":    bracket_id,
        "match_type":    1,
        "open_page":     page,
        "start_date":    start_date,
        "end_date":      end_date,
        "schedule":      0,
        "academy":       0,
        "status":        "scheduled",
        "report_status": 0,
        "as_referee":    0,
        "team":          0,
        "teamPlayer":    0,
        "location":      0,
        "group":         "",
        "groups":        "",
        "match_number":  "",
    }
    r = requests.get(_EVENTS_API_URL, params=params, headers=_HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text


# ---------------------------------------------------------------------------
# Bracket discovery (optional — used to validate / refresh static configs)
# ---------------------------------------------------------------------------

def discover_bracket_id(event_id: int, age_uid: int, schedule_type: str) -> Optional[int]:
    """
    Fetch the event iframe page and parse the ``scheduleConfig.filter.brackets``
    value.  Returns None if the page is unreachable or the config is absent.

    This is used by tests and can be used to refresh EVENT_REGISTRY when a new
    season spins up with different bracket IDs.
    """
    url = f"{_IFRAME_BASE}/{schedule_type}/{event_id}/{age_uid}/1"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("[mlsnext-events] bracket discovery failed %s: %s", url, exc)
        return None
    m = _BRACKETS_RE.search(r.text)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[datetime]:
    text = raw.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    logger.debug("[mlsnext-events] unparseable date: %r", raw)
    return None


def _parse_score(score_text: str) -> Tuple[Optional[int], Optional[int], bool]:
    """
    Parse a score string and return (home_score, away_score, is_shootout).

    Handles:
        "1 : 4"            → (1, 4, False)
        "1 : 1\n(4 : 3)"  → (1, 1, True)   regulation score kept; shootout noted
        "TBD"              → (None, None, False)
    """
    text = score_text.strip()
    # Regulation score
    m = _SCORE_RE.search(text)
    if not m:
        return None, None, False
    home = int(m.group(1))
    away = int(m.group(2))
    # Detect penalty shootout extension
    is_shootout = bool(_PENALTY_RE.search(text))
    return home, away, is_shootout


def _parse_page(
    html: str,
    event_cfg: EventConfig,
    schedule_entry: _ScheduleEntry,
    season: Optional[str],
    source_url: str,
) -> List[Dict]:
    """
    Parse one page of Modular11 events HTML into match row dicts.

    Returns a list of rows shaped for ``tournament_matches_writer.insert_tournament_matches``.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: List[Dict] = []

    age_label = AGE_GROUPS.get(schedule_entry.age_uid, f"uid{schedule_entry.age_uid}")

    for row in soup.select("div.row.table-content-row.hidden-xs"):
        cols = row.find_all("div", recursive=False)
        if len(cols) < 5:
            continue

        # --- col 0: match ID + gender ---
        id_parts = [t.strip() for t in cols[0].get_text("\n").split("\n") if t.strip()]
        platform_match_id = id_parts[0] if id_parts else None
        gender_raw = id_parts[1].upper() if len(id_parts) > 1 else ""
        gender = _GENDER_MAP.get(gender_raw, "male")

        # --- col 1: date + field ---
        date_parts = [t.strip() for t in cols[1].get_text("\n").split("\n") if t.strip()]
        match_date: Optional[datetime] = _parse_date(date_parts[0]) if date_parts else None
        field_name = date_parts[1] if len(date_parts) > 1 else None

        # --- col 3: competition type + division ---
        div_parts = [t.strip() for t in cols[3].get_text("\n").split("\n") if t.strip()]
        competition = div_parts[0] if div_parts else schedule_entry.schedule_type
        division = div_parts[1] if len(div_parts) > 1 else None

        # --- col 4: home/away team names ---
        home_el = row.select_one("div.container-first-team p")
        away_el = row.select_one("div.container-second-team p")
        if not home_el or not away_el:
            continue
        home_team = (home_el.get("data-title") or home_el.get_text(strip=True)).strip()
        away_team = (away_el.get("data-title") or away_el.get_text(strip=True)).strip()
        if not home_team or not away_team:
            continue
        if home_team.lower() in {"tbd", "bye"} or away_team.lower() in {"tbd", "bye"}:
            continue

        # --- score ---
        score_el = row.select_one("div.container-score")
        home_score: Optional[int] = None
        away_score: Optional[int] = None
        is_shootout = False
        if score_el:
            home_score, away_score, is_shootout = _parse_score(score_el.get_text())

        status = "final" if home_score is not None else "scheduled"

        # Shootout wins are noted in bracket_round so the raw result is preserved.
        bracket_round_note = "penalty shootout" if is_shootout else None

        rows.append({
            "home_team_name":    home_team,
            "away_team_name":    away_team,
            "home_score":        home_score,
            "away_score":        away_score,
            "match_date":        match_date,
            "age_group":         age_label,
            "gender":            gender,
            "division":          division,
            "season":            season,
            "tournament_name":   event_cfg.tournament_name,
            "flight":            schedule_entry.schedule_type,
            "group_name":        field_name,
            "bracket_round":     bracket_round_note,
            "match_type":        "group_play",
            "status":            status,
            "source":            "mlsnext",
            "source_url":        source_url,
            "platform_match_id": platform_match_id,
        })

    return rows


# ---------------------------------------------------------------------------
# Per-schedule-entry scrape
# ---------------------------------------------------------------------------

def _scrape_schedule_entry(
    event_cfg: EventConfig,
    entry: _ScheduleEntry,
    *,
    season: Optional[str],
    rate_limit: float = 0.4,
) -> List[Dict]:
    """Paginate through all pages for one (event, age, bracket) combo."""
    age_label = AGE_GROUPS.get(entry.age_uid, f"uid{entry.age_uid}")
    source_url = (
        f"{_IFRAME_BASE}/{entry.schedule_type}"
        f"/{event_cfg.event_id}/{entry.age_uid}/1"
    )
    all_rows: List[Dict] = []

    for page in range(1, 200):   # hard cap — tournaments won't exceed 5,000 pages
        try:
            html = _fetch_page(
                event_cfg.event_id,
                entry.age_uid,
                entry.bracket_id,
                page,
                event_cfg.start_date,
                event_cfg.end_date,
            )
        except requests.RequestException as exc:
            logger.warning(
                "[mlsnext-events] fetch failed event=%d age=%s page=%d: %s",
                event_cfg.event_id, age_label, page, exc,
            )
            break

        page_rows = _parse_page(html, event_cfg, entry, season, source_url)
        if not page_rows:
            break   # empty page = end of results
        all_rows.extend(page_rows)

        logger.debug(
            "[mlsnext-events] event=%d %s age=%s page=%d → %d rows",
            event_cfg.event_id, entry.schedule_type, age_label, page, len(page_rows),
        )
        time.sleep(rate_limit)

    logger.info(
        "[mlsnext-events] event=%d %s/%s → %d matches",
        event_cfg.event_id, entry.schedule_type, age_label, len(all_rows),
    )
    return all_rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_mlsnext_event_matches(
    event_id: int,
    *,
    season: Optional[str] = None,
    rate_limit: float = 0.4,
) -> List[Dict]:
    """
    Scrape all matches for one MLS NEXT event (Fest or Flex).

    Args:
        event_id:    Modular11 event ID — 75 for Fest, 88 for Flex.
        season:      Season tag to attach to each row (e.g. "2025-26").
        rate_limit:  Seconds to sleep between page fetches.

    Returns:
        List of match dicts shaped for
        ``tournament_matches_writer.insert_tournament_matches``.
    """
    cfg = EVENT_REGISTRY.get(event_id)
    if cfg is None:
        known = ", ".join(str(k) for k in sorted(EVENT_REGISTRY))
        raise ValueError(
            f"Unknown event_id={event_id!r}. "
            f"Known IDs: {known}. "
            "Add a new EventConfig to EVENT_REGISTRY to support it."
        )

    logger.info(
        "[mlsnext-events] scraping %s (event_id=%d), %d schedule entries",
        cfg.tournament_name, event_id, len(cfg.schedules),
    )

    all_rows: List[Dict] = []
    for entry in cfg.schedules:
        rows = _scrape_schedule_entry(cfg, entry, season=season, rate_limit=rate_limit)
        all_rows.extend(rows)

    logger.info(
        "[mlsnext-events] %s total → %d match rows",
        cfg.tournament_name, len(all_rows),
    )
    return all_rows


def scrape_all_mlsnext_events(
    *,
    event_ids: Optional[Sequence[int]] = None,
    season: Optional[str] = None,
    rate_limit: float = 0.4,
) -> List[Dict]:
    """
    Scrape all registered MLS NEXT events (or a subset).

    Args:
        event_ids:  Subset of event IDs to scrape (default: all in EVENT_REGISTRY).
        season:     Season tag.
        rate_limit: Per-page sleep between fetches.

    Returns:
        Combined list of match dicts for all requested events.
    """
    ids = list(event_ids) if event_ids is not None else sorted(EVENT_REGISTRY)
    combined: List[Dict] = []
    for eid in ids:
        rows = scrape_mlsnext_event_matches(eid, season=season, rate_limit=rate_limit)
        combined.extend(rows)
    return combined
