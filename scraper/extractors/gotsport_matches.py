"""
GotSport per-event match (schedule) extractor.

DISCOVERY FLOW (current, observed 2026)
----------------------------------------
GotSport's bare /schedules URL redirects to the event home page with no
schedule data. The actual schedule lives behind division-group URLs:

  1. Fetch event home:
       https://system.gotsport.com/org_event/events/{event_id}
     Parse all href="/org_event/events/{event_id}/schedules?group=<ID>"
     links to discover division group IDs.

  2. For each group ID, fetch all matches:
       https://system.gotsport.com/org_event/events/{event_id}/schedules
           ?date=All&group=<ID>
     This returns a page of one <table> per date bucket, each with
     7 columns: Match # | Time | Home Team | Results | Away Team |
     Location | Division. All matches for the division appear here
     with no further pagination.

  3. Parse match rows with Strategy C (column-position parser) and
     accumulate across all groups.

STRATEGY FALLBACKS
------------------
Strategy A — rows with class="match" or data-match-id (older layout).
Strategy B — generic schedule tables with home/away column headers.
Strategy C — GotSport's current per-date-group table format (primary).

Output shape (one dict per match row):

    {
      "home_team_name": str,            # raw as scraped
      "away_team_name": str,            # raw as scraped
      "home_club_canonical": str,       # via _canonical() — linker input
      "away_club_canonical": str,
      "home_score": Optional[int],
      "away_score": Optional[int],
      "match_date": Optional[datetime], # UTC naive
      "age_group": Optional[str],
      "gender": Optional[str],          # "M" / "F" / None
      "division": Optional[str],
      "status": str,                    # "scheduled" | "final" | "cancelled" | ...
      "platform_match_id": Optional[str],
      "source": "gotsport",
      "source_url": str,
      "event_id": int,                  # GotSport platform id, not FK
    }

`home_club_id` / `away_club_id` are intentionally NOT populated here —
they are resolved by a separate linker job.

Output shape (one dict per match row):

    {
      "home_team_name": str,            # raw as scraped
      "away_team_name": str,            # raw as scraped
      "home_club_canonical": str,       # via _canonical() — linker input
      "away_club_canonical": str,
      "home_score": Optional[int],
      "away_score": Optional[int],
      "match_date": Optional[datetime], # UTC naive
      "age_group": Optional[str],
      "gender": Optional[str],          # "M" / "F" / None
      "division": Optional[str],
      "status": str,                    # "scheduled" | "final"
      "platform_match_id": Optional[str],
      "source": "gotsport",
      "source_url": str,
      "event_id": int,                  # GotSport platform id, not FK
    }

`home_club_id` / `away_club_id` are intentionally NOT populated here —
they are resolved by a separate linker job. This mirrors how
`event_teams.canonical_club_id` is handled in Domain 4: scrape-time
linker coupling hides scraping bugs behind dedup logic.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS, USER_AGENT  # noqa: E402
from normalizer import _canonical  # noqa: E402
from utils.retry import retry_with_backoff, TransientError  # noqa: E402

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": USER_AGENT}
_BASE = "https://system.gotsport.com"
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS_CODES
    return False


def _get_with_retry(url: str, timeout: int = 20) -> requests.Response:
    def _fetch() -> requests.Response:
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if _is_retryable(exc):
                raise TransientError(str(exc)) from exc
            raise

    return retry_with_backoff(
        _fetch,
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
        label=f"gotsport-matches:{url}",
    )


# ---------------------------------------------------------------------------
# Group discovery
# ---------------------------------------------------------------------------

def _discover_group_ids(event_id: int | str) -> List[str]:
    """Fetch the event home page and return all division group IDs found.

    The event home embeds links of the form
        /org_event/events/{event_id}/schedules?group=<ID>
    for every division in the event. Collecting them is enough to build
    the ``?date=All&group=<ID>`` URLs that return the full per-division
    schedule as static HTML.
    """
    url = f"{_BASE}/org_event/events/{event_id}"
    try:
        r = _get_with_retry(url)
    except (TransientError, requests.RequestException) as exc:
        logger.error("[gotsport-matches] event home fetch failed event=%s: %s", event_id, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    pattern = re.compile(
        r"/org_event/events/" + re.escape(str(event_id)) + r"/schedules\?(?:.*&)?group=(\d+)"
    )
    seen: set = set()
    group_ids: List[str] = []
    for a in soup.find_all("a", href=True):
        m = pattern.search(a["href"])
        if m:
            gid = m.group(1)
            if gid not in seen:
                seen.add(gid)
                group_ids.append(gid)

    logger.info("[gotsport-matches] event %s → %d group(s) discovered", event_id, len(group_ids))
    return group_ids


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------

# GotSport emits scores in several shapes:
#   "3-2"            bare score
#   "W 3-2" / "L 1-3" result-prefixed
#   "F 1-0" / "0-0 FF" forfeit marker (trailing or embedded F/FF)
#   "2-2 (4-3)"      draw + shootout (PK score in parens)
#   ""               empty cell → scheduled / BYE
# Empty-string and "BYE" are handled explicitly by the caller, not here.
_SCORE_PATTERN = re.compile(
    r"^\s*(?:[WLDTF]\s+)?"                       # optional result/forfeit prefix (W/L/D/T/F)
    r"(\d{1,2})\s*[-–:]\s*(\d{1,2})"              # main score
    r"(?:\s*\((\d{1,2})\s*[-–:]\s*(\d{1,2})\))?"  # optional shootout
    r"(?:\s+FF|\s+F)?\s*$",                       # optional trailing forfeit marker
    re.IGNORECASE,
)
# Word-boundary score finder used inside larger free-text cells (e.g. a
# "Home vs Away 3-1" merged cell). We need negative-digit lookarounds so
# year-numbers in a team name like "FC 2010-2012" don't get parsed as a
# score (otherwise "10-20" would be picked up and "FC 2012" left as the
# residual name).
_INLINE_SCORE_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[-–]\s*(\d{1,2})(?!\d)")
_BYE_PATTERN = re.compile(r"^\s*bye\s*$", re.IGNORECASE)
# "FF" / "forfeit" anywhere, OR a leading "F " before a score (GotSport's
# forfeit-with-awarded-score notation, e.g. "F 1-0").
_FORFEIT_PATTERN = re.compile(
    r"(?:\b(?:ff|forfeit)\b|^\s*F\s+\d)",
    re.IGNORECASE,
)
_AGE_PATTERN = re.compile(r"\b([BG]?)(U\d{2}|\d{4})\b", re.IGNORECASE)
# GotSport date strings vary: "2026-03-14 15:30", "3/14/2026 3:30 PM",
# "Sat, Mar 14 2026 3:30 PM". Try strict ISO first, then fall back.
_DATE_FORMATS: Tuple[str, ...] = (
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%a, %b %d %Y %I:%M %p",
    "%b %d %Y %I:%M %p",
    "%b %d, %Y %I:%M %p",
    "%b %d, %Y",
)


def _parse_date(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    raw = re.sub(r"\s+", " ", raw).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _parse_score(raw: str) -> Tuple[Optional[int], Optional[int]]:
    """Return (home_score, away_score) or (None, None).

    Accepts bare "3-2", "W 3-2"/"L 1-3", "F 1-0"/"0-0 FF" forfeit variants,
    and "2-2 (4-3)" shootouts. The main (pre-parens) score is what lands
    in the DB; the shootout PK score is discarded (schema has no column
    for it). An empty cell is a scheduled/BYE row — the caller decides.
    """
    if not raw:
        return None, None
    m = _SCORE_PATTERN.match(raw)
    if not m:
        return None, None
    try:
        return int(m.group(1)), int(m.group(2))
    except ValueError:
        return None, None


def _is_bye_cell(raw: str) -> bool:
    """True if a cell represents a BYE (no match actually played)."""
    return bool(raw) and bool(_BYE_PATTERN.match(raw))


def _parse_age_gender(raw: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract age group + gender from a bracket/division label.

    Examples:
      "B15 Premier"   → ("U15", "M")
      "G13 Elite"     → ("U13", "F")
      "U12 Boys"      → ("U12", "M")
      "U16 Girls"     → ("U16", "F")
    """
    if not raw:
        return None, None
    low = raw.lower()
    gender: Optional[str] = None
    if re.search(r"\bgirls?\b", low) or re.search(r"\bg\d{2}\b", low):
        gender = "F"
    elif re.search(r"\bboys?\b", low) or re.search(r"\bb\d{2}\b", low):
        gender = "M"

    age: Optional[str] = None
    m = re.search(r"\b[BG](\d{2})\b", raw, re.IGNORECASE)
    if m:
        age = f"U{m.group(1)}"
    else:
        m = re.search(r"\bU(\d{1,2})\b", raw, re.IGNORECASE)
        if m:
            age = f"U{m.group(1).zfill(2)}"

    return age, gender


def _normalize_status(
    raw: Optional[str],
    home_score: Optional[int],
    away_score: Optional[int],
    score_cell: Optional[str] = None,
) -> str:
    if raw:
        low = raw.lower().strip()
        if "final" in low or "complete" in low or "fin" == low:
            return "final"
        if "cancel" in low:
            return "cancelled"
        if "forfeit" in low or "ff" == low:
            return "forfeit"
        if "postpon" in low:
            return "postponed"
    # Forfeit markers appear in the score cell itself: "F 1-0", "0-0 FF".
    if score_cell and _FORFEIT_PATTERN.search(score_cell):
        return "forfeit"
    if home_score is not None and away_score is not None:
        return "final"
    return "scheduled"


# ---------------------------------------------------------------------------
# Strategy C — GotSport per-date-group schedule table parser
# ---------------------------------------------------------------------------

# Timezone abbreviations appended to GotSport time strings (e.g. "9:15 AM CST").
_TZ_SUFFIX_RE = re.compile(r"\s+[A-Z]{2,4}$")

# Column header → canonical key mapping for Strategy C.
_SCHED_HDR_MAP: List[Tuple[str, str]] = [
    ("match", "match_num"),      # "Match #" / "Match#" / "Match"
    ("time", "datetime"),        # "Time" / "Date/Time"
    ("date", "datetime"),
    ("home team", "home"),       # "Home Team"
    ("home", "home"),
    ("result", "score"),         # "Results" / "Result" / "Score"
    ("score", "score"),
    ("away team", "away"),       # "Away Team"
    ("away", "away"),
    ("division", "division"),    # "Division" / "Bracket"
    ("bracket", "division"),
    ("group", "division"),
]

# Status keywords embedded in the time cell after a double-newline.
_STATUS_CELL_KEYWORDS = {
    "canceled", "cancelled", "rescheduled", "postponed",
    "forfeit", "ff", "field change",
}


def _parse_gotsport_schedule_tables(
    soup: BeautifulSoup,
    event_id: int | str,
    source_url: str,
    default_age: Optional[str],
    default_gender: Optional[str],
    default_division: Optional[str],
    default_season: Optional[str],
    default_league: Optional[str],
    stats: Optional[Dict[str, int]],
) -> List[Dict[str, Any]]:
    """Strategy C — GotSport's current per-date-group table layout.

    Each ``?date=All&group=<ID>`` page contains one ``<table>`` per date
    bucket, all with headers::

        Match # | Time | Home Team | Results | Away Team | Location | Division

    The Time cell may embed a status tag after a blank line::

        "Feb 14, 2026\\n9:15 AM CST"                 → no status
        "Mar 07, 2026\\n1:00 PM CST\\n\\nCanceled"   → status = canceled

    Scores: ``"X - Y"`` for played matches; ``"-"`` for unplayed.
    """
    rows: List[Dict[str, Any]] = []

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        raw_headers = [_row_text(th).lower() for th in header_row.find_all(["th", "td"])]
        if not raw_headers:
            continue

        # Build column-index map from header names.
        idx: Dict[str, int] = {}
        for col_i, hdr in enumerate(raw_headers):
            for keyword, key in _SCHED_HDR_MAP:
                if keyword in hdr and key not in idx:
                    idx[key] = col_i
                    break

        # Must have at minimum a home and an away column to be a schedule table.
        if "home" not in idx or "away" not in idx:
            continue

        for tr in table.find_all("tr"):
            if tr.find("th") and not tr.find("td"):
                continue  # header-only row
            tds = tr.find_all("td")
            min_col = max(idx.get("home", 0), idx.get("away", 0))
            if len(tds) <= min_col:
                continue

            home_name = _row_text(tds[idx["home"]]) if "home" in idx else ""
            away_name = _row_text(tds[idx["away"]]) if "away" in idx else ""
            if not home_name or not away_name:
                continue
            if _is_bye_cell(home_name) or _is_bye_cell(away_name):
                continue

            # Time cell: split into date/time lines and optional status tag.
            match_date = None
            status_from_cell: Optional[str] = None
            if "datetime" in idx and idx["datetime"] < len(tds):
                # Use .strings to get text nodes separated naturally.
                cell_lines = [
                    s.strip() for s in tds[idx["datetime"]].strings if s.strip()
                ]
                date_parts: List[str] = []
                for line in cell_lines:
                    if line.lower() in _STATUS_CELL_KEYWORDS or any(
                        kw in line.lower() for kw in _STATUS_CELL_KEYWORDS
                    ):
                        status_from_cell = line
                    else:
                        date_parts.append(line)
                date_time_str = _TZ_SUFFIX_RE.sub("", " ".join(date_parts)).strip()
                match_date = _parse_date(date_time_str)

            # Score cell: "X - Y" → final; "-" or empty → not yet played.
            home_score: Optional[int] = None
            away_score: Optional[int] = None
            score_cell_text: Optional[str] = None
            if "score" in idx and idx["score"] < len(tds):
                score_cell_text = _row_text(tds[idx["score"]])
                # "X - Y" with optional spaces around dash.
                sm = re.match(r"^(\d{1,2})\s*-\s*(\d{1,2})$", score_cell_text)
                if sm:
                    home_score, away_score = int(sm.group(1)), int(sm.group(2))

            # Division and age/gender.
            division: Optional[str] = None
            if "division" in idx and idx["division"] < len(tds):
                division = _row_text(tds[idx["division"]]) or None
            age_group, gender = _parse_age_gender(division or "")
            age_group = age_group or default_age
            gender = gender or default_gender
            division = division or default_division

            # Platform match id.
            platform_match_id: Optional[str] = None
            if "match_num" in idx and idx["match_num"] < len(tds):
                platform_match_id = _row_text(tds[idx["match_num"]]) or None

            status = _normalize_status(
                status_from_cell, home_score, away_score, score_cell_text
            )

            home_canonical = _canonical(home_name)
            away_canonical = _canonical(away_name)
            if not home_canonical or not away_canonical:
                if stats is not None:
                    stats["dropped_non_canonicalizable"] = (
                        stats.get("dropped_non_canonicalizable", 0) + 1
                    )
                logger.warning(
                    "[gotsport-matches] dropping non-canonicalizable: home=%r away=%r",
                    home_name, away_name,
                )
                continue

            rows.append({
                "home_team_name": home_name,
                "away_team_name": away_name,
                "home_club_canonical": home_canonical,
                "away_club_canonical": away_canonical,
                "home_score": home_score,
                "away_score": away_score,
                "match_date": match_date,
                "age_group": age_group,
                "gender": gender,
                "division": division,
                "season": default_season,
                "league": default_league,
                "status": status,
                "platform_match_id": platform_match_id,
                "source": "gotsport",
                "source_url": source_url,
                "event_id": int(event_id) if str(event_id).isdigit() else None,
            })

    return rows


# ---------------------------------------------------------------------------
# Row extractor
# ---------------------------------------------------------------------------

def _row_text(cell) -> str:
    if cell is None:
        return ""
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _extract_matches_from_html(
    html: str,
    event_id: int | str,
    source_url: str,
    default_age: Optional[str] = None,
    default_gender: Optional[str] = None,
    default_division: Optional[str] = None,
    default_season: Optional[str] = None,
    default_league: Optional[str] = None,
    stats: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """
    Parse matches out of a GotSport schedule page's HTML.

    Strategy
    --------
    1. Walk every ``<tr>`` inside every ``<table>`` on the page.
    2. A match row has cells that include BOTH a home-team label and
       an away-team label. We try two layouts:

         - "Home vs Away" in a single cell (with optional score spans)
         - separate home/away cells (positional)

    3. We resolve date/score/status from sibling cells and fall back
       to caller-supplied defaults for age/gender/division.

    ``stats``, if supplied, is mutated in place. The currently-tracked
    counters are:

      - ``dropped_non_canonicalizable``: rows whose home or away team
        name normalised to an empty string via ``_canonical()`` and were
        therefore dropped instead of emitted with a raw fallback. Such
        names would otherwise pollute the canonical-club linker's
        training set downstream.
    """
    soup = BeautifulSoup(html, "lxml")
    matches: List[Dict[str, Any]] = []

    # Strategy C — GotSport's current per-date-group table format (primary path).
    # Handles pages fetched via ?date=All&group=<ID>: one <table> per date bucket,
    # headers = Match # | Time | Home Team | Results | Away Team | Location | Division.
    c_matches = _parse_gotsport_schedule_tables(
        soup,
        event_id=event_id,
        source_url=source_url,
        default_age=default_age,
        default_gender=default_gender,
        default_division=default_division,
        default_season=default_season,
        default_league=default_league,
        stats=stats,
    )
    if c_matches:
        return _dedup_matches(c_matches)

    # Strategy A — rows with class "match" or data-match-id attributes.
    # Many GotSport events render one match per `<tr class="match">`.
    for tr in soup.select("tr.match, tr[data-match-id]"):
        rows = _extract_matches_from_tr(
            tr,
            event_id=event_id,
            source_url=source_url,
            default_age=default_age,
            default_gender=default_gender,
            default_division=default_division,
            default_season=default_season,
            default_league=default_league,
            stats=stats,
        )
        matches.extend(rows)

    if matches:
        return _dedup_matches(matches)

    # Strategy B — generic schedule table fallback.
    for table in soup.find_all("table"):
        headers = [_row_text(th).lower() for th in table.find_all("th")]
        # Heuristic: the table contains BOTH a home and an away label.
        if not any("home" in h for h in headers) or not any("away" in h for h in headers):
            # Also accept an "opponent / vs" style single-col schedule.
            if not any("opponent" in h or "vs" in h or "match" in h for h in headers):
                continue
        for tr in table.find_all("tr"):
            if tr.find("th") and not tr.find("td"):
                continue  # header row
            rows = _extract_matches_from_tr(
                tr,
                event_id=event_id,
                source_url=source_url,
                default_age=default_age,
                default_gender=default_gender,
                default_division=default_division,
                default_season=default_season,
                default_league=default_league,
                stats=stats,
            )
            matches.extend(rows)

    return _dedup_matches(matches)


def _extract_matches_from_tr(
    tr,
    *,
    event_id: int | str,
    source_url: str,
    default_age: Optional[str],
    default_gender: Optional[str],
    default_division: Optional[str],
    default_season: Optional[str],
    default_league: Optional[str],
    stats: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Return one OR MORE match dicts for a single ``<tr>``.

    Most GotSport schedule pages render exactly one match per ``<tr>``, but
    a few layouts pack two matches into the same row (alternating
    home/away/score columns, e.g. when the page is a side-by-side bracket
    view). When that shape is detected — the row carries 2+ ``.home``
    selectors, 2+ ``.away`` selectors, or 2+ inline-score patterns and
    the team-name slot count divides evenly — we slice the row and run
    the per-match extractor on each pair.
    """
    home_els = tr.select(".home, .home-team, [data-side='home']")
    away_els = tr.select(".away, .away-team, [data-side='away']")
    score_home_els = tr.select(".home-score, [data-home-score]")
    score_away_els = tr.select(".away-score, [data-away-score]")

    # Multi-match-per-row shape: 2+ home AND 2+ away selectors, equal
    # counts on each side. The shared cells (date, division, status) are
    # broadcast to every sub-match.
    if len(home_els) >= 2 and len(away_els) >= 2 and len(home_els) == len(away_els):
        out: List[Dict[str, Any]] = []
        for i in range(len(home_els)):
            sub = _extract_match_from_tr(
                tr,
                event_id=event_id,
                source_url=source_url,
                default_age=default_age,
                default_gender=default_gender,
                default_division=default_division,
                default_season=default_season,
                default_league=default_league,
                stats=stats,
                _pair_index=i,
                _home_el_override=home_els[i],
                _away_el_override=away_els[i],
                _score_home_el_override=(
                    score_home_els[i] if i < len(score_home_els) else None
                ),
                _score_away_el_override=(
                    score_away_els[i] if i < len(score_away_els) else None
                ),
            )
            if sub is not None:
                out.append(sub)
        if out:
            return out
        # Fall through: detection misfired, try the single-match path.

    single = _extract_match_from_tr(
        tr,
        event_id=event_id,
        source_url=source_url,
        default_age=default_age,
        default_gender=default_gender,
        default_division=default_division,
        default_season=default_season,
        default_league=default_league,
        stats=stats,
    )
    return [single] if single is not None else []


def _extract_match_from_tr(
    tr,
    *,
    event_id: int | str,
    source_url: str,
    default_age: Optional[str],
    default_gender: Optional[str],
    default_division: Optional[str],
    default_season: Optional[str],
    default_league: Optional[str],
    stats: Optional[Dict[str, int]] = None,
    _pair_index: int = 0,
    _home_el_override=None,
    _away_el_override=None,
    _score_home_el_override=None,
    _score_away_el_override=None,
) -> Optional[Dict[str, Any]]:
    # First try stable class selectors. The ``_*_override`` params are the
    # multi-match-per-row hook: when ``_extract_matches_from_tr`` slices a
    # row into N sub-matches it passes the i-th home/away/score cell here.
    home_el = _home_el_override if _home_el_override is not None else tr.select_one(".home, .home-team, [data-side='home']")
    away_el = _away_el_override if _away_el_override is not None else tr.select_one(".away, .away-team, [data-side='away']")
    score_home_el = _score_home_el_override if _score_home_el_override is not None else tr.select_one(".home-score, [data-home-score]")
    score_away_el = _score_away_el_override if _score_away_el_override is not None else tr.select_one(".away-score, [data-away-score]")
    date_el = tr.select_one(".match-date, .date, time")
    status_el = tr.select_one(".status, .match-status")
    division_el = tr.select_one(".division, .bracket, .age-group")

    home_name = _row_text(home_el)
    away_name = _row_text(away_el)

    # BYE row — one side literally says BYE. Not a real match; skip.
    if _is_bye_cell(home_name) or _is_bye_cell(away_name):
        logger.debug(
            "[gotsport-matches] BYE row skipped: home=%r away=%r",
            home_name, away_name,
        )
        return None

    # Per-pair match id wins in multi-match-per-row mode: check the home
    # cell subtree first (its own attr or any descendant carrying one),
    # fall back to the row-level attr only when nothing pair-local exists.
    match_id_attr: Optional[str] = None
    if home_el is not None:
        try:
            match_id_attr = home_el.get("data-match-id")
        except AttributeError:
            match_id_attr = None
        if not match_id_attr:
            inner = home_el.select_one("[data-match-id]") if hasattr(home_el, "select_one") else None
            if inner is not None:
                match_id_attr = inner.get("data-match-id")
    if not match_id_attr:
        match_id_attr = tr.get("data-match-id") or (
            tr.select_one("[data-match-id]").get("data-match-id")
            if tr.select_one("[data-match-id]") else None
        )

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    score_cell_text: Optional[str] = None
    if score_home_el is not None:
        try:
            home_score = int(_row_text(score_home_el))
        except (ValueError, TypeError):
            home_score = None
    if score_away_el is not None:
        try:
            away_score = int(_row_text(score_away_el))
        except (ValueError, TypeError):
            away_score = None

    match_date = _parse_date(_row_text(date_el)) if date_el else None

    # Positional-cell fallback: runs when either names or scores are
    # missing from class-based selectors. GotSport often labels home/away
    # team cells but leaves the middle score cell un-classed, so
    # class-selector scrapes land names but not scores.
    needs_tds_scan = (
        not home_name or not away_name
        or (home_score is None and away_score is None)
    )
    if needs_tds_scan:
        tds = tr.find_all("td")
        texts = [_row_text(td) for td in tds]
        if not texts:
            return None
        # Look for a "Home vs Away" cell.
        vs_text: Optional[str] = None
        for t in texts:
            if re.search(r"\bvs\.?\b", t, re.IGNORECASE):
                vs_text = t
                break
        if vs_text:
            # Extract the score FIRST (before name-splitting), using a
            # word-boundary regex so a team name containing year-numbers
            # like "FC 2010-2012" doesn't get its year eaten by score
            # parsing. Strip the matched score off the merged text before
            # splitting names so the residual name halves are clean.
            score_text = vs_text
            score_m = _INLINE_SCORE_PATTERN.search(vs_text) if (
                home_score is None and away_score is None
            ) else None
            if score_m:
                home_score = int(score_m.group(1))
                away_score = int(score_m.group(2))
                # Strip the score chunk out of the cell before splitting names.
                score_text = (
                    vs_text[: score_m.start()] + " " + vs_text[score_m.end() :]
                )
            parts = re.split(
                r"\bvs\.?\b", score_text, maxsplit=1, flags=re.IGNORECASE
            )
            if len(parts) == 2:
                home_name = home_name or parts[0].strip()
                away_name = away_name or parts[1].strip()

        # BYE cell — not a real match, skip the row entirely.
        for t in texts:
            if _is_bye_cell(t):
                logger.debug("[gotsport-matches] BYE row skipped: %s", texts)
                return None

        # Try score-shaped cells. Remember the cell for forfeit detection.
        if home_score is None or away_score is None:
            for t in texts:
                hs, as_ = _parse_score(t)
                if hs is not None:
                    home_score, away_score = hs, as_
                    score_cell_text = t
                    break
                if _FORFEIT_PATTERN.search(t):
                    # "FF" / "Forfeit" with no score — remember the cell
                    # so _normalize_status tags it as forfeit.
                    score_cell_text = t

        # Try date-shaped cells.
        if match_date is None:
            for t in texts:
                d = _parse_date(t)
                if d is not None:
                    match_date = d
                    break

    if not home_name or not away_name:
        return None

    # Age + gender: cell-level division first, fall back to defaults.
    division_text = _row_text(division_el) if division_el else None
    age_group, gender = _parse_age_gender(division_text or "")
    age_group = age_group or default_age
    gender = gender or default_gender
    division = division_text or default_division

    status_raw = _row_text(status_el) if status_el else None
    status = _normalize_status(status_raw, home_score, away_score, score_cell_text)

    # Empty score cell with no match-date → future / TBD match, leave as
    # "scheduled" with null scores (the DB default). This is the expected
    # state for a scrape of an upcoming weekend's schedule.
    if home_score is None and away_score is None and status == "scheduled" and match_date is None:
        logger.debug(
            "[gotsport-matches] scheduled row with no date: %s vs %s",
            home_name, away_name,
        )

    # Drop rows whose team names cannot be canonicalised. A raw fallback
    # ("home_club_canonical": _canonical(name) or name) used to live here,
    # but emitting non-canonicalisable names corrupts the canonical-club
    # linker's training set downstream — the linker fuzzy-matches these
    # garbage strings against canonical_clubs and either spawns spurious
    # aliases or pollutes its match-rate stats. Better to drop the row,
    # count it, and log it so an operator can investigate the source HTML.
    home_canonical = _canonical(home_name)
    away_canonical = _canonical(away_name)
    if not home_canonical or not away_canonical:
        if stats is not None:
            stats["dropped_non_canonicalizable"] = (
                stats.get("dropped_non_canonicalizable", 0) + 1
            )
        logger.warning(
            "[gotsport-matches] dropping row with non-canonicalizable team "
            "name(s): home=%r away=%r (canonicals: %r, %r)",
            home_name, away_name, home_canonical, away_canonical,
        )
        return None

    return {
        "home_team_name": home_name,
        "away_team_name": away_name,
        "home_club_canonical": home_canonical,
        "away_club_canonical": away_canonical,
        "home_score": home_score,
        "away_score": away_score,
        "match_date": match_date,
        "age_group": age_group,
        "gender": gender,
        "division": division,
        "season": default_season,
        "league": default_league,
        "status": status,
        "platform_match_id": str(match_id_attr) if match_id_attr else None,
        "source": "gotsport",
        "source_url": source_url,
        "event_id": int(event_id) if str(event_id).isdigit() else None,
    }


def _dedup_matches(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """De-duplicate within a single scrape based on platform id or natural key.

    The natural-key path truncates ``match_date`` to whole-second precision
    in the dedup key only — the persisted ``match_date`` on the row dict
    is left untouched. Two parsing paths can emit the same logical match
    with microsecond-different timestamps (e.g. one path passes the cell
    text through a date library that stamps now()-style microseconds),
    and we don't want those to escape dedup as two rows.
    """
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("platform_match_id"):
            key: Tuple = ("pid", r["source"], r["platform_match_id"])
        else:
            md = r.get("match_date")
            if isinstance(md, datetime):
                md = md.replace(microsecond=0)
            key = (
                "nat",
                r["home_team_name"],
                r["away_team_name"],
                md,
                r.get("age_group") or "",
                r.get("gender") or "",
            )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def scrape_gotsport_matches(
    event_id: int | str,
    *,
    source_url: Optional[str] = None,
    default_age: Optional[str] = None,
    default_gender: Optional[str] = None,
    default_division: Optional[str] = None,
    default_season: Optional[str] = None,
    default_league: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch all GotSport match rows for ``event_id``.

    Discovery flow:
    1. Fetch the event home page and collect all ``?group=<ID>`` links.
    2. For each group, fetch ``?date=All&group=<ID>`` and parse with
       Strategy C (column-position table parser).
    3. If no groups are found, fall back to a direct ``source_url`` fetch
       parsed with the legacy Strategy A/B parsers.

    Returns a list of dicts shaped for
    ``scraper.ingest.matches_writer.insert_matches``.
    """
    stats: Dict[str, int] = {"dropped_non_canonicalizable": 0}

    # --- Group-based discovery (primary path) --------------------------------
    group_ids = _discover_group_ids(event_id)
    if group_ids:
        all_matches: List[Dict[str, Any]] = []
        for group_id in group_ids:
            group_url = (
                f"{_BASE}/org_event/events/{event_id}"
                f"/schedules?date=All&group={group_id}"
            )
            logger.debug("[gotsport-matches] fetching group %s: %s", group_id, group_url)
            try:
                r = _get_with_retry(group_url)
            except (TransientError, requests.RequestException) as exc:
                logger.warning(
                    "[gotsport-matches] fetch failed event=%s group=%s: %s",
                    event_id, group_id, exc,
                )
                continue
            rows = _extract_matches_from_html(
                r.text,
                event_id=event_id,
                source_url=group_url,
                default_age=default_age,
                default_gender=default_gender,
                default_division=default_division,
                default_season=default_season,
                default_league=default_league,
                stats=stats,
            )
            all_matches.extend(rows)

        all_matches = _dedup_matches(all_matches)
        logger.info(
            "[gotsport-matches] event %s → %d matches across %d groups "
            "(dropped %d non-canonicalizable rows)",
            event_id, len(all_matches), len(group_ids),
            stats["dropped_non_canonicalizable"],
        )
        return all_matches

    # --- Legacy fallback (direct URL, older event layouts) -------------------
    url = source_url or f"{_BASE}/org_event/events/{event_id}/schedules"
    logger.info("[gotsport-matches] no groups found, fetching %s", url)
    try:
        r = _get_with_retry(url)
    except (TransientError, requests.RequestException) as exc:
        logger.error("[gotsport-matches] fetch failed for event %s: %s", event_id, exc)
        return []
    matches = _extract_matches_from_html(
        r.text,
        event_id=event_id,
        source_url=url,
        default_age=default_age,
        default_gender=default_gender,
        default_division=default_division,
        default_season=default_season,
        default_league=default_league,
        stats=stats,
    )
    logger.info(
        "[gotsport-matches] event %s → %d matches "
        "(dropped %d non-canonicalizable rows)",
        event_id, len(matches), stats["dropped_non_canonicalizable"],
    )
    return matches
