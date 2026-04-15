"""
GotSport per-event match (schedule) extractor.

GotSport exposes a "Schedules" / "Results" HTML page per event at a URL
of the form:

    https://system.gotsport.com/org_event/events/{event_id}/schedules

Each row on this page corresponds to one scheduled or completed game.
We extract one dict per row and shape it for the `matches` table in
Domain 5 of the Path A data model.

Notes on the HTML shape
-----------------------
GotSport schedule markup is not perfectly uniform across events, but
every variant we've seen renders one `<tr>` per match inside one or
more `<table>` blocks. The columns we care about (date, home team,
score, away team, age/gender/division, optional platform match id)
are each surfaced with either a stable CSS class or a stable text
layout. This extractor is defensive: it tries multiple strategies to
pick up each field and only emits a row when it can extract at least
home + away team names.

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

from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS  # noqa: E402
from normalizer import _canonical  # noqa: E402
from utils.retry import retry_with_backoff, TransientError  # noqa: E402

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
    )
}
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
# Field parsers
# ---------------------------------------------------------------------------

_SCORE_PATTERN = re.compile(r"^\s*(\d{1,2})\s*[-–:]\s*(\d{1,2})\s*$")
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
    if not raw:
        return None, None
    m = _SCORE_PATTERN.match(raw)
    if not m:
        return None, None
    try:
        return int(m.group(1)), int(m.group(2))
    except ValueError:
        return None, None


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


def _normalize_status(raw: Optional[str], home_score: Optional[int], away_score: Optional[int]) -> str:
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
    if home_score is not None and away_score is not None:
        return "final"
    return "scheduled"


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
    """
    soup = BeautifulSoup(html, "lxml")
    matches: List[Dict[str, Any]] = []

    # Strategy A — rows with class "match" or data-match-id attributes.
    # Many GotSport events render one match per `<tr class="match">`.
    for tr in soup.select("tr.match, tr[data-match-id]"):
        row = _extract_match_from_tr(
            tr,
            event_id=event_id,
            source_url=source_url,
            default_age=default_age,
            default_gender=default_gender,
            default_division=default_division,
            default_season=default_season,
            default_league=default_league,
        )
        if row is not None:
            matches.append(row)

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
            row = _extract_match_from_tr(
                tr,
                event_id=event_id,
                source_url=source_url,
                default_age=default_age,
                default_gender=default_gender,
                default_division=default_division,
                default_season=default_season,
                default_league=default_league,
            )
            if row is not None:
                matches.append(row)

    return _dedup_matches(matches)


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
) -> Optional[Dict[str, Any]]:
    # First try stable class selectors.
    home_el = tr.select_one(".home, .home-team, [data-side='home']")
    away_el = tr.select_one(".away, .away-team, [data-side='away']")
    score_home_el = tr.select_one(".home-score, [data-home-score]")
    score_away_el = tr.select_one(".away-score, [data-away-score]")
    date_el = tr.select_one(".match-date, .date, time")
    status_el = tr.select_one(".status, .match-status")
    division_el = tr.select_one(".division, .bracket, .age-group")

    home_name = _row_text(home_el)
    away_name = _row_text(away_el)

    match_id_attr = tr.get("data-match-id") or (
        tr.select_one("[data-match-id]").get("data-match-id")
        if tr.select_one("[data-match-id]") else None
    )

    home_score: Optional[int] = None
    away_score: Optional[int] = None
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

    # Fallback: positional cells.
    if not home_name or not away_name:
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
            parts = re.split(r"\bvs\.?\b", vs_text, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                home_name = home_name or parts[0].strip()
                away_name = away_name or parts[1].strip()
                # A trailing score like "Team A 3 - 1 Team B" may get merged in.
                score_m = re.search(r"(\d{1,2})\s*[-–]\s*(\d{1,2})", vs_text)
                if score_m and home_score is None and away_score is None:
                    # Only adopt when home/away names don't themselves end in digits.
                    home_score = int(score_m.group(1))
                    away_score = int(score_m.group(2))
                    # Strip trailing numbers from names.
                    home_name = re.sub(r"\s+\d{1,2}\s*$", "", home_name).strip()
                    away_name = re.sub(r"^\s*\d{1,2}\s+", "", away_name).strip()

        # Try score-shaped cells.
        if home_score is None or away_score is None:
            for t in texts:
                hs, as_ = _parse_score(t)
                if hs is not None:
                    home_score, away_score = hs, as_
                    break

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
    status = _normalize_status(status_raw, home_score, away_score)

    return {
        "home_team_name": home_name,
        "away_team_name": away_name,
        "home_club_canonical": _canonical(home_name) or home_name,
        "away_club_canonical": _canonical(away_name) or away_name,
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
    """De-duplicate within a single scrape based on platform id or natural key."""
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if r.get("platform_match_id"):
            key: Tuple = ("pid", r["source"], r["platform_match_id"])
        else:
            key = (
                "nat",
                r["home_team_name"],
                r["away_team_name"],
                r.get("match_date"),
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
    """
    Fetch the GotSport schedules page for ``event_id`` and extract match rows.

    Returns a list of dicts shaped for ``scraper.ingest.matches_writer.insert_matches``.
    """
    url = source_url or f"{_BASE}/org_event/events/{event_id}/schedules"
    logger.info("[gotsport-matches] fetching %s", url)
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
    )
    logger.info("[gotsport-matches] event %s → %d matches", event_id, len(matches))
    return matches
