"""
MLS NEXT match schedule extractor.

Fetches match results from the Modular11 platform that powers MLS NEXT
schedules at https://www.mlssoccer.com/mlsnext/schedule/.

ENDPOINT
--------
    GET https://www.modular11.com/public_schedule/league/get_matches
    Params:
        tournament  = 12          (MLS NEXT league ID)
        match_type  = 2           (league matches)
        gender      = 0           (0=all, 1=male, 2=female)
        age         = <age_uid>   (see AGE_GROUPS below)
        open_page   = <N>         (0-based page index, 25 rows per page)
        start_date  = "YYYY-MM-DD HH:mm:ss"
        end_date    = "YYYY-MM-DD HH:mm:ss"
        status      = "all"
        schedule    = 0
        academy     = 0

    Headers: Origin + Referer pointing to www.modular11.com (enforced by CORS).
    Response: HTML fragment (not JSON) — desktop + mobile duplicates; parse only desktop.

HTML STRUCTURE (confirmed April 2026 against live endpoint)
-----------------------------------------------------------
Match rows: div.container-row > div.row.table-content-row.hidden-xs
    [js-match-group]          → division name ("Mid-Atlantic")
    First col-sm-1.pad-0      → match ID + gender ("100568\\nMALE")
    col-sm-2 (first)          → date/time text node ("09/06/25 08:30am")
    container-first-team p    → home team name
    container-second-team p   → away team name
    span.score-match-table    → score ("2\\xa0:\\xa01") or "TBD"

Pagination: div.current-pages text "X page out of Y"; open_page is 0-indexed.

AGE GROUPS
----------
    U13 → age_uid=21
    U14 → age_uid=22
    U15 → age_uid=33
    U16 → age_uid=14
    U17 → age_uid=15
    U19 → age_uid=26
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.modular11.com/public_schedule/league/get_matches"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.modular11.com/schedule?year=21",
    "Origin": "https://www.modular11.com",
}

_TOURNAMENT_ID = 12   # MLS NEXT
_MATCH_TYPE = 2       # league matches

# MLS NEXT season runs roughly August–June.
_DEFAULT_SEASON = "2025-26"
_DEFAULT_START = "2025-08-01 00:00:00"
_DEFAULT_END   = "2026-06-30 23:59:59"

# Modular11 age-group UIDs → (age_label, gender_code)
# Gender is embedded in each row (MALE/FEMALE), so we scrape gender=0 (both)
# per age group and read gender from the row.
AGE_GROUPS: Dict[int, str] = {
    21: "U13",
    22: "U14",
    33: "U15",
    14: "U16",
    15: "U17",
    26: "U19",
}

_GENDER_MAP = {"MALE": "M", "FEMALE": "F"}

_DATE_FORMATS = [
    "%m/%d/%y %I:%M%p",   # "09/06/25 08:30am"
    "%m/%d/%y %I:%M %p",
    "%m/%d/%Y %I:%M%p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%y",
    "%m/%d/%Y",
]

_SCORE_RE = re.compile(r"(\d+)\xa0:\xa0(\d+)")


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _fetch_page(age_uid: int, page: int, start_date: str, end_date: str) -> str:
    """Fetch one page of match HTML for a given age-group UID."""
    params = {
        "tournament": _TOURNAMENT_ID,
        "match_type": _MATCH_TYPE,
        "gender": 0,
        "age": age_uid,
        "open_page": page,
        "start_date": start_date,
        "end_date": end_date,
        "schedule": 0,
        "status": "all",
        "academy": 0,
    }
    r = requests.get(_BASE_URL, params=params, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return r.text


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
    logger.debug("[mlsnext-matches] unparseable date: %r", raw)
    return None


def _get_total_pages(soup: BeautifulSoup) -> int:
    """Parse 'X page out of Y' from the paginator div."""
    el = soup.find("div", class_="current-pages")
    if not el:
        return 1
    m = re.search(r"out of (\d+)", el.get_text())
    return int(m.group(1)) if m else 1


def _parse_html_page(
    html: str,
    age_label: str,
    league_name: Optional[str],
    season: Optional[str],
    source_url: str,
) -> Tuple[List[Dict], int]:
    """Parse one page of Modular11 HTML.

    Returns (rows, total_pages).
    """
    soup = BeautifulSoup(html, "lxml")
    total_pages = _get_total_pages(soup)
    rows: List[Dict] = []

    # Desktop-only rows (hidden-xs hides mobile duplicates).
    for row in soup.select("div.container-row > div.row.table-content-row.hidden-xs"):
        division = row.get("js-match-group") or ""

        # --- Match ID + gender ---
        id_col = row.find("div", class_=lambda c: c and "col-sm-1" in c and "pad-0" in c)
        if not id_col:
            continue
        id_parts = [t.strip() for t in id_col.get_text("\n").split("\n") if t.strip()]
        platform_match_id = id_parts[0] if id_parts else None
        gender_raw = id_parts[1].upper() if len(id_parts) > 1 else ""
        gender = _GENDER_MAP.get(gender_raw)

        # --- Date (first text node of the first col-sm-2) ---
        date_col = row.find("div", class_=lambda c: c and "col-sm-2" in c)
        match_date: Optional[datetime] = None
        if date_col:
            date_text = next(
                (t.strip() for t in date_col.strings if t.strip()),
                "",
            )
            if date_text:
                match_date = _parse_date(date_text)

        # --- Team names ---
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

        # --- Score ---
        home_score: Optional[int] = None
        away_score: Optional[int] = None
        score_el = row.select_one("span.score-match-table")
        if score_el:
            score_text = score_el.get_text(strip=True)
            m = _SCORE_RE.search(score_text)
            if m:
                home_score = int(m.group(1))
                away_score = int(m.group(2))

        status = "final" if (home_score is not None and away_score is not None) else "scheduled"

        rows.append({
            "home_team_name":    home_team,
            "away_team_name":    away_team,
            "home_score":        home_score,
            "away_score":        away_score,
            "match_date":        match_date,
            "age_group":         age_label,
            "gender":            gender,
            "division":          division or None,
            "season":            season,
            "league":            league_name or "MLS NEXT",
            "status":            status,
            "source":            "mlsnext",
            "source_url":        source_url,
            "platform_match_id": platform_match_id,
        })

    return rows, total_pages


# ---------------------------------------------------------------------------
# Per-age-group scrape
# ---------------------------------------------------------------------------

def _scrape_age_group(
    age_uid: int,
    age_label: str,
    *,
    league_name: Optional[str],
    season: Optional[str],
    start_date: str,
    end_date: str,
    rate_limit: float = 0.5,
) -> List[Dict]:
    """Scrape all pages for one age-group UID and return match rows."""
    source_url = (
        f"{_BASE_URL}?tournament={_TOURNAMENT_ID}&match_type={_MATCH_TYPE}"
        f"&age={age_uid}&status=all"
    )

    try:
        first_html = _fetch_page(age_uid, 0, start_date, end_date)
    except requests.RequestException as exc:
        logger.error("[mlsnext-matches] fetch failed age=%s: %s", age_label, exc)
        return []

    first_rows, total_pages = _parse_html_page(
        first_html, age_label, league_name, season, source_url,
    )
    all_rows = list(first_rows)

    logger.info(
        "[mlsnext-matches] age=%s → %d page(s), fetching…",
        age_label, total_pages,
    )

    for page in range(1, total_pages):
        time.sleep(rate_limit)
        try:
            html = _fetch_page(age_uid, page, start_date, end_date)
        except requests.RequestException as exc:
            logger.warning(
                "[mlsnext-matches] page %d/%d failed age=%s: %s",
                page + 1, total_pages, age_label, exc,
            )
            continue
        rows, _ = _parse_html_page(html, age_label, league_name, season, source_url)
        all_rows.extend(rows)

    logger.info(
        "[mlsnext-matches] age=%s → %d match rows across %d page(s)",
        age_label, len(all_rows), total_pages,
    )
    return all_rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_mlsnext_matches(
    *,
    league_name: Optional[str] = None,
    season: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    age_uids: Optional[List[int]] = None,
    max_workers: int = 3,
) -> List[Dict]:
    """Scrape all MLS NEXT match rows for a season.

    Iterates over all age groups (U13–U19), paginates through Modular11
    HTML pages, and returns rows shaped for ``insert_matches()``.

    Args:
        league_name:  Override for the ``league`` column (default "MLS NEXT").
        season:       Season tag (e.g. "2025-26").
        start_date:   Season start in ``"YYYY-MM-DD HH:mm:ss"`` format.
        end_date:     Season end in ``"YYYY-MM-DD HH:mm:ss"`` format.
        age_uids:     Subset of Modular11 age-group UIDs to scrape (default: all).
        max_workers:  ThreadPoolExecutor concurrency for age groups (default 3).

    Returns:
        List of match dicts for ``matches_writer.insert_matches``.
        ``home_club_id`` / ``away_club_id`` are absent; the linker resolves them.
    """
    sd = start_date or _DEFAULT_START
    ed = end_date or _DEFAULT_END
    effective_league = league_name or "MLS NEXT"
    uids = age_uids or list(AGE_GROUPS.keys())

    logger.info(
        "[mlsnext-matches] scraping %d age group(s): %s → %s",
        len(uids), sd, ed,
    )

    all_rows: List[Dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(
                _scrape_age_group,
                uid,
                AGE_GROUPS[uid],
                league_name=effective_league,
                season=season,
                start_date=sd,
                end_date=ed,
            ): AGE_GROUPS[uid]
            for uid in uids
            if uid in AGE_GROUPS
        }
        for f in as_completed(futs):
            age_label = futs[f]
            try:
                rows = f.result()
            except Exception as exc:
                logger.error("[mlsnext-matches] age=%s failed: %s", age_label, exc)
                rows = []
            all_rows.extend(rows)

    logger.info(
        "[mlsnext-matches] total → %d match rows across %d age group(s)",
        len(all_rows), len(uids),
    )
    return all_rows
