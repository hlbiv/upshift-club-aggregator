"""
SincSports match/schedule extractor.

Fetches match results from soccer.sincsports.com tournament schedule pages.

DISCOVERY FLOW:
  1. Fetch division listing:
       https://soccer.sincsports.com/schedule.aspx?div=N&tid=<TID>&year=<YEAR>&stid=<TID>&syear=<YEAR>
     Parse all division links to get (div_code, division_name) pairs.
     Division link pattern: schedule.aspx?tid=X&year=Y&stid=X&syear=Y&div=U15M01

  2. For each division, fetch:
       https://soccer.sincsports.com/schedule.aspx?tid=<TID>&year=<YEAR>&stid=<TID>&syear=<YEAR>&div=<DIV_CODE>
     The page is server-rendered ASP.NET — match data is in the initial HTML.

MATCH ROW STRUCTURE (observed April 2026, Concorde Fire Challenge Cup):
  Match data is in <table> rows. Each match row contains cells with:
    - Date (e.g. "Saturday2/28/2026") or date header
    - Time (e.g. "8:00 AM")
    - Game # (e.g. "#00001")
    - Home team prefixed with "H:" (e.g. "H: Coastal Rush ECNL RL B11")
    - Away team prefixed with "A:" (e.g. "A: FTL UTD 2011 ECNL")
    - Scores (home_score, away_score as separate digits)
    - Division name + venue

KNOWN TIDS (tournament source ids):
  Demo-critical:
    CONCFC  — Concorde Fire Challenge Cup Boys
    CONCG   — Concorde Fire Challenge Cup Girls
  Regional (in leagues_master.csv):
    GULFC, HOOVHAV, MISSFSC2, APPHIGHSC, REDRV, KHILL,
    HFCSPRCL, BAMABLST, PALMETTO, BAYOUCTY, CAROCLS,
    SHOWME, BADGER, CORNHSK, SCCLCUP3
"""

from __future__ import annotations

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE_URL = "https://soccer.sincsports.com"

_AGE_RE = re.compile(r"\bU(?:nder\s+)?(\d{1,2})\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b(boys?|girls?|male|female|men|women)\b", re.IGNORECASE)
_GENDER_MAP = {
    "boy": "M", "boys": "M", "male": "M", "men": "M",
    "girl": "G", "girls": "G", "female": "G", "women": "G",
}

# "H: Team Name" or "A: Team Name" prefixes
_HOME_RE = re.compile(r"^H:\s*(.+)$", re.IGNORECASE)
_AWAY_RE = re.compile(r"^A:\s*(.+)$", re.IGNORECASE)

_DATE_FORMATS = [
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
]

# Year to use when year can't be determined from URL
_DEFAULT_YEAR = 2026


def _parse_age_gender(text: str) -> Tuple[Optional[str], Optional[str]]:
    age_m = _AGE_RE.search(text)
    age_group = f"U{age_m.group(1)}" if age_m else None
    gender_m = _GENDER_RE.search(text)
    gender = _GENDER_MAP.get(gender_m.group(1).lower()) if gender_m else None
    return age_group, gender


def _parse_date(date_str: str, time_str: str = "", year: int = _DEFAULT_YEAR) -> Optional[datetime]:
    # SincSports dates: "2/28/2026" with optional time "8:00 AM"
    text = f"{date_str} {time_str}".strip() if time_str else date_str.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    # Try inserting year if missing
    if "/" in date_str and date_str.count("/") == 1:
        text2 = f"{date_str}/{year} {time_str}".strip()
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(text2, fmt)
            except ValueError:
                continue
    logger.debug("[SincSports matches] unparseable date: %r %r", date_str, time_str)
    return None


def _get_year_from_url(url: str) -> int:
    m = re.search(r"[?&]year=(\d{4})", url)
    return int(m.group(1)) if m else _DEFAULT_YEAR


def _fetch_division_links(tid: str, year: int) -> List[Tuple[str, str, str]]:
    """
    Fetch division listing and return list of (div_code, division_name, full_url).
    """
    url = f"{_BASE_URL}/schedule.aspx?div=N&tid={tid}&year={year}&stid={tid}&syear={year}"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=25)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("[SincSports matches] division listing failed tid=%s: %s", tid, exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    divisions: List[Tuple[str, str, str]] = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]div=([^&]+)", href)
        if not m:
            continue
        div_code = m.group(1)
        if div_code in seen or div_code.upper() == "N":
            continue
        seen.add(div_code)
        div_name = a.get_text(strip=True)
        full_url = urljoin(_BASE_URL + "/", href)
        divisions.append((div_code, div_name, full_url))

    logger.info("[SincSports matches] tid=%s year=%d → %d divisions", tid, year, len(divisions))
    return divisions


def _fetch_division_schedule(
    div_url: str,
    div_code: str,
    div_name: str,
    tournament_name: str,
    season: Optional[str],
) -> List[Dict]:
    year = _get_year_from_url(div_url)
    try:
        r = requests.get(div_url, headers=_HEADERS, timeout=25)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.debug("[SincSports matches] fetch failed div=%s: %s", div_code, exc)
        return []

    return _parse_division_html(r.text, div_url, div_name, tournament_name, season, year)


def _parse_division_html(
    html: str,
    source_url: str,
    div_name: str,
    tournament_name: str,
    season: Optional[str],
    year: int,
) -> List[Dict]:
    """
    Parse a SincSports division schedule page.

    Match rows contain cells with "H:" and "A:" prefixed team names.
    Scores appear as individual digit cells immediately after team cells.
    """
    soup = BeautifulSoup(html, "lxml")
    age_group, gender = _parse_age_gender(div_name)
    records: List[Dict] = []

    current_date: Optional[str] = None

    for table in soup.find_all("table"):
        for tr in table.find_all("tr"):
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(["td", "th"])]
            if not cells:
                continue

            # Detect date header rows — contain a date like "2/28/2026" or day+date
            date_m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", " ".join(cells))
            if date_m:
                current_date = date_m.group(1)

            # Find home/away team cells
            home_raw = away_raw = None
            time_str = ""
            game_num = None
            home_score = away_score = None

            for i, cell in enumerate(cells):
                hm = _HOME_RE.match(cell)
                if hm:
                    home_raw = hm.group(1).strip()
                am = _AWAY_RE.match(cell)
                if am:
                    away_raw = am.group(1).strip()

                # Time
                if re.match(r"^\d{1,2}:\d{2}\s*(AM|PM)$", cell, re.IGNORECASE):
                    time_str = cell

                # Game number
                if re.match(r"^#\d+", cell):
                    game_num = cell.lstrip("#")

            if not home_raw or not away_raw:
                continue

            # Scores: look for standalone digit(s) in cells after teams
            score_cells = [c for c in cells if re.match(r"^\d{1,2}$", c)]
            if len(score_cells) >= 2:
                try:
                    home_score = int(score_cells[0])
                    away_score = int(score_cells[1])
                except ValueError:
                    pass

            status = "final" if (home_score is not None and away_score is not None) else "scheduled"
            match_date = _parse_date(current_date or "", time_str, year) if current_date else None

            records.append({
                "home_team_name":    home_raw,
                "away_team_name":    away_raw,
                "home_score":        home_score,
                "away_score":        away_score,
                "match_date":        match_date,
                "age_group":         age_group,
                "gender":            gender,
                "division":          div_name,
                "season":            season,
                "tournament_name":   tournament_name,
                "match_type":        "group",
                "status":            status,
                "source":            "sincsports",
                "source_url":        source_url,
                "platform_match_id": game_num,
            })

    return records


def scrape_sincsports_matches(
    tid: str,
    tournament_name: str,
    season: Optional[str] = None,
    year: int = _DEFAULT_YEAR,
) -> List[Dict]:
    """
    Scrape all match rows for a SincSports tournament.

    Args:
        tid:             SincSports tournament id (e.g. "CONCFC")
        tournament_name: Human-readable name (e.g. "Concorde Fire Challenge Cup Boys")
        season:          Season tag (e.g. "2025-26")
        year:            Tournament year for URL construction (default 2026)

    Returns:
        List of match row dicts for tournament_matches_writer.
    """
    divisions = _fetch_division_links(tid, year)
    if not divisions:
        logger.warning("[SincSports matches] tid=%s → no divisions found", tid)
        return []

    all_rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {
            ex.submit(
                _fetch_division_schedule,
                div_url, div_code, div_name, tournament_name, season
            ): (div_code, div_name)
            for div_code, div_name, div_url in divisions
        }
        for f in as_completed(futs):
            div_code, div_name = futs[f]
            rows = f.result()
            all_rows.extend(rows)
            if rows:
                logger.debug("[SincSports matches] %s (%s) → %d rows", div_name, div_code, len(rows))

    if not all_rows:
        logger.warning(
            "[SincSports matches] tid=%s → 0 matches parsed across %d divisions. "
            "Run with --dry-run and DEBUG logging to inspect HTML structure.",
            tid, len(divisions)
        )
    else:
        logger.info("[SincSports matches] tid=%s (%s) → %d matches across %d divisions",
                    tid, tournament_name, len(all_rows), len(divisions))

    return all_rows
