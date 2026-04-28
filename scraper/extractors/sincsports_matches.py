"""
SincSports match/schedule extractor.

Fetches the schedule page for a SincSports tournament (soccer.sincsports.com)
and parses match results into rows for the ``tournament_matches`` table.

SCHEDULE PAGE URL:
  https://soccer.sincsports.com/schedule.aspx?tid=<TID>

The schedule page renders plain HTML (no JS required) with one or more
<table> blocks — one per age-group/gender division. Each row is a match:
  td[0] = Date/time (e.g. "Sat 03/15 10:00 AM")
  td[1] = Home team name
  td[2] = Score or "vs" if not yet played (e.g. "2-1", "0-0", "vs")
  td[3] = Away team name
  td[4] = Field/venue (optional)

Division label appears as a header row above each group of matches.
The division text (e.g. "U13 Boys Premier", "U15 Girls Championship")
is used to extract age_group and gender.

KNOWN TIDS (tournament source ids):
  Demo-critical:
    CONCFC  — Concorde Fire Challenge Cup Boys
    CONCG   — Concorde Fire Challenge Cup Girls
  Regional (already in leagues_master.csv):
    GULFC, HOOVHAV, MISSFSC2, APPHIGHSC, REDRV, KHILL,
    HFCSPRCL, BAMABLST, PALMETTO, BAYOUCTY, CAROCLS,
    SHOWME, BADGER, CORNHSK, SCCLCUP3

NOTE: This extractor was written against the observed page structure but
has not yet been run live. On first run validate the HTML shape matches
expectations and adjust the column index if needed. Enable DEBUG logging
to see raw row data:
  python3 run.py --source sincsports-matches --tid CONCFC --dry-run
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE_URL = "https://soccer.sincsports.com"
_SCHEDULE_PATH = "/schedule.aspx"

# Age group extraction: "U13", "U15", "U14 Boys", "U11 Girls", etc.
_AGE_RE = re.compile(r"\bU(\d{1,2})\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b(boys?|girls?|male|female|men|women)\b", re.IGNORECASE)
_GENDER_MAP = {
    "boy": "M", "boys": "M", "male": "M", "men": "M",
    "girl": "G", "girls": "G", "female": "G", "women": "G",
}

# Score parsing: "2-1", "0-0", "3 - 2" (with spaces)
_SCORE_RE = re.compile(r"^(\d+)\s*-\s*(\d+)$")

# Date parsing — SincSports observed formats:
#   "Sat 03/15 10:00 AM"  → no year (infer current season)
#   "03/15/2026 10:00 AM"
_DATE_FORMATS = [
    "%a %m/%d %I:%M %p",   # "Sat 03/15 10:00 AM" (no year)
    "%m/%d/%Y %I:%M %p",   # "03/15/2026 10:00 AM"
    "%m/%d/%Y %H:%M",      # "03/15/2026 14:00"
    "%m/%d %I:%M %p",      # "03/15 10:00 AM" (no day name, no year)
]

# Minimum columns a row needs to be a match row (not a header).
_MIN_MATCH_COLS = 3


def _extract_tid(url: str) -> Optional[str]:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    return qs.get("tid", [None])[0]


def _parse_age_gender(text: str) -> Tuple[Optional[str], Optional[str]]:
    age_m = _AGE_RE.search(text)
    age_group = f"U{age_m.group(1)}" if age_m else None
    gender_m = _GENDER_RE.search(text)
    gender = _GENDER_MAP.get(gender_m.group(1).lower()) if gender_m else None
    return age_group, gender


def _parse_score(text: str) -> Tuple[Optional[int], Optional[int]]:
    text = text.strip()
    m = _SCORE_RE.match(text)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def _parse_date(text: str, season_year: int = 2026) -> Optional[datetime]:
    text = text.strip()
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(text, fmt)
            # If format has no year component, attach the season year.
            if dt.year == 1900:
                dt = dt.replace(year=season_year)
            return dt
        except ValueError:
            continue
    logger.debug("[SincSports matches] unparseable date: %r", text)
    return None


def _fetch_schedule(tid: str) -> Tuple[str, str]:
    url = f"{_BASE_URL}{_SCHEDULE_PATH}?tid={tid}"
    logger.info("[SincSports matches] fetching schedule: %s", url)
    r = requests.get(url, headers=_HEADERS, timeout=25)
    r.raise_for_status()
    return r.text, url


def _is_division_header(row) -> Optional[str]:
    """Return division text if this <tr> is a division/age-group header, else None."""
    tds = row.find_all(["td", "th"])
    if len(tds) == 1:
        text = tds[0].get_text(separator=" ", strip=True)
        if text and len(text) > 2:
            return text
    # Some SincSports pages use a <tr> with colspan spanning all columns.
    if len(tds) >= 1:
        first = tds[0]
        colspan = int(first.get("colspan", 1))
        if colspan >= 3:
            text = first.get_text(separator=" ", strip=True)
            if text:
                return text
    return None


def parse_schedule_html(
    html: str,
    source_url: str,
    tournament_name: str,
    season: Optional[str] = None,
) -> List[Dict]:
    """
    Parse SincSports schedule HTML into match rows.

    Returns list of dicts shaped for tournament_matches_writer.insert_tournament_matches().
    """
    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []

    # Infer season year for date parsing.
    season_year = 2026
    if season:
        m = re.search(r"(\d{4})", season)
        if m:
            season_year = int(m.group(1))

    current_division: Optional[str] = None
    current_age_group: Optional[str] = None
    current_gender: Optional[str] = None

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            # Check for division header.
            div_text = _is_division_header(row)
            if div_text:
                current_division = div_text
                current_age_group, current_gender = _parse_age_gender(div_text)
                logger.debug("[SincSports matches] division: %r → age=%s gender=%s",
                             div_text, current_age_group, current_gender)
                continue

            tds = row.find_all("td")
            if len(tds) < _MIN_MATCH_COLS:
                continue

            col_texts = [td.get_text(separator=" ", strip=True) for td in tds]

            # Expect: date, home_team, score_or_vs, away_team [, field]
            # Skip obvious header rows.
            if any(kw in col_texts[0].lower() for kw in ("date", "time", "home", "away")):
                continue

            date_text = col_texts[0]
            home_team = col_texts[1] if len(col_texts) > 1 else ""
            score_text = col_texts[2] if len(col_texts) > 2 else ""
            away_team = col_texts[3] if len(col_texts) > 3 else ""

            if not home_team or not away_team:
                continue
            if home_team.lower() in ("home", "team", "tbd", "bye") or \
               away_team.lower() in ("away", "team", "tbd", "bye"):
                continue

            score_lower = score_text.lower().strip()
            is_played = score_lower not in ("vs", "v", "", "-", "tbd")
            home_score, away_score = _parse_score(score_text) if is_played else (None, None)
            status = "final" if (home_score is not None and away_score is not None) else "scheduled"

            match_date = _parse_date(date_text, season_year)

            records.append({
                "home_team_name":  home_team,
                "away_team_name":  away_team,
                "home_score":      home_score,
                "away_score":      away_score,
                "match_date":      match_date,
                "age_group":       current_age_group,
                "gender":          current_gender,
                "division":        current_division,
                "season":          season,
                "tournament_name": tournament_name,
                "match_type":      "group",
                "status":          status,
                "source":          "sincsports",
                "source_url":      source_url,
                "platform_match_id": None,
            })

    return records


def scrape_sincsports_matches(
    tid: str,
    tournament_name: str,
    season: Optional[str] = None,
) -> List[Dict]:
    """
    Scrape all match rows for a SincSports tournament id.

    Args:
        tid:             SincSports tournament id (e.g. "CONCFC")
        tournament_name: Human-readable name (e.g. "Concorde Fire Challenge Cup Boys")
        season:          Season tag (e.g. "2025-26"). Used for date year inference + row stamping.

    Returns:
        List of match row dicts for tournament_matches_writer.
    """
    try:
        html, source_url = _fetch_schedule(tid)
    except requests.RequestException as exc:
        logger.error("[SincSports matches] failed to fetch schedule (tid=%s): %s", tid, exc)
        return []

    rows = parse_schedule_html(html, source_url, tournament_name, season=season)

    if not rows:
        logger.warning("[SincSports matches] tid=%s → 0 matches parsed. "
                       "Check that schedule.aspx?tid=%s has match data and "
                       "the HTML structure matches expected column layout.", tid, tid)
    else:
        logger.info("[SincSports matches] tid=%s (%s) → %d matches", tid, tournament_name, len(rows))

    return rows
