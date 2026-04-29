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

KNOWN_TIDS = [
    "CONCFC", "CONCG",
    "GULFC", "HOOVHAV", "MISSFSC2", "APPHIGHSC", "REDRV", "KHILL",
    "HFCSPRCL", "BAMABLST", "PALMETTO", "BAYOUCTY", "CAROCLS",
    "SHOWME", "BADGER", "CORNHSK", "SCCLCUP3",
]

_EVENTS_URL = f"{_BASE_URL}/events.aspx"


def fetch_sincsports_event_tids() -> List[Tuple[str, str]]:
    """Scrape soccer.sincsports.com/events.aspx and return (tid, name) pairs.

    Used by the batch handler instead of the static KNOWN_TIDS list so that
    newly-posted tournaments are picked up automatically.
    """
    try:
        r = requests.get(_EVENTS_URL, headers=_HEADERS, timeout=25)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("[SincSports matches] events.aspx fetch failed: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    seen: set = set()
    results: List[Tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "TTIntro" not in href:
            continue
        m = re.search(r"[?&]tid=([^&]+)", href)
        if not m:
            continue
        tid = m.group(1)
        if tid in seen:
            continue
        seen.add(tid)
        name = a.get_text(strip=True)
        results.append((tid, name))

    logger.info("[SincSports matches] events.aspx → %d tournament(s) discovered", len(results))
    return results

_AGE_RE = re.compile(r"\bU(?:nder\s+)?(\d{1,2})\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b(boys?|girls?|male|female|men|women)\b", re.IGNORECASE)
_GENDER_MAP = {
    "boy": "M", "boys": "M", "male": "M", "men": "M",
    "girl": "G", "girls": "G", "female": "G", "women": "G",
}

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

    SincSports returns 403 when no tournament exists for the requested year
    (not an auth error). Falls back to year-1 automatically so tournaments
    that ran in 2025 but haven't yet in 2026 are still scraped.
    """
    years_to_try = [year, year - 1] if year > 2000 else [year]
    actual_year: Optional[int] = None

    for yr in years_to_try:
        url = f"{_BASE_URL}/schedule.aspx?div=N&tid={tid}&year={yr}&stid={tid}&syear={yr}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=25)
            r.raise_for_status()
            actual_year = yr
            break
        except requests.HTTPError as exc:
            is_403 = exc.response is not None and exc.response.status_code == 403
            if is_403:
                if yr != years_to_try[-1]:
                    logger.debug(
                        "[SincSports matches] 403 for tid=%s year=%d — retrying with year=%d",
                        tid, yr, yr - 1,
                    )
                # 403 = no schedule published for this year; try next or give up.
                continue
            logger.error("[SincSports matches] division listing failed tid=%s: %s", tid, exc)
            return []
        except requests.RequestException as exc:
            logger.error("[SincSports matches] division listing failed tid=%s: %s", tid, exc)
            return []

    if actual_year is None:
        # All years returned 403 — tournament is listed but schedule not yet published.
        logger.debug(
            "[SincSports matches] tid=%s: no schedule published for years %s (403 on all)",
            tid, years_to_try,
        )
        return []

    if actual_year != year:
        logger.info(
            "[SincSports matches] tid=%s: fell back from year=%d to year=%d",
            tid, year, actual_year,
        )

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

    logger.info("[SincSports matches] tid=%s year=%d → %d divisions", tid, actual_year, len(divisions))
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

    Actual DOM structure (observed April 2026, soccer.sincsports.com):
      <div class="form-row game-row">
        <div class="col-md-3 d-cell">          ← date + time + game#
          <div class="row">
            <div class="col-6">
              <span>Saturday</span>
              <span>2/28/2026</span>
            </div>
            <div class="col-6 ...">
              <span>8:00 AM</span>
              <span>#00001</span>
            </div>
          </div>
        </div>
        <div class="col-md-5">
          <div class="row">
            <div class="col-9">
              <div class="hometeam">
                <a href="/team/..."><div class="hora">H:</div></a>
                <a href="schedule.aspx?...">Team Name</a>
              </div>
              <div class="awayteam">
                <a href="/team/..."><div class="hora">A:</div></a>
                <a href="schedule.aspx?...">Team Name</a>
              </div>
            </div>
            <div class="col-3 text-right">   ← scores (color-styled divs, skip class="clear")
              <div style="color:...">1</div>
              <div class="clear"></div>
              <div style="color:...">1</div>
            </div>
          </div>
        </div>
        <div class="col-md-4">               ← division label + venue link
          ...
        </div>
      </div>
    """
    soup = BeautifulSoup(html, "lxml")
    age_group, gender = _parse_age_gender(div_name)
    records: List[Dict] = []

    game_rows = soup.find_all("div", class_=lambda c: c and "game-row" in c.split())
    logger.debug("[SincSports matches] %s → %d game-row divs found", div_name, len(game_rows))

    for row in game_rows:
        # ── Date / time / game# ─────────────────────────────────────────────
        date_cell = row.find("div", class_=lambda c: c and "d-cell" in c.split())
        date_str = time_str = game_num = None
        if date_cell:
            spans = [s.get_text(strip=True) for s in date_cell.find_all("span") if s.get_text(strip=True)]
            for span in spans:
                if re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", span):
                    date_str = span
                elif re.match(r"^\d{1,2}:\d{2}\s*(AM|PM)$", span, re.IGNORECASE):
                    time_str = span
                elif re.match(r"^#\d+", span):
                    game_num = span.lstrip("#").strip()

        # ── Team names ───────────────────────────────────────────────────────
        home_div = row.find("div", class_="hometeam")
        away_div = row.find("div", class_="awayteam")
        if not home_div or not away_div:
            continue

        # Second <a> is the team name link; first <a> wraps the "H:"/"A:" label div
        home_links = home_div.find_all("a")
        away_links = away_div.find_all("a")
        if len(home_links) < 2 or len(away_links) < 2:
            continue
        home_raw = home_links[-1].get_text(strip=True)
        away_raw = away_links[-1].get_text(strip=True)
        if not home_raw or not away_raw:
            continue

        # ── Scores ───────────────────────────────────────────────────────────
        home_score = away_score = None
        score_col = row.find("div", class_=lambda c: c and "col-3" in c.split() and "text-right" in c.split())
        if score_col:
            score_divs = [
                d for d in score_col.find_all("div", recursive=False)
                if "clear" not in (d.get("class") or [])
            ]
            score_texts = [d.get_text(strip=True) for d in score_divs if re.match(r"^\d{1,2}$", d.get_text(strip=True))]
            if len(score_texts) >= 2:
                try:
                    home_score = int(score_texts[0])
                    away_score = int(score_texts[1])
                except ValueError:
                    pass

        status = "final" if (home_score is not None and away_score is not None) else "scheduled"
        match_date = _parse_date(date_str or "", time_str or "", year) if date_str else None

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
