"""
Custom extractor for SincSports soccer event pages (soccer.sincsports.com).

SincSports is a widely used youth soccer tournament and club management platform.
Many clubs and leagues run their events through SincSports, especially across the
Southeast, Midwest, and Gulf Coast regions.

SCRAPING STRATEGY:
  SincSports event pages use a tid= (tournament ID) parameter in their URLs.
  The team list for any event is available as static HTML at:
    https://soccer.sincsports.com/TTTeamList.aspx?tid=<TID>

  The page renders plain HTML (no JS required). Each age-group/gender division
  is a separate <table> with columns: Team | Club | State | (optional columns)

  Column index 1 = Club name
  Column index 2 = US state abbreviation

  All teams for an event appear on a single page — no pagination is needed.
  We extract unique clubs and their home state.

URL PATTERNS HANDLED:
  - https://soccer.sincsports.com/TTIntro.aspx?tid=GULFC
  - https://soccer.sincsports.com/details.aspx?tid=GULFC&tab=5&...
  - https://soccer.sincsports.com/TTTeamList.aspx?tid=GULFC
  - https://soccer.sincsports.com/schedule.aspx?tid=GULFC
  - https://soccer.sincsports.com/TTApply0.aspx?tid=GULFC
  - Any other soccer.sincsports.com URL with a tid= parameter

KNOWN EVENT SEEDS (April 2026):
  GULFC      — Coastal Soccer Invitational (AL/FL/LA/MS; ~54 clubs)
  HOOVHAV    — Hoover Havoc (AL; ~42 clubs)
  MISSFSC2   — MR Spring Classic (MS; ~30 clubs)
  APPHIGHSC  — Appalachian Highlands Spring Cup (TN/NC/VA; ~17 clubs)
  HFCSPRCL   — Hattiesburg FC Spring Classic (MS)
  REDRV      — Red River Classic (TX/LA/AR)
  KHILL      — Kings Hill Tournament (TN/KY)
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict, Set
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE_URL = "https://soccer.sincsports.com"
_TEAM_LIST_PATH = "/TTTeamList.aspx"

_SKIP_CLUB_NAMES: frozenset = frozenset({
    "club",
    "no club",
    "",
    "tbd",
    "n/a",
})


def _extract_tid(url: str) -> str | None:
    """
    Extract the tournament ID (tid) from a SincSports URL query string.

    SincSports event URLs carry the tournament ID exclusively as a query
    parameter (e.g. ``?tid=GULFC``).  Returns the tid string, or None
    if no tid parameter is present.
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    if "tid" in qs:
        return qs["tid"][0]
    return None


def _fetch_team_list(tid: str) -> tuple[str, str]:
    """
    Fetch the raw HTML of the TTTeamList page for *tid*.

    Returns a (html, canonical_url) tuple.

    SincSports TTTeamList pages deliver all teams in a single HTML response
    (verified April 2026 across events with 2 – 334 team rows).  There is
    no server-side HTTP pagination for the team list; the division selector
    visible on-page is a JavaScript-only client-side filter, not a link to
    a separate page.
    """
    url = f"{_BASE_URL}{_TEAM_LIST_PATH}?tid={tid}"
    logger.info("[SincSports] Fetching team list: %s", url)
    r = requests.get(url, headers=_HEADERS, timeout=25)
    r.raise_for_status()
    return r.text, url


def _parse_clubs_from_html(html: str, source_url: str, league_name: str) -> List[Dict]:
    """
    Parse club records from the TTTeamList HTML.

    Each division's teams are in a separate <table>. Rows have:
      td[0] = Team name
      td[1] = Club name
      td[2] = State abbreviation
      td[3] = (optional) division/request info

    Returns a list of club dicts with keys:
      club_name, league_name, city, state, source_url, source_type
    """
    soup = BeautifulSoup(html, "lxml")

    seen_clubs: Set[str] = set()
    records: List[Dict] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            tds = row.find_all("td")
            if len(tds) < 3:
                continue

            club_name = tds[1].get_text(strip=True)
            state = tds[2].get_text(strip=True)

            if not club_name:
                continue

            # Skip header rows and placeholder clubs
            club_lower = club_name.lower().strip()
            if club_lower in _SKIP_CLUB_NAMES:
                continue
            # Skip clubs whose name is a known UI label
            if club_lower in {"club", "state", "team", "request"}:
                continue
            # SincSports sometimes shows "NO CLUB (XX)" for unaffiliated teams
            if club_lower.startswith("no club"):
                continue

            # Deduplicate within this event
            key = club_lower
            if key in seen_clubs:
                continue
            seen_clubs.add(key)

            # Validate state — must be 2-letter uppercase abbreviation or empty
            if len(state) != 2 or not state.isalpha():
                state = ""

            records.append({
                "club_name":   club_name,
                "league_name": league_name,
                "city":        "",
                "state":       state.upper(),
                "source_url":  source_url,
                "source_type": "sincsports",
            })

    return records


@register(r"sincsports\.com")
def scrape_sincsports(url: str, league_name: str) -> List[Dict]:
    """
    Extractor for SincSports soccer event pages.

    Accepts any soccer.sincsports.com URL containing a tid= parameter.
    Fetches the team list page and extracts unique clubs with their home state.
    """
    tid = _extract_tid(url)
    if not tid:
        logger.error("[SincSports] Could not extract tid from URL: %s", url)
        return []

    logger.info("[SincSports] Event tid=%s  league='%s'", tid, league_name)

    try:
        html, team_list_url = _fetch_team_list(tid)
    except requests.RequestException as exc:
        logger.error("[SincSports] Failed to fetch team list (tid=%s): %s", tid, exc)
        return []

    records = _parse_clubs_from_html(html, team_list_url, league_name)

    if not records:
        logger.warning("[SincSports] No clubs found for tid=%s", tid)
    else:
        logger.info("[SincSports] tid=%s → %d unique clubs", tid, len(records))

    return records
