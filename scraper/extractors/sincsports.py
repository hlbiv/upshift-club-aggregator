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

# The actual team-list tables on TTTeamList.aspx have a header row whose
# first three cells (case-insensitive) are Team / Club / State. Any other
# <table> on the page is configuration/settings UI (the "Display
# Settings" panel, the "Sort By" panel, etc.). Walking those tables blindly
# is what produced junk canonical_clubs rows like "SINC Content Manager",
# "Merge Tourneys", "USYS", "US Club", and a 494-character blob of the
# whole settings panel concatenated together (canonical_clubs ids 15479-15483).
_TEAM_TABLE_HEADERS: frozenset = frozenset({"team", "club", "state"})

# Defensive secondary guard. Even if a future SincSports page somehow
# threads a settings string into a Team/Club/State row, these patterns
# (lower-cased exact matches and substring needles) catch the obvious
# nav/UI strings that have leaked into canonical_clubs in the past.
_NAV_STRING_EXACT: frozenset = frozenset({
    "sinc content manager",
    "merge tourneys",
    "tourneys",
    "display settings",
    "team list",
    "schedule",
    "schedules",
    "divisions",
    "edit selection",
    "default division",
    "advanced sort",
    "sort by team name",
    "sort by seed",
    "sort by usa rank",
    "sort by age",
    "sort by gender",
    "venues",
    "request",
    "preview",
    "needed",
    "status",
    "nationals",
    "co ed",
    "co-ed",
    "select",
    "pictures",
    "seed",
    "us",
    "usys",
    "us club",
    "usa rank",
    "team link",
    "adult",
})
_NAV_STRING_SUBSTRINGS: tuple = (
    "display settings",
    "sort by team",
    "venues through",
    "default division",
    "edit selection",
)
# Real club names are short. Anything longer than this is almost
# certainly a concatenated nav blob (the worst observed offender was
# 494 characters).
_MAX_CLUB_NAME_LEN = 80


def _is_nav_string(name: str) -> bool:
    """Return True if *name* looks like a SincSports UI/nav label rather
    than a real club name."""
    if not name:
        return True
    if len(name) > _MAX_CLUB_NAME_LEN:
        return True
    lower = name.lower().strip()
    if lower in _NAV_STRING_EXACT:
        return True
    if any(needle in lower for needle in _NAV_STRING_SUBSTRINGS):
        return True
    # A real club name has at least one alphabetic character and is not
    # purely punctuation/whitespace.
    if not any(c.isalpha() for c in name):
        return True
    return False


def _is_team_table(table) -> bool:
    """A SincSports team-list table has a first row containing the
    headers Team / Club / State (case-insensitive). Returns False for
    every other <table> on the page (Display Settings panel, Divisions
    selector, etc.)."""
    first_row = table.find("tr")
    if not first_row:
        return False
    cells = [c.get_text(strip=True).lower() for c in first_row.find_all(["td", "th"])]
    if len(cells) < 3:
        return False
    return _TEAM_TABLE_HEADERS.issubset(set(cells))


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
        # Only walk tables whose first row is a Team/Club/State header.
        # Other <table> tags on this page are settings/nav UI panels —
        # blindly reading td[1] from them produced junk canonical_clubs
        # rows like "SINC Content Manager", "Merge Tourneys", "USYS",
        # "US Club", and a 494-char blob of the Display Settings panel.
        if not _is_team_table(table):
            continue

        rows = table.find_all("tr")
        # Skip the header row itself.
        for row in rows[1:]:
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
            # Defensive secondary guard against any nav/settings strings
            # that somehow slip past the table-shape filter.
            if _is_nav_string(club_name):
                logger.warning(
                    "[SincSports] Rejected nav-string club name: %r (source=%s)",
                    club_name[:80],
                    source_url,
                )
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
