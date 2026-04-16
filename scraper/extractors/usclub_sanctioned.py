"""
US Club Soccer sanctioned tournament discovery extractor.

Scrapes the public tournament list at:
    https://usclubsoccer.org/list-of-sanctioned-tournaments/

The page renders a month-grouped HTML table with columns:
    Tournament Name (linked) | Dates | State | Host Club | Age Groups

For each row we:
  1. Extract tournament metadata (name, dates, state, host, ages).
  2. Classify the tournament link by platform:
     - GotSport (system.gotsport.com) -- extract event ID if present
     - SincSports (soccer.sincsports.com) -- extract tid if present
     - Other (custom club sites)
  3. Yield an EventMeta suitable for upserting to the events table.

This is a *discovery* scraper: it populates event metadata but does NOT
scrape team lists.  Downstream runners (gotsport_events_runner,
events_runner) handle team-level data once event IDs / tids are known.

PUBLIC API
----------
``scrape_usclub_sanctioned(dry_run)`` returns a list of DiscoveredTournament.
``parse_tournament_table(html)`` is the HTML-only entry point for tests.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_LIST_URL = "https://usclubsoccer.org/list-of-sanctioned-tournaments/"


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class DiscoveredTournament:
    """One row from the sanctioned tournament list page."""
    name: str
    url: Optional[str]
    start_date: Optional[str]  # ISO yyyy-mm-dd
    end_date: Optional[str]
    state: Optional[str]
    host_club: Optional[str]
    age_groups: Optional[str]  # raw string, e.g. "U8-U19 Boys and Girls"
    platform: str  # "gotsport", "sincsports", or "other"
    gotsport_event_id: Optional[str] = None
    sincsports_tid: Optional[str] = None


# ---------------------------------------------------------------------------
# Platform classification
# ---------------------------------------------------------------------------

_GOTSPORT_EVENT_RE = re.compile(
    r"gotsport\.com/org_event/events/(\d+)", re.IGNORECASE
)
_GOTSPORT_DOMAIN_RE = re.compile(r"gotsport\.com|gotsoccer\.com", re.IGNORECASE)
_SINCSPORTS_TID_RE = re.compile(
    r"sincsports\.com/[^?]*\?.*tid=([A-Za-z0-9_-]+)", re.IGNORECASE
)
_SINCSPORTS_DOMAIN_RE = re.compile(r"sincsports\.com", re.IGNORECASE)


def classify_platform(url: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Classify a tournament URL by platform.

    Returns (platform, gotsport_event_id, sincsports_tid).
    """
    m = _GOTSPORT_EVENT_RE.search(url)
    if m:
        return "gotsport", m.group(1), None
    if _GOTSPORT_DOMAIN_RE.search(url):
        return "gotsport", None, None

    m = _SINCSPORTS_TID_RE.search(url)
    if m:
        return "sincsports", None, m.group(1)
    if _SINCSPORTS_DOMAIN_RE.search(url):
        return "sincsports", None, None

    return "other", None, None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _parse_date_range(
    raw: str, default_year: int = 2026,
) -> Tuple[Optional[str], Optional[str]]:
    """Parse date ranges like 'April 1-5', 'May 16-17, 2026'.

    Returns (start_date, end_date) as ISO strings or (None, None).
    """
    cleaned = raw.replace(",", "").strip()
    if not cleaned:
        return None, None

    # "Month Day-Day [Year]"
    m = re.match(
        r"^(\w+)\s+(\d{1,2})\s*[-\u2013]\s*(\d{1,2})(?:\s+(\d{4}))?$",
        cleaned, re.IGNORECASE,
    )
    if m:
        month = _MONTHS.get(m.group(1).lower())
        if month is None:
            return None, None
        year = int(m.group(4)) if m.group(4) else default_year
        return (
            f"{year:04d}-{month:02d}-{int(m.group(2)):02d}",
            f"{year:04d}-{month:02d}-{int(m.group(3)):02d}",
        )

    # "Month Day [Year]" (single date)
    m = re.match(
        r"^(\w+)\s+(\d{1,2})(?:\s+(\d{4}))?$", cleaned, re.IGNORECASE,
    )
    if m:
        month = _MONTHS.get(m.group(1).lower())
        if month is None:
            return None, None
        year = int(m.group(3)) if m.group(3) else default_year
        d = f"{year:04d}-{month:02d}-{int(m.group(2)):02d}"
        return d, d

    # "Month Day - Month Day [Year]" (cross-month)
    m = re.match(
        r"^(\w+)\s+(\d{1,2})\s*[-\u2013]\s*(\w+)\s+(\d{1,2})(?:\s+(\d{4}))?$",
        cleaned, re.IGNORECASE,
    )
    if m:
        m1 = _MONTHS.get(m.group(1).lower())
        m2 = _MONTHS.get(m.group(3).lower())
        if m1 is None or m2 is None:
            return None, None
        year = int(m.group(5)) if m.group(5) else default_year
        return (
            f"{year:04d}-{m1:02d}-{int(m.group(2)):02d}",
            f"{year:04d}-{m2:02d}-{int(m.group(4)):02d}",
        )

    return None, None


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def parse_tournament_table(html: str) -> List[DiscoveredTournament]:
    """Parse the sanctioned tournaments page HTML.

    The page uses WordPress tables organized by month. Each row has 4-5
    columns: Name (linked), Dates, State, Host Club, Age Groups.
    """
    soup = BeautifulSoup(html, "html.parser")
    tournaments: List[DiscoveredTournament] = []

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue

        for tr in rows:
            cells = tr.find_all("td")
            if len(cells) < 4:
                continue

            # Cell 0: Tournament name (possibly with <a> link)
            name_cell = cells[0]
            link_tag = name_cell.find("a")
            url = link_tag["href"].strip() if link_tag and link_tag.get("href") else None
            name = name_cell.get_text(strip=True)

            # Skip header-like rows
            if not name or re.match(r"tournament\s*name", name, re.IGNORECASE):
                continue
            if re.match(r"coming\s*soon", name, re.IGNORECASE):
                continue

            dates_raw = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            state = cells[2].get_text(strip=True) if len(cells) > 2 else None
            host_club = cells[3].get_text(strip=True) if len(cells) > 3 else None
            age_groups = cells[4].get_text(strip=True) if len(cells) > 4 else None

            # Normalize empty strings to None
            state = state or None
            host_club = host_club or None
            age_groups = age_groups or None

            start_date, end_date = _parse_date_range(dates_raw)

            platform, gs_id, sc_tid = ("other", None, None)
            if url:
                platform, gs_id, sc_tid = classify_platform(url)

            tournaments.append(DiscoveredTournament(
                name=name,
                url=url,
                start_date=start_date,
                end_date=end_date,
                state=state,
                host_club=host_club,
                age_groups=age_groups,
                platform=platform,
                gotsport_event_id=gs_id,
                sincsports_tid=sc_tid,
            ))

    return tournaments


# ---------------------------------------------------------------------------
# Live scraper
# ---------------------------------------------------------------------------

def scrape_usclub_sanctioned() -> List[DiscoveredTournament]:
    """Fetch and parse the US Club Soccer sanctioned tournaments page."""
    logger.info("[usclub-sanctioned] Fetching %s", _LIST_URL)
    r = requests.get(_LIST_URL, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    logger.info("[usclub-sanctioned] Fetched %d KB", len(r.text) // 1024)

    tournaments = parse_tournament_table(r.text)
    logger.info("[usclub-sanctioned] Parsed %d tournaments", len(tournaments))

    gs = [t for t in tournaments if t.platform == "gotsport"]
    sc = [t for t in tournaments if t.platform == "sincsports"]
    ot = [t for t in tournaments if t.platform == "other"]
    logger.info(
        "[usclub-sanctioned] Platform breakdown: GotSport=%d  SincSports=%d  Other=%d",
        len(gs), len(sc), len(ot),
    )

    return tournaments
