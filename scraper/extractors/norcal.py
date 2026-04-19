"""
Custom extractor for NorCal Premier Soccer (/clubs/).

The clubs page has a table with columns: Club | Location | Region
Each row is a member club. Region gives a sub-geography within NorCal.

Structure:

- ``parse_html(html, source_url, league_name)`` is a pure function that
  parses pre-fetched HTML into record dicts. Exposed at module level so
  the ``--source replay-html`` handler (see ``run.py``) can replay
  archived raw HTML without re-fetching.
- ``scrape_norcal`` is the registered scraper — it fetches the live
  ``/clubs/`` page and delegates parsing to ``parse_html``.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}

# The canonical URL for the NorCal clubs directory. Used as the default
# ``source_url`` for records produced by replayed or synthetic HTML that
# did not carry their own URL.
CLUBS_URL = "https://norcalpremier.com/clubs/"


def parse_html(
    html: str,
    source_url: Optional[str] = None,
    league_name: Optional[str] = None,
) -> List[Dict]:
    """Pure parser for the NorCal Premier /clubs/ page.

    Iterates the member-club table (columns: Club | Location | Region)
    and yields one record dict per row.

    Args:
        html: Raw HTML of the ``/clubs/`` page.
        source_url: The URL the HTML was fetched from. Defaults to the
            canonical ``CLUBS_URL`` when not supplied (e.g. replay of an
            older archive that lost the original URL).
        league_name: League name to stamp on each record. May be ``None``
            when called from the replay handler — callers that need a
            concrete league should pass one.

    Returns:
        A list of dicts, one per club, with keys:
        ``club_name``, ``league_name``, ``city``, ``state``,
        ``source_url``, ``region``, ``website``.
    """
    resolved_source_url = source_url or CLUBS_URL

    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []

    # The page has a table with th headers: Club, Location, Region
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not any("club" in h for h in headers):
            continue

        col_club = next((i for i, h in enumerate(headers) if "club" in h), 0)
        col_location = next((i for i, h in enumerate(headers) if "location" in h or "city" in h), None)
        col_region = next((i for i, h in enumerate(headers) if "region" in h), None)

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells or len(cells) <= col_club:
                continue
            # Capture any website link from the club name cell
            a_tag = cells[col_club].find("a", href=True)
            website = ""
            if a_tag:
                href = a_tag["href"].strip()
                if href.startswith("http"):
                    website = href
            club_name = cells[col_club].get_text(strip=True)
            if not club_name or len(club_name) < 2:
                continue

            location = (
                cells[col_location].get_text(strip=True)
                if col_location is not None and len(cells) > col_location
                else ""
            )
            region = (
                cells[col_region].get_text(strip=True)
                if col_region is not None and len(cells) > col_region
                else ""
            )

            records.append({
                "club_name": club_name,
                "league_name": league_name,
                "city": location,
                "state": "CA",      # NorCal is California-only
                "source_url": resolved_source_url,
                "region": region,
                "website": website,
            })

    return records


@register(r"norcalpremier\.com")
def scrape_norcal(url: str, league_name: str) -> List[Dict]:
    # Always use the /clubs/ directory page regardless of the configured URL
    clubs_url = CLUBS_URL
    logger.info("[NorCal custom] Scraping %s", clubs_url)

    try:
        r = requests.get(clubs_url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("NorCal fetch failed: %s", exc)
        return []

    records = parse_html(r.text, source_url=clubs_url, league_name=league_name)
    logger.info("[NorCal custom] Found %d clubs", len(records))
    return records
