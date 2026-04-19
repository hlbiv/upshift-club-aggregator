"""
Custom extractor for Super Y League (now USL Youth).

Super Y is a national independent youth league with multiple regional events
on GotSport. The largest known event is SoCal-only; additional regional events
are scraped when discovered.

GotSport events:
  33123 – Super Y SoCal (165 clubs, Southern California)
  36295 – Super Y Texas region (9 clubs, Austin metro)

Historical club list also scraped from sylsoccer.com/clubs (2018 data),
which covers clubs from all regions before the USL Youth rebrand.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}

_GOTSPORT_EVENTS = [
    (33123, "Super Y SoCal"),
    (36295, "Super Y Texas"),
]

_SYL_CLUBS_URL = "https://www.sylsoccer.com/clubs-home"


def _parse_syl_clubs_html(html: str, source_url: str) -> List[Dict]:
    """
    Pure-function parser for the Super Y / USL-Y historical club list.

    The sylsoccer.com clubs page is a static nav-list of ``<a>`` links; we
    pick out anchors whose href contains ``/page/show/`` or ``/clubs/`` and
    treat the link text as the club name.

    Returns dicts with ``league_name`` left empty — the caller fills that
    in from the invocation context.
    """
    records: List[Dict] = []
    soup = BeautifulSoup(html, "lxml")
    seen: set = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/page/show/" in href or "/clubs/" in href.lower():
            name = a.get_text(strip=True)
            if name and len(name) > 2 and name.lower() not in seen:
                seen.add(name.lower())
                records.append({
                    "club_name": name,
                    "league_name": "",   # filled in by caller
                    "city": "",
                    "state": "",
                    "source_url": source_url,
                })

    return records


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser exposed to --source replay-html.

    Parses a sylsoccer.com-style clubs-list page (the simplest single-URL
    path in this extractor). Live runs of :func:`scrape_supery` also walk
    multiple GotSport events; those are replayed via the gotsport-backed
    parse_html paths on the sibling NPL extractors.
    """
    url = source_url or _SYL_CLUBS_URL
    records = _parse_syl_clubs_html(html, url)
    for rec in records:
        rec["league_name"] = league_name
    return records


def _scrape_syl_clubs() -> List[Dict]:
    """Scrape the Super Y League / USL-Y historical member club list."""
    try:
        r = requests.get(_SYL_CLUBS_URL, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.warning("[Super Y] sylsoccer.com fetch failed: %s", exc)
        return []

    records = _parse_syl_clubs_html(r.text, _SYL_CLUBS_URL)
    logger.info("[Super Y] sylsoccer.com → %d historical clubs", len(records))
    return records


@register(r"supery\.org|sylsoccer\.com")
def scrape_supery(url: str, league_name: str) -> List[Dict]:
    logger.info("[Super Y custom] Scraping GotSport events + historical club list")

    seen: set = set()
    records: List[Dict] = []

    for event_id, division in _GOTSPORT_EVENTS:
        if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
            from storage import save_teams_csv, save_contacts_csv
            label = f"{league_name} – {division}"
            teams, contacts = scrape_gotsport_teams(event_id, label, state="")
            save_teams_csv(teams, label)
            save_contacts_csv(contacts, label)

        for rec in scrape_gotsport_event(event_id, league_name, state=""):
            key = rec["club_name"].lower().strip()
            if key not in seen:
                seen.add(key)
                records.append(rec)

    for rec in _scrape_syl_clubs():
        rec["league_name"] = league_name
        key = rec["club_name"].lower().strip()
        if key not in seen:
            seen.add(key)
            records.append(rec)

    logger.info("[Super Y custom] %d unique clubs total", len(records))
    return records
