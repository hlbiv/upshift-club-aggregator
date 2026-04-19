"""
Custom extractor for EDP Soccer (edpsoccer.com).

EDP Soccer (Elite Development Platform) is a youth soccer league operating
across NJ, PA, DE, MD, NY, CT, VA, and FL.

SCRAPING STRATEGY:
  edpsoccer.com is Wix-rendered with no public static club directory. However,
  EDP runs all its leagues and tournaments through GotSport.

  Step 1 — Dynamic discovery:
    Fetch known EDP pages from edpsoccer.com (leagues, tournaments, etc.) and
    scan each page's HTML for embedded GotSport event IDs (pattern: events/NNNNN).

  Step 2 — Known event seed:
    A curated list of confirmed EDP GotSport event IDs is used as a seed to
    guarantee coverage even if a page changes.  These are filtered to "EDP-branded"
    events whose nav text begins with "EDP" on GotSport.

  Step 3 — Merge and deduplicate:
    Scrape each unique event ID via scrape_gotsport_event() and return the union
    of all clubs, deduplicated.

CONFIRMED EDP GOTSPORT EVENTS (April 2026):
  47702 — EDP League Spring 2026         (566 clubs)
  44329 — EDP League Fall 2025           (525 clubs)
  44330 — EDP Futures Fall 2025          (176 clubs)
  49601 — EDP Futures Spring 2026        (151 clubs)
  44410 — EDP Florida League 2025-26     (141 clubs)
  49540 — EDP Florida League Spring 26   (140 clubs)
  46334 — EDP League Mini Late Fall 2025 (144 clubs)
  44331 — EDP Futures MD CMSSL Fall 2025  (54 clubs)
  44332 — EDP Futures MD SoccerPlex Fall  (32 clubs)
  49602 — EDP Futures MD CMSSL Spring 26  (53 clubs)
  49603 — EDP Futures MD SoccerPlex Spr   (36 clubs)
  41053 — EDP Fall Kickoff NJ 2025        (71 clubs)
  34957 — EDP Fall Kickoff NJ 2024        (80 clubs)

SOURCE: GotSport event pages embedded in edpsoccer.com/leagues and related pages.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict, Set

import requests

from extractors.registry import register
from extractors.gotsport import parse_gotsport_event_html, scrape_gotsport_event

logger = logging.getLogger(__name__)


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one EDP-hosted GotSport event-clubs page.

    EDP orchestrates many GotSport events (see ``_KNOWN_EDP_EVENT_IDS``);
    the live ``scrape_edp`` entry point loops over them, filters to
    EDP-branded events, and dedups. ``parse_html`` receives a single
    pre-fetched page at a time (replay flow) so it delegates directly to
    ``parse_gotsport_event_html`` and leaves cross-page dedup /
    name-filtering in the orchestrator.
    """
    return parse_gotsport_event_html(html, source_url, league_name=league_name)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}
_BASE_URL = "https://www.edpsoccer.com"

_GOTSPORT_ID_RE = re.compile(r"events/(\d{4,6})")

# Club names that are actually league/org labels, not real clubs
_NOT_A_CLUB: Set[str] = {
    "edp soccer",
    "edp soccer league",
    "edp",
    "elite development platform",
    "gotsport",
    "schedule",
}


def _is_valid_club(club_name: str) -> bool:
    """Return False if the name looks like a league/org label rather than a club."""
    low = club_name.lower().strip()
    if low in _NOT_A_CLUB:
        return False
    if low.startswith("zz-"):
        return False
    return True

_KNOWN_EDP_EVENT_IDS: List[int] = [
    47702,  # EDP League Spring 2026        (current, ~566 clubs)
    44329,  # EDP League Fall 2025          (525 clubs)
    44330,  # EDP Futures Fall 2025         (176 clubs)
    49601,  # EDP Futures Spring 2026       (151 clubs)
    44410,  # EDP Florida League 2025-26    (141 clubs)
    49540,  # EDP Florida League Spring 26  (140 clubs)
    46334,  # EDP League Mini Late Fall 1   (144 clubs)
    44331,  # EDP Futures MD CMSSL Fall 25  (54 clubs)
    44332,  # EDP Futures MD SoccerPlex F   (32 clubs)
    49602,  # EDP Futures MD CMSSL Spr 26   (53 clubs)
    49603,  # EDP Futures MD SoccerPlex S   (36 clubs)
    41053,  # EDP Fall Kickoff NJ 2025      (71 clubs)
    34957,  # EDP Fall Kickoff NJ 2024      (80 clubs)
]

_DISCOVERY_PAGES: List[str] = [
    "/leagues",
    "/edp-league",
    "/fall-classic",
    "/spring-kickoff-pa",
    "/cup-spring",
    "/summer-classic",
    "/winter-classic",
    "/futures",
    "/ct-championship-league",
]


def _discover_event_ids() -> Set[int]:
    """
    Scrape EDP Soccer pages for embedded GotSport event IDs.
    Returns a set of integer event IDs.
    """
    event_ids: Set[int] = set()
    for path in _DISCOVERY_PAGES:
        url = f"{_BASE_URL}{path}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
            if r.status_code == 200:
                for m in _GOTSPORT_ID_RE.finditer(r.text):
                    event_ids.add(int(m.group(1)))
        except Exception as exc:
            logger.debug("[EDP] Could not fetch %s: %s", url, exc)
    logger.info("[EDP] Discovered %d candidate GotSport event IDs from EDP pages", len(event_ids))
    return event_ids


def _is_edp_event(event_id: int) -> bool:
    """
    Confirm a GotSport event ID belongs to EDP by checking that the nav text
    starts with 'EDP'. Returns False on fetch error (conservative).
    """
    url = f"https://system.gotsport.com/org_event/events/{event_id}"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200:
            return False
        nav_match = re.search(r"Toggle navigation(.{0,120})", r.text)
        if nav_match:
            nav_text = nav_match.group(1).strip().lower()
            return nav_text.startswith("edp")
        return False
    except Exception as exc:
        logger.debug("[EDP] Could not verify event %s: %s", event_id, exc)
        return False


@register(r"edpsoccer\.com")
def scrape_edp(url: str, league_name: str) -> List[Dict]:
    logger.info("[EDP custom] Starting scrape via GotSport")

    # Seed with confirmed known IDs
    all_event_ids: Set[int] = set(_KNOWN_EDP_EVENT_IDS)

    # Augment with dynamically discovered IDs (skip in CI / fast-rebuild mode)
    import os
    if not os.environ.get("EDP_SKIP_DISCOVERY"):
        try:
            discovered = _discover_event_ids()
            # Only add newly discovered IDs; verify they're EDP before including
            new_ids = discovered - all_event_ids
            if new_ids:
                logger.info("[EDP] Verifying %d newly discovered event IDs", len(new_ids))
                for eid in new_ids:
                    if _is_edp_event(eid):
                        logger.info("[EDP] Confirmed new EDP event: %d", eid)
                        all_event_ids.add(eid)
        except Exception as exc:
            logger.warning("[EDP] Dynamic discovery failed: %s", exc)
    else:
        logger.info("[EDP] Skipping dynamic discovery (EDP_SKIP_DISCOVERY set)")

    logger.info("[EDP] Total EDP GotSport event IDs to scrape: %d", len(all_event_ids))

    seen_clubs: Set[str] = set()
    all_records: List[Dict] = []

    for event_id in sorted(all_event_ids):
        try:
            records = scrape_gotsport_event(event_id, league_name, state="")
        except Exception as exc:
            logger.warning("[EDP] GotSport event %s failed: %s", event_id, exc)
            continue

        new = [rec for rec in records
               if rec["club_name"] not in seen_clubs and _is_valid_club(rec["club_name"])]
        seen_clubs.update(rec["club_name"] for rec in new)
        all_records.extend(new)
        logger.info("[EDP] Event %s → %d clubs (%d new)", event_id, len(records), len(new))

    logger.info("[EDP custom] Total unique clubs: %d", len(all_records))
    return all_records
