"""
Custom extractor for Elite 64.

Elite 64 is a US Club Soccer invitational program for boys and girls, hosted
on GotSport (administered through the USYS National League platform).

GotSport event IDs for 2024-25 season:
  BOYS_EVENT_ID  = 38227  (January Quarters Boys — 60 clubs)
  GIRLS_EVENT_ID = 38229  (January Quarters Girls — 37 clubs)

Both programs are scraped separately and merged into one output file.
A WARNING is logged if either event returns zero clubs — this means the
event ID has changed for the new season. Update the constants below from:
  https://www.thenationalleague.com/schedules-results/
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import (
    parse_gotsport_event_html,
    scrape_gotsport_event,
    scrape_gotsport_teams,
)

logger = logging.getLogger(__name__)


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one Elite 64 GotSport event-clubs page.

    Elite 64 orchestrates two GotSport events (boys + girls) and merges
    + dedups them in the live ``scrape_elite64`` entry point. ``parse_html``
    handles a single pre-fetched page (replay flow) and delegates directly
    to ``parse_gotsport_event_html``; cross-page merge/dedup stays in the
    orchestrator.
    """
    return parse_gotsport_event_html(html, source_url, league_name=league_name)

# Elite 64 GotSport event IDs — update each season
# Source: https://www.thenationalleague.com/schedules-results/
BOYS_EVENT_ID = 38227    # January Quarters Boys (2024-25)
GIRLS_EVENT_ID = 38229   # January Quarters Girls (2024-25)

_SEASON = "2024-25"


@register(r"usclubsoccer\.org/programs/leagues")
def scrape_elite64(url: str, league_name: str) -> List[Dict]:
    """
    Scrape clubs from the Elite 64 boys and girls GotSport events and
    merge them into a single deduplicated list.
    A warning is logged if either event returns zero clubs.
    """
    logger.info(
        "[Elite 64] Using GotSport event IDs boys=%d girls=%d (season %s)",
        BOYS_EVENT_ID,
        GIRLS_EVENT_ID,
        _SEASON,
    )

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv

        boys_teams, boys_contacts = scrape_gotsport_teams(
            BOYS_EVENT_ID, league_name, state=""
        )
        girls_teams, girls_contacts = scrape_gotsport_teams(
            GIRLS_EVENT_ID, league_name, state=""
        )
        save_teams_csv(boys_teams + girls_teams, league_name)
        save_contacts_csv(boys_contacts + girls_contacts, league_name)

    boys_clubs = scrape_gotsport_event(BOYS_EVENT_ID, league_name, state="")
    girls_clubs = scrape_gotsport_event(GIRLS_EVENT_ID, league_name, state="")

    if not boys_clubs:
        logger.warning(
            "[Elite 64] Boys event %d returned 0 clubs — the event may be "
            "private/login-required or the event ID has changed for the current "
            "season.  Update BOYS_EVENT_ID in extractors/elite64.py from: "
            "https://www.thenationalleague.com/schedules-results/",
            BOYS_EVENT_ID,
        )
    if not girls_clubs:
        logger.warning(
            "[Elite 64] Girls event %d returned 0 clubs — the event may be "
            "private/login-required or the event ID has changed for the current "
            "season.  Update GIRLS_EVENT_ID in extractors/elite64.py from: "
            "https://www.thenationalleague.com/schedules-results/",
            GIRLS_EVENT_ID,
        )

    all_clubs = boys_clubs + girls_clubs

    seen: set[str] = set()
    deduped: List[Dict] = []
    for club in all_clubs:
        key = club["club_name"].strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(club)

    logger.info(
        "[Elite 64] boys=%d girls=%d merged=%d unique clubs",
        len(boys_clubs),
        len(girls_clubs),
        len(deduped),
    )
    return deduped
