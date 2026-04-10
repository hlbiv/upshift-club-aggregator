"""
Custom extractor for US Club Soccer Elite 64.

Elite 64 is a top-tier national invitational program for boys and girls
hosted on GotSport.  The event IDs change each season — update the
constants below when US Club Soccer announces new event IDs.

Season 2024-25 event IDs (sourced from usclubsoccer.org/programs/leagues/):
  Boys:  51459   (Elite 64 Boys 2024-25)
  Girls: 51460   (Elite 64 Girls 2024-25)

NOTE: GotSport restricts the clubs tab for private/invitational events
to authenticated participants.  If the scraper returns zero clubs for
either ID, it logs a WARNING — this is expected when the event is
private or the event IDs have changed for the current season.
Update BOYS_EVENT_ID / GIRLS_EVENT_ID to the new IDs each year.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

BOYS_EVENT_ID = 51459
GIRLS_EVENT_ID = 51460
_SEASON = "2024-25"


@register(r"usclubsoccer\.org/programs/leagues")
def scrape_elite64(url: str, league_name: str) -> List[Dict]:
    """
    Scrape clubs from the Elite 64 boys and girls GotSport events and
    merge them into a single list deduplicated by club name.
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
            "season.  Update BOYS_EVENT_ID in extractors/elite64.py.",
            BOYS_EVENT_ID,
        )
    if not girls_clubs:
        logger.warning(
            "[Elite 64] Girls event %d returned 0 clubs — the event may be "
            "private/login-required or the event ID has changed for the current "
            "season.  Update GIRLS_EVENT_ID in extractors/elite64.py.",
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
