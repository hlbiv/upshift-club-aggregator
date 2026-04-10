"""
Custom extractor for Elite 64.

Elite 64 is administered by US Club Soccer as a top-tier national invitational
club-based league, hosted on GotSport (USYS National League platform).
Both boys and girls programs run as separate GotSport events each season.

GotSport event IDs for 2024-25 season:
  Regular season (combined boys + girls):
    35565  Elite 64 Regular Season (171 clubs — both programs)

  National playoff events by gender (for targeted boys/girls splits):
    BOYS_EVENT_ID  = 38227  (January Quarters Boys — 60 clubs)
    GIRLS_EVENT_ID = 38229  (January Quarters Girls — 37 clubs)

NOTE: GotSport event IDs change each season. Update the constants below
when US Club Soccer announces new season registration. Source for current IDs:
  https://www.thenationalleague.com/schedules-results/
A WARNING is logged for any event returning zero clubs so operators know when
IDs need to be refreshed.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

# 2024-25 season — Elite 64 GotSport event IDs
# Main regular season event (boys + girls combined)
REGULAR_SEASON_EVENT_ID = 35565

# Gender-split national event IDs (used for UPSHIFT_SCRAPE_TEAMS team-level data)
BOYS_EVENT_ID = 38227
GIRLS_EVENT_ID = 38229

_SEASON = "2024-25"


@register(r"usclubsoccer\.org/programs/leagues")
def scrape_elite64(url: str, league_name: str) -> List[Dict]:
    """
    Scrape clubs from the Elite 64 regular season GotSport event and optionally
    enrich with team-level data from the separate boys/girls national events.
    Both boys and girls clubs appear in the combined regular season event.
    """
    logger.info(
        "[Elite 64] Regular season event=%d; boys=%d girls=%d (season %s)",
        REGULAR_SEASON_EVENT_ID,
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

    clubs = scrape_gotsport_event(REGULAR_SEASON_EVENT_ID, league_name, state="")

    if not clubs:
        logger.warning(
            "[Elite 64] Regular season event %d returned 0 clubs — the event "
            "may be private/login-required or the event ID has changed for the "
            "current season.  Also try boys event %d and girls event %d. "
            "Update REGULAR_SEASON_EVENT_ID / BOYS_EVENT_ID / GIRLS_EVENT_ID "
            "in extractors/elite64.py from: "
            "https://www.thenationalleague.com/schedules-results/",
            REGULAR_SEASON_EVENT_ID,
            BOYS_EVENT_ID,
            GIRLS_EVENT_ID,
        )

    logger.info(
        "[Elite 64] event %d → %d clubs (boys+girls combined)",
        REGULAR_SEASON_EVENT_ID,
        len(clubs),
    )
    return clubs
