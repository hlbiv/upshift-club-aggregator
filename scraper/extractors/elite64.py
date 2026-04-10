"""
Custom extractor for USYS National League Elite 64 / Club Premier 1.

Elite 64 (rebranded to NL Club Premier 1 for 2025-26) is the top tier of the
USYS National League club-based competition. It runs on GotSport with separate
events per conference plus national winter showcase events.

Source: https://www.usysnationalleague.com/schedules-results/

2024-25 season event IDs (Club Premier 1 — formerly Elite 64):
  Conference events (boys + girls combined, one per conference):
    50936  Frontier
    50937  Great Lakes
    50938  Midwest
    50939  Northeast
    50940  Pacific
    50941  Piedmont
    50942  Southeast

  Winter national showcase events (boys + girls combined):
    50935  November (exactly 64 clubs — the Elite 64 national invitational)
    50898  January

NOTE: Event IDs change each season. Update the CONFERENCE_EVENT_IDS and
WINTER_EVENT_IDS lists below when USYS National League publishes new IDs at:
  https://www.usysnationalleague.com/schedules-results/
A WARNING is logged for any event that returns zero clubs.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

# Club Premier 1 (formerly Elite 64) — one GotSport event per conference
# Update each season from https://www.usysnationalleague.com/schedules-results/
CONFERENCE_EVENT_IDS = [50936, 50937, 50938, 50939, 50940, 50941, 50942]

# Winter national showcase events (November 64-club invitational + January event)
WINTER_EVENT_IDS = [50935, 50898]

_SEASON = "2024-25"


@register(r"usclubsoccer\.org/programs/leagues|usysnationalleague\.com|thenationalleague\.com")
def scrape_elite64(url: str, league_name: str) -> List[Dict]:
    """
    Scrape clubs from all Elite 64 / NL Club Premier 1 GotSport events and
    merge them into a single deduplicated list.
    """
    all_event_ids = CONFERENCE_EVENT_IDS + WINTER_EVENT_IDS
    logger.info(
        "[Elite 64] Scraping %d GotSport events (season %s): %s",
        len(all_event_ids),
        _SEASON,
        all_event_ids,
    )

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv

        all_teams: List[Dict] = []
        all_contacts: List[Dict] = []
        for event_id in all_event_ids:
            teams, contacts = scrape_gotsport_teams(event_id, league_name, state="")
            all_teams.extend(teams)
            all_contacts.extend(contacts)
        save_teams_csv(all_teams, league_name)
        save_contacts_csv(all_contacts, league_name)

    all_records: List[Dict] = []
    for event_id in all_event_ids:
        clubs = scrape_gotsport_event(event_id, league_name, state="")
        if not clubs:
            logger.warning(
                "[Elite 64] Event %d returned 0 clubs — event may be private "
                "or the event ID has changed.  Check %s and update "
                "CONFERENCE_EVENT_IDS / WINTER_EVENT_IDS in extractors/elite64.py.",
                event_id,
                "https://www.usysnationalleague.com/schedules-results/",
            )
        all_records.extend(clubs)

    seen: set[str] = set()
    deduped: List[Dict] = []
    for club in all_records:
        key = club["club_name"].strip().lower()
        if key and key not in seen:
            seen.add(key)
            deduped.append(club)

    logger.info(
        "[Elite 64] raw=%d unique=%d clubs from %d events",
        len(all_records),
        len(deduped),
        len(all_event_ids),
    )
    return deduped
