"""
Custom extractor for Arizona Soccer Club League.

Uses the Arizona Soccer Association's Advanced Leagues on GotSport:
  44446 – 2025-26 Arizona Advanced Leagues (52 clubs, current season)
  4987  – earlier Arizona Soccer Club League event (99 clubs, broader list)

Both events are merged and deduplicated for maximum club coverage.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_EVENTS = [
    (44446, "AZ Advanced Leagues 2025-26"),
    (4987,  "AZ Soccer Club League (historical)"),
]
_STATE = "AZ"


@register(r"azsoccerleague\.com|azsoccerassociation\.org")
def scrape_az_soccer(url: str, league_name: str) -> List[Dict]:
    logger.info("[AZ Soccer custom] Fetching %d GotSport events", len(_EVENTS))

    seen: set = set()
    records: List[Dict] = []

    for event_id, division in _EVENTS:
        if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
            from storage import save_teams_csv, save_contacts_csv
            label = f"{league_name} – {division}"
            teams, contacts = scrape_gotsport_teams(event_id, label, state=_STATE)
            save_teams_csv(teams, label)
            save_contacts_csv(contacts, label)

        for rec in scrape_gotsport_event(event_id, league_name, state=_STATE):
            key = rec["club_name"].lower().strip()
            if key not in seen:
                seen.add(key)
                records.append(rec)

    logger.info("[AZ Soccer custom] %d unique clubs", len(records))
    return records
