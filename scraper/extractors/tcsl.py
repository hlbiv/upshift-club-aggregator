"""
Custom extractor for US Club NPL – Texas Club Soccer League (TCSL).

TCSL operates three separate GotSport events for the 2024-25 season:
  50731 – TCSL Saturday   (85 clubs, Dallas metro area)
  50733 – TCSL Sunday     (45 clubs)
  50734 – TCSL Select     (38 clubs)

All three are combined and deduplicated before returning.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_EVENTS = [
    (50731, "TCSL Saturday"),
    (50733, "TCSL Sunday"),
    (50734, "TCSL Select"),
]
_STATE = "TX"


@register(r"texasclubsoccer\.com")
def scrape_tcsl(url: str, league_name: str) -> List[Dict]:
    logger.info("[TCSL custom] Fetching %d GotSport events", len(_EVENTS))

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

    logger.info("[TCSL custom] %d unique clubs across %d events", len(records), len(_EVENTS))
    return records
