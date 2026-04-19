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
from extractors.gotsport import (
    parse_gotsport_event_html,
    scrape_gotsport_event,
    scrape_gotsport_teams,
)

logger = logging.getLogger(__name__)

_EVENTS = [
    (50731, "TCSL Saturday"),
    (50733, "TCSL Sunday"),
    (50734, "TCSL Select"),
]
_STATE = "TX"


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one TCSL GotSport event-clubs page.

    TCSL orchestrates three GotSport events (Saturday + Sunday + Select)
    and dedups them in ``scrape_tcsl``. For replay we parse one pre-fetched
    page at a time; cross-event dedup stays in the orchestrator. The TX
    ``state`` is stamped here so single-page replays produce canonical
    TCSL records.
    """
    return parse_gotsport_event_html(
        html, source_url, league_name=league_name, state=_STATE,
    )


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
