"""
Custom extractor for US Club NPL – Frontier Premier League.

GotSport events covering Frontier Premier League (midwest/south region NPL):
  44015 – Frontier Premier League 2024     (15 clubs; MO/AR/OK/TX)
  50987 – Frontier Premier League 2025-26B (17 clubs; OK division)
  50988 – Frontier Premier League 2025-26A (14 clubs; MO/AR/OK/TX)

All three events are merged and deduplicated. Clubs span MO, AR, OK, TX.
Events 50987 and 50988 are two divisions of the same 2025-26 season.
State left empty because Frontier covers multiple states.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import (
    parse_event_clubs_html,
    scrape_gotsport_event,
    scrape_gotsport_teams,
)

logger = logging.getLogger(__name__)

_EVENTS = [
    (44015, "Frontier Premier 2024"),
    (50987, "Frontier Premier 2025-26B (OK)"),
    (50988, "Frontier Premier 2025-26A"),
]
_STATE = ""


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """Pure-function parser for a single archived Frontier NPL GotSport clubs page."""
    return parse_event_clubs_html(
        html,
        source_url=source_url,
        league_name=league_name,
        state=_STATE,
    )


@register(r"frontierpremiersoccer\.com")
def scrape_frontier(url: str, league_name: str) -> List[Dict]:
    logger.info("[Frontier NPL custom] Fetching %d GotSport events", len(_EVENTS))

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

    logger.info("[Frontier NPL custom] %d unique clubs", len(records))
    return records
