"""
Custom extractor for Sunshine State Soccer League (SSSL) – Florida NPL member.

GotSport events covering SSSL / Florida youth soccer clubs:
  4697  – Florida Soccer Club League (220 clubs, broad Florida registry)
  32708 – SSSL active season event  (93 clubs, current season)

Both events are merged and deduplicated for full coverage.
State is FL; city is not available from the GotSport clubs list endpoint.
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
    (4697,  "SSSL Florida registry"),
    (32708, "SSSL active season"),
]
_STATE = "FL"


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one SSSL GotSport event-clubs page.

    SSSL orchestrates two GotSport events and merges + dedups them in
    ``scrape_sssl``. For replay we parse one pre-fetched page at a time;
    cross-event dedup stays in the orchestrator. The FL ``state`` is
    stamped here so single-page replays produce canonical SSSL records.
    """
    return parse_gotsport_event_html(
        html, source_url, league_name=league_name, state=_STATE,
    )


@register(r"sssl\.net")
def scrape_sssl(url: str, league_name: str) -> List[Dict]:
    logger.info("[SSSL custom] Fetching %d GotSport events", len(_EVENTS))

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

    logger.info("[SSSL custom] %d unique clubs", len(records))
    return records
