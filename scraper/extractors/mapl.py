"""
Custom extractor for US Club NPL – Mid-Atlantic Premier League (MAPL).

GotSport events:
  45036 – MAPL current season (50 clubs: NJ, NY, PA, MD, DE region)
  36297 – MAPL prior season  (20 clubs, for additional coverage)

Multi-state; state left empty.
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
    (45036, "MAPL current"),
    (36297, "MAPL prior"),
]
_STATE = ""


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one MAPL GotSport event-clubs page.

    MAPL orchestrates two GotSport events (current + prior season) and
    dedups them in ``scrape_mapl``. ``parse_html`` receives a single
    pre-fetched page at a time (replay) and delegates directly to
    ``parse_gotsport_event_html``. Cross-event dedup stays in the
    orchestrator.
    """
    return parse_gotsport_event_html(html, source_url, league_name=league_name)


@register(r"mapl-soccer\.com|midatlanticpremierleague\.com")
def scrape_mapl(url: str, league_name: str) -> List[Dict]:
    logger.info("[MAPL custom] Fetching %d GotSport events", len(_EVENTS))

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

    logger.info("[MAPL custom] %d unique clubs", len(records))
    return records
