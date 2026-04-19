"""
Custom extractor for SOCAL Soccer League.

The official site (socalsoccerleague.org) links to GotSport event 43086.
Uses the shared gotsport helper to fetch and filter the clubs table.

With --teams flag: also scrapes each club's detail page to collect all
registered teams (age groups, gender, division) + contact directory.
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

_GOTSPORT_EVENT_ID = 43086
_STATE = "CA"


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for the SOCAL GotSport event-clubs page.

    SOCAL has a single GotSport event (``_GOTSPORT_EVENT_ID = 43086``), so
    ``parse_html`` is a straight delegation to ``parse_gotsport_event_html``.
    The CA ``state`` stamp lives in the live ``scrape_socal`` orchestrator;
    replay flows pass state via caller context if needed.
    """
    return parse_gotsport_event_html(
        html, source_url, league_name=league_name, state=_STATE,
    )


@register(r"socalsoccerleague\.org")
def scrape_socal(url: str, league_name: str) -> List[Dict]:
    logger.info("[SOCAL custom] Using GotSport event %d", _GOTSPORT_EVENT_ID)

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv
        teams, contacts = scrape_gotsport_teams(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
        save_teams_csv(teams, league_name)
        save_contacts_csv(contacts, league_name)

    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
