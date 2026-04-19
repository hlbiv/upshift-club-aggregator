"""
Custom extractor for US Club NPL – Mountain West NPL.

GotSport event:
  44839 – JPL Mountain West NPL 2025-26 (32 clubs: CO, SD, WY, UT, NV, NM, MT)

Multi-state coverage; state left empty.
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

_GOTSPORT_EVENT_ID = 44839
_STATE = ""


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """Pure-function parser for the archived Mountain West NPL GotSport clubs page."""
    return parse_gotsport_event_html(
        html,
        url=source_url,
        league_name=league_name,
        state=_STATE,
    )


@register(r"mountainwestnpl\.com")
def scrape_mountain_west(url: str, league_name: str) -> List[Dict]:
    logger.info("[Mountain West NPL custom] GotSport event %d", _GOTSPORT_EVENT_ID)

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv
        teams, contacts = scrape_gotsport_teams(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
        save_teams_csv(teams, league_name)
        save_contacts_csv(contacts, league_name)

    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
