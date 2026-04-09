"""
Custom extractor for US Club NPL – Central States NPL.

GotSport event:
  46428 – Central States NPL (7 clubs: Gateway Rush, Gretna Elite, JB Marine,
           Kansas City Surf, and others in the MO/NE/IA/KS region)

Small league; multi-state coverage so state is left empty.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_GOTSPORT_EVENT_ID = 46428
_STATE = ""


@register(r"centralstatesnpl\.com")
def scrape_central_states(url: str, league_name: str) -> List[Dict]:
    logger.info("[Central States NPL custom] GotSport event %d", _GOTSPORT_EVENT_ID)

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv
        teams, contacts = scrape_gotsport_teams(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
        save_teams_csv(teams, league_name)
        save_contacts_csv(contacts, league_name)

    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
