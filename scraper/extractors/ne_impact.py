"""
Custom extractor for US Club NPL – New England Impact NPL.

GotSport event:
  21393 – New England Impact NPL (21 clubs: MA, ME, NH, CT, RI, VT)

Multi-state; state left empty.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_GOTSPORT_EVENT_ID = 21393
_STATE = ""


@register(r"newenglandimpact\.com|impactnpl\.com")
def scrape_ne_impact(url: str, league_name: str) -> List[Dict]:
    logger.info("[NE Impact NPL custom] GotSport event %d", _GOTSPORT_EVENT_ID)

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv
        teams, contacts = scrape_gotsport_teams(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
        save_teams_csv(teams, league_name)
        save_contacts_csv(contacts, league_name)

    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
