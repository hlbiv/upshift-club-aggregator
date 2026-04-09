"""
Custom extractor for Michigan State Premier Soccer Program (MSPSP).

MSPSP manages its schedule through GotSport (event 50611 for Spring 2026).
The clubs page is plain HTML — no JS required.

With --teams flag: also scrapes each club's detail page to collect all
registered teams (age groups, gender, division) + contact directory.
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)

_GOTSPORT_EVENT_ID = 50611
_STATE = "MI"


@register(r"mspsp\.org")
def scrape_mspsp(url: str, league_name: str) -> List[Dict]:
    logger.info("[MSPSP custom] Using GotSport event %d", _GOTSPORT_EVENT_ID)

    if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
        from storage import save_teams_csv, save_contacts_csv
        teams, contacts = scrape_gotsport_teams(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
        save_teams_csv(teams, league_name)
        save_contacts_csv(contacts, league_name)

    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state=_STATE)
