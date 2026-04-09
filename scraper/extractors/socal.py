"""
Custom extractor for SOCAL Soccer League.

The official site (socalsoccerleague.org) links to GotSport event 43086.
Uses the shared gotsport helper to fetch and filter the clubs table.
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event

logger = logging.getLogger(__name__)

_GOTSPORT_EVENT_ID = 43086


@register(r"socalsoccerleague\.org")
def scrape_socal(url: str, league_name: str) -> List[Dict]:
    logger.info("[SOCAL custom] Using GotSport event %d", _GOTSPORT_EVENT_ID)
    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state="CA")
