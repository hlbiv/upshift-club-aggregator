"""
Custom extractor for Michigan State Premier Soccer Program (MSPSP).

MSPSP manages its schedule through GotSport (event 50611 for Spring 2026).
The clubs page is plain HTML — no JS required.
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event

logger = logging.getLogger(__name__)

_GOTSPORT_EVENT_ID = 50611


@register(r"mspsp\.org")
def scrape_mspsp(url: str, league_name: str) -> List[Dict]:
    logger.info("[MSPSP custom] Using GotSport event %d", _GOTSPORT_EVENT_ID)
    return scrape_gotsport_event(_GOTSPORT_EVENT_ID, league_name, state="MI")
