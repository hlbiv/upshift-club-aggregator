"""
Stub extractors for leagues where the canonical URL does not expose a public
club list accessible to static or GotSport scraping.

Each stub returns an empty list and logs a WARNING with the reason so future
maintainers know which investigation has already been done.

Leagues covered:

  CCL (Club Champions League)
    URL: clubchampionsleague.com → redirects to clubchampions.org
    Reason: Site is fully JavaScript-rendered (no GotSport embeds found,
    /clubs returns 404). Needs a Playwright extractor or a GotSport event ID
    to be identified before this can produce club data.

  NPL (US Club Soccer NPL hub page)
    URL: usclubsoccer.org/npl/
    Reason: The hub page lists programs and links, not individual clubs.
    All NPL member clubs ARE covered by the individual NPL member-league
    extractors (GLA, NISL, FCL, Red River, MN, SAPL, PNW, etc.).
    Scraping this hub would duplicate those clubs with garbage nav text.

  SCCL (Southeastern Clubs Champions League)
    URL: sccl.org in leagues_master.csv is wrong — sccl.org redirects to
    sccld.org (Santa Clara County Library District), which has nothing to do
    with soccer. The correct SCCL soccer website has not been identified as of
    April 2026. Once the correct URL is found, update leagues_master.csv and
    replace or remove this stub.
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register

logger = logging.getLogger(__name__)


@register(r"clubchampionsleague\.com|clubchampions\.org")
def scrape_ccl(url: str, league_name: str) -> List[Dict]:
    logger.warning(
        "[CCL] Club Champions League has no publicly accessible static club "
        "list. The site (clubchampions.org) is JavaScript-rendered with no "
        "GotSport event IDs found. Needs a Playwright extractor or GotSport "
        "event ID. Returning 0 clubs."
    )
    return []


@register(r"usclubsoccer\.org/npl/?$")
def scrape_npl_hub(url: str, league_name: str) -> List[Dict]:
    logger.warning(
        "[NPL] The US Club Soccer NPL hub page (usclubsoccer.org/npl/) does "
        "not list individual clubs — it is a program overview page. All NPL "
        "member clubs are covered by the individual NPL member-league "
        "extractors (GLA, NISL, FCL, Red River, MN, SAPL, PNW, etc.). "
        "Returning 0 clubs to avoid nav-text pollution."
    )
    return []


@register(r"sccl\.org")
def scrape_sccl(url: str, league_name: str) -> List[Dict]:
    logger.warning(
        "[SCCL] The URL sccl.org in leagues_master.csv is incorrect — "
        "sccl.org redirects to sccld.org (Santa Clara County Library District), "
        "not the Southeastern Clubs Champions League. The correct SCCL soccer "
        "URL has not been identified as of April 2026. Update leagues_master.csv "
        "with the correct URL once found. Returning 0 clubs."
    )
    return []
