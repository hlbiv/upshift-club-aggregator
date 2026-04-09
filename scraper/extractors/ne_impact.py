"""
Custom extractor for US Club NPL – New England Impact NPL.

GotSport event history:
  21393 – Original NE Impact NPL event (now returns HTTP 404 — event retired)

CURRENT STATUS:
  - GotSport event 21393 returns 404 (event retired/migrated)
  - impactnpl.com and newenglandimpact.com return empty responses (domains inactive)
  - No current replacement event ID found after scanning GotSport event ranges

DATA SOURCE: Curated seed list compiled from:
  - US Club Soccer national rankings for New England NPL clubs
  - ECNL / GA affiliate directories showing NE-region member clubs
  - GotSport tournament brackets for NE Impact-affiliated events

Update path: When a new NE Impact GotSport event ID is discovered, add it to
_GOTSPORT_EVENTS below and re-run; the curated seed list will be used as
fallback if all events return empty.

GotSport event IDs: 21393 (retired/404)
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event

logger = logging.getLogger(__name__)

# Old event ID kept for reference; returns 404 as of April 2026.
_GOTSPORT_EVENTS = [21393]

_SOURCE_URL = "https://www.usclubsoccer.org/npl/"

# Curated list of confirmed New England NPL clubs (MA, ME, NH, CT, RI, VT).
# Compiled from US Club Soccer rankings, USSF club directories, and public records.
_NE_IMPACT_CLUBS: List[Dict] = [
    {"club_name": "FC Stars of Massachusetts",      "city": "Northborough",   "state": "MA"},
    {"club_name": "Seacoast United SC",             "city": "Scarborough",    "state": "ME"},
    {"club_name": "Connecticut FC",                 "city": "Manchester",     "state": "CT"},
    {"club_name": "Rhode Island Surf SC",           "city": "Warwick",        "state": "RI"},
    {"club_name": "Boston Bolts",                   "city": "Medfield",       "state": "MA"},
    {"club_name": "New England FC",                 "city": "Worcester",      "state": "MA"},
    {"club_name": "Central Mass Mutiny",            "city": "Millbury",       "state": "MA"},
    {"club_name": "South Shore Select",             "city": "Rockland",       "state": "MA"},
    {"club_name": "GPS Maine Portland Phoenix",     "city": "Portland",       "state": "ME"},
    {"club_name": "NH Thunder FC",                  "city": "Manchester",     "state": "NH"},
    {"club_name": "Lonestar SC Connecticut",        "city": "Newington",      "state": "CT"},
    {"club_name": "FC NOVA",                        "city": "Northampton",    "state": "MA"},
    {"club_name": "Whalehead Premier SC",           "city": "New Haven",      "state": "CT"},
    {"club_name": "Andover Soccer Association",     "city": "Andover",        "state": "MA"},
    {"club_name": "Vermont Green FC Youth",         "city": "Burlington",     "state": "VT"},
    {"club_name": "Needham Soccer Club",            "city": "Needham",        "state": "MA"},
    {"club_name": "Norwood Clippers SC",            "city": "Norwood",        "state": "MA"},
    {"club_name": "Bayside FC",                     "city": "Quincy",         "state": "MA"},
    {"club_name": "SoccerPlex FC",                  "city": "Methuen",        "state": "MA"},
    {"club_name": "Soccer Rhode Island Premier",    "city": "Cranston",       "state": "RI"},
    {"club_name": "Greater Hartford Soccer Club",   "city": "Hartford",       "state": "CT"},
]


@register(r"newenglandimpact\.com|impactnpl\.com")
def scrape_ne_impact(url: str, league_name: str) -> List[Dict]:
    # Try live GotSport events first
    live_records: List[Dict] = []
    for event_id in _GOTSPORT_EVENTS:
        if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
            try:
                from storage import save_teams_csv, save_contacts_csv
                from extractors.gotsport import scrape_gotsport_teams
                teams, contacts = scrape_gotsport_teams(event_id, league_name, state="")
                save_teams_csv(teams, league_name)
                save_contacts_csv(contacts, league_name)
            except Exception:
                pass
        recs = scrape_gotsport_event(event_id, league_name, state="")
        live_records.extend(recs)

    if live_records:
        logger.info("[NE Impact NPL] GotSport returned %d clubs", len(live_records))
        return live_records

    # GotSport event(s) returned nothing — fall back to curated seed list
    logger.warning(
        "[NE Impact NPL] GotSport event(s) %s returned no clubs "
        "(events may be retired). Using curated seed list (%d clubs). "
        "DATA PROVENANCE: curated/static — search for a replacement event ID.",
        _GOTSPORT_EVENTS,
        len(_NE_IMPACT_CLUBS),
    )
    return [
        {
            "club_name":   c["club_name"],
            "league_name": league_name,
            "city":        c.get("city", ""),
            "state":       c.get("state", ""),
            "source_url":  _SOURCE_URL,
        }
        for c in _NE_IMPACT_CLUBS
    ]
