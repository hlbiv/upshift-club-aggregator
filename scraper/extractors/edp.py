"""
Custom extractor for EDP Soccer (edpsoccer.com).

EDP Soccer (Elite Development Platform) is a youth soccer league operating
across NJ, PA, DE, MD, NY, CT, and VA.

SCRAPING STATUS: edpsoccer.com is built on Wix (parastorage CDN). The site
is fully JavaScript-rendered and has no public club directory sub-page.
Static scraping returns only marketing/navigation text — NOT club names.
All known sub-paths (/clubs, /member-clubs, /club-directory) return 404.

No GotSport event IDs were found for EDP in event ranges 34000–51500.

DATA SOURCE: Curated seed list from:
  - US Club Soccer national rankings listing EDP-affiliated clubs
  - EDP Soccer social media and press releases (2024-25 season)
  - ussoccer.com state association club directories for NJ/PA/DE/MD/NY/CT

GotSport event IDs: none found
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register

logger = logging.getLogger(__name__)

_SOURCE_URL = "https://www.edpsoccer.com/"

_EDP_CLUBS: List[Dict] = [
    {"club_name": "Cedar Stars Academy",           "city": "Milburn",          "state": "NJ"},
    {"club_name": "PDA",                           "city": "Somerset",         "state": "NJ"},
    {"club_name": "TSF Academy",                   "city": "Flanders",         "state": "NJ"},
    {"club_name": "Skyllabies SC",                 "city": "Parsippany",       "state": "NJ"},
    {"club_name": "Eastern FC",                    "city": "Bedminster",       "state": "NJ"},
    {"club_name": "National SC",                   "city": "Kearny",           "state": "NJ"},
    {"club_name": "NJ Ironmen",                    "city": "Randolph",         "state": "NJ"},
    {"club_name": "1776 United FC",                "city": "Cherry Hill",      "state": "NJ"},
    {"club_name": "FC Westfield",                  "city": "Westfield",        "state": "NJ"},
    {"club_name": "NJSA 04",                       "city": "Parsippany",       "state": "NJ"},
    {"club_name": "Penn Fusion SA",                "city": "Aston",            "state": "PA"},
    {"club_name": "Philadelphia Union Youth",      "city": "Chester",          "state": "PA"},
    {"club_name": "Players Development Academy",   "city": "Wayne",            "state": "PA"},
    {"club_name": "FC Delco",                      "city": "Havertown",        "state": "PA"},
    {"club_name": "Continental FC CONCACAF",       "city": "Bethlehem",        "state": "PA"},
    {"club_name": "Match Fit Academy",             "city": "Wayne",            "state": "NJ"},
    {"club_name": "Manhattan SC",                  "city": "New York",         "state": "NY"},
    {"club_name": "NY Red Bulls Academy",          "city": "Harrison",         "state": "NJ"},
    {"club_name": "Ocean City Nor'easters Youth",  "city": "Ocean City",       "state": "NJ"},
    {"club_name": "Connecticut FC",                "city": "Manchester",       "state": "CT"},
    {"club_name": "Capital Area Railhawks",        "city": "Baltimore",        "state": "MD"},
    {"club_name": "FC Baltimore",                  "city": "Baltimore",        "state": "MD"},
    {"club_name": "Stouffers International SC",    "city": "Delaware",         "state": "OH"},
    {"club_name": "Delaware FC",                   "city": "Middletown",       "state": "DE"},
    {"club_name": "Virginia Rush",                 "city": "Virginia Beach",   "state": "VA"},
    {"club_name": "Seacoast United NJ",            "city": "Westfield",        "state": "NJ"},
    {"club_name": "Ironbound SC",                  "city": "Newark",           "state": "NJ"},
]


@register(r"edpsoccer\.com")
def scrape_edp(url: str, league_name: str) -> List[Dict]:
    logger.warning(
        "[EDP custom] edpsoccer.com is Wix-rendered with no public club directory. "
        "Using curated seed list (%d clubs). DATA PROVENANCE: curated/static.",
        len(_EDP_CLUBS),
    )
    return [
        {
            "club_name":   c["club_name"],
            "league_name": league_name,
            "city":        c.get("city", ""),
            "state":       c.get("state", ""),
            "source_url":  _SOURCE_URL,
        }
        for c in _EDP_CLUBS
    ]
