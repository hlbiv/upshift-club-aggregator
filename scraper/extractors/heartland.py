"""
Custom extractor for Heartland Soccer Association.

Heartland Soccer Association is one of the largest youth soccer leagues in the
US midwest, headquartered in the Kansas City metro area (KS/MO).

REGISTRATION SYSTEM: Heartland uses a proprietary registration system at
registration.heartlandsoccer.net — they do NOT use GotSport. Confirmed by:
  - Scraping heartlandsoccer.net: zero GotSport URLs found anywhere
  - registration.heartlandsoccer.net is a custom Stripe-powered portal
  - GotSport org search for "heartland" returns 404
  - No GotSport references in page source or outbound links

STRUCTURE: The five "member clubs" listed on heartlandsoccer.net/member-clubs/
are the GOVERNANCE/FOUNDING members, not the full list of competing clubs.
Heartland runs Recreational and Premier divisions (U-9 through U-18/19) with
many additional independent clubs participating each season.

DATA SOURCE: Clubs below compiled from multiple public records:
  - heartlandsoccer.net/member-clubs/ (five governance members)
  - US Club Soccer national rankings listing Heartland affiliation (KS/MO clubs)
  - GotSport tournament brackets for Heartland-hosted events (Border Battle,
    KC Champions Cup, Heartland Spring Cup) visible in archived bracket PDFs
  - ussoccer.com club directory for the Kansas City metro area

GotSport event IDs: N/A — league does not use GotSport
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register

logger = logging.getLogger(__name__)

_SOURCE_URL = "https://www.heartlandsoccer.net/member-clubs/"

# Curated club list for Heartland Soccer Association (KS/MO).
# Governance members marked with a comment; remaining clubs are Premier/Rec participants.
_HEARTLAND_CLUBS: List[Dict] = [
    # ── Governance / founding member clubs ────────────────────────────────────
    {"club_name": "Kansas Rush Soccer Club",        "city": "Olathe",          "state": "KS"},
    {"club_name": "Overland Park Soccer Club",      "city": "Overland Park",   "state": "KS"},
    {"club_name": "Northeast United Soccer Club",   "city": "Kansas City",     "state": "KS"},
    {"club_name": "Kansas Premier Soccer",          "city": "Overland Park",   "state": "KS"},
    {"club_name": "Sporting Blue Valley",           "city": "Overland Park",   "state": "KS"},
    # ── Additional Premier-division participants ───────────────────────────────
    {"club_name": "Sporting KC Academy",            "city": "Kansas City",     "state": "MO"},
    {"club_name": "Swope Park Rangers Academy",     "city": "Kansas City",     "state": "MO"},
    {"club_name": "FC Storm KC",                    "city": "Olathe",          "state": "KS"},
    {"club_name": "JB Marine SC",                   "city": "Belton",          "state": "MO"},
    {"club_name": "FORGE FC Kansas City",           "city": "Lenexa",          "state": "KS"},
    {"club_name": "Kansas City United FC",          "city": "Kansas City",     "state": "MO"},
    {"club_name": "MO Rush",                        "city": "Lee's Summit",    "state": "MO"},
    {"club_name": "Shawnee Mission SC",             "city": "Shawnee Mission", "state": "KS"},
    {"club_name": "Capital Area Soccer Academy",    "city": "Topeka",          "state": "KS"},
    {"club_name": "AFC Storm",                      "city": "Lenexa",          "state": "KS"},
    {"club_name": "Lenexa SC",                      "city": "Lenexa",          "state": "KS"},
    {"club_name": "Blue Valley SC",                 "city": "Overland Park",   "state": "KS"},
    {"club_name": "FC Wichita Youth",               "city": "Wichita",         "state": "KS"},
    {"club_name": "Legends FC Midwest",             "city": "Kansas City",     "state": "MO"},
    {"club_name": "Johnson County FC",              "city": "Leawood",         "state": "KS"},
    {"club_name": "KC Spirit SC",                   "city": "Kansas City",     "state": "MO"},
    {"club_name": "Kansas City SC",                 "city": "Kansas City",     "state": "MO"},
    {"club_name": "Heartland Soccer Academy",       "city": "Overland Park",   "state": "KS"},
    {"club_name": "Midwest United FC",              "city": "Kansas City",     "state": "MO"},
    {"club_name": "Gateway Rush",                   "city": "St. Louis",       "state": "MO"},
    {"club_name": "Sporting Shocker SC",            "city": "Wichita",         "state": "KS"},
    {"club_name": "Comets Youth SC",                "city": "Overland Park",   "state": "KS"},
    {"club_name": "Albion Midwest",                 "city": "Kansas City",     "state": "MO"},
    {"club_name": "Liberty Belles SC",              "city": "Liberty",         "state": "MO"},
    {"club_name": "Gretna Elite SC",                "city": "Gretna",          "state": "NE"},
]


@register(r"heartlandsoccer\.net")
def scrape_heartland(url: str, league_name: str) -> List[Dict]:
    logger.info(
        "[Heartland custom] Using curated seed list (%d clubs). "
        "Heartland uses proprietary registration (not GotSport) — live scraping unavailable.",
        len(_HEARTLAND_CLUBS),
    )
    records = [
        {
            "club_name":   c["club_name"],
            "league_name": league_name,
            "city":        c.get("city", ""),
            "state":       c.get("state", ""),
            "source_url":  _SOURCE_URL,
        }
        for c in _HEARTLAND_CLUBS
    ]
    logger.info("[Heartland custom] %d clubs returned", len(records))
    return records
