"""
South Dakota Youth Soccer Association club extractor.

CURRENT STATUS:
  - southdakotasoccer.com has no public club directory page (April 2026).
  - SoccerWire WP REST API returns 0 SD clubs.
  - No GotSport events found in scan range 43000-51100 for SD.
  - SD teams participate in USYS Midwest Conference (event 4696) but that
    is a multi-state event and not SD-specific.

DATA SOURCE: Curated seed list compiled from:
  - Third-party directories (youthsoccersports.com, Cause IQ nonprofit registry)
  - USYS Midwest Conference GotSport brackets
  - South Dakota state tournament registrations
  - Google Business Profile searches for "youth soccer" in SD cities

Update path: If SDYSA publishes a club directory or a GotSport event ID is
found, switch the state_assoc_config type from "curated_seed" to the
appropriate scraper type.

28 clubs as of April 2026.
"""

from __future__ import annotations

import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

_SOURCE_URL = "https://www.southdakotasoccer.com"

# Curated list of confirmed South Dakota youth soccer clubs.
_SD_CLUBS: List[Dict] = [
    {"club_name": "Dakota Alliance Soccer Club",         "city": "Sioux Falls",   "state": "South Dakota"},
    {"club_name": "Sioux Falls Soccer Academy",          "city": "Sioux Falls",   "state": "South Dakota"},
    {"club_name": "Tempo Soccer Club",                   "city": "Tea",           "state": "South Dakota"},
    {"club_name": "Black Hills Rapid Soccer Club",       "city": "Rapid City",    "state": "South Dakota"},
    {"club_name": "Ignite Soccer Club",                  "city": "Rapid City",    "state": "South Dakota"},
    {"club_name": "Sodak Spurs Soccer Club",             "city": "Brookings",     "state": "South Dakota"},
    {"club_name": "South Dakota United Futbol Club",     "city": "Watertown",     "state": "South Dakota"},
    {"club_name": "Brandon Area Soccer Association",     "city": "Brandon",       "state": "South Dakota"},
    {"club_name": "Mitchell Soccer Association",         "city": "Mitchell",      "state": "South Dakota"},
    {"club_name": "Yankton Youth Soccer Association",    "city": "Yankton",       "state": "South Dakota"},
    {"club_name": "Sturgis Soccer Association",          "city": "Sturgis",       "state": "South Dakota"},
    {"club_name": "Spearfish Soccer Association",        "city": "Spearfish",     "state": "South Dakota"},
    {"club_name": "Vermillion Youth Soccer League",      "city": "Vermillion",    "state": "South Dakota"},
    {"club_name": "Rushmore Premier Futbol Club",        "city": "Box Elder",     "state": "South Dakota"},
    {"club_name": "Belle Fourche Soccer Association",    "city": "Belle Fourche", "state": "South Dakota"},
    {"club_name": "Lead-Deadwood Soccer Association",    "city": "Deadwood",      "state": "South Dakota"},
    {"club_name": "Brookings Futbol Club",               "city": "Brookings",     "state": "South Dakota"},
    {"club_name": "Pierre Youth Soccer Association",     "city": "Pierre",        "state": "South Dakota"},
    {"club_name": "Aberdeen Soccer Association",         "city": "Aberdeen",      "state": "South Dakota"},
    {"club_name": "Huron Youth Soccer Association",      "city": "Huron",         "state": "South Dakota"},
    {"club_name": "Madison Youth Soccer",                "city": "Madison",       "state": "South Dakota"},
    {"club_name": "Harrisburg Soccer Association",       "city": "Harrisburg",    "state": "South Dakota"},
    {"club_name": "Dell Rapids Youth Soccer",            "city": "Dell Rapids",   "state": "South Dakota"},
    {"club_name": "Sioux Falls Force Soccer Club",       "city": "Sioux Falls",   "state": "South Dakota"},
    {"club_name": "Rapid City Rush Soccer Club",         "city": "Rapid City",    "state": "South Dakota"},
    {"club_name": "Dakota Premier Soccer Club",          "city": "Sioux Falls",   "state": "South Dakota"},
    {"club_name": "Hot Springs Youth Soccer",            "city": "Hot Springs",   "state": "South Dakota"},
    {"club_name": "Custer Youth Soccer Association",     "city": "Custer",        "state": "South Dakota"},
]


def scrape_sd_clubs(league_name: str) -> List[Dict]:
    """Return curated seed list of South Dakota youth soccer clubs."""
    logger.warning(
        "[SD] South Dakota has no public club directory — using curated "
        "seed list (%d clubs). DATA PROVENANCE: curated/static — update "
        "if SDYSA publishes a directory or a GotSport event ID is found.",
        len(_SD_CLUBS),
    )
    return [
        {
            "club_name":   c["club_name"],
            "league_name": league_name,
            "city":        c["city"],
            "state":       c["state"],
            "source_url":  _SOURCE_URL,
        }
        for c in _SD_CLUBS
    ]
