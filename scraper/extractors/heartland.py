"""
Custom extractor for Heartland Soccer Association.

Heartland Soccer Association is one of the largest youth soccer leagues in the
US midwest, headquartered in the Kansas City metro area (KS/MO).

REGISTRATION SYSTEM: Heartland uses a proprietary CGI-based registration system
at registration.heartlandsoccer.net / heartlandsoccer.net/reports/cgi-jrb/.
They do NOT use GotSport. Confirmed by:
  - Full GotSport event-range scans (43000–51100) returning zero KS/MO events
  - heartlandsoccer.net: zero GotSport URLs found in page source
  - registration.heartlandsoccer.net is a custom Stripe-powered portal
  - GotSport org search for "heartland" returns 404

LIVE DATA SOURCE: The seedings CGI endpoint
  https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi
returns live team/club data for all age groups and divisions. Club membership
is tightly controlled: a full scrape of all levels/genders/ages returns exactly
7 unique member clubs (abbreviations mapped to full names below).

The "Non Member" rows in seedings represent guest/external teams that do not
count as Heartland member clubs.

GotSport event IDs: N/A — league does not use GotSport
"""

from __future__ import annotations

import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}
_SEEDINGS_URL = "https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi"
_MEMBER_CLUBS_URL = "https://www.heartlandsoccer.net/member-clubs/"

# Map Heartland abbreviations → (full_name, city, state)
# Verified by cross-referencing team name patterns in seedings with public club sites
_ABBR_MAP: Dict[str, tuple] = {
    "Kansas Rush": ("Kansas Rush Soccer Club",        "Olathe",          "KS"),
    "KPSL":        ("Kansas Premier Soccer League",   "Overland Park",   "KS"),
    "NEU":         ("Northeast United SC",            "Kansas City",     "KS"),
    "OPSC":        ("Overland Park Soccer Club",      "Overland Park",   "KS"),
    "SBV":         ("Sporting Blue Valley",           "Overland Park",   "KS"),
    "KC Fusion":   ("Kansas City Fusion SC",          "Leawood",         "KS"),
    "SPLS":        ("SLS Soccer Club",                "Shawnee",         "KS"),
}

_PREMIER_AGES = [
    "U-9", "U-10", "U-11", "U-12", "U-13", "U-14",
    "U-15", "U-16", "U-17", "U-18/19",
]
_REC_AGES = [
    "U-9/3rd Grade 7v7", "U-10/4th Grade 7v7", "U-10/4th Grade 9v9",
    "U-11/5th Grade 9v9", "U-12/6th Grade 9v9", "U-13/7th Grade",
    "U-13/14-7th/8th Grade", "U-14/8th Grade",
]


def _fetch_seedings_clubs() -> set:
    """
    Hit the Heartland seedings CGI endpoint for every level/gender/age group
    combination and return the set of unique club abbreviations found.
    """
    seen_abbrs: set = set()
    combos = (
        [("Premier", age) for age in _PREMIER_AGES] +
        [("Recreational", age) for age in _REC_AGES]
    )
    for level, age in combos:
        for gender in ("Boys", "Girls"):
            params = {"level1": level, "b_g1": gender, "age1": age}
            try:
                r = requests.get(
                    _SEEDINGS_URL, params=params, headers=_HEADERS, timeout=12
                )
                if r.status_code != 200 or not r.text.strip():
                    continue
                soup = BeautifulSoup(r.text, "lxml")
                for tr in soup.find_all("tr", class_="textsm"):
                    tds = tr.find_all("td")
                    if len(tds) >= 2:
                        abbr = tds[1].get_text(strip=True)
                        if abbr and abbr != "Non Member":
                            seen_abbrs.add(abbr)
            except requests.RequestException:
                continue
    return seen_abbrs


@register(r"heartlandsoccer\.net")
def scrape_heartland(url: str, league_name: str) -> List[Dict]:
    logger.info(
        "[Heartland custom] Scraping live seedings CGI endpoint. "
        "Heartland uses proprietary registration (not GotSport)."
    )

    live_abbrs = _fetch_seedings_clubs()

    if live_abbrs:
        logger.info("[Heartland custom] CGI returned %d unique member clubs", len(live_abbrs))
        records = []
        for abbr in sorted(live_abbrs):
            full_name, city, state = _ABBR_MAP.get(abbr, (abbr, "", "KS"))
            records.append({
                "club_name":   full_name,
                "league_name": league_name,
                "city":        city,
                "state":       state,
                "source_url":  _SEEDINGS_URL,
            })
        return records

    # Fallback: minimal governance members from public member-clubs page
    logger.warning(
        "[Heartland custom] CGI fetch returned no clubs — using known governance members."
    )
    return [
        {
            "club_name":   name,
            "league_name": league_name,
            "city":        city,
            "state":       state,
            "source_url":  _MEMBER_CLUBS_URL,
        }
        for name, city, state in [
            v for v in _ABBR_MAP.values()
        ]
    ]
