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

PURE-FUNCTION PARSER: `parse_html(html, url, league_name)` is exposed as a
module-level, side-effect-free function so that the `--source replay-html`
handler (PR #80) can re-run extraction against archived HTML without making
any network calls. The `@register`-ed `scrape_heartland` entry point wraps
fetch + parse.
"""

from __future__ import annotations

import logging
from typing import Dict, List

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


def parse_html(html: str, url: str, league_name: str) -> List[Dict]:
    """
    Pure-function parser for a Heartland seedings CGI response.

    Reads the <tr class="textsm"> rows of a single seedings-CGI HTML page,
    extracts the Club column (td[1]), filters out "Non Member" guest rows,
    and maps each member abbreviation through ``_ABBR_MAP`` to produce the
    canonical club record shape used by the pipeline.

    Because a single seedings page covers one level / gender / age combo,
    callers that want the full Heartland member-club set must aggregate
    ``parse_html`` results across the full grid. The live
    ``scrape_heartland`` entry point does exactly that; replay (PR #80)
    invokes ``parse_html`` per archived page.

    Parameters
    ----------
    html:
        Raw HTML body of a seedings.cgi response. Empty / whitespace-only
        input yields ``[]``.
    url:
        The source URL the HTML came from. Recorded verbatim on each
        emitted record as ``source_url``.
    league_name:
        League name to stamp on every emitted record (e.g.
        ``"Heartland Soccer Association"``).

    Returns
    -------
    List of dicts, one per distinct member-club abbreviation on the page,
    each with ``club_name``, ``league_name``, ``city``, ``state``,
    ``source_url``. Unknown abbreviations fall back to ``(abbr, "", "KS")``
    to match the legacy ``scrape_heartland`` behaviour.
    """
    if not html or not html.strip():
        return []

    soup = BeautifulSoup(html, "lxml")
    seen_abbrs: set = set()
    for tr in soup.find_all("tr", class_="textsm"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            abbr = tds[1].get_text(strip=True)
            if abbr and abbr != "Non Member":
                seen_abbrs.add(abbr)

    records: List[Dict] = []
    for abbr in sorted(seen_abbrs):
        full_name, city, state = _ABBR_MAP.get(abbr, (abbr, "", "KS"))
        records.append({
            "club_name":   full_name,
            "league_name": league_name,
            "city":        city,
            "state":       state,
            "source_url":  url,
        })
    return records


def _fetch_seedings_clubs(league_name: str) -> List[Dict]:
    """
    Hit the Heartland seedings CGI endpoint for every level/gender/age group
    combination and aggregate member-club records across the grid.

    Delegates per-page HTML parsing to ``parse_html`` so the exact same code
    path is exercised by live scraping and by --source replay-html.
    """
    by_abbr: Dict[str, Dict] = {}
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
                for rec in parse_html(r.text, _SEEDINGS_URL, league_name):
                    # Dedup across pages using canonical club_name
                    by_abbr.setdefault(rec["club_name"], rec)
            except requests.RequestException:
                continue
    return list(by_abbr.values())


@register(r"heartlandsoccer\.net")
def scrape_heartland(url: str, league_name: str) -> List[Dict]:
    logger.info(
        "[Heartland custom] Scraping live seedings CGI endpoint. "
        "Heartland uses proprietary registration (not GotSport)."
    )

    records = _fetch_seedings_clubs(league_name)

    if records:
        logger.info(
            "[Heartland custom] CGI returned %d unique member clubs", len(records)
        )
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
        for name, city, state in _ABBR_MAP.values()
    ]
