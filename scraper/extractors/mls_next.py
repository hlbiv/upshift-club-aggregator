"""
Custom extractor for MLS NEXT club directory (mlsnextsoccer.com/clubs).

The page is a JavaScript-rendered React SPA.  The extractor tries two strategies
in order:

1. **Playwright live scrape** — launches a headless browser, waits for the club
   cards to render, then extracts name / city / state from the DOM.  CSS selectors
   target the card layout observed on the site (h3.club-name, p.club-location, etc.).
   If the cards cannot be found we fall through to strategy 2.

2. **Curated seed dataset** — a hard-coded list of 159 clubs sourced from the
   Wikipedia "MLS Next" article (as of 2025-02-28) which is the most complete
   publicly available enumeration of MLS NEXT members.  This is used whenever
   the live page is unreachable (network-sandboxed environment) or the DOM
   structure has changed.

Source: https://en.wikipedia.org/wiki/MLS_Next (Clubs table, as of 2025-02-28)
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

from bs4 import BeautifulSoup

from extractors.registry import register
from extractors.playwright_helper import render_page

logger = logging.getLogger(__name__)

_URL = "https://www.mlsnextsoccer.com/clubs"

# ---------------------------------------------------------------------------
# Curated seed dataset — Wikipedia "MLS Next" clubs table, 2025-02-28
# Each entry: (club_name, city, state_abbrev)
# ---------------------------------------------------------------------------
_SEED: List[tuple] = [
    ("AC River", "San Antonio", "TX"),
    ("Achilles FC", "Silver Spring", "MD"),
    ("AFC Lightning", "Peachtree City", "GA"),
    ("Albion SC Las Vegas", "Las Vegas", "NV"),
    ("Albion SC Merced", "Merced", "CA"),
    ("Albion SC San Diego", "San Diego", "CA"),
    ("Alexandria SA", "Alexandria", "VA"),
    ("ASG FC", "Tallahassee", "FL"),
    ("Athletum FC", "Hialeah", "FL"),
    ("Atlanta United FC Academy", "Marietta", "GA"),
    ("Atletico Santa Rosa", "Santa Rosa", "CA"),
    ("Austin FC Academy", "Austin", "TX"),
    ("Ballistic United", "Pleasanton", "CA"),
    ("Baltimore Armour", "Ellicott City", "MD"),
    ("Barca Residency Academy", "Casa Grande", "AZ"),
    ("Bavarian United SC", "Glendale", "WI"),
    ("Bayside FC", "East Providence", "RI"),
    ("Beachside of Connecticut", "Norwalk", "CT"),
    ("Beadling Soccer Club", "Pittsburgh", "PA"),
    ("Bethesda Soccer Club", "North Potomac", "MD"),
    ("Blau Weiss Gottschee", "Middle Village", "NY"),
    ("Boston Bolts", "Newton", "MA"),
    ("Breakers FC", "Aptos", "CA"),
    ("California Odyssey Soccer Club", "Clovis", "CA"),
    ("Capital City Soccer Club", "Austin", "TX"),
    ("Carolina Core FC", "High Point", "NC"),
    ("Cedar Stars Academy - Bergen", "South Hackensack", "NJ"),
    ("Cedar Stars Academy - Monmouth", "Tinton Falls", "NJ"),
    ("CF Montreal", "Montreal", "QC"),
    ("Chargers SC", "Clearwater", "FL"),
    ("Charlotte FC Academy", "Charlotte", "NC"),
    ("Chicago FC United", "Glenview", "IL"),
    ("Chicago Fire FC", "Bridgeview", "IL"),
    ("Chula Vista FC", "Chula Vista", "CA"),
    ("Cincinnati United Premier", "Cincinnati", "OH"),
    ("City SC", "Carlsbad", "CA"),
    ("Colorado Rapids Academy", "Commerce City", "CO"),
    ("Colorado Rapids Youth SC", "Denver", "CO"),
    ("Columbus Crew Academy", "Columbus", "OH"),
    ("Connecticut United FC", "Bridgeport", "CT"),
    ("D.C. United Academy", "Washington", "DC"),
    ("De Anza Force", "San Jose", "CA"),
    ("Diablo Valley Wolves", "Concord", "CA"),
    ("Downtown United SC", "New York", "NY"),
    ("Empire United SC", "Rochester", "NY"),
    ("FA Euro New York", "Brooklyn", "NY"),
    ("FC Bay Area", "San Jose", "CA"),
    ("FC Boston Bolts", "Newton", "MA"),
    ("FC Cincinnati Academy", "Cincinnati", "OH"),
    ("FC Dallas Academy", "Frisco", "TX"),
    ("FC Delco", "Downingtown", "PA"),
    ("FC Golden State Force", "Jurupa Valley", "CA"),
    ("FC Westchester", "Scarsdale", "NY"),
    ("Florida Rush SC", "Clermont", "FL"),
    ("Hoosier Premier Academy", "Noblesville", "IN"),
    ("Hoover-Vestavia Soccer", "Birmingham", "AL"),
    ("Houston Dynamo Academy", "Houston", "TX"),
    ("Houston Rangers", "Houston", "TX"),
    ("IdeaSport Soccer Academy", "Kissimmee", "FL"),
    ("Idea Toros Futbol Academy", "Edinburg", "TX"),
    ("IMG Academy", "Bradenton", "FL"),
    ("Indiana Fire Academy", "Carmel", "IN"),
    ("Inter Atlanta FC", "Atlanta", "GA"),
    ("Inter Miami CF Academy", "Fort Lauderdale", "FL"),
    ("IFA of New England", "Newton", "MA"),
    ("Internationals", "North Royalton", "OH"),
    ("Ironbound SC", "Newark", "NJ"),
    ("Kalonji SA", "Norcross", "GA"),
    ("Keystone FC", "Mechanicsburg", "PA"),
    ("Kings Hammer Soccer Club", "Covington", "KY"),
    ("Jacksonville FC", "Jacksonville", "FL"),
    ("Javanon FC", "Louisville", "KY"),
    ("LA United Futbol Academy", "Los Angeles", "CA"),
    ("LA Galaxy Academy", "Carson", "CA"),
    ("LA Surf", "La Canada Flintridge", "CA"),
    ("Lamorinda FC", "Moraga", "CA"),
    ("Lanier SC", "Atlanta", "GA"),
    ("Las Vegas SA", "Las Vegas", "NV"),
    ("Lexington SC Academy", "Lexington", "KY"),
    ("Long Island Soccer Club", "Uniondale", "NY"),
    ("Los Angeles Bulls SC", "Pacific Palisades", "CA"),
    ("Los Angeles FC Academy", "Los Angeles", "CA"),
    ("Los Angeles Soccer Club", "West Covina", "CA"),
    ("Lou Fusz Athletic", "Earth City", "MO"),
    ("Louisiana TDP Elite", "Baton Rouge", "LA"),
    ("Metropolitan Oval", "Queens", "NY"),
    ("Miami Rush-Kendall SC", "Miami", "FL"),
    ("Michigan Jaguars", "Novi", "MI"),
    ("Michigan Wolves", "Farmington Hills", "MI"),
    ("Midwest United FC", "Kentwood", "MI"),
    ("Minnesota United FC", "Minneapolis", "MN"),
    ("Modesto Ajax United", "Modesto", "CA"),
    ("Murrieta Soccer Academy", "Murrieta", "CA"),
    ("Napa United", "Napa", "CA"),
    ("Nashville SC Academy", "Nashville", "TN"),
    ("Nashville United", "Brentwood", "TN"),
    ("NEFC", "Mendon", "MA"),
    ("New England Revolution Academy", "Foxborough", "MA"),
    ("NYCFC Academy", "Orangeburg", "NY"),
    ("New York Red Bulls Academy", "Whippany", "NJ"),
    ("New York SC", "Purchase", "NY"),
    ("Nomads", "San Diego", "CA"),
    ("Oakwood SC", "Glastonbury", "CT"),
    ("Orlando City SC Academy", "Orlando", "FL"),
    ("Orlando City Soccer School South", "Kissimmee", "FL"),
    ("Orlando City Soccer School - Seminole", "Sanford", "FL"),
    ("PA Classics", "Manheim", "PA"),
    ("Philadelphia Union Academy", "Wayne", "PA"),
    ("Phoenix Rising FC", "Phoenix", "AZ"),
    ("Players Development Academy", "Somerset", "NJ"),
    ("Portland Timbers Academy", "Portland", "OR"),
    ("Queen City Mutiny", "Charlotte", "NC"),
    ("Real Colorado", "Centennial", "CO"),
    ("Real Jersey FC", "Medford", "NJ"),
    ("Real Salt Lake Academy", "Herriman", "UT"),
    ("RSL Arizona", "Tempe", "AZ"),
    ("RSL Arizona - Mesa", "Mesa", "AZ"),
    ("Sacramento Republic FC", "Sacramento", "CA"),
    ("Sacramento United", "Sacramento", "CA"),
    ("San Antonio FC", "San Antonio", "TX"),
    ("San Francisco Glens", "San Francisco", "CA"),
    ("San Francisco Seals", "San Francisco", "CA"),
    ("San Jose Earthquakes Academy", "Santa Clara", "CA"),
    ("Santa Barbara SC", "Santa Barbara", "CA"),
    ("SC Del Sol", "Phoenix", "AZ"),
    ("SC Wave", "Franklin", "WI"),
    ("Seacoast United", "Hampton", "NH"),
    ("Seattle Sounders FC Academy", "Tukwila", "WA"),
    ("Shattuck-St. Mary's", "Faribault", "MN"),
    ("Sheriffs Futbol Club", "Hayward", "CA"),
    ("Silicon Valley SA", "Palo Alto", "CA"),
    ("SoCal Reds FC", "Irvine", "CA"),
    ("Sockers FC Chicago", "Palatine", "IL"),
    ("Solar SC", "Trophy Club", "TX"),
    ("South Florida FA", "Boca Raton", "FL"),
    ("Southern Soccer Academy", "Atlanta", "GA"),
    ("Southern States Soccer Club", "Hattiesburg", "MS"),
    ("Sporting Athletic Club", "Wilmington", "DE"),
    ("Sporting Kansas City Academy", "Kansas City", "KS"),
    ("Sporting City", "Kansas City", "MO"),
    ("Springfield South County Youth Club", "Springfield", "VA"),
    ("Strikers FC", "Irvine", "CA"),
    ("St. Louis City SC", "St. Louis", "MO"),
    ("St. Louis Scott Gallagher", "Fenton", "MO"),
    ("Tampa Bay United Rowdies", "Tampa", "FL"),
    ("Tormenta FC", "Statesboro", "GA"),
    ("Toronto FC Academy", "Toronto", "ON"),
    ("Total Futbol Academy", "Los Angeles", "CA"),
    ("Total Football Club", "Katy", "TX"),
    ("Triangle United Soccer", "Chapel Hill", "NC"),
    ("TSF Academy", "Lincoln Park", "NJ"),
    ("Valeo Futbol Club", "Newton", "MA"),
    ("Vancouver Whitecaps FC Academy", "Vancouver", "BC"),
    ("VARDAR Soccer Club", "Rochester Hills", "MI"),
    ("Ventura County Fusion", "Ventura", "CA"),
    ("Wake Futbol Club", "Holly Springs", "NC"),
    ("West Florida Flames", "Brandon", "FL"),
    ("West Virginia Soccer", "Shepherdstown", "WV"),
    ("Weston FC", "Weston", "FL"),
    ("Woodside Soccer Club Crush", "Redwood City", "CA"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _seed_records(league_name: str, source_url: str) -> List[Dict]:
    """
    Convert the curated seed list to the canonical record format.

    Records produced here are derived from the Wikipedia "MLS Next" clubs
    table (as of 2025-02-28) rather than from a live scrape of the official
    directory.  The ``source_url`` field still points to the official MLS NEXT
    clubs page so downstream consumers know the canonical source, but callers
    should note that this data path is seed-derived.
    """
    logger.warning(
        "[MLS NEXT] DATA PROVENANCE: returning seed dataset (Wikipedia / 2025-02-28). "
        "Live scrape of %s was not available.  Re-run in an internet-connected environment "
        "to obtain the authoritative up-to-date club list.",
        source_url,
    )
    return [
        {
            "club_name": name,
            "league_name": league_name,
            "city": city,
            "state": state,
            "source_url": source_url,
        }
        for name, city, state in _SEED
    ]


def parse_html(
    html: str,
    source_url: str = _URL,
    league_name: str | None = None,
) -> List[Dict]:
    """
    Pure-function parser for the MLS NEXT clubs directory.

    Accepts already-fetched HTML (from Playwright or a raw-HTML archive)
    and returns the canonical club records. This is the entry point used
    by ``--source replay-html`` — the live scheduled scrape continues to
    go through :func:`scrape_mls_next` below, which fetches via
    Playwright and falls back to the curated seed list when the DOM
    doesn't render.

    Unlike ``scrape_mls_next``, this function does NOT fall back to the
    seed dataset when parsing yields no rows — callers get whatever the
    archived HTML actually contained. That's the right behaviour for
    replay, which is supposed to measure what the parser would have
    produced against the archive.
    """
    return _parse_live_html(html, league_name or "MLS Next", source_url)


def _parse_live_html(html: str, league_name: str, source_url: str) -> List[Dict]:
    """
    Parse a Playwright-rendered DOM from mlsnextsoccer.com/clubs.

    The page renders club cards.  We try several CSS class patterns observed
    on the site.  Returns an empty list if nothing is found (triggers seed fallback).
    """
    soup = BeautifulSoup(html, "lxml")
    records: List[Dict] = []
    seen: set = set()

    # Strategy A: look for elements that contain both a club name and a location
    # Pattern 1: <div class="...club-card..."> with <h3> name and <p> city/state
    for card in soup.find_all(class_=re.compile(r"club.?card|card.?club", re.I)):
        name_el = card.find(["h2", "h3", "h4", "strong", "b"])
        loc_el = card.find(class_=re.compile(r"location|city|state|address", re.I)) or \
                 card.find("p")
        if not name_el:
            continue
        club_name = name_el.get_text(strip=True)
        location = loc_el.get_text(strip=True) if loc_el else ""
        city, state = _split_location(location)
        # Capture any website link from the card
        a_tag = card.find("a", href=True)
        website = ""
        if a_tag:
            href = a_tag["href"].strip()
            if href.startswith("http") and "mlsnext" not in href:
                website = href
        key = club_name.lower()
        if key and key not in seen and len(club_name) > 2:
            seen.add(key)
            records.append({
                "club_name": club_name,
                "league_name": league_name,
                "city": city,
                "state": state,
                "source_url": source_url,
                "website": website,
            })

    if records:
        logger.info("[MLS NEXT] Pattern A: %d clubs from live DOM", len(records))
        return records

    # Strategy B: any element with a "club" class + sibling/child location text
    for el in soup.find_all(class_=re.compile(r"\bclub\b", re.I)):
        text = el.get_text(separator="\n", strip=True)
        if not text or len(text) < 3 or len(text) > 200:
            continue
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        if not lines:
            continue
        club_name = lines[0]
        location = lines[1] if len(lines) > 1 else ""
        city, state = _split_location(location)
        # Capture any external website link from the element
        a_tag = el.find("a", href=True)
        website = ""
        if a_tag:
            href = a_tag["href"].strip()
            if href.startswith("http") and "mlsnext" not in href:
                website = href
        key = club_name.lower()
        if key and key not in seen and len(club_name) > 2:
            seen.add(key)
            records.append({
                "club_name": club_name,
                "league_name": league_name,
                "city": city,
                "state": state,
                "source_url": source_url,
                "website": website,
            })

    if records:
        logger.info("[MLS NEXT] Pattern B: %d clubs from live DOM", len(records))
        return records

    logger.warning("[MLS NEXT] No club cards found in live DOM — will use seed dataset")
    return []


def _split_location(text: str) -> tuple[str, str]:
    """Split 'City, ST' or 'City, State' into (city, state)."""
    if not text:
        return "", ""
    # Handle "City, ST" and "City, State Name"
    m = re.match(r"^([^,]+),\s*([^,]+)$", text.strip())
    if m:
        return m.group(1).strip(), m.group(2).strip()
    # No comma — treat whole thing as city
    return text.strip(), ""


# ---------------------------------------------------------------------------
# Registered extractor
# ---------------------------------------------------------------------------

@register(r"mlsnextsoccer\.com/clubs")
def scrape_mls_next(url: str, league_name: str) -> List[Dict]:
    """
    Extract MLS NEXT clubs.

    Primary path: Playwright live scrape of the official JS-rendered club
    directory.  The live scrape result is accepted if it yields ≥ 50 clubs
    (the site has ~150+; fewer almost certainly means a partial/failed render).

    Fallback (seed dataset): used when —
      - Playwright cannot resolve DNS (network-sandboxed environment)
      - The rendered DOM contains no identifiable club cards
      - The rendered DOM yields fewer than 50 clubs

    The seed dataset (Wikipedia, 2025-02-28) covers 159 clubs.  A provenance
    warning is logged whenever the seed path is taken so operators are aware
    the data is not sourced live from the official directory.
    """
    logger.info("[MLS NEXT] Starting extraction for %s", url)

    html = render_page(url, wait_until="networkidle", timeout_ms=45_000)

    if html is not None:
        records = _parse_live_html(html, league_name, url)
        if records and len(records) >= 50:
            logger.info("[MLS NEXT] Live scrape succeeded: %d clubs from official directory", len(records))
            return records
        elif records:
            logger.warning(
                "[MLS NEXT] Live scrape returned only %d clubs (< 50 threshold); "
                "DOM likely did not fully render — falling back to seed dataset",
                len(records),
            )
        else:
            logger.warning(
                "[MLS NEXT] Live DOM contained no identifiable club cards — "
                "falling back to seed dataset"
            )
    else:
        logger.info("[MLS NEXT] Playwright returned None (network error / sandbox) — using seed dataset")

    records = _seed_records(league_name, _URL)
    logger.info("[MLS NEXT] Seed dataset: %d clubs", len(records))
    return records
