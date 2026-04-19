"""
SoccerWire soccer club directory extractor.

SoccerWire (soccerwire.com) maintains a WordPress-based youth soccer club
directory with 1,000+ clubs across the US. The site uses a client-side
JavaScript filter, so state filtering requires fetching individual club pages.

SCRAPING STRATEGY:
  1. Fetch all club slugs via WP REST API (paginated, 100/page; ~11 calls).
  2. Filter slugs to candidates for the target state using a keyword map
     (city names, state names common in that state's club slugs).
  3. Fetch individual club pages in parallel; extract:
       - Location: "City, ST, United States"
       - Elite Youth League Memberships: [...]
  4. Return clubs whose state field matches the target.

The slug-level keyword filter dramatically reduces page fetches — a full
statewide run typically requires only 5–25 individual page fetches rather
than all 1,000+ clubs.

COVERAGE (verified April 2026):
  HI: Kona Crush Academy, ALBION SC Hawaii, Honolulu Bulls, Hawaii Surf,
      Hawaii Rush, Hawaii Rush Big Island
  LA: Louisiana TDP Elite, Louisiana Fire SC, Baton Rouge SC
  MA: GPS Massachusetts, FCUSA Massachusetts, Boston Bolts
  MS: Tupelo FC, Mississippi Rush United, Mississippi Rush
  NE: Omaha United SC, Villarreal Nebraska Academy, Sporting Nebraska,
      Omaha FC, Nebraska Select
  RI: Rhode Island Surf
  SC: South Carolina Surf, South Carolina United FC
  WI: Rush Wisconsin, Waukesha SC, Madison 56ers, FC Wisconsin Eclipse

NOTES:
  - ND and SD have zero clubs in the SoccerWire directory (April 2026).
  - US Club Soccer does not publish a standalone member club directory;
    US Club Soccer–affiliated clubs on SoccerWire are identified via the
    'Memberships' field (e.g. South Carolina United FC shows 'U.S. Club Soccer').
  - AYSO recreational regions are not listed on SoccerWire. The competitive
    AYSO United program (ayso.org/ayso-united/) does appear via individual
    club pages where teams label themselves "AYSO United [Region]".
"""

from __future__ import annotations

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_API_BASE = "https://www.soccerwire.com/wp-json/wp/v2/clubs"
_CLUB_URL_TPL = "https://www.soccerwire.com/club/{slug}/"
_PAGE_SIZE = 100
_CACHE_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "soccerwire_slugs_cache.json")

# Per-state keyword lists for slug-level pre-filtering.
# Each keyword is a lowercase substring to look for in the club's slug.
# Over-inclusive is fine — we verify state from the individual page.
_STATE_SLUG_KEYWORDS: Dict[str, List[str]] = {
    "AL": ["alabama", "huntsville", "birmingham", "mobile", "montgomery", "tuscaloosa", "decatur", "hoover"],
    "AK": ["alaska", "anchorage", "fairbanks", "juneau", "ketchikan", "sitka"],
    "AZ": ["arizona", "phoenix", "tucson", "mesa", "scottsdale", "tempe", "chandler", "gilbert", "glendale", "peoria"],
    "AR": ["arkansas", "little-rock", "fayetteville", "springdale", "jonesboro", "fort-smith"],
    "CA": ["california", "los-angeles", "san-diego", "san-francisco", "sacramento", "fresno", "san-jose", "bay-area", "norcal", "socal"],
    "CO": ["colorado", "denver", "boulder", "fort-collins", "colorado-springs", "aurora", "lakewood"],
    "CT": ["connecticut", "hartford", "bridgeport", "new-haven", "stamford", "waterbury"],
    "DE": ["delaware", "wilmington", "dover", "newark"],
    "FL": ["florida", "miami", "orlando", "tampa", "jacksonville", "fort-lauderdale", "boca-raton", "palm-beach"],
    "GA": ["georgia", "atlanta", "savannah", "columbus", "macon", "augusta", "athens"],
    "HI": ["hawaii", "honolulu", "maui", "aloha", "hilo", "kailua", "kona", "big-island", "oahu"],
    "ID": ["idaho", "boise", "nampa", "meridian", "pocatello", "idaho-falls"],
    "IL": ["illinois", "chicago", "aurora", "naperville", "rockford", "joliet", "waukegan", "elgin", "elmhurst", "wheaton"],
    "IN": ["indiana", "indianapolis", "fort-wayne", "evansville", "south-bend", "carmel"],
    "IA": ["iowa", "des-moines", "cedar-rapids", "davenport", "sioux-city", "iowa-city", "waterloo", "ames"],
    "KS": ["kansas", "wichita", "overland-park", "kansas-city", "topeka", "olathe"],
    "KY": ["kentucky", "louisville", "lexington", "owensboro", "bowling-green"],
    "LA": ["louisiana", "new-orleans", "baton-rouge", "shreveport", "lafayette", "acadiana", "lake-charles", "kenner"],
    "ME": ["maine", "portland", "lewiston", "auburn", "bangor"],
    "MD": ["maryland", "baltimore", "frederick", "bethesda", "rockville", "silver-spring", "annapolis"],
    "MA": ["massachusetts", "boston", "worcester", "springfield", "lowell", "cambridge", "newton", "brookline", "needham", "wellesley", "waltham", "plymouth"],
    "MI": ["michigan", "detroit", "grand-rapids", "lansing", "ann-arbor", "flint", "kalamazoo"],
    "MN": ["minnesota", "minneapolis", "saint-paul", "duluth", "rochester", "bloomington", "edina", "maple-grove"],
    "MS": ["mississippi", "jackson", "gulfport", "hattiesburg", "biloxi", "meridian", "tupelo", "southaven", "madison"],
    "MO": ["missouri", "st-louis", "kansas-city", "columbia-mo", "independence", "lee-s-summit"],
    "MT": ["montana", "billings", "missoula", "great-falls", "bozeman", "butte"],
    "NE": ["nebraska", "omaha", "lincoln-ne", "bellevue", "grand-island", "kearney", "fremont", "lincoln"],
    "NV": ["nevada", "las-vegas", "henderson", "reno", "sparks", "north-las-vegas"],
    "NH": ["new-hampshire", "manchester-nh", "nashua", "concord-nh"],
    "NJ": ["new-jersey", "newark", "jersey-city", "paterson", "trenton", "camden", "princeton"],
    "NM": ["new-mexico", "albuquerque", "santa-fe", "las-cruces", "rio-rancho"],
    "NY": ["new-york", "brooklyn", "bronx", "queens", "buffalo", "rochester-ny", "yonkers", "albany"],
    "NC": ["north-carolina", "charlotte", "raleigh", "greensboro", "durham", "winston-salem", "fayetteville", "cary", "wilmington-nc"],
    "ND": ["north-dakota", "fargo", "bismarck", "grand-forks", "minot"],
    "OH": ["ohio", "columbus-oh", "cleveland", "cincinnati", "toledo", "akron", "dayton"],
    "OK": ["oklahoma", "oklahoma-city", "tulsa", "norman", "broken-arrow", "lawton"],
    "OR": ["oregon", "portland-or", "salem-or", "eugene", "gresham", "hillsboro", "beaverton", "bend"],
    "PA": ["pennsylvania", "philadelphia", "pittsburgh", "allentown", "erie-pa"],
    "RI": ["rhode-island", "providence", "cranston", "warwick", "pawtucket", "newport"],
    "SC": ["south-carolina", "columbia-sc", "charleston", "greenville-sc", "spartanburg", "myrtle-beach", "palmetto"],
    "SD": ["south-dakota", "sioux-falls", "rapid-city", "aberdeen-sd", "brookings", "watertown"],
    "TN": ["tennessee", "nashville", "memphis", "knoxville", "chattanooga", "clarksville", "murfreesboro"],
    "TX": ["texas", "houston", "dallas", "san-antonio", "austin", "fort-worth", "el-paso", "plano", "frisco", "mckinney"],
    "UT": ["utah", "salt-lake", "provo", "ogden", "orem", "st-george", "logan", "sandy", "west-jordan", "layton", "la-roca"],
    "VT": ["vermont", "burlington-vt", "south-burlington", "colchester", "essex-junction"],
    "VA": ["virginia", "richmond", "norfolk", "chesapeake", "arlington", "virginia-beach", "roanoke", "alexandria"],
    "WA": ["washington", "seattle", "spokane", "tacoma", "bellevue-wa", "olympia", "bothell"],
    "WV": ["west-virginia", "charleston-wv", "huntington", "morgantown", "parkersburg"],
    "WI": ["wisconsin", "milwaukee", "madison-wi", "green-bay", "racine", "kenosha", "appleton", "waukesha", "oshkosh", "eau-claire", "madison-5", "rush-wi", "fc-wi"],
    "WY": ["wyoming", "cheyenne", "casper", "laramie", "gillette"],
}

# US state abbreviation to full name
_STATE_FULL: Dict[str, str] = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming",
}

_FULL_TO_ABBR: Dict[str, str] = {v.lower(): k for k, v in _STATE_FULL.items()}


def _state_abbr(state_name: str) -> str:
    """Convert a full state name to a 2-letter abbreviation, or return as-is if already 2 letters."""
    if len(state_name) == 2 and state_name.isupper():
        return state_name
    return _FULL_TO_ABBR.get(state_name.lower(), "")


def _fetch_all_slugs() -> List[str]:
    """
    Fetch all club slugs from the SoccerWire WP REST API.

    Checks for a cached slug list at _CACHE_PATH first (written by this
    function on the first successful run). The cache avoids re-fetching
    the same ~1,100 slugs on every scraper invocation.

    Returns a list of slug strings.
    """
    if os.path.exists(_CACHE_PATH):
        try:
            with open(_CACHE_PATH) as f:
                cache = json.load(f)
                if isinstance(cache, list) and cache:
                    logger.debug("[SoccerWire] Using slug cache (%d slugs)", len(cache))
                    return cache
        except (json.JSONDecodeError, OSError):
            pass

    logger.info("[SoccerWire] Fetching club slug list from WP REST API...")
    slugs: List[str] = []
    page = 1
    while True:
        url = f"{_API_BASE}?per_page={_PAGE_SIZE}&page={page}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            for club in data:
                slugs.append(club["slug"])
            total_pages = int(r.headers.get("X-WP-TotalPages", 1))
            logger.debug("[SoccerWire] Fetched page %d/%d (%d slugs so far)", page, total_pages, len(slugs))
            if page >= total_pages:
                break
            page += 1
        except requests.RequestException as exc:
            logger.warning("[SoccerWire] Failed to fetch slug page %d: %s", page, exc)
            break

    if slugs:
        try:
            os.makedirs(os.path.dirname(_CACHE_PATH), exist_ok=True)
            with open(_CACHE_PATH, "w") as f:
                json.dump(slugs, f)
            logger.info("[SoccerWire] Cached %d slugs to %s", len(slugs), _CACHE_PATH)
        except OSError as exc:
            logger.warning("[SoccerWire] Could not write slug cache: %s", exc)

    return slugs


def _candidate_slugs(slugs: List[str], state_abbr: str) -> List[str]:
    """Return slugs that likely belong to `state_abbr` based on keyword matching."""
    keywords = _STATE_SLUG_KEYWORDS.get(state_abbr, [])
    if not keywords:
        return slugs  # No keyword map → fetch all (slow but correct)
    return [s for s in slugs if any(kw in s for kw in keywords)]


def _parse_club_page_html(html: str, slug: str, url: str) -> Optional[Dict]:
    """
    Pure-function parse of a SoccerWire club detail page.

    Extracts club name, location (city/state), and membership tags from
    the page HTML. Returns ``None`` when the page has no usable data.
    """
    soup = BeautifulSoup(html, "lxml")
    main = soup.find("main") or soup.body
    text = main.get_text(separator="\n") if main else ""

    # Extract Location field: "Location:\nCity, ST, Country"
    loc_m = re.search(r"Location:\s*\n?\s*([^\n]+)", text)
    location = loc_m.group(1).strip() if loc_m else ""
    state_code = ""
    city = ""
    if location:
        parts = [p.strip() for p in location.split(",")]
        for i, p in enumerate(parts):
            if len(p) == 2 and p.isupper() and p.isalpha() and p in _STATE_FULL:
                state_code = p
                city = parts[i - 1].strip() if i > 0 else ""
                break

    h1 = soup.find("h1")
    name = h1.get_text(strip=True) if h1 else slug.replace("-", " ").title()
    # Unescape HTML entities (e.g. "–" for "–")
    name = BeautifulSoup(name, "lxml").get_text()

    # Extract membership tags (if any)
    mem_m = re.findall(
        r"Memberships?:\s*\n?((?:[^\n]+\n?)+?)(?=\n\n|\nMain website|\nFEATURE|\Z)",
        text,
    )
    memberships: List[str] = []
    if mem_m:
        for raw in re.split(r"[\n,]", mem_m[0]):
            m = raw.strip()
            if m and "membership" not in m.lower() and not m.startswith("FEATURE") and len(m) > 2:
                memberships.append(m)

    return {
        "slug": slug,
        "club_name": name,
        "state": state_code,
        "city": city,
        "location": location,
        "memberships": memberships,
        "source_url": url,
    }


def _slug_from_url(url: str) -> str:
    """Best-effort slug extraction from a SoccerWire club URL."""
    m = re.search(r"/club/([^/?#]+)", url)
    return m.group(1) if m else ""


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser exposed to --source replay-html.

    Given the HTML of a single SoccerWire club detail page
    (``soccerwire.com/club/<slug>/``), return a list containing one
    normaliser-shaped club record. Returns ``[]`` when the page carries
    no usable data (e.g. redirect stub).

    The live :func:`scrape_soccerwire` path also walks the WP REST API to
    build a slug list; that index step has no persistent HTML and is
    not replayable. Individual club-page HTML is, which is what this
    parser targets.
    """
    slug = _slug_from_url(source_url) or "unknown"
    rec = _parse_club_page_html(html, slug, source_url)
    if not rec or not rec.get("club_name"):
        return []
    return [{
        "club_name":   rec["club_name"],
        "league_name": league_name,
        "city":        rec["city"],
        "state":       rec["state"],
        "source_url":  rec["source_url"],
        "source_type": "soccerwire",
    }]


def _fetch_club_page(slug: str) -> Optional[Dict]:
    """
    Fetch a single SoccerWire club page and return a parsed club record,
    or None if the page does not exist or has no usable data.
    """
    url = _CLUB_URL_TPL.format(slug=slug)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code == 404:
            return None
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.debug("[SoccerWire] Fetch failed for %s: %s", slug, exc)
        return None

    return _parse_club_page_html(r.text, slug, url)


def scrape_soccerwire_state(
    state: str,
    league_name: str,
    max_workers: int = 15,
) -> List[Dict]:
    """
    Return all SoccerWire clubs for a given US state.

    Args:
        state:       US state name (e.g. "Hawaii") or 2-letter code (e.g. "HI").
        league_name: League name to attach to each returned record.
        max_workers: Number of parallel page-fetch workers.

    Returns:
        List of club dicts ready for the normalizer.
    """
    target_abbr = _state_abbr(state)
    if not target_abbr:
        logger.warning("[SoccerWire] Could not resolve state '%s' to abbreviation — skipping", state)
        return []

    logger.info("[SoccerWire] Fetching clubs for state=%s (%s)", target_abbr, state)

    all_slugs = _fetch_all_slugs()
    if not all_slugs:
        logger.warning("[SoccerWire] No slugs fetched — check API availability")
        return []

    candidates = _candidate_slugs(all_slugs, target_abbr)
    logger.info(
        "[SoccerWire] %d candidate slugs for %s (from %d total)",
        len(candidates), target_abbr, len(all_slugs),
    )

    if not candidates:
        logger.info("[SoccerWire] No slug-keyword candidates for %s", target_abbr)
        return []

    records: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_fetch_club_page, slug): slug for slug in candidates}
        for fut in as_completed(futs):
            result = fut.result()
            if result and result["state"] == target_abbr and result["club_name"]:
                records.append({
                    "club_name":   result["club_name"],
                    "league_name": league_name,
                    "city":        result["city"],
                    "state":       result["state"],
                    "source_url":  result["source_url"],
                    "source_type": "soccerwire",
                })

    logger.info("[SoccerWire] %s → %d verified clubs", target_abbr, len(records))
    return records


@register(r"soccerwire\.com")
def scrape_soccerwire(url: str, league_name: str) -> List[Dict]:
    """
    Extractor for SoccerWire league entries.

    When called via the state_assoc route, the state is read from the
    state_assoc_config.json entry (which sets the league 'state' key via
    state_assoc.py). When called directly from a leagues_master.csv entry,
    all clubs across all states are returned (slow — use state-filtered
    paths instead).
    """
    # The caller may embed ?state=XX in the URL for targeted scraping
    state_m = re.search(r"[?&]state=([A-Z]{2})", url)
    if state_m:
        state = state_m.group(1)
    else:
        logger.warning(
            "[SoccerWire] No state filter in URL '%s'. "
            "Use the state_assoc config for state-targeted scraping. "
            "Returning 0 clubs to avoid a full 1000+ page crawl.",
            url,
        )
        return []

    return scrape_soccerwire_state(state, league_name)
