"""
Configuration for the Upshift Data scraper.

Leagues are loaded dynamically from the seed CSV files in data/:
  - data/leagues_master.csv            — full league inventory
  - data/usys_state_associations_seed.csv — 54 USYS state associations (adds region hint)

Only leagues with has_public_clubs = True are loaded.

Each entry in LEAGUES contains:
  name            — display name
  url             — page to scrape
  js_required     — whether headless browser is needed
  state           — default state string (for state-association entries)
  tier            — numeric tier (1=national elite, 4=state hub)
  priority        — 'high' | 'medium' | 'low'
  gender          — 'boys' | 'girls' | 'boys_and_girls'
  geographic_scope — 'national' | 'regional' | 'state'
  league_family   — governing ecosystem label
  governing_body  — umbrella org
  notes           — original notes from CSV
"""

from __future__ import annotations

import os
import csv
from typing import List, Dict

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_DIR = "output"
LEAGUES_DIR = "output/leagues"
MASTER_CSV = "output/master.csv"

# ---------------------------------------------------------------------------
# Scraping settings
# ---------------------------------------------------------------------------

FUZZY_THRESHOLD = 88          # RapidFuzz similarity score (0–100) to consider clubs identical
PLAYWRIGHT_TIMEOUT = 30_000   # ms to wait for JS pages to settle
PLAYWRIGHT_WAIT_FOR = "networkidle"

# ---------------------------------------------------------------------------
# Retry / backoff settings
# ---------------------------------------------------------------------------

MAX_RETRIES = 3                  # Maximum number of retry attempts after first failure
RETRY_BASE_DELAY_SECONDS = 2.0   # Seconds before first retry; doubles each subsequent attempt

# Source types that require a headless browser (JS-rendered content)
_JS_SOURCE_TYPES = {
    "club_directory",
    "directory",
    "members",
    "membership",
    "homepage",
    "league_page",
    "program",
    "official_site",
    "official_directory",
    "official_league_page",
    "official_program_page",
    "official_competition_page",
}

# Source types that are typically static HTML (no JS needed)
_STATIC_SOURCE_TYPES = {
    "state_association_hub",
    "news",
    "official_org_page",
    "staff_directory",
}


def _is_js_required(source_type: str) -> bool:
    st = source_type.strip().lower()
    if st in _STATIC_SOURCE_TYPES:
        return False
    return True  # Default to JS for unknown/JS types


# ---------------------------------------------------------------------------
# Load USYS state associations for region metadata
# ---------------------------------------------------------------------------

def _load_state_region_map() -> Dict[str, str]:
    """Return {association_name: state_or_region} from usys_state_associations_seed.csv."""
    path = os.path.join(_DATA_DIR, "usys_state_associations_seed.csv")
    mapping: Dict[str, str] = {}
    if not os.path.exists(path):
        return mapping
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            assoc = row.get("association_name", "").strip()
            region = row.get("state_or_region", "").strip()
            if assoc and region:
                mapping[assoc] = region
    return mapping


# ---------------------------------------------------------------------------
# Load LEAGUES from leagues_master.csv
# ---------------------------------------------------------------------------

def _load_leagues() -> List[Dict]:
    path = os.path.join(_DATA_DIR, "leagues_master.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Seed file not found: {path}")

    state_map = _load_state_region_map()
    leagues: List[Dict] = []
    seen_keys: set = set()

    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            # Skip leagues that don't expose a public club directory
            if row.get("has_public_clubs", "False").strip() != "True":
                continue

            url = row.get("official_url", "").strip()
            if not url:
                continue

            name = row.get("league_name", "").strip()

            # Deduplicate on (url, name) — same URL is allowed for distinct leagues
            # (e.g. Pre-ECNL Boys/Girls share the same directory page but are different products)
            key = (url, name)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            source_type = row.get("source_type", "").strip()

            # For USYS state associations, look up the region abbreviation
            state_region = state_map.get(name, "")

            leagues.append({
                "name": name,
                "url": url,
                "js_required": _is_js_required(source_type),
                "state": state_region,          # e.g. "Alabama", "Cal North"
                "tier": _safe_int(row.get("tier_numeric", "")),
                "priority": row.get("scrape_priority", "medium").strip(),
                "gender": row.get("gender", "").strip(),
                "geographic_scope": row.get("geographic_scope", "").strip(),
                "league_family": row.get("league_family", "").strip(),
                "governing_body": row.get("governing_body", "").strip(),
                "source_type": source_type,
                "notes": row.get("notes", "").strip(),
            })

    return leagues


def _safe_int(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 99


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

LEAGUES: List[Dict] = _load_leagues()


def get_leagues(
    priority: str | None = None,
    tier: int | None = None,
    gender: str | None = None,
    scope: str | None = None,
) -> List[Dict]:
    """
    Return a filtered subset of LEAGUES.

    Parameters
    ----------
    priority : 'high' | 'medium' | 'low' | None  — filter by scrape_priority
    tier     : 1 | 2 | 3 | 4 | None              — filter by tier_numeric
    gender   : 'boys' | 'girls' | 'boys_and_girls' | None
    scope    : 'national' | 'regional' | 'state' | None
    """
    result = LEAGUES
    if priority:
        result = [lg for lg in result if lg["priority"] == priority]
    if tier is not None:
        result = [lg for lg in result if lg["tier"] == tier]
    if gender:
        result = [lg for lg in result if gender in lg["gender"]]
    if scope:
        result = [lg for lg in result if lg["geographic_scope"] == scope]
    return result
