"""
enrich_clubs.py — Batch-enrich ECNL/Pre-ECNL club records with city + state
data from the AthleteOne get-club-info endpoint.

Usage:
    python enrich_clubs.py [--dry-run] [--leagues ecnl-boys,ecnl-girls] [--out output/clubs_enriched.csv]

How it works:
  1. Reads all per-league team CSVs under output/teams/ that have non-empty
     club_id and event_id columns (only ECNL-family leagues populate these).
  2. De-dupes by (club_id, event_id) — one API call per unique club.
  3. Calls api.athleteone.com/api/Script/get-club-info/{event_id}/{club_id}/12
     to get the club's display name and mailing address.
  4. Parses city and state from the address line ("West Linn, Oregon 97068").
  5. Writes output/clubs_enriched.csv with columns:
       club_id, club_name_official, club_name_scraped, city, state,
       address_line1, address_zip, event_id, league_name, source_url

The enriched file is intentionally separate from master.csv so it can be
merged/reviewed independently before updating the canonical club graph.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://theecnl.com/",
    "Accept": "*/*",
}

_BASE = "https://api.athleteone.com/api/Script/get-club-info"
_ORG_ID = 12

TEAMS_DIR = os.path.join(os.path.dirname(__file__), "output", "teams")
OUT_DEFAULT = os.path.join(os.path.dirname(__file__), "output", "clubs_enriched.csv")

ENRICHED_COLUMNS = [
    "club_id", "club_name_official", "club_name_scraped",
    "address_line1", "city", "state", "zip",
    "website",
    "event_id", "league_name", "api_url",
]


# ---------------------------------------------------------------------------
# Address parsing
# ---------------------------------------------------------------------------

_CITY_STATE_ZIP_RE = re.compile(
    r"^(?P<city>.+?),\s*(?P<state>[A-Za-z ]+?)\s+(?P<zip>\d{5}(?:-\d{4})?)$"
)


def _parse_address(divs: List[str]) -> Dict[str, str]:
    """
    Extract address fields from the list of <div> text nodes returned by
    get-club-info.  Typical format:
        divs[0] = "Oregon Premier FC"      ← official club name
        divs[1] = "19995 SW Stafford Road Suite C"  ← address line 1
        divs[2] = "West Linn, Oregon 97068"           ← city/state/zip

    Returns dict with keys: city, state, zip, address_line1
    """
    result = {"city": "", "state": "", "zip": "", "address_line1": ""}

    for div in divs[1:4]:
        m = _CITY_STATE_ZIP_RE.match(div.strip())
        if m:
            result["city"] = m.group("city").strip().title()
            result["state"] = m.group("state").strip().title()
            result["zip"] = m.group("zip").strip()
        elif result["address_line1"] == "" and div.strip() and not div[0].isdigit() is False:
            result["address_line1"] = div.strip()

    # address_line1 is the first non-city-state-zip non-name div
    if not result["address_line1"] and len(divs) > 1:
        result["address_line1"] = divs[1].strip()

    return result


# ---------------------------------------------------------------------------
# API fetch
# ---------------------------------------------------------------------------

def fetch_club_info(event_id: str, club_id: str) -> Optional[Dict]:
    """
    Call get-club-info and return a dict with official name + address fields,
    or None on failure.
    """
    url = f"{_BASE}/{event_id}/{club_id}/{_ORG_ID}"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 50:
            logger.debug("get-club-info %s/%s: status=%d", event_id, club_id, r.status_code)
            return None
    except Exception as exc:
        logger.debug("get-club-info %s/%s error: %s", event_id, club_id, exc)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    divs = [d.get_text(strip=True) for d in soup.find_all("div") if d.get_text(strip=True)]

    if not divs:
        return None

    official_name = divs[0].strip()
    addr = _parse_address(divs)

    # Extract any external website link from the response (AthleteOne may embed
    # the club's website as an <a href> pointing outside athleteone.com).
    website = ""
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        if href.startswith("http") and "athleteone" not in href and "athlete-one" not in href:
            website = href
            break

    return {
        "club_name_official": official_name,
        "address_line1": addr["address_line1"],
        "city": addr["city"],
        "state": addr["state"],
        "zip": addr["zip"],
        "website": website,
        "api_url": url,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_club_id_pairs(league_filter: Optional[List[str]] = None) -> List[Dict]:
    """
    Read all team CSVs under TEAMS_DIR, collect rows that have club_id + event_id.
    Return one representative row per unique (club_id, event_id) pair.
    """
    if not os.path.isdir(TEAMS_DIR):
        logger.error("Teams directory not found: %s", TEAMS_DIR)
        return []

    rows: Dict[Tuple[str, str], Dict] = {}
    for fname in sorted(os.listdir(TEAMS_DIR)):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(TEAMS_DIR, fname)
        df = pd.read_csv(path, dtype=str).fillna("")
        if "club_id" not in df.columns or "event_id" not in df.columns:
            continue
        df = df[df["club_id"].str.strip() != ""]

        if league_filter:
            league_col = df.get("league_name", pd.Series(dtype=str))
            mask = league_col.str.lower().apply(
                lambda x: any(f.lower() in x for f in league_filter)
            )
            df = df[mask]

        for _, row in df.iterrows():
            key = (row["club_id"].strip(), row["event_id"].strip())
            if key not in rows:
                rows[key] = {
                    "club_id": key[0],
                    "event_id": key[1],
                    "club_name_scraped": row.get("club_name", ""),
                    "league_name": row.get("league_name", ""),
                }

    logger.info("Loaded %d unique (club_id, event_id) pairs from %s", len(rows), TEAMS_DIR)
    return list(rows.values())


def enrich(pairs: List[Dict], dry_run: bool = False, max_workers: int = 10) -> pd.DataFrame:
    """
    Batch-fetch get-club-info for every pair and return enriched DataFrame.
    """
    if dry_run:
        logger.info("[dry-run] Would fetch %d club-info records", len(pairs))
        for p in pairs[:5]:
            logger.info("  event_id=%s club_id=%s (%s)", p["event_id"], p["club_id"], p["club_name_scraped"])
        return pd.DataFrame()

    results: List[Dict] = []

    def _fetch_one(pair: Dict) -> Optional[Dict]:
        info = fetch_club_info(pair["event_id"], pair["club_id"])
        time.sleep(0.05)  # polite rate limit
        if info is None:
            return None
        return {**pair, **info}

    logger.info("Fetching club info for %d clubs (max_workers=%d)...", len(pairs), max_workers)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_fetch_one, p): p for p in pairs}
        done = 0
        for f in as_completed(futs):
            done += 1
            result = f.result()
            if result:
                results.append(result)
            if done % 50 == 0 or done == len(pairs):
                logger.info("  Progress: %d / %d (ok=%d)", done, len(pairs), len(results))

    df = pd.DataFrame(results)
    if df.empty:
        return df

    for col in ENRICHED_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[ENRICHED_COLUMNS]


def main():
    parser = argparse.ArgumentParser(description="Enrich ECNL club records with city/state from AthleteOne")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be fetched, no HTTP calls")
    parser.add_argument("--leagues", default="", help="Comma-separated league name substrings to filter (default: all)")
    parser.add_argument("--out", default=OUT_DEFAULT, help=f"Output CSV path (default: {OUT_DEFAULT})")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent HTTP workers (default: 10)")
    args = parser.parse_args()

    league_filter = [x.strip() for x in args.leagues.split(",") if x.strip()] or None

    pairs = load_club_id_pairs(league_filter)
    if not pairs:
        logger.error("No club_id/event_id pairs found. Run the ECNL scraper first.")
        sys.exit(1)

    df = enrich(pairs, dry_run=args.dry_run, max_workers=args.workers)

    if args.dry_run or df.empty:
        return

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    df.to_csv(args.out, index=False)
    logger.info("Wrote %d enriched club records to %s", len(df), args.out)

    # Summary stats
    cities = df["city"].replace("", pd.NA).dropna()
    logger.info("City coverage: %d / %d (%.0f%%)", len(cities), len(df), 100 * len(cities) / max(len(df), 1))
    if len(cities):
        top_states = df["state"].value_counts().head(10)
        logger.info("Top states:\n%s", top_states.to_string())


if __name__ == "__main__":
    main()
