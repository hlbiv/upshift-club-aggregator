"""
Upshift Club Aggregator — main entry point.

Usage:
    python run.py                 # scrape all leagues in config.py
    python run.py --league "AYSO" # scrape a single named league
    python run.py --dry-run       # print summary without writing files
"""

from __future__ import annotations

import argparse
import logging
import sys
import os

import pandas as pd

# Allow importing sibling modules when running from the scraper/ directory
sys.path.insert(0, os.path.dirname(__file__))

from config import LEAGUES
from scraper_static import scrape_static
from scraper_js import scrape_js
from normalizer import normalize, deduplicate
from storage import save_league_csv, append_to_master

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run")


def scrape_league(league: dict, dry_run: bool = False) -> pd.DataFrame:
    """Scrape, normalise, and deduplicate clubs for a single league."""
    name = league["name"]
    url = league["url"]
    logger.info("=" * 60)
    logger.info("Processing league: %s", name)

    # Step 1 & 2 — scrape
    if league.get("js_required"):
        raw = scrape_js(url, name)
    else:
        raw = scrape_static(url, name)

    if not raw:
        logger.warning("No clubs found for league: %s", name)
        return pd.DataFrame()

    df = pd.DataFrame(raw)

    # Inject default state if the page didn't provide it
    if league.get("state") and "state" in df.columns:
        df["state"] = df["state"].where(df["state"].str.strip() != "", league["state"])

    # Step 3 — normalize
    df = normalize(df)

    # Step 4 — deduplicate within this league
    df = deduplicate(df)

    logger.info("League '%s': %d clubs after dedup", name, len(df))

    # Step 5 — save per-league CSV
    if not dry_run:
        path = save_league_csv(df, name)
        logger.info("Saved: %s", path)
    else:
        logger.info("[dry-run] Would save %d clubs for league '%s'", len(df), name)

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Upshift Club Aggregator")
    parser.add_argument(
        "--league",
        metavar="NAME",
        help="Scrape only the league matching this name (partial, case-insensitive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run scraper but do not write any files",
    )
    args = parser.parse_args()

    target_leagues = LEAGUES
    if args.league:
        target_leagues = [
            lg for lg in LEAGUES if args.league.lower() in lg["name"].lower()
        ]
        if not target_leagues:
            logger.error("No league found matching: %s", args.league)
            sys.exit(1)

    all_frames = []
    for league in target_leagues:
        df = scrape_league(league, dry_run=args.dry_run)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        logger.warning("No data collected.")
        return

    # Step 4 — cross-league deduplication in master dataset
    master = pd.concat(all_frames, ignore_index=True)
    master = deduplicate(master)

    if not args.dry_run:
        path = append_to_master(master)
        logger.info("Master dataset saved: %s (%d clubs)", path, len(master))
    else:
        logger.info("[dry-run] Master dataset would contain %d clubs", len(master))

    # Summary
    print("\n" + "=" * 60)
    print(f"  Total clubs collected : {len(master)}")
    print(f"  Leagues processed     : {len(target_leagues)}")
    if not args.dry_run:
        print(f"  Output directory      : output/")
    print("=" * 60)


if __name__ == "__main__":
    main()
