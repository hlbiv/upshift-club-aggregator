"""
merge_websites.py — Merge discovered websites back into master.csv.

Reads website data from two sources:
  1. output/website_enrichment_progress.json (Brave Search results)
  2. output/clubs_enriched.csv (ECNL AthleteOne API results)

Matches by club name (case-insensitive) and updates the website column
in master.csv. Does not overwrite existing websites.

Usage:
    python merge_websites.py [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
MASTER_PATH = os.path.join(OUTPUT_DIR, "master.csv")
BRAVE_PATH = os.path.join(OUTPUT_DIR, "website_enrichment_progress.json")
ENRICHED_PATH = os.path.join(OUTPUT_DIR, "clubs_enriched.csv")


def _load_brave_websites() -> dict[str, str]:
    """Load Brave Search results keyed by club name (lowercase)."""
    if not os.path.exists(BRAVE_PATH):
        logger.warning("Brave checkpoint not found: %s", BRAVE_PATH)
        return {}

    with open(BRAVE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    processed = data.get("processed", {})
    websites: dict[str, str] = {}

    # Brave checkpoint keys are club names (when run in CSV mode)
    for key, val in processed.items():
        if val.get("status") == "found" and val.get("website"):
            websites[key.lower().strip()] = val["website"]

    logger.info("Brave Search: %d websites loaded", len(websites))
    return websites


def _load_enriched_websites() -> dict[str, str]:
    """Load ECNL enriched club websites keyed by club name (lowercase)."""
    if not os.path.exists(ENRICHED_PATH):
        logger.warning("Enriched CSV not found: %s", ENRICHED_PATH)
        return {}

    df = pd.read_csv(ENRICHED_PATH, dtype=str).fillna("")
    websites: dict[str, str] = {}

    for _, row in df.iterrows():
        website = row.get("website", "").strip()
        if not website:
            continue

        # Try official name first, then scraped name
        for name_col in ("club_name_official", "club_name_scraped"):
            name = row.get(name_col, "").strip()
            if name:
                websites[name.lower()] = website

    logger.info("ECNL enriched: %d websites loaded", len(websites))
    return websites


def main():
    parser = argparse.ArgumentParser(description="Merge discovered websites into master.csv")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    if not os.path.exists(MASTER_PATH):
        logger.error("master.csv not found: %s", MASTER_PATH)
        sys.exit(1)

    # Load master
    df = pd.read_csv(MASTER_PATH, dtype=str).fillna("")
    if "website" not in df.columns:
        df["website"] = ""
    if "website_source" not in df.columns:
        df["website_source"] = ""

    total_before = (df["website"].str.strip() != "").sum()
    logger.info("master.csv: %d clubs, %d already have websites", len(df), total_before)

    # Load website sources
    brave_websites = _load_brave_websites()
    enriched_websites = _load_enriched_websites()

    # Merge — don't overwrite existing websites
    updated_brave = 0
    updated_enriched = 0

    for idx, row in df.iterrows():
        if row["website"].strip():
            continue  # Already has a website

        club_name = row.get("club_name", "").strip().lower()
        canonical = row.get("canonical_name", "").strip().lower()

        # Try enriched (ECNL) first — higher confidence
        for name in (club_name, canonical):
            if name and name in enriched_websites:
                df.at[idx, "website"] = enriched_websites[name]
                df.at[idx, "website_source"] = "ecnl_directory"
                updated_enriched += 1
                break
        else:
            # Try Brave Search
            for name in (club_name, canonical):
                if name and name in brave_websites:
                    df.at[idx, "website"] = brave_websites[name]
                    df.at[idx, "website_source"] = "brave_search"
                    updated_brave += 1
                    break

    total_after = (df["website"].str.strip() != "").sum()

    print("\n" + "=" * 60)
    print("WEBSITE MERGE SUMMARY")
    print("=" * 60)
    print(f"  Total clubs in master.csv:   {len(df)}")
    print(f"  Websites before merge:       {total_before}")
    print(f"  Added from ECNL enrichment:  {updated_enriched}")
    print(f"  Added from Brave Search:     {updated_brave}")
    print(f"  Total added:                 {updated_enriched + updated_brave}")
    print(f"  Websites after merge:        {total_after}")
    print(f"  Coverage:                    {100 * total_after / len(df):.1f}%")
    print(f"  Still missing:               {len(df) - total_after}")
    print("=" * 60)

    if args.dry_run:
        print("\n[dry-run] No changes written.")
    else:
        df.to_csv(MASTER_PATH, index=False)
        logger.info("master.csv updated: %s", MASTER_PATH)


if __name__ == "__main__":
    main()
