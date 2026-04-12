"""
merge_websites.py — Merge discovered websites back into master.csv.

Reads website data from three sources (in priority order):
  1. PostgreSQL canonical_clubs table (if DATABASE_URL is set)
  2. output/clubs_enriched.csv (ECNL AthleteOne API results)
  3. output/website_enrichment_progress.json (Brave Search results,
     keyed by DB id — resolved to club names via the DB)

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

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
MASTER_PATH = os.path.join(OUTPUT_DIR, "master.csv")
BRAVE_PATH = os.path.join(OUTPUT_DIR, "website_enrichment_progress.json")
ENRICHED_PATH = os.path.join(OUTPUT_DIR, "clubs_enriched.csv")


def _load_db_websites() -> dict[str, str]:
    """Pull all clubs with websites from PostgreSQL canonical_clubs."""
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        logger.info("DATABASE_URL not set — skipping DB source")
        return {}

    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 not installed — skipping DB source")
        return {}

    websites: dict[str, str] = {}
    try:
        conn = psycopg2.connect(db_url)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT club_name_canonical, website FROM canonical_clubs "
                "WHERE website IS NOT NULL AND website != ''"
            )
            for name, url in cur.fetchall():
                if name and url:
                    websites[name.lower().strip()] = url.strip()
        conn.close()
        logger.info("PostgreSQL: %d clubs with websites", len(websites))
    except Exception as exc:
        logger.warning("DB query failed: %s", exc)

    return websites


def _load_brave_websites_with_db() -> dict[str, str]:
    """
    Load Brave Search results. The checkpoint keys may be DB IDs (numeric)
    or club names. For numeric keys, resolve to club names via the DB.
    """
    if not os.path.exists(BRAVE_PATH):
        logger.warning("Brave checkpoint not found: %s", BRAVE_PATH)
        return {}

    with open(BRAVE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    processed = data.get("processed", {})

    # Check if keys are numeric (DB mode) or club names (CSV mode)
    sample_keys = list(processed.keys())[:5]
    is_db_mode = all(k.isdigit() for k in sample_keys) if sample_keys else False

    id_to_name: dict[str, str] = {}
    if is_db_mode:
        logger.info("Brave checkpoint uses DB IDs — resolving to club names via DB")
        db_url = os.environ.get("DATABASE_URL", "")
        if db_url:
            try:
                import psycopg2
                conn = psycopg2.connect(db_url)
                with conn.cursor() as cur:
                    # Fetch all club ID→name mappings
                    cur.execute("SELECT id, club_name_canonical FROM canonical_clubs")
                    for cid, cname in cur.fetchall():
                        id_to_name[str(cid)] = cname.lower().strip()
                conn.close()
                logger.info("Resolved %d DB IDs to club names", len(id_to_name))
            except Exception as exc:
                logger.warning("Could not resolve DB IDs: %s", exc)
        else:
            logger.warning("Brave checkpoint uses DB IDs but DATABASE_URL not set — cannot resolve")

    websites: dict[str, str] = {}
    for key, val in processed.items():
        if val.get("status") == "found" and val.get("website"):
            if is_db_mode:
                name = id_to_name.get(key, "")
            else:
                name = key.lower().strip()

            if name:
                websites[name] = val["website"]

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

    # Load website sources (priority order: DB > ECNL > Brave)
    db_websites = _load_db_websites()
    enriched_websites = _load_enriched_websites()
    brave_websites = _load_brave_websites_with_db()

    # Merge — don't overwrite existing websites
    updated_db = 0
    updated_enriched = 0
    updated_brave = 0

    for idx, row in df.iterrows():
        if row["website"].strip():
            continue

        club_name = row.get("club_name", "").strip().lower()
        canonical = row.get("canonical_name", "").strip().lower()
        names = [n for n in (club_name, canonical) if n]

        # 1. Try DB (highest confidence — includes Brave results already written)
        for name in names:
            if name in db_websites:
                df.at[idx, "website"] = db_websites[name]
                df.at[idx, "website_source"] = "database"
                updated_db += 1
                break
        else:
            # 2. Try ECNL enriched
            for name in names:
                if name in enriched_websites:
                    df.at[idx, "website"] = enriched_websites[name]
                    df.at[idx, "website_source"] = "ecnl_directory"
                    updated_enriched += 1
                    break
            else:
                # 3. Try Brave (resolved from checkpoint)
                for name in names:
                    if name in brave_websites:
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
    print(f"  Added from database:         {updated_db}")
    print(f"  Added from ECNL enrichment:  {updated_enriched}")
    print(f"  Added from Brave Search:     {updated_brave}")
    print(f"  Total added:                 {updated_db + updated_enriched + updated_brave}")
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
