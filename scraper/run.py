"""
Upshift Club Aggregator — main entry point.

Usage examples:
    python run.py                              # all scrapeable leagues
    python run.py --priority high             # only high-priority leagues
    python run.py --tier 1                    # only Tier 1 (national elite)
    python run.py --tier 4                    # only USYS state associations
    python run.py --gender boys               # boys leagues only
    python run.py --scope national            # national-scope only
    python run.py --league "ECNL Boys"        # single named league (partial match)
    python run.py --dry-run                   # summarise without writing files
    python run.py --list                      # print all configured leagues and exit
"""

from __future__ import annotations

import argparse
import logging
import sys
import os

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from config import LEAGUES, get_leagues
from scraper_static import scrape_static
from scraper_js import scrape_js
from normalizer import normalize, deduplicate
from storage import save_league_csv, append_to_master
import extractors.registry as _extractor_registry

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
    logger.info(
        "League: %s  |  Tier %s  |  %s  |  %s",
        name,
        league.get("tier", "?"),
        league.get("priority", "?"),
        "JS" if league.get("js_required") else "Static",
    )

    # Check for a custom extractor first
    custom = _extractor_registry.get_extractor(url)
    if custom:
        logger.info("Using custom extractor: %s", custom.__name__)
        raw = custom(url, name)
    elif league.get("js_required"):
        raw = scrape_js(url, name)
    else:
        raw = scrape_static(url, name)

    if not raw:
        logger.warning("No clubs found for league: %s", name)
        if not dry_run:
            save_league_csv(pd.DataFrame(), name)
        return pd.DataFrame()

    df = pd.DataFrame(raw)

    # Inject default state from the seed (for state-association entries)
    state_default = league.get("state", "")
    if state_default and "state" in df.columns:
        df["state"] = df["state"].where(df["state"].str.strip() != "", state_default)

    df = normalize(df)
    df = deduplicate(df)

    logger.info("'%s': %d clubs after dedup", name, len(df))

    if not dry_run:
        path = save_league_csv(df, name)
        logger.info("Saved: %s", path)
    elif dry_run:
        logger.info("[dry-run] Would save %d clubs for '%s'", len(df), name)

    return df


def _print_league_list(leagues: list[dict]) -> None:
    print(f"\n{'#':>3}  {'Pri':>6}  {'Tier':>4}  {'JS':>2}  {'Scope':>10}  League")
    print("-" * 75)
    for i, lg in enumerate(leagues, 1):
        js_flag = "Y" if lg["js_required"] else "N"
        print(
            f"{i:>3}  {lg['priority']:>6}  {lg['tier']:>4}  {js_flag:>2}"
            f"  {lg['geographic_scope']:>10}  {lg['name']}"
        )
    print(f"\nTotal: {len(leagues)} leagues\n")


def _write_website_coverage(frames: list[pd.DataFrame]) -> None:
    """Write a website coverage report to output/website_coverage.txt."""
    import datetime
    from config import LEAGUES_DIR
    output_dir = os.path.dirname(LEAGUES_DIR)
    report_path = os.path.join(output_dir, "website_coverage.txt")
    os.makedirs(output_dir, exist_ok=True)

    lines = [
        f"Website Coverage Report — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
    ]

    total_clubs = 0
    total_with_website = 0

    for df in frames:
        if df.empty:
            continue
        league = df["league_name"].iloc[0] if "league_name" in df.columns else "Unknown"
        n = len(df)
        if "website" in df.columns:
            with_site = df["website"].fillna("").str.strip().ne("").sum()
        else:
            with_site = 0
        pct = (with_site / n * 100) if n > 0 else 0
        lines.append(f"  {league:<50}  {with_site:>4}/{n:<4}  ({pct:.0f}%)")
        total_clubs += n
        total_with_website += with_site

    lines.append("")
    lines.append("=" * 60)
    overall_pct = (total_with_website / total_clubs * 100) if total_clubs > 0 else 0
    lines.append(f"  TOTAL  {total_with_website}/{total_clubs} clubs have a website ({overall_pct:.1f}%)")
    lines.append("")

    report = "\n".join(lines)
    with open(report_path, "w") as f:
        f.write(report)
    logger.info("Website coverage report written to %s", report_path)
    print(report)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upshift Club Aggregator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--league", metavar="NAME",
                        help="Scrape only leagues whose name contains NAME (case-insensitive)")
    parser.add_argument("--priority", choices=["high", "medium", "low"],
                        help="Filter by scrape priority")
    parser.add_argument("--tier", type=int, metavar="N",
                        help="Filter by tier number (1=national elite … 4=state association)")
    parser.add_argument("--gender", choices=["boys", "girls", "boys_and_girls"],
                        help="Filter by gender program")
    parser.add_argument("--scope", choices=["national", "national_regional", "regional", "state"],
                        help="Filter by geographic scope")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run scraper but do not write any files")
    parser.add_argument("--list", action="store_true",
                        help="Print all configured leagues and exit")
    parser.add_argument("--teams", action="store_true",
                        help="Also scrape team-level data (age groups, contacts) where available. "
                             "For GotSport leagues this makes one additional HTTP request per club.")
    args = parser.parse_args()

    # Signal to GotSport (and future) extractors that team-level data is wanted.
    # We use an env var so extractors don't need a different function signature.
    if args.teams:
        os.environ["UPSHIFT_SCRAPE_TEAMS"] = "1"

    # Build the target league list
    target_leagues = get_leagues(
        priority=args.priority,
        tier=args.tier,
        gender=args.gender,
        scope=args.scope,
    )

    if args.league:
        target_leagues = [
            lg for lg in target_leagues
            if args.league.lower() in lg["name"].lower()
        ]
        if not target_leagues:
            logger.error("No league found matching: %s", args.league)
            sys.exit(1)

    if args.list:
        _print_league_list(target_leagues)
        return

    if not target_leagues:
        logger.error("No leagues match the given filters.")
        sys.exit(1)

    logger.info("Processing %d league(s)", len(target_leagues))

    all_frames = []
    for league in target_leagues:
        df = scrape_league(league, dry_run=args.dry_run)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        logger.warning("No data collected.")
        return

    # Cross-league deduplication in master dataset
    master = pd.concat(all_frames, ignore_index=True)
    master = deduplicate(master)

    if not args.dry_run:
        path = append_to_master(master)
        logger.info("Master dataset saved: %s (%d clubs)", path, len(master))
    else:
        logger.info("[dry-run] Master dataset would contain %d clubs", len(master))

    if not args.dry_run:
        _write_website_coverage(all_frames)

    print("\n" + "=" * 60)
    print(f"  Total clubs collected : {len(master)}")
    print(f"  Leagues processed     : {len(target_leagues)}")
    if not args.dry_run:
        print(f"  Output directory      : output/")
    print("=" * 60)


if __name__ == "__main__":
    main()
