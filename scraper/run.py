"""
Upshift Data — main entry point.

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
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from config import LEAGUES, get_leagues
from scraper_static import scrape_static
from scraper_js import scrape_js
from normalizer import normalize, deduplicate
from storage import save_league_csv, append_to_master
import extractors.registry as _extractor_registry
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind as DbFailureKind,
    classify_exception as _db_classify_exception,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run")


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------

class FailureKind(str, Enum):
    TIMEOUT = "timeout"
    NETWORK = "network"
    PARSE_ERROR = "parse_error"
    ZERO_RESULTS = "zero_results"
    UNKNOWN = "unknown"


@dataclass
class LeagueFailure:
    league_name: str
    url: str
    kind: FailureKind
    detail: str = ""


# NOTE: markers compared against `.lower()`-ed message and exc_type, so
# every entry must be lowercase to match. "TimeoutError".lower() is
# "timeouterror".
_TIMEOUT_MARKERS = ("timeout", "timed out", "timeouterror")
_NETWORK_MARKERS = (
    "connectionerror", "connection", "network", "dns",
    "err_name_not_resolved", "err_connection", "err_internet",
    "transient",
)
_PARSE_MARKERS = (
    "beautifulsoup", "parseerror", "parse", "valueerror",
    "keyerror", "attributeerror", "indexerror",
)


def _classify_exception(exc: Exception) -> FailureKind:
    """Map an exception to a FailureKind using message content and type."""
    msg = str(exc).lower()
    exc_type = type(exc).__name__.lower()

    if any(m in msg or m in exc_type for m in _TIMEOUT_MARKERS):
        return FailureKind.TIMEOUT

    if any(m in msg or m in exc_type for m in _NETWORK_MARKERS):
        return FailureKind.NETWORK

    if isinstance(exc, (ValueError, KeyError, AttributeError, IndexError)):
        return FailureKind.PARSE_ERROR
    if any(m in msg or m in exc_type for m in _PARSE_MARKERS):
        return FailureKind.PARSE_ERROR

    return FailureKind.UNKNOWN


# ---------------------------------------------------------------------------
# Core scraping logic
# ---------------------------------------------------------------------------

def _scraper_key_for(league: dict) -> str:
    """Stable key per league-scraper pair for scrape_run_logs rollups."""
    base = league.get("scraper_key") or league["name"].lower().replace(" ", "-")
    return base


def scrape_league(
    league: dict,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, str, Optional[LeagueFailure]]:
    """Scrape, normalise, and deduplicate clubs for a single league.

    Returns (df, extractor_name, failure) where:
      - df            is the resulting DataFrame (may be empty on failure)
      - extractor_name identifies which code path produced the data
      - failure       is a LeagueFailure if something went wrong, else None

    Also persists a row to `scrape_run_logs` for every invocation — the
    row starts as status='running' and transitions to ok|failed|partial
    at the end. Dry-run invocations skip the log write entirely.
    """
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

    failure: Optional[LeagueFailure] = None
    # Persistent run log — no-ops if DATABASE_URL unset or dry-run.
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=_scraper_key_for(league),
            league_name=name,
        )
        run_log.start(source_url=url)

    def _fail(kind: FailureKind, exc_or_msg) -> None:
        if run_log is not None:
            run_log.finish_failed(
                DbFailureKind(kind.value),
                error_message=str(exc_or_msg),
            )

    # Check for a custom extractor first
    custom = _extractor_registry.get_extractor(url)
    if custom:
        logger.info("Using custom extractor: %s", custom.__name__)
        extractor_name = custom.__name__
        try:
            raw = custom(url, name)
        except Exception as exc:
            kind = _classify_exception(exc)
            failure = LeagueFailure(name, url, kind, str(exc))
            logger.error("Custom extractor %s failed for %s: %s", custom.__name__, name, exc)
            if not dry_run:
                save_league_csv(pd.DataFrame(), name)
            _fail(kind, exc)
            return pd.DataFrame(), extractor_name, failure
    elif league.get("js_required"):
        extractor_name = "scraper_js"
        try:
            raw = scrape_js(url, name)
        except Exception as exc:
            kind = _classify_exception(exc)
            failure = LeagueFailure(name, url, kind, str(exc))
            logger.error("JS scrape failed for %s: %s", name, exc)
            if not dry_run:
                save_league_csv(pd.DataFrame(), name)
            _fail(kind, exc)
            return pd.DataFrame(), extractor_name, failure
    else:
        extractor_name = "scraper_static"
        try:
            raw = scrape_static(url, name)
        except Exception as exc:
            kind = _classify_exception(exc)
            failure = LeagueFailure(name, url, kind, str(exc))
            logger.error("Static scrape failed for %s: %s", name, exc)
            if not dry_run:
                save_league_csv(pd.DataFrame(), name)
            _fail(kind, exc)
            return pd.DataFrame(), extractor_name, failure

    if not raw:
        logger.warning(
            "[ZERO-CLUBS] League '%s' returned 0 clubs — possible stale event ID, "
            "bad URL, or site structure change. Investigate before next run.",
            name,
        )
        failure = LeagueFailure(name, url, FailureKind.ZERO_RESULTS, "scraper returned empty list")
        if not dry_run:
            save_league_csv(pd.DataFrame(), name)
        _fail(FailureKind.ZERO_RESULTS, "scraper returned empty list")
        return pd.DataFrame(), extractor_name, failure

    df = pd.DataFrame(raw)

    # Inject default state from the seed (for state-association entries).
    # Skip rows tagged _state_derived=True — those come from multi-state GotSport
    # events where state is deliberately left blank for downstream enrichment.
    state_default = league.get("state", "")
    if state_default and "state" in df.columns:
        if "_state_derived" in df.columns:
            mask_no_state = df["state"].str.strip() == ""
            mask_not_derived = ~df["_state_derived"].fillna(False)
            df.loc[mask_no_state & mask_not_derived, "state"] = state_default
            df = df.drop(columns=["_state_derived"])
        else:
            df["state"] = df["state"].where(df["state"].str.strip() != "", state_default)

    df = normalize(df)
    df = deduplicate(df)

    club_count = len(df)
    if club_count == 0:
        logger.warning(
            "[ZERO-CLUBS] League '%s' had raw records but 0 clubs after normalize/dedup — "
            "check normalizer filters or data quality.",
            name,
        )
    elif club_count < 3:
        logger.warning(
            "[LOW-CLUBS] League '%s' has only %d club(s) after dedup — "
            "suspiciously low; verify source data or event ID.",
            name, club_count,
        )

    logger.info("'%s': %d clubs after dedup", name, club_count)

    if not dry_run:
        path = save_league_csv(df, name)
        logger.info("Saved: %s", path)
    elif dry_run:
        logger.info("[dry-run] Would save %d clubs for '%s'", len(df), name)

    # Persist run outcome. CSV output is the aggregator's current
    # "record" — the number of clubs written is reported as
    # records_created. When the scraper gets a DB-resident upsert path
    # (separate PR), this should split into created vs updated.
    if run_log is not None:
        if club_count == 0:
            run_log.finish_partial(
                records_failed=0,
                error_message="scraped records collapsed to 0 after normalize/dedup",
            )
        else:
            run_log.finish_ok(records_created=club_count)

    return df, extractor_name, None


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------

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


def _print_failure_summary(failures: List[LeagueFailure]) -> None:
    """Print a structured failure summary grouped by failure type."""
    if not failures:
        return

    by_kind: dict[FailureKind, List[LeagueFailure]] = defaultdict(list)
    for f in failures:
        by_kind[f.kind].append(f)

    print("\n" + "=" * 60)
    print(f"  FAILURE SUMMARY ({len(failures)} league(s) failed)")
    print("=" * 60)

    kind_labels = {
        FailureKind.TIMEOUT: "Timeout",
        FailureKind.NETWORK: "Network / DNS",
        FailureKind.PARSE_ERROR: "Parse Error",
        FailureKind.ZERO_RESULTS: "Zero Results",
        FailureKind.UNKNOWN: "Unknown Error",
    }

    for kind in [FailureKind.TIMEOUT, FailureKind.NETWORK, FailureKind.PARSE_ERROR,
                 FailureKind.ZERO_RESULTS, FailureKind.UNKNOWN]:
        group = by_kind.get(kind, [])
        if not group:
            continue
        print(f"\n  [{kind_labels[kind]}] — {len(group)} league(s)")
        for f in group:
            detail = f" ({f.detail[:80]})" if f.detail else ""
            print(f"    • {f.league_name}{detail}")
            print(f"      {f.url}")

    print("=" * 60)


def _write_website_coverage(frame_entries: list[tuple[str, pd.DataFrame]]) -> None:
    """Write a per-extractor website coverage report to output/website_coverage.txt."""
    import datetime
    from config import LEAGUES_DIR
    output_dir = os.path.dirname(LEAGUES_DIR)
    report_path = os.path.join(output_dir, "website_coverage.txt")
    os.makedirs(output_dir, exist_ok=True)

    extractor_totals: dict[str, list[int]] = {}
    for extractor_name, df in frame_entries:
        if df.empty:
            continue
        n = len(df)
        if "website" in df.columns:
            with_site = int(df["website"].fillna("").str.strip().ne("").sum())
        else:
            with_site = 0
        if extractor_name not in extractor_totals:
            extractor_totals[extractor_name] = [0, 0]
        extractor_totals[extractor_name][0] += n
        extractor_totals[extractor_name][1] += with_site

    lines = [
        f"Website Coverage Report — {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 60,
        "",
    ]

    total_clubs = 0
    total_with_website = 0

    for extractor_name in sorted(extractor_totals):
        n, with_site = extractor_totals[extractor_name]
        pct = (with_site / n * 100) if n > 0 else 0
        lines.append(f"  {extractor_name:<50}  {with_site:>4}/{n:<4}  ({pct:.0f}%)")
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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upshift Data",
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
    parser.add_argument("--source", metavar="NAME",
                        help="Run a non-league job. Supported: 'link-canonical-clubs'. "
                             "Skips the league-scraping loop entirely.")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="Cap the number of rows processed by --source jobs that "
                             "support it (e.g. link-canonical-clubs).")
    args = parser.parse_args()

    # --source dispatcher — non-league jobs short-circuit here.
    if args.source:
        if args.source == "link-canonical-clubs":
            from canonical_club_linker import run_cli as _run_linker
            rc = _run_linker(dry_run=args.dry_run, limit=args.limit)
            sys.exit(rc)
        logger.error("Unknown --source: %s", args.source)
        sys.exit(2)

    if args.teams:
        os.environ["UPSHIFT_SCRAPE_TEAMS"] = "1"

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
    frame_entries: list[tuple[str, pd.DataFrame]] = []  # (extractor_name, df)
    all_failures: List[LeagueFailure] = []
    zero_club_leagues: list[str] = []
    low_club_leagues: list[tuple[str, int]] = []

    for league in target_leagues:
        df, extractor_name, failure = scrape_league(league, dry_run=args.dry_run)
        if failure is not None:
            all_failures.append(failure)
        if not df.empty:
            all_frames.append(df)
        frame_entries.append((extractor_name, df))
        n = len(df)
        if n == 0:
            zero_club_leagues.append(league["name"])
        elif n < 3:
            low_club_leagues.append((league["name"], n))

    if not all_frames:
        logger.warning("No data collected.")
        _print_failure_summary(all_failures)
        return

    master = pd.concat(all_frames, ignore_index=True)
    master = deduplicate(master)

    if not args.dry_run:
        path = append_to_master(master)
        logger.info("Master dataset saved: %s (%d clubs)", path, len(master))
    else:
        logger.info("[dry-run] Master dataset would contain %d clubs", len(master))

    if not args.dry_run:
        _write_website_coverage(frame_entries)

    print("\n" + "=" * 60)
    print(f"  Total clubs collected : {len(master)}")
    print(f"  Leagues processed     : {len(target_leagues)}")
    print(f"  Leagues succeeded     : {len(target_leagues) - len(all_failures)}")
    print(f"  Leagues failed        : {len(all_failures)}")
    if not args.dry_run:
        print(f"  Output directory      : output/")
    print("=" * 60)

    _print_failure_summary(all_failures)

    if zero_club_leagues or low_club_leagues:
        print("\n[VALIDATION] Leagues requiring review:")
        for name in zero_club_leagues:
            print(f"  [ZERO-CLUBS]  {name}")
            logger.warning("[VALIDATION] Zero clubs: %s", name)
        for name, count in low_club_leagues:
            print(f"  [LOW-CLUBS]   {name}  ({count} club(s))")
            logger.warning("[VALIDATION] Suspiciously low club count (%d): %s", count, name)
        print()


if __name__ == "__main__":
    try:
        main()
    finally:
        # Release the module-level scrape_run_logger connection so the
        # process exits cleanly even if main() raised.
        try:
            from scrape_run_logger import close_connection as _close_conn
            _close_conn()
        except Exception:
            pass
