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
from alerts import alert_scraper_failure

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
        alert_scraper_failure(
            scraper_key=_scraper_key_for(league),
            failure_kind=kind.value,
            error_message=str(exc_or_msg),
            source_url=url,
            league_name=name,
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
# Alternative dispatchers: per-source scraper + rollup jobs
# ---------------------------------------------------------------------------

def _run_source(args) -> None:
    """Dispatch --source KEY to the appropriate non-league scraper."""
    key = args.source
    if key == "gotsport-matches":
        if not args.event_id:
            logger.error("--source gotsport-matches requires --event-id")
            sys.exit(2)
        _run_gotsport_matches(
            event_id=args.event_id,
            season=args.season,
            league_name=args.league_name,
            dry_run=args.dry_run,
        )
        return
    if key in ("sincsports-events", "sincsports_events"):
        from events_runner import run_sincsports_events, print_summary
        outcomes = run_sincsports_events(dry_run=args.dry_run, only_tid=args.tid)
        print_summary(outcomes)
        return
    if key in ("link-canonical-clubs", "link_canonical_clubs"):
        from canonical_club_linker import run_cli as _run_linker
        rc = _run_linker(dry_run=args.dry_run, limit=args.limit)
        sys.exit(rc)
    if key in ("sincsports-rosters", "sincsports_rosters"):
        from rosters_runner import run_sincsports_rosters, print_summary
        outcomes = run_sincsports_rosters(dry_run=args.dry_run, only_tid=args.tid)
        print_summary(outcomes)
        return
    if key in ("gotsport-events", "gotsport_events"):
        from gotsport_events_runner import run_gotsport_events
        from gotsport_events_runner import print_summary as _gs_print_summary
        event_ids = [args.event_id] if args.event_id else None
        outcomes = run_gotsport_events(
            dry_run=args.dry_run, event_ids=event_ids, limit=args.limit,
        )
        _gs_print_summary(outcomes)
        return
    if key in (
        "totalglobalsports-events", "totalglobalsports_events",
        "tgs-events", "tgs_events",
    ):
        from totalglobalsports_events_runner import (
            run_totalglobalsports_events,
            print_summary as _tgs_print_summary,
        )
        event_ids = [args.event_id] if args.event_id else None
        outcomes = run_totalglobalsports_events(
            dry_run=args.dry_run,
            event_ids=event_ids,
            limit=args.limit,
            season=args.season or "2025-26",
        )
        _tgs_print_summary(outcomes)
        return
    if key in ("gotsport-matches-batch", "gotsport_matches_batch"):
        from gotsport_matches_runner import run_gotsport_matches_batch
        from gotsport_matches_runner import print_summary as _gmb_print_summary
        outcomes = run_gotsport_matches_batch(
            dry_run=args.dry_run,
            event_id=args.event_id,
            limit=args.limit,
        )
        _gmb_print_summary(outcomes)
        return
    if key in ("gotsport-rosters", "gotsport_rosters"):
        from gotsport_rosters_runner import run_gotsport_rosters
        from gotsport_rosters_runner import print_summary as _gr_print_summary
        outcomes = run_gotsport_rosters(
            dry_run=args.dry_run,
            event_id=args.event_id,
            limit=args.limit,
        )
        _gr_print_summary(outcomes)
        return
    if key in ("tryouts-wordpress", "tryouts_wordpress"):
        from tryouts_runner import run_tryouts_wordpress, print_summary
        outcomes = run_tryouts_wordpress(dry_run=args.dry_run, limit=args.limit)
        print_summary(outcomes)
        return
    if key in ("tryouts-gotsport", "tryouts_gotsport"):
        logger.error(
            "--source tryouts-gotsport is no longer supported. GotSport "
            "disallows automated event discovery via robots.txt and the "
            "public Rankings API does not include tryouts. See "
            "tryouts_runner.py module docstring for details."
        )
        sys.exit(2)
    if key == "tryouts":
        from tryouts_runner import run_tryouts, print_summary
        outcomes = run_tryouts(dry_run=args.dry_run, limit=args.limit)
        print_summary(outcomes)
        return
    if key in ("youth-coaches", "youth_coaches"):
        from youth_coach_runner import run_youth_coaches, print_summary as _yc_print_summary
        result = run_youth_coaches(
            dry_run=args.dry_run,
            limit=args.limit,
            state=args.state,
            platform_family=args.platform_family,
        )
        _yc_print_summary(result)
        return
    if key in ("club-enrichment", "club_enrichment"):
        from enrichment_runner import run_club_enrichment, print_summary as _ce_print_summary
        outcome = run_club_enrichment(
            dry_run=args.dry_run,
            only_club_id=int(args.event_id) if args.event_id else None,
            force=getattr(args, "force", False),
            limit=args.limit,
        )
        _ce_print_summary(outcome)
        return
    if key in ("club-dedup", "club_dedup"):
        from dedup.club_dedup import run_club_dedup, print_report
        pairs = run_club_dedup(
            threshold=0.85,
            dry_run=args.dry_run,
            state=args.state if hasattr(args, "state") else None,
        )
        print_report(pairs)
        return
    if key in ("usclub-sanctioned", "usclub_sanctioned"):
        from usclub_events_runner import run_usclub_events, print_summary as _uc_print_summary
        outcomes = run_usclub_events(
            dry_run=args.dry_run,
            season=args.season or "2025-26",
        )
        _uc_print_summary(outcomes)
        return
    if key in ("usclub-seeds", "usclub_seeds"):
        from usclub_events_runner import run_usclub_events, print_summary as _uc_print_summary
        outcomes = run_usclub_events(
            dry_run=args.dry_run,
            skip_discovery=True,
            season=args.season or "2025-26",
        )
        _uc_print_summary(outcomes)
        return
    if key in ("usclub-id", "usclub_id"):
        from usclub_id_runner import run_usclub_id, print_summary as _uid_print_summary
        outcomes = run_usclub_id(
            dry_run=args.dry_run,
            limit=args.limit,
        )
        _uid_print_summary(outcomes)
        return
    if key in ("duda-360player-clubs", "duda_360player_clubs"):
        from duda_360player_clubs_runner import (
            run_duda_360player_clubs,
            print_summary as _d360_print_summary,
        )
        outcome = run_duda_360player_clubs(
            dry_run=args.dry_run,
            limit=args.limit,
        )
        _d360_print_summary(outcome)
        return
    logger.error("Unknown --source key: %s", key)
    sys.exit(2)


def _run_gotsport_matches(
    *,
    event_id: str,
    season: Optional[str],
    league_name: Optional[str],
    dry_run: bool,
) -> None:
    from extractors.gotsport_matches import scrape_gotsport_matches
    from ingest.matches_writer import insert_matches

    scraper_key = f"gotsport-matches:{event_id}"
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name=league_name or f"gotsport-event-{event_id}",
        )
        run_log.start(source_url=f"https://system.gotsport.com/org_event/events/{event_id}/schedules")

    try:
        rows = scrape_gotsport_matches(
            event_id,
            default_season=season,
            default_league=league_name,
        )
    except Exception as exc:
        kind = _classify_exception(exc)
        logger.error("[gotsport-matches] failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(DbFailureKind(kind.value), error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=f"https://system.gotsport.com/org_event/events/{event_id}/schedules",
            league_name=league_name or f"gotsport-event-{event_id}",
        )
        return

    if not rows:
        logger.warning("[gotsport-matches] event %s → 0 matches", event_id)
        if run_log is not None:
            run_log.finish_partial(records_failed=0, error_message="no matches extracted")
        return

    if dry_run:
        logger.info("[dry-run] would upsert %d matches for event %s", len(rows), event_id)
        return

    counts = insert_matches(rows, dry_run=False)
    logger.info(
        "[gotsport-matches] event %s → inserted=%d updated=%d skipped=%d",
        event_id, counts["inserted"], counts["updated"], counts["skipped"],
    )
    if run_log is not None:
        run_log.finish_ok(
            records_created=counts["inserted"],
            records_updated=counts["updated"],
            records_failed=counts["skipped"],
        )

    # Post-run scrape_health reconcile — soft failure only.
    try:
        from reconcilers import end_of_run_reconcile
        end_of_run_reconcile()
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("end_of_run_reconcile skipped: %s", exc)


def _run_rollup(args) -> None:
    key = args.rollup
    if key == "club-results":
        from rollups.club_results import recompute_club_results

        # Build scope label for the run-log row so an operator
        # browsing scrape_run_logs can tell scoped reruns apart from
        # the nightly full recompute.
        scope_parts = []
        if args.season:
            scope_parts.append(f"season={args.season}")
        if args.league:
            scope_parts.append(f"league={args.league}")
        scope_label = " ".join(scope_parts) if scope_parts else "all"

        scraper_key = "rollup:club-results"
        run_log: Optional[ScrapeRunLogger] = None
        if not args.dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"club_results rollup ({scope_label})",
            )
            run_log.start(source_url="derived:matches")

        try:
            result = recompute_club_results(
                dry_run=args.dry_run,
                season=args.season,
                league=args.league,
            )
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error("[rollup:club-results] failed: %s", exc)
            if run_log is not None:
                run_log.finish_failed(DbFailureKind(kind.value), error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url="derived:matches",
                league_name="club_results rollup",
            )
            return

        logger.info(
            "[rollup:club-results] rows_written=%d skipped_linker_pending=%d",
            result["rows_written"], result["skipped_linker_pending"],
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=result["rows_written"],
                records_updated=0,
                records_failed=result["skipped_linker_pending"],
            )
        return

    if key == "scrape-health":
        from rollups.scrape_health import recompute_scrape_health

        scraper_key = "rollup:scrape-health"
        run_log: Optional[ScrapeRunLogger] = None
        if not args.dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name="scrape_health rollup",
            )
            run_log.start(source_url="derived:scrape_run_logs")

        try:
            result = recompute_scrape_health(dry_run=args.dry_run)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error("[rollup:scrape-health] failed: %s", exc)
            if run_log is not None:
                run_log.finish_failed(DbFailureKind(kind.value), error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url="derived:scrape_run_logs",
                league_name="scrape_health rollup",
            )
            return

        logger.info(
            "[rollup:scrape-health] rows_written=%d by_status=%s",
            result["rows_written"], result["by_status"],
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=result["rows_written"],
                records_updated=0,
                records_failed=0,
            )
        return

    if key == "retention-prune":
        from rollups.retention import prune_retention

        scraper_key = "rollup:retention-prune"
        run_log: Optional[ScrapeRunLogger] = None
        if not args.dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name="retention prune",
            )
            run_log.start(source_url="derived:scrape_run_logs+coach_scrape_snapshots")

        try:
            result = prune_retention(dry_run=args.dry_run)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error("[rollup:retention-prune] failed: %s", exc)
            if run_log is not None:
                run_log.finish_failed(DbFailureKind(kind.value), error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url="derived:scrape_run_logs+coach_scrape_snapshots",
                league_name="retention prune",
            )
            return

        logger.info(
            "[rollup:retention-prune] scrape_run_logs_deleted=%d "
            "coach_scrape_snapshots_deleted=%d",
            result["scrape_run_logs_deleted"],
            result["coach_scrape_snapshots_deleted"],
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=0,
                records_updated=(
                    result["scrape_run_logs_deleted"]
                    + result["coach_scrape_snapshots_deleted"]
                ),
                records_failed=0,
            )
        return

    logger.error("Unknown --rollup key: %s", key)
    sys.exit(2)


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
    parser.add_argument("--source", metavar="KEY",
                        help="Run a non-league scraper by key. Supported: "
                             "'gotsport-matches' (requires --event-id), "
                             "'gotsport-matches-batch' (batch matches for all GotSport events), "
                             "'gotsport-events' (populates events + event_teams from GotSport), "
                             "'totalglobalsports-events' / 'tgs-events' (populates events + event_teams from TotalGlobalSports), "
                             "'gotsport-rosters' (populates club_roster_snapshots from GotSport rosters), "
                             "'sincsports-events' (populates events + event_teams), "
                             "'sincsports-rosters' (populates club_roster_snapshots + roster_diffs), "
                             "'tryouts-wordpress' (populates tryouts from WordPress club sites), "
                             "'tryouts' (wordpress source + status expiry; see tryouts_runner.py for why GotSport tryout discovery is not supported), "
                             "'youth-coaches' (scrapes youth club staff pages into coach_discoveries), "
                             "'link-canonical-clubs' (resolves event_teams.canonical_club_id), "
                             "'club-enrichment' (enrich canonical_clubs with logo/socials/status), "
                             "'club-dedup' (fuzzy dedup report for canonical_clubs), "
                             "'usclub-sanctioned' (discover US Club Soccer sanctioned tournaments + seed National Cup/NPL events), "
                             "'usclub-seeds' (seed only — National Cup + NPL Finals GotSport events, skip discovery), "
                             "'usclub-id' (discover US Club iD National Pool / Training Center articles via SoccerWire WP REST API; "
                             "scaffold only — body parsing + player_id_selections rows arrive in a follow-up PR), "
                             "'duda-360player-clubs' (probe Duda CMS + 360Player club sites; "
                             "writes Event JSON-LD into tryouts; coach_discoveries collected but not written this PR).")
    parser.add_argument("--event-id", metavar="ID",
                        help="GotSport event id for --source gotsport-matches or gotsport-events.")
    parser.add_argument("--season", metavar="SEASON",
                        help="Season tag (e.g. '2025-26') to stamp on scraped/rollup rows.")
    parser.add_argument("--league-name", metavar="NAME",
                        help="League name to tag on match rows (e.g. 'ECNL Boys National').")
    parser.add_argument("--tid", metavar="TID",
                        help="When --source=sincsports-events, scrape a single tid instead "
                             "of iterating the full seed list.")
    parser.add_argument("--limit", type=int, metavar="N",
                        help="Cap the number of rows processed by --source jobs that "
                             "support it (e.g. link-canonical-clubs, youth-coaches).")
    parser.add_argument("--state", metavar="ST",
                        help="State filter for --source youth-coaches or club-dedup (e.g. GA, CA).")
    parser.add_argument("--force", action="store_true",
                        help="For --source club-enrichment: re-enrich clubs that already have data.")
    parser.add_argument("--platform-family",
                        choices=["sportsengine", "leagueapps", "wordpress", "unknown"],
                        dest="platform_family",
                        help="Platform family filter for --source youth-coaches.")
    parser.add_argument("--rollup", choices=["club-results", "scrape-health", "retention-prune"],
                        help="Run a derived-data rollup over existing DB rows.")
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # Alternative dispatchers: --source (non-league scrapers) / --rollup
    # These short-circuit the league-iteration path. They log to
    # scrape_run_logs with their own scraper_key.
    # ------------------------------------------------------------------
    if args.rollup:
        _run_rollup(args)
        return
    if args.source:
        _run_source(args)
        return

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
