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
from typing import Callable, List, Optional

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from config import LEAGUES, get_leagues
from scraper_static import scrape_static
from scraper_js import scrape_js
from normalizer import normalize, deduplicate
from storage import save_league_csv, append_to_master
import extractors.registry as _extractor_registry
from scrape_run_logger import ScrapeRunLogger, FailureKind
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
# `FailureKind` is imported from scrape_run_logger (single source of truth).
# The DB CHECK constraint on scrape_run_logs.failure_kind is locked to its
# values; see lib/db/src/schema/scrape-health.ts.


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
                kind,
                error_message=str(exc_or_msg),
            )
        alert_scraper_failure(
            scraper_key=_scraper_key_for(league),
            failure_kind=kind.value,
            error_message=str(exc_or_msg),
            source_url=url,
            league_name=name,
        )

    # FK to the owning scrape_run_logs row, threaded into scrape_static /
    # scrape_js so the raw-HTML archive row is tied back to this run for
    # post-mortem replay. None when we're in dry-run / no-DB mode.
    # Custom-extractor path doesn't yet thread run_id; follow-up if needed.
    run_log_id = run_log.run_id if run_log is not None else None

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
            raw = scrape_js(url, name, scrape_run_log_id=run_log_id)
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
            raw = scrape_static(url, name, scrape_run_log_id=run_log_id)
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
    if club_count == 0:
        # Post-dedup zero-results is the same failure as pre-dedup
        # zero-results — the source returned data but normalize/dedup
        # collapsed it to nothing. Route through `_fail` so the run
        # lands in `scrape_run_logs.status = 'failed'` with
        # `failure_kind = 'zero_results'` (was previously logged as
        # PARTIAL, which masked the data-quality issue).
        msg = "scraped records collapsed to 0 after normalize/dedup"
        failure = LeagueFailure(name, url, FailureKind.ZERO_RESULTS, msg)
        _fail(FailureKind.ZERO_RESULTS, msg)
        return df, extractor_name, failure

    if run_log is not None:
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
#
# --source handlers: each handler takes an argparse.Namespace and returns
# None. They are kept small on purpose so adding a new --source value
# means adding ONE entry to SOURCE_HANDLERS + ONE entry to SOURCE_HELP —
# no more editing a long `if key in (...)` chain that collides with every
# parallel PR.
#
# Imports are kept inside handlers so importing run.py does not pull
# every runner's transitive dependency tree (Playwright, psycopg2, etc.).
# ---------------------------------------------------------------------------


def _handle_gotsport_matches(args: argparse.Namespace) -> None:
    if not args.event_id:
        logger.error("--source gotsport-matches requires --event-id")
        sys.exit(2)
    _run_gotsport_matches(
        event_id=args.event_id,
        season=args.season,
        league_name=args.league_name,
        dry_run=args.dry_run,
    )


def _handle_ga_matches(args: argparse.Namespace) -> None:
    import os
    cookie = os.environ.get("GOTSPORT_SESSION_COOKIE")
    if not cookie:
        logger.warning("[ga-matches] GOTSPORT_SESSION_COOKIE not set; may hit CAPTCHA")
    _run_gotsport_matches(
        event_id="42137",
        season=args.season,
        league_name=args.league_name or "Girls Academy",
        dry_run=args.dry_run,
        session_cookie=cookie,
    )


def _handle_odp_rosters(args: argparse.Namespace) -> None:
    from odp_runner import run_odp_rosters, print_summary as _odp_print_summary
    summary = run_odp_rosters(
        dry_run=args.dry_run,
        limit=args.limit,
        state=args.state,
    )
    _odp_print_summary(summary)


def _handle_sincsports_events(args: argparse.Namespace) -> None:
    from events_runner import run_sincsports_events, print_summary
    outcomes = run_sincsports_events(dry_run=args.dry_run, only_tid=args.tid)
    print_summary(outcomes)


def _handle_link_canonical_clubs(args: argparse.Namespace) -> None:
    from canonical_club_linker import run_cli as _run_linker
    rc = _run_linker(dry_run=args.dry_run, limit=args.limit)
    sys.exit(rc)


def _handle_link_canonical_schools(args: argparse.Namespace) -> None:
    from canonical_school_linker import run_cli as _run_linker
    rc = _run_linker(dry_run=args.dry_run, limit=args.limit)
    sys.exit(rc)


def _handle_nav_leaked_names_detect(args: argparse.Namespace) -> None:
    from nav_leaked_names_detector import run_cli as _run_detector
    rc = _run_detector(
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=getattr(args, "full_scan", False),
    )
    sys.exit(rc)


def _handle_numeric_only_names_detect(args: argparse.Namespace) -> None:
    from numeric_only_name_detector import run_cli as _run_detector
    rc = _run_detector(
        dry_run=args.dry_run,
        limit=args.limit,
        full_scan=getattr(args, "full_scan", False),
    )
    sys.exit(rc)


def _handle_coach_pollution_detect(args: argparse.Namespace) -> None:
    from coach_pollution_detector import run_cli as _run_detector
    # NOTE: unlike the other *-detect sources this one uses `--commit`
    # (default False = dry-run) instead of `--dry-run` (default False =
    # commit). Historical scans of every coach_discoveries row are a
    # higher-blast-radius operation than the nightly incremental-window
    # detectors, so the default is "just show me what you'd do".
    rc = _run_detector(
        commit=getattr(args, "commit", False),
        limit=args.limit,
        window_days=getattr(args, "window_days", None),
    )
    sys.exit(rc)


def _handle_coach_ui_fragment_detect(args: argparse.Namespace) -> None:
    from coach_ui_fragment_detector import run_cli as _run_detector
    # Second-wave complement to coach-pollution-detect. Same `--commit`
    # (default dry-run) safety inversion for the same reason — historical
    # full-table scans default to "show me", not "apply".
    rc = _run_detector(
        commit=getattr(args, "commit", False),
        limit=args.limit,
        window_days=getattr(args, "window_days", None),
    )
    sys.exit(rc)


def _handle_sincsports_rosters(args: argparse.Namespace) -> None:
    from rosters_runner import run_sincsports_rosters, print_summary
    outcomes = run_sincsports_rosters(dry_run=args.dry_run, only_tid=args.tid)
    print_summary(outcomes)


def _handle_gotsport_events(args: argparse.Namespace) -> None:
    from gotsport_events_runner import run_gotsport_events
    from gotsport_events_runner import print_summary as _gs_print_summary
    event_ids = [args.event_id] if args.event_id else None
    outcomes = run_gotsport_events(
        dry_run=args.dry_run, event_ids=event_ids, limit=args.limit,
    )
    _gs_print_summary(outcomes)


def _handle_totalglobalsports_events(args: argparse.Namespace) -> None:
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


def _handle_gotsport_matches_batch(args: argparse.Namespace) -> None:
    from gotsport_matches_runner import run_gotsport_matches_batch
    from gotsport_matches_runner import print_summary as _gmb_print_summary
    outcomes = run_gotsport_matches_batch(
        dry_run=args.dry_run,
        event_id=args.event_id,
        limit=args.limit,
    )
    _gmb_print_summary(outcomes)


def _handle_sincsports_matches(args: argparse.Namespace) -> None:
    from extractors.sincsports_matches import (
        scrape_sincsports_matches, KNOWN_TIDS, fetch_sincsports_event_tids,
    )
    from ingest.tournament_matches_writer import insert_tournament_matches

    if args.tid:
        tid_name_pairs = [(args.tid, args.league_name or f"SincSports Tournament {args.tid}")]
    else:
        # Auto-discover from events.aspx; fall back to KNOWN_TIDS on failure.
        discovered = fetch_sincsports_event_tids()
        if discovered:
            tid_name_pairs = discovered
        else:
            logger.warning("[sincsports-matches] events.aspx discovery failed — using KNOWN_TIDS fallback")
            tid_name_pairs = [(t, f"SincSports Tournament {t}") for t in KNOWN_TIDS]

    totals = {"inserted": 0, "updated": 0, "skipped": 0}

    for tid, tournament_name in tid_name_pairs:
        rows = scrape_sincsports_matches(
            tid=tid,
            tournament_name=tournament_name,
            season=args.season,
            year=2026,
        )
        if not rows:
            logger.warning("[sincsports-matches] tid=%s → 0 matches", tid)
            continue
        if args.dry_run:
            logger.info("[dry-run] would upsert %d tournament matches for tid=%s", len(rows), tid)
            continue
        counts = insert_tournament_matches(rows, dry_run=False)
        logger.info(
            "[sincsports-matches] tid=%s → inserted=%d updated=%d skipped=%d",
            tid, counts["inserted"], counts["updated"], counts["skipped"],
        )
        for k in totals:
            totals[k] += counts.get(k, 0)

    if not args.dry_run and len(tid_name_pairs) > 1:
        logger.info(
            "[sincsports-matches] batch total → inserted=%d updated=%d skipped=%d",
            totals["inserted"], totals["updated"], totals["skipped"],
        )


def _handle_athleteone_matches(args: argparse.Namespace) -> None:
    from extractors.athleteone_matches import scrape_athleteone_matches, ALL_ORG_SEASONS
    from ingest.matches_writer import insert_matches
    from ingest.tournament_matches_writer import insert_tournament_matches

    league_rows, tournament_rows = scrape_athleteone_matches(
        org_season_ids=None,  # all ECNL org_seasons
        season=args.season,
    )

    if args.dry_run:
        logger.info("[dry-run] would upsert %d league matches + %d tournament matches",
                    len(league_rows), len(tournament_rows))
        return

    total_league = {"inserted": 0, "updated": 0, "skipped": 0}
    if league_rows:
        counts = insert_matches(league_rows, dry_run=False)
        for k in total_league:
            total_league[k] += counts.get(k, 0)

    total_tourn = {"inserted": 0, "updated": 0, "skipped": 0}
    if tournament_rows:
        counts = insert_tournament_matches(tournament_rows, dry_run=False)
        for k in total_tourn:
            total_tourn[k] += counts.get(k, 0)

    logger.info(
        "[athleteone-matches] league → inserted=%d updated=%d skipped=%d | "
        "tournament → inserted=%d updated=%d skipped=%d",
        total_league["inserted"], total_league["updated"], total_league["skipped"],
        total_tourn["inserted"], total_tourn["updated"], total_tourn["skipped"],
    )


def _handle_totalglobalsports_matches(args: argparse.Namespace) -> None:
    from extractors.totalglobalsports_matches import scrape_totalglobalsports_matches, KNOWN_EVENT_IDS
    from ingest.matches_writer import insert_matches

    event_ids = [str(args.event_id)] if args.event_id else KNOWN_EVENT_IDS
    totals = {"inserted": 0, "updated": 0, "skipped": 0}

    for eid in event_ids:
        league_name = (args.league_name if args.event_id else None) or f"TGS Event {eid}"
        rows = scrape_totalglobalsports_matches(
            eid,
            league_name=league_name,
            season=args.season,
        )
        if not rows:
            logger.warning("[tgs-matches] event=%s → 0 matches", eid)
            continue
        if args.dry_run:
            logger.info("[dry-run] would upsert %d matches for tgs event=%s", len(rows), eid)
            continue
        counts = insert_matches(rows, dry_run=False)
        logger.info(
            "[tgs-matches] event=%s → inserted=%d updated=%d skipped=%d",
            eid, counts["inserted"], counts["updated"], counts["skipped"],
        )
        for k in totals:
            totals[k] += counts.get(k, 0)

    if not args.dry_run and len(event_ids) > 1:
        logger.info(
            "[tgs-matches] batch total → inserted=%d updated=%d skipped=%d",
            totals["inserted"], totals["updated"], totals["skipped"],
        )


def _handle_mlsnext_matches(args: argparse.Namespace) -> None:
    from extractors.mlsnext_matches import scrape_mlsnext_matches
    from ingest.matches_writer import insert_matches

    rows = scrape_mlsnext_matches(
        league_name=args.league_name or "MLS NEXT",
        season=args.season,
    )
    if not rows:
        logger.warning("[mlsnext-matches] 0 matches scraped")
        return
    if args.dry_run:
        logger.info("[dry-run] would upsert %d MLS NEXT matches", len(rows))
        return
    counts = insert_matches(rows, dry_run=False)
    logger.info(
        "[mlsnext-matches] inserted=%d updated=%d skipped=%d",
        counts["inserted"], counts["updated"], counts["skipped"],
    )


def _handle_gotsport_rosters(args: argparse.Namespace) -> None:
    from gotsport_rosters_runner import run_gotsport_rosters
    from gotsport_rosters_runner import print_summary as _gr_print_summary
    outcomes = run_gotsport_rosters(
        dry_run=args.dry_run,
        event_id=args.event_id,
        limit=args.limit,
    )
    _gr_print_summary(outcomes)


def _handle_maxpreps_rosters(args: argparse.Namespace) -> None:
    from maxpreps_runner import run_maxpreps_rosters, print_summary
    outcome = run_maxpreps_rosters(
        dry_run=args.dry_run,
        limit=args.limit,
        state=args.state,
    )
    print_summary(outcome)


def _handle_tryouts_wordpress(args: argparse.Namespace) -> None:
    from tryouts_runner import run_tryouts_wordpress, print_summary
    outcomes = run_tryouts_wordpress(dry_run=args.dry_run, limit=args.limit)
    print_summary(outcomes)


def _handle_tryouts_gotsport(args: argparse.Namespace) -> None:
    logger.error(
        "--source tryouts-gotsport is no longer supported. GotSport "
        "disallows automated event discovery via robots.txt and the "
        "public Rankings API does not include tryouts. See "
        "tryouts_runner.py module docstring for details."
    )
    sys.exit(2)


def _handle_tryouts(args: argparse.Namespace) -> None:
    from tryouts_runner import run_tryouts, print_summary
    outcomes = run_tryouts(dry_run=args.dry_run, limit=args.limit)
    print_summary(outcomes)


def _handle_youth_coaches(args: argparse.Namespace) -> None:
    from youth_coach_runner import run_youth_coaches, print_summary as _yc_print_summary
    result = run_youth_coaches(
        dry_run=args.dry_run,
        limit=args.limit,
        state=args.state,
        platform_family=args.platform_family,
    )
    _yc_print_summary(result)


def _handle_squarespace_clubs(args: argparse.Namespace) -> None:
    from squarespace_clubs_runner import (
        run_squarespace_clubs,
        print_summary as _sq_print_summary,
        DEFAULT_LIMIT as _SQ_DEFAULT_LIMIT,
    )
    outcome = run_squarespace_clubs(
        dry_run=args.dry_run,
        limit=args.limit if args.limit is not None else _SQ_DEFAULT_LIMIT,
        state=args.state,
    )
    _sq_print_summary(outcome)


def _handle_sportsengine_clubs(args: argparse.Namespace) -> None:
    from sportsengine_clubs_runner import (
        run_sportsengine_clubs,
        print_summary as _se_print_summary,
        DEFAULT_LIMIT as _SE_DEFAULT_LIMIT,
    )
    outcome = run_sportsengine_clubs(
        dry_run=args.dry_run,
        limit=args.limit if args.limit is not None else _SE_DEFAULT_LIMIT,
        state=args.state,
    )
    _se_print_summary(outcome)


def _handle_club_enrichment(args: argparse.Namespace) -> None:
    from enrichment_runner import run_club_enrichment, print_summary as _ce_print_summary
    outcome = run_club_enrichment(
        dry_run=args.dry_run,
        only_club_id=int(args.event_id) if args.event_id else None,
        force=getattr(args, "force", False),
        limit=args.limit,
    )
    _ce_print_summary(outcome)


def _handle_club_dedup(args: argparse.Namespace) -> None:
    from dedup.club_dedup import run_club_dedup, print_report
    pairs = run_club_dedup(
        threshold=0.85,
        dry_run=args.dry_run,
        state=args.state if hasattr(args, "state") else None,
        persist=getattr(args, "persist", False),
    )
    print_report(pairs)


def _handle_college_dedup(args: argparse.Namespace) -> None:
    from dedup.college_dedup import run_college_dedup, print_report
    pairs = run_college_dedup(
        threshold=getattr(args, "threshold", 0.85),
        dry_run=args.dry_run,
        division=getattr(args, "division", None),
        gender=getattr(args, "gender", None),
        persist=getattr(args, "persist", False),
    )
    print_report(pairs)


def _handle_club_dedup_resolve(args: argparse.Namespace) -> None:
    # Tiered resolver — auto-merges high-confidence pairs, writes a
    # review CSV for the rest. **DEFAULTS TO DRY-RUN** even when
    # --dry-run is absent; pass --no-dry-run to actually mutate.
    # (--dry-run remains supported for symmetry with other --source
    # jobs, and forces dry-run regardless of --no-dry-run.)
    from dedup.__main__ import run_resolve, print_summary
    commit = bool(getattr(args, "no_dry_run", False)) and not args.dry_run
    try:
        summary = run_resolve(
            threshold=0.85,
            state=args.state if hasattr(args, "state") else None,
            dry_run=not commit,
        )
    except RuntimeError as exc:
        logger.error("club-dedup-resolve: %s", exc)
        sys.exit(1)
    print_summary(summary)


def _handle_usclub_sanctioned(args: argparse.Namespace) -> None:
    from usclub_events_runner import run_usclub_events, print_summary as _uc_print_summary
    outcomes = run_usclub_events(
        dry_run=args.dry_run,
        season=args.season or "2025-26",
    )
    _uc_print_summary(outcomes)


def _handle_usclub_seeds(args: argparse.Namespace) -> None:
    from usclub_events_runner import run_usclub_events, print_summary as _uc_print_summary
    outcomes = run_usclub_events(
        dry_run=args.dry_run,
        skip_discovery=True,
        season=args.season or "2025-26",
    )
    _uc_print_summary(outcomes)


def _handle_usclub_id(args: argparse.Namespace) -> None:
    from usclub_id_runner import run_usclub_id, print_summary as _uid_print_summary
    outcomes = run_usclub_id(
        dry_run=args.dry_run,
        limit=args.limit,
    )
    _uid_print_summary(outcomes)


def _handle_ussoccer_ynt(args: argparse.Namespace) -> None:
    from ynt_runner import run_ussoccer_ynt, print_summary as _ynt_print_summary
    summary = run_ussoccer_ynt(
        dry_run=args.dry_run,
        limit=args.limit,
    )
    _ynt_print_summary(summary)


def _handle_youtube_ecnl(args: argparse.Namespace) -> None:
    from youtube_runner import (
        run_youtube_channel,
        print_summary as _yt_print_summary,
    )
    summary = run_youtube_channel(
        handle="@TheECNL",
        league_name="ECNL",
        source_platform="youtube",
        dry_run=args.dry_run,
    )
    _yt_print_summary(summary)


def _handle_mlsnext_video(args: argparse.Namespace) -> None:
    from mlsnext_video_runner import (
        run_mlsnext_video,
        print_summary as _mlsv_print_summary,
    )
    summary = run_mlsnext_video(dry_run=args.dry_run)
    _mlsv_print_summary(summary)


def _handle_replay_html(args: argparse.Namespace) -> None:
    """
    Replay archived HTML through the extractor registry.

    Reads rows from ``raw_html_archive`` matching ``--run-id``,
    downloads the gzipped blob for each from Replit Object Storage,
    decompresses, and feeds the HTML back through the per-site
    extractor that matches the stored ``source_url``. The goal is to
    re-parse without re-fetching — handy for testing extractor changes
    against a fixed corpus, or recovering rows after a parse regression.

    Scope limit (this PR): only extractors that expose a pure-function
    parser (module-level ``parse_html(html, source_url=..., league_name=...)``)
    are replayable. Registered extractors today all take ``(url,
    league_name)`` and fetch internally; those are skipped with a
    warning. Refactoring them to accept pre-fetched HTML is a follow-up.

    Defaults to dry-run — pass ``--no-dry-run`` to actually write any
    rows the replayed parser emits. (Today every extractor-dispatch
    path is a skip-with-warning, so the flag is effectively a no-op
    until a follow-up wires at least one pure parser through.)
    """
    run_id = getattr(args, "run_id", None)
    if not run_id:
        logger.error(
            "--source replay-html requires --run-id <uuid>. "
            "Query raw_html_archive for the run you want to replay."
        )
        sys.exit(2)

    # Lazy import: only pull psycopg2 / extractors when this handler
    # actually runs, so `python3 run.py --help` stays cheap.
    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error(
            "--source replay-html: psycopg2 is not installed; cannot "
            "query raw_html_archive."
        )
        sys.exit(2)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.error(
            "--source replay-html: DATABASE_URL is not set; cannot "
            "query raw_html_archive."
        )
        sys.exit(2)

    try:
        conn = psycopg2.connect(dsn)
    except Exception as exc:
        logger.error("--source replay-html: DB connect failed: %s", exc)
        sys.exit(1)

    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT sha256, source_url "
                "FROM raw_html_archive "
                "WHERE scrape_run_log_id = %s "
                "ORDER BY archived_at ASC",
                (run_id,),
            )
            rows = cur.fetchall()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows:
        logger.warning(
            "[replay-html] no archived HTML for run_id %s — nothing to replay.",
            run_id,
        )
        return

    from utils.html_archive import fetch_archived_html

    dry_run = bool(getattr(args, "dry_run", False)) or not bool(
        getattr(args, "no_dry_run", False)
    )

    summary = {
        "pages_replayed": 0,
        "extractors_matched": 0,
        "extractors_skipped_not_pure": 0,
        "no_extractor": 0,
        "parse_errors": 0,
        "rows_written": 0,
    }
    skipped_extractors: set[str] = set()

    for sha256, source_url in rows:
        summary["pages_replayed"] += 1

        try:
            html = fetch_archived_html(sha256)
        except Exception as exc:
            logger.error(
                "[replay-html] fetch failed for sha256=%s source_url=%s: %s",
                sha256, source_url, exc,
            )
            summary["parse_errors"] += 1
            continue

        if html is None:  # pragma: no cover — fetch raises on failure
            logger.error(
                "[replay-html] fetch returned None for sha256=%s", sha256,
            )
            summary["parse_errors"] += 1
            continue

        extractor = _extractor_registry.get_extractor(source_url)
        if extractor is None:
            logger.warning(
                "[replay-html] no extractor matched source_url=%s (sha256=%s)",
                source_url, sha256,
            )
            summary["no_extractor"] += 1
            continue

        summary["extractors_matched"] += 1

        # Convention for pure-function parsing: the extractor's module
        # exposes a top-level `parse_html(html, source_url=..., league_name=...)`
        # (or positional equivalent) returning List[Dict]. Any extractor
        # without this attribute is a `run(url)`-style scraper that
        # fetches internally and cannot be replayed yet.
        module = sys.modules.get(extractor.__module__)
        parse_html_fn = getattr(module, "parse_html", None) if module else None

        if not callable(parse_html_fn):
            if extractor.__module__ not in skipped_extractors:
                logger.warning(
                    "[replay-html] extractor module %s has no pure-function "
                    "parse_html(html, ...) — replay skipped. A follow-up PR "
                    "will refactor this extractor to accept pre-fetched HTML.",
                    extractor.__module__,
                )
                skipped_extractors.add(extractor.__module__)
            summary["extractors_skipped_not_pure"] += 1
            continue

        try:
            records = parse_html_fn(
                html,
                source_url=source_url,
                league_name=None,
            )
        except TypeError:
            # Fall back to positional if the parser signature is simpler.
            try:
                records = parse_html_fn(html)
            except Exception as exc:
                logger.error(
                    "[replay-html] parse_html raised for %s (sha256=%s): %s",
                    extractor.__module__, sha256, exc,
                )
                summary["parse_errors"] += 1
                continue
        except Exception as exc:
            logger.error(
                "[replay-html] parse_html raised for %s (sha256=%s): %s",
                extractor.__module__, sha256, exc,
            )
            summary["parse_errors"] += 1
            continue

        n_records = len(records) if records else 0
        if dry_run:
            logger.info(
                "[replay-html] [dry-run] %s → %d record(s) from sha256=%s",
                extractor.__module__, n_records, sha256,
            )
        else:
            # No generic write path — each extractor family writes to a
            # different table. Until a pure-function parser is wired
            # through with an explicit writer, treat --no-dry-run as a
            # count-only pass so the handler is safe to invoke.
            logger.info(
                "[replay-html] %s → %d record(s) from sha256=%s "
                "(write path not yet wired; see follow-up)",
                extractor.__module__, n_records, sha256,
            )
        summary["rows_written"] += n_records

    print("=" * 60)
    print(f"  replay-html summary (run_id={run_id})")
    print("=" * 60)
    for k, v in summary.items():
        print(f"  {k:>28} : {v}")
    print("=" * 60)


def _handle_duda_360player_clubs(args: argparse.Namespace) -> None:
    from duda_360player_clubs_runner import (
        run_duda_360player_clubs,
        print_summary as _d360_print_summary,
    )
    outcome = run_duda_360player_clubs(
        dry_run=args.dry_run,
        limit=args.limit,
    )
    _d360_print_summary(outcome)


def _handle_ncaa_rosters(args: argparse.Namespace) -> None:
    """NCAA D1 roster scrape — single school OR bulk enumeration.

    Two modes, mutually exclusive:

    * ``--school-url`` (+ optional ``--school-name``, ``--state``) —
      single-program scrape. Writes to ``colleges`` + ``college_coaches``
      + ``college_roster_history`` via ``ingest.ncaa_roster_writer``.

    * ``--all`` — bulk enumeration. Iterates every row in ``colleges``
      matching ``--division`` + ``--gender`` (both required) via
      ``extractors.ncaa_soccer_rosters.scrape_college_rosters``. Requires the
      ``colleges`` table to be populated first (PR-1: ``--source ncaa-seed-d1``,
      or existing DB-seed rows).

    Optional flags in both modes: ``--division`` (D1/D2/D3, default D1),
    ``--gender`` (mens/womens, default mens), ``--dry-run``.
    """
    school_url = getattr(args, "school_url", None)
    run_all = bool(getattr(args, "all", False))

    if school_url and run_all:
        logger.error("--source ncaa-rosters: --all and --school-url are mutually exclusive")
        sys.exit(2)
    if not school_url and not run_all:
        logger.error("--source ncaa-rosters requires exactly one of --school-url or --all")
        sys.exit(2)

    division = getattr(args, "division", None) or "D1"
    gender = getattr(args, "gender", None) or "mens"
    gender_program = {"boys": "mens", "girls": "womens"}.get(gender, gender)
    if gender_program not in ("mens", "womens"):
        logger.error("--source ncaa-rosters: --gender must be mens|womens (got %r)", gender)
        sys.exit(2)

    if run_all:
        backfill_seasons = int(getattr(args, "backfill_seasons", 0) or 0)
        if backfill_seasons < 0:
            logger.error(
                "--source ncaa-rosters: --backfill-seasons must be >= 0 (got %d)",
                backfill_seasons,
            )
            sys.exit(2)
        force_covid = bool(getattr(args, "force_covid", False))
        max_age_days = int(getattr(args, "max_age_days", 30) or 30)
        force_rescrape = bool(getattr(args, "force_rescrape", False))
        _run_ncaa_rosters_all(
            division=division,
            gender_program=gender_program,
            dry_run=bool(getattr(args, "dry_run", False)),
            backfill_seasons=backfill_seasons,
            skip_fresh_days=int(getattr(args, "skip_fresh_days", 30) or 30),
            force_rescrape=force_rescrape,
            force_historical=getattr(args, "force_historical", None),
            force_covid=force_covid,
            max_age_days=max_age_days,
        )
        return

    school_name = getattr(args, "school_name", None) or _derive_school_name(school_url)
    state = getattr(args, "state", None)

    from extractors.ncaa_soccer_rosters import scrape_school_url as _scrape_school
    from extractors.ncaa_soccer_rosters import SCRAPER_KEY_MAP as _SCRAPER_KEY_MAP

    scraper_key = _SCRAPER_KEY_MAP.get(division, f"ncaa-{division.lower()}-rosters")
    run_log: Optional[ScrapeRunLogger] = None
    if not args.dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name=f"NCAA {division} rosters",
        )
        run_log.start(source_url=school_url)

    try:
        parsed = _scrape_school(
            school_url,
            name=school_name,
            division=division,
            gender_program=gender_program,
            state=state,
        )
    except Exception as exc:
        kind = _classify_exception(exc)
        logger.error("[ncaa-rosters] scrape failed for %s: %s", school_url, exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=school_url,
            league_name=f"NCAA {division} rosters",
        )
        sys.exit(1)

    n_players = len(parsed["players"])
    n_coaches = len(parsed["coaches"])
    logger.info(
        "[ncaa-rosters] %s (%s %s) → %d player(s), %d head coach row(s), sidearm=%s",
        parsed["college"]["name"], division, gender_program,
        n_players, n_coaches, parsed["sidearm"],
    )

    if args.dry_run:
        logger.info("[ncaa-rosters] [dry-run] skipping DB writes")
        return

    from ingest.ncaa_roster_writer import (
        upsert_college as _upsert_college,
        upsert_coaches as _upsert_coaches,
        upsert_roster_players as _upsert_players,
    )
    from dataclasses import asdict as _asdict

    sport = getattr(args, "sport", None) or "soccer"
    parsed["college"]["sport"] = sport
    college_id, college_inserted = _upsert_college(parsed["college"])
    if college_id is None:
        logger.error("[ncaa-rosters] college upsert returned no id; aborting write")
        if run_log is not None:
            run_log.finish_failed(
                FailureKind.UNKNOWN,
                error_message="college upsert returned no id",
            )
        sys.exit(1)

    coach_counts = _upsert_coaches(
        parsed["coaches"], college_id=college_id,
    )
    player_counts = _upsert_players(
        [_asdict(p) for p in parsed["players"]],
        college_id=college_id,
        academic_year=parsed["academic_year"],
    )

    logger.info(
        "[ncaa-rosters] college_id=%d (new=%s) coaches inserted=%d updated=%d "
        "players inserted=%d updated=%d skipped=%d",
        college_id, college_inserted,
        coach_counts["inserted"], coach_counts["updated"],
        player_counts["inserted"], player_counts["updated"], player_counts["skipped"],
    )
    if run_log is not None:
        run_log.finish_ok(
            records_created=player_counts["inserted"] + coach_counts["inserted"]
                            + (1 if college_inserted else 0),
            records_updated=player_counts["updated"] + coach_counts["updated"]
                            + (0 if college_inserted else 1),
            records_failed=player_counts["skipped"] + coach_counts["skipped"],
        )


def _run_ncaa_rosters_all(
    *,
    division: str,
    gender_program: str,
    dry_run: bool,
    backfill_seasons: int = 0,
    skip_fresh_days: int = 30,
    force_rescrape: bool = False,
    force_historical: Optional[str] = None,
    force_covid: bool = False,
    max_age_days: int = 30,
) -> None:
    """Dispatch to the pre-existing bulk enumerator.

    ``scrape_college_rosters`` in ``extractors.ncaa_soccer_rosters`` handles
    per-run logging (one ``scrape_run_logs`` row per division), rate
    limiting (1.5s between schools/seasons), and write-through.

    ``backfill_seasons=0`` is today's behavior (current season only).
    Positive N pulls prior seasons via the /roster/<YYYY> (SIDEARM) or
    /roster/season/<YYYY> (Nuxt) URL pattern; writer uses the same
    natural key so re-runs are idempotent.

    ``force_covid=False`` (default) skips the 2020-21 season entirely.
    Pass True to bypass the guard (e.g. for targeted investigation).

    ``skip_fresh_days`` and ``force_rescrape`` control the per-season
    ``should_scrape`` guard inside the loop. ``max_age_days`` is a
    coarser pre-filter that removes colleges from the candidate list
    before any HTTP requests are made (pass ``force_rescrape=True`` or
    ``max_age_days=0`` to bypass it).
    """
    from extractors.ncaa_soccer_rosters import scrape_college_rosters

    logger.info(
        "[ncaa-rosters] --all division=%s gender=%s dry_run=%s backfill_seasons=%d "
        "skip_fresh_days=%d force_rescrape=%s force_historical=%s force_covid=%s max_age_days=%d",
        division, gender_program, dry_run, backfill_seasons,
        skip_fresh_days, force_rescrape, force_historical, force_covid, max_age_days,
    )
    result = scrape_college_rosters(
        division=division,
        gender=gender_program,
        dry_run=dry_run,
        backfill_seasons=backfill_seasons,
        skip_fresh_days=skip_fresh_days,
        force_rescrape=force_rescrape,
        force_historical=force_historical,
        force_covid=force_covid,
        max_age_days=max_age_days,
    )
    logger.info(
        "[ncaa-rosters] --all done: scraped=%d skipped_fresh=%d inserted=%d updated=%d errors=%d covid_skipped=%d",
        result.get("scraped", 0),
        result.get("skipped_fresh", 0),
        result.get("rows_inserted", 0),
        result.get("rows_updated", 0),
        result.get("errors", 0),
        result.get("covid_skipped", 0),
    )


def _handle_ncaa_resolve_urls(args: argparse.Namespace) -> None:
    """Resolve ``colleges.soccer_program_url`` via SIDEARM probing.

    Iterates rows where ``soccer_program_url IS NULL`` (scoped by
    optional ``--division``, default D1), composes the canonical
    SIDEARM roster URL from ``colleges.website``, probes it, and
    ``UPDATE``s the row on hit. Misses are logged for operator review.

    Respects ``--limit N`` for smoke-testing. Rate-limits at 1.0s
    between rows (lighter than the roster scrape because these are
    HEAD requests).
    """
    from extractors.ncaa_directory import (
        resolve_soccer_program_url,
        USER_AGENT as _NCAA_UA,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-resolve-urls] psycopg2 not available; cannot query DB")
        sys.exit(1)

    division = getattr(args, "division", None) or "D1"
    dry_run = bool(getattr(args, "dry_run", False))
    limit = getattr(args, "limit", None)
    rate_delay = 1.0

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-resolve-urls] DATABASE_URL env var not set")
        sys.exit(1)

    import requests as _requests
    import time as _time

    session = _requests.Session()
    session.headers.update({
        "User-Agent": _NCAA_UA,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key="ncaa-resolve-urls",
            league_name=f"NCAA {division} URL resolver",
        )
        run_log.start()

    resolved = 0
    missed = 0
    errors = 0
    unresolved_names: list[str] = []

    try:
        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                select_sql = """
                    SELECT id, name, website, gender_program
                    FROM colleges
                    WHERE division = %s
                      AND soccer_program_url IS NULL
                      AND website IS NOT NULL
                    ORDER BY name
                """
                if limit is not None:
                    select_sql += f" LIMIT {int(limit)}"
                cur.execute(select_sql, (division,))
                rows = cur.fetchall()

            logger.info(
                "[ncaa-resolve-urls] %d %s college(s) to resolve%s",
                len(rows), division, " (dry-run)" if dry_run else "",
            )

            for row in rows:
                college_id, name, website, gender_program = row
                try:
                    url = resolve_soccer_program_url(
                        website, gender_program, session=session
                    )
                except Exception as exc:
                    logger.warning(
                        "[ncaa-resolve-urls] resolver error for %s: %s",
                        name, exc,
                    )
                    errors += 1
                    _time.sleep(rate_delay)
                    continue

                if url is None:
                    missed += 1
                    unresolved_names.append(name)
                    _time.sleep(rate_delay)
                    continue

                if dry_run:
                    logger.info(
                        "[ncaa-resolve-urls] [dry-run] would set %s → %s",
                        name, url,
                    )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE colleges SET soccer_program_url = %s WHERE id = %s",
                            (url, college_id),
                        )
                    conn.commit()

                resolved += 1
                _time.sleep(rate_delay)

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[ncaa-resolve-urls] done: resolved=%d missed=%d errors=%d%s",
        resolved, missed, errors, " (dry-run)" if dry_run else "",
    )
    if unresolved_names:
        logger.info(
            "[ncaa-resolve-urls] unresolved (manual fill needed): %s",
            ", ".join(unresolved_names[:25])
            + (f" … ({len(unresolved_names) - 25} more)" if len(unresolved_names) > 25 else ""),
        )

    if run_log is not None:
        run_log.finish_ok(
            records_created=0,
            records_updated=resolved,
            records_failed=errors,
        )


def _handle_ncaa_resolve_urls_wikipedia(args: argparse.Namespace) -> None:
    """Resolve ``colleges.soccer_program_url`` via per-program Wikipedia infoboxes.

    Closes the URL-coverage gap that ``ncaa-resolve-urls`` can't reach
    on its own: that handler requires ``colleges.website IS NOT NULL``,
    but the D1 stats.ncaa.org seeder and the D1/D2 Wikipedia list-page
    seeders don't write a ``website`` column. As of 2026-04-22 only
    ~22-24% of D1/D2 rows have ``soccer_program_url`` populated; the
    inline head-coach extractor already hits 80-95% **of-URL**, so
    everything here lifts the of-all coverage too.

    Strategy (per ``inline_coach_production_measure.md`` recommendation
    #3):

      1. Walk each Wikipedia "List of NCAA Division ..." page and
         capture each row's ``/wiki/<Article>`` link.
      2. Batch-fetch each program article's wikitext via the MediaWiki
         API and pull the ``| website = ...`` infobox value.
      3. Hand the discovered website to the existing SIDEARM probe
         (``resolve_soccer_program_url``) and write the verified URL
         back to ``colleges`` — also backfilling
         ``colleges.website`` when previously NULL.

    Scoped to D1/D2 only — D3 already has ~65% URL coverage via the
    Wikipedia category seeder, and D3 program articles are sparser
    (many D3 schools don't have their own Wikipedia article). NAIA
    has its own ``naia-resolve-urls`` flow.

    Args:
        --division D1|D2  (default: both — runs D1 then D2)
        --gender mens|womens|both  (default: both)
        --limit N  (cap total UPDATE attempts across all (div,gender) cells)
        --dry-run  (parse + probe only; no DB writes)
    """
    from extractors.ncaa_wikipedia_program_urls import (
        discover_program_urls,
        fetch_program_articles,
        fetch_program_websites,
        normalize_school_name,
        supported_divisions as wiki_supported_divisions,
        ProgramArticleRef,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-resolve-urls-wikipedia] psycopg2 not available; cannot query DB")
        sys.exit(1)

    division_arg = getattr(args, "division", None)
    if division_arg is None:
        divisions = list(wiki_supported_divisions())
    elif division_arg in wiki_supported_divisions():
        divisions = [division_arg]
    else:
        logger.error(
            "--source ncaa-resolve-urls-wikipedia: --division must be one of %s "
            "(got %r). D3 has its own URL-coverage path via the category seeder; "
            "NAIA has --source naia-resolve-urls.",
            wiki_supported_divisions(), division_arg,
        )
        sys.exit(2)

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source ncaa-resolve-urls-wikipedia: --gender must be mens|womens|both "
            "(got %r)", gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    limit = getattr(args, "limit", None)
    rate_delay = 1.0

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-resolve-urls-wikipedia] DATABASE_URL env var not set")
        sys.exit(1)

    import requests as _requests
    import time as _time

    session = _requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
    })

    grand = {"resolved": 0, "missed_no_article": 0, "missed_no_website": 0,
             "missed_no_sidearm": 0, "errors": 0, "websites_backfilled": 0}
    remaining_budget: Optional[int] = int(limit) if limit is not None else None

    try:
        conn = psycopg2.connect(database_url)
        try:
            for division in divisions:
                for gender in genders:
                    if remaining_budget is not None and remaining_budget <= 0:
                        break

                    scraper_key = f"ncaa-resolve-urls-wikipedia-{division.lower()}-{gender}"
                    run_log: Optional[ScrapeRunLogger] = None
                    if not dry_run:
                        run_log = ScrapeRunLogger(
                            scraper_key=scraper_key,
                            league_name=f"NCAA {division} {gender} URL resolver (wikipedia)",
                        )
                        run_log.start()

                    # Phase 1: get all (school_name, article_title) refs
                    # from the Wikipedia list page.
                    try:
                        refs = fetch_program_articles(division, gender, session=session)
                    except Exception as exc:
                        kind = _classify_exception(exc)
                        logger.error(
                            "[ncaa-resolve-urls-wikipedia] %s %s list fetch failed: %s",
                            division, gender, exc,
                        )
                        if run_log is not None:
                            run_log.finish_failed(kind, error_message=str(exc))
                        grand["errors"] += 1
                        continue

                    refs_by_norm = {
                        normalize_school_name(r.school_name): r for r in refs
                    }

                    # Phase 2: select the colleges rows that actually
                    # need a URL. We restrict to (division, gender)
                    # here so each cell's UPDATE budget is isolated.
                    select_sql = """
                        SELECT id, name, website
                        FROM colleges
                        WHERE division = %s
                          AND gender_program = %s
                          AND soccer_program_url IS NULL
                        ORDER BY name
                    """
                    cell_limit = remaining_budget
                    if cell_limit is not None:
                        select_sql += f" LIMIT {int(cell_limit)}"
                    with conn.cursor() as cur:
                        cur.execute(select_sql, (division, gender))
                        rows = cur.fetchall()

                    # Phase 3: which article titles do we actually need?
                    # Only fetch wikitext for matched rows.
                    matched: list[tuple[int, str, Optional[str], ProgramArticleRef]] = []
                    unmatched_names: list[str] = []
                    for row in rows:
                        college_id, name, website = row
                        norm = normalize_school_name(name)
                        ref = refs_by_norm.get(norm)
                        if ref is None:
                            unmatched_names.append(name)
                            continue
                        matched.append((college_id, name, website, ref))

                    if unmatched_names:
                        grand["missed_no_article"] += len(unmatched_names)
                        logger.info(
                            "[ncaa-resolve-urls-wikipedia] %s %s: %d row(s) had no "
                            "Wikipedia article match (sample: %s)",
                            division, gender, len(unmatched_names),
                            ", ".join(unmatched_names[:10])
                            + (f" … (+{len(unmatched_names) - 10})"
                               if len(unmatched_names) > 10 else ""),
                        )

                    titles_needed = sorted({r.article_title for (_id, _n, _w, r) in matched})
                    websites_map = fetch_program_websites(
                        titles_needed, session=session
                    ) if titles_needed else {}

                    # Phase 4: probe + write
                    discoveries = discover_program_urls(
                        [r for (_id, _n, _w, r) in matched],
                        gender,
                        session=session,
                        websites_override=websites_map,
                    )
                    discovery_by_title = {d.article_title: d for d in discoveries}

                    cell_resolved = 0
                    cell_missed_no_website = 0
                    cell_missed_no_sidearm = 0
                    cell_websites_backfilled = 0

                    for (college_id, name, existing_website, ref) in matched:
                        if remaining_budget is not None and remaining_budget <= 0:
                            break
                        d = discovery_by_title.get(ref.article_title)
                        if d is None:
                            cell_missed_no_website += 1
                            continue
                        if not d.website:
                            cell_missed_no_website += 1
                            continue
                        if not d.soccer_program_url:
                            cell_missed_no_sidearm += 1
                            # Still backfill website if we discovered one
                            # and the row doesn't have one yet.
                            if not existing_website and not dry_run:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        "UPDATE colleges SET website = %s "
                                        "WHERE id = %s AND website IS NULL",
                                        (d.website, college_id),
                                    )
                                conn.commit()
                                cell_websites_backfilled += 1
                            continue

                        if dry_run:
                            logger.info(
                                "[ncaa-resolve-urls-wikipedia] [dry-run] "
                                "would set %s (%s %s) → %s",
                                name, division, gender, d.soccer_program_url,
                            )
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE colleges "
                                    "SET soccer_program_url = %s, "
                                    "    website = COALESCE(website, %s) "
                                    "WHERE id = %s",
                                    (d.soccer_program_url, d.website, college_id),
                                )
                            conn.commit()
                            if not existing_website:
                                cell_websites_backfilled += 1

                        cell_resolved += 1
                        if remaining_budget is not None:
                            remaining_budget -= 1
                        _time.sleep(rate_delay)

                    grand["resolved"] += cell_resolved
                    grand["missed_no_website"] += cell_missed_no_website
                    grand["missed_no_sidearm"] += cell_missed_no_sidearm
                    grand["websites_backfilled"] += cell_websites_backfilled

                    logger.info(
                        "[ncaa-resolve-urls-wikipedia] %s %s: rows=%d matched=%d "
                        "resolved=%d no_website=%d no_sidearm=%d backfilled_websites=%d%s",
                        division, gender, len(rows), len(matched),
                        cell_resolved, cell_missed_no_website,
                        cell_missed_no_sidearm, cell_websites_backfilled,
                        " (dry-run)" if dry_run else "",
                    )

                    if run_log is not None:
                        run_log.finish_ok(
                            records_created=0,
                            records_updated=cell_resolved,
                            records_failed=cell_missed_no_website + cell_missed_no_sidearm,
                        )

                if remaining_budget is not None and remaining_budget <= 0:
                    break
        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[ncaa-resolve-urls-wikipedia] done: resolved=%d "
        "missed_no_article=%d missed_no_website=%d missed_no_sidearm=%d "
        "errors=%d websites_backfilled=%d%s",
        grand["resolved"], grand["missed_no_article"],
        grand["missed_no_website"], grand["missed_no_sidearm"],
        grand["errors"], grand["websites_backfilled"],
        " (dry-run)" if dry_run else "",
    )


def _handle_ncaa_discover_urls_google(args: argparse.Namespace) -> None:
    """Fill ``colleges.soccer_program_url`` (or ``website``) via Google CSE.

    Targets college rows where ``soccer_program_url IS NULL``.  Uses a
    two-pass query strategy per school (see
    ``extractors.ncaa_discover_urls_google`` for full detail):

      Pass 1 — ``"<name> <state> mens/womens soccer roster"``
        Finds the roster page directly. ~60-70% expected hit rate.

      Pass 2 — ``"<name> <state> athletics"`` (fallback)
        Finds the athletics homepage when pass 1 misses; writes to
        ``colleges.website`` so ``ncaa-resolve-urls`` can pick it up.

    Env vars required: ``GOOGLE_CSE_API_KEY``, ``GOOGLE_CSE_CX``.

    Optional flags:
      --division D1|D2|D3|NAIA  (default: all divisions with NULL URLs)
      --gender mens|womens|both  (default: both)
      --limit N  (default: 100; free tier is 100 queries/day)
      --dry-run
    """
    from extractors.ncaa_discover_urls_google import (
        discover_soccer_url,
        _QuotaExhausted,
    )
    from extractors.ncaa_directory import (
        resolve_soccer_program_url,
        USER_AGENT as _NCAA_UA,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-discover-urls-google] psycopg2 not available")
        sys.exit(1)

    api_key = os.environ.get("GOOGLE_CSE_API_KEY", "").strip()
    cx = os.environ.get("GOOGLE_CSE_CX", "").strip()
    if not api_key or not cx:
        logger.error(
            "[ncaa-discover-urls-google] GOOGLE_CSE_API_KEY and GOOGLE_CSE_CX "
            "env vars are required"
        )
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-discover-urls-google] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = getattr(args, "division", None)
    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    dry_run = bool(getattr(args, "dry_run", False))
    limit = getattr(args, "limit", None) or 100

    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "[ncaa-discover-urls-google] --gender must be mens|womens|both "
            "(got %r)", gender_arg,
        )
        sys.exit(2)

    import requests as _requests
    import time as _time

    sidearm_session = _requests.Session()
    sidearm_session.headers.update({
        "User-Agent": _NCAA_UA,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })
    cse_session = _requests.Session()

    resolved_direct = 0
    resolved_via_sidearm = 0
    website_filled = 0
    missed = 0
    errors = 0
    quota_hit = False

    try:
        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                base_sql = """
                    SELECT id, name, state, gender_program
                    FROM colleges
                    WHERE soccer_program_url IS NULL
                """
                clauses: list[str] = []
                params: list = []
                if division_arg:
                    clauses.append("AND division = %s")
                    params.append(division_arg)
                if gender_arg != "both":
                    clauses.append("AND gender_program = %s")
                    params.append(gender_arg)
                sql = base_sql + " ".join(clauses) + " ORDER BY name LIMIT %s"
                params.append(int(limit))
                cur.execute(sql, params)
                rows = cur.fetchall()

            logger.info(
                "[ncaa-discover-urls-google] %d row(s) with NULL soccer_program_url "
                "to query%s",
                len(rows), " (dry-run)" if dry_run else "",
            )
            est_cost_usd = len(rows) * 2 / 1000 * 5  # 2 passes × $5/1000 queries
            logger.info(
                "[ncaa-discover-urls-google] estimated CSE cost: $%.2f "
                "(%.0f queries × $5/1000)",
                est_cost_usd, len(rows) * 2,
            )

            for college_id, name, state, gender_program in rows:
                gender = gender_program if gender_program in ("mens", "womens") else "mens"
                tag = f"{name} ({gender})"
                try:
                    result = discover_soccer_url(
                        name, state, gender,
                        api_key=api_key, cx=cx, session=cse_session,
                    )
                except _QuotaExhausted:
                    logger.warning(
                        "[ncaa-discover-urls-google] quota exhausted after %d "
                        "queries — run again tomorrow",
                        resolved_direct + resolved_via_sidearm + website_filled + missed,
                    )
                    quota_hit = True
                    break
                except Exception as exc:
                    logger.warning(
                        "[ncaa-discover-urls-google] error for %s: %s", tag, exc
                    )
                    errors += 1
                    _time.sleep(1.0)
                    continue

                if result is None:
                    logger.debug(
                        "[ncaa-discover-urls-google] MISS %s — no classifiable result",
                        tag,
                    )
                    missed += 1
                    _time.sleep(1.0)
                    continue

                url, kind = result

                if kind == "soccer_program_url":
                    if dry_run:
                        logger.info(
                            "[ncaa-discover-urls-google] [dry-run] %s → soccer_program_url=%s",
                            tag, url,
                        )
                    else:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE colleges SET soccer_program_url = %s WHERE id = %s",
                                (url, college_id),
                            )
                        conn.commit()
                        logger.info(
                            "[ncaa-discover-urls-google] %s → soccer_program_url=%s",
                            tag, url,
                        )
                    resolved_direct += 1

                elif kind == "website":
                    # Try SIDEARM probe on the discovered website to get the
                    # soccer-specific URL in one step.
                    probed_url: Optional[str] = None
                    try:
                        probed_url = resolve_soccer_program_url(
                            url, gender, session=sidearm_session
                        )
                    except Exception as exc:
                        logger.debug(
                            "[ncaa-discover-urls-google] SIDEARM probe failed "
                            "for %s (%s): %s", tag, url, exc,
                        )

                    if probed_url:
                        if dry_run:
                            logger.info(
                                "[ncaa-discover-urls-google] [dry-run] %s → "
                                "website=%s soccer_program_url=%s",
                                tag, url, probed_url,
                            )
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """UPDATE colleges
                                       SET website = COALESCE(website, %s),
                                           soccer_program_url = %s
                                       WHERE id = %s""",
                                    (url, probed_url, college_id),
                                )
                            conn.commit()
                        resolved_via_sidearm += 1
                    else:
                        # Fill website only; ncaa-resolve-urls can retry later
                        # with the newly populated website column.
                        if dry_run:
                            logger.info(
                                "[ncaa-discover-urls-google] [dry-run] %s → "
                                "website=%s (SIDEARM miss — URL only)",
                                tag, url,
                            )
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE colleges SET website = COALESCE(website, %s) "
                                    "WHERE id = %s",
                                    (url, college_id),
                                )
                            conn.commit()
                        website_filled += 1

                _time.sleep(1.5)  # polite between CSE queries

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            sidearm_session.close()
        except Exception:
            pass
        try:
            cse_session.close()
        except Exception:
            pass

    total_updated = resolved_direct + resolved_via_sidearm + website_filled
    logger.info(
        "[ncaa-discover-urls-google] done: "
        "soccer_program_url_direct=%d soccer_program_url_via_sidearm=%d "
        "website_only=%d missed=%d errors=%d quota_hit=%s%s",
        resolved_direct, resolved_via_sidearm,
        website_filled, missed, errors, quota_hit,
        " (dry-run)" if dry_run else "",
    )
    if total_updated > 0:
        logger.info(
            "[ncaa-discover-urls-google] total rows updated: %d "
            "(run ncaa-resolve-urls after to probe website-only fills)",
            total_updated,
        )


def _handle_ncaa_enrich_websites_ncaaid(args: argparse.Namespace) -> None:
    """Fill ``colleges.website`` from stats.ncaa.org team pages via ``ncaa_id``.

    For each college row with ``ncaa_id IS NOT NULL AND website IS NULL``,
    fetches ``https://stats.ncaa.org/team/<ncaa_id>``, extracts the outbound
    athletics-homepage link, and writes it to ``colleges.website``.

    After this source runs, execute ``--source ncaa-resolve-urls`` to do the
    SIDEARM probe that fills ``soccer_program_url`` from the newly-written
    ``website`` values.

    stats.ncaa.org is already successfully scraped by ``ncaa-seed-d1``.
    Individual team pages are static HTML (not SPA) so no Playwright needed.

    Optional flags:
      --division D1|D2|D3|NAIA  (default: D1 — only D1 rows carry ncaa_id)
      --limit N   (default: 200)
      --dry-run
    """
    import time as _time

    from extractors.ncaa_enrich_websites import (
        _make_session,
        _fetch_html,
        _team_url,
        extract_school_website,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-enrich-websites-ncaaid] psycopg2 not available")
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-enrich-websites-ncaaid] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = getattr(args, "division", None) or "D1"
    dry_run = bool(getattr(args, "dry_run", False))
    limit = int(getattr(args, "limit", None) or 200)

    session = _make_session()

    filled = 0
    missed = 0
    errors = 0

    try:
        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                sql = """
                    SELECT id, name, division, gender_program, ncaa_id
                    FROM colleges
                    WHERE ncaa_id IS NOT NULL
                      AND website IS NULL
                      AND division = %s
                    ORDER BY name
                    LIMIT %s
                """
                cur.execute(sql, (division_arg, limit))
                rows = cur.fetchall()

            logger.info(
                "[ncaa-enrich-websites-ncaaid] %d %s college(s) with ncaa_id "
                "but no website%s",
                len(rows), division_arg, " (dry-run)" if dry_run else "",
            )

            for college_id, name, division, gender_program, ncaa_id in rows:
                tag = f"{name} ({division} {gender_program})"
                url = _team_url(ncaa_id)

                html = _fetch_html(url, session)
                _time.sleep(1.5)

                if not html:
                    logger.debug(
                        "[ncaa-enrich-websites-ncaaid] could not fetch %s for %s",
                        url, tag,
                    )
                    errors += 1
                    continue

                website = extract_school_website(html, ncaa_id)
                if not website:
                    logger.debug(
                        "[ncaa-enrich-websites-ncaaid] no website found for %s", tag,
                    )
                    missed += 1
                    continue

                if dry_run:
                    logger.info(
                        "[ncaa-enrich-websites-ncaaid] [dry-run] %s → website=%s",
                        tag, website,
                    )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE colleges SET website = %s WHERE id = %s",
                            (website, college_id),
                        )
                    conn.commit()
                    logger.info(
                        "[ncaa-enrich-websites-ncaaid] %s → website=%s", tag, website,
                    )
                filled += 1

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[ncaa-enrich-websites-ncaaid] done: filled=%d missed=%d errors=%d%s",
        filled, missed, errors, " (dry-run)" if dry_run else "",
    )
    if filled > 0 and not dry_run:
        logger.info(
            "[ncaa-enrich-websites-ncaaid] run --source ncaa-resolve-urls "
            "to SIDEARM-probe the %d newly-filled website rows",
            filled,
        )


def _handle_ncaa_discover_urls_ncsa(args: argparse.Namespace) -> None:
    """Fill ``colleges.soccer_program_url`` via NCSA Sports college directory.

    Replaces / complements ``ncaa-discover-urls-google`` for installs where
    the Google CSE engine cannot search the entire web (engines created after
    Google's January 20 2026 policy change are permanently site-restricted).

    Scrapes:
      https://www.ncsasports.org/womens-soccer/colleges
      https://www.ncsasports.org/mens-soccer/colleges

    For each school with NULL ``soccer_program_url`` the handler:
      1. Fetches the school's NCSA profile page.
      2. Extracts the outbound athletics / program URL.
      3. If it's a direct soccer URL → writes to ``colleges.soccer_program_url``.
      4. If it's an athletics homepage → SIDEARM-probes it for the soccer
         page; writes both ``website`` + ``soccer_program_url`` on success.

    Optional flags:
      --division D1|D2|D3|NAIA  (default: all)
      --gender mens|womens|both  (default: both)
      --limit N  (default: 500)
      --dry-run
    """
    import time as _time

    from extractors.ncaa_discover_urls_ncsa import (
        _make_session,
        _fetch_html,
        _DIRECTORY_URLS,
        parse_directory_page,
        extract_program_url_from_profile,
        classify_url,
        _best_match,
    )
    from extractors.ncaa_directory import (
        resolve_soccer_program_url,
        USER_AGENT as _NCAA_UA,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-discover-urls-ncsa] psycopg2 not available")
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-discover-urls-ncsa] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = getattr(args, "division", None)
    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    dry_run = bool(getattr(args, "dry_run", False))
    limit = int(getattr(args, "limit", None) or 500)

    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "[ncaa-discover-urls-ncsa] --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    import requests as _requests

    ncsa_session = _make_session()
    sidearm_session = _requests.Session()
    sidearm_session.headers.update({
        "User-Agent": _NCAA_UA,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })

    resolved_direct = 0
    resolved_via_sidearm = 0
    website_filled = 0
    missed = 0
    errors = 0

    try:
        conn = psycopg2.connect(database_url)
        try:
            # Load all colleges with NULL soccer_program_url once.
            with conn.cursor() as cur:
                base_sql = """
                    SELECT id, name, division, gender_program
                    FROM colleges
                    WHERE soccer_program_url IS NULL
                """
                clauses: list[str] = []
                params: list = []
                if division_arg:
                    clauses.append("AND division = %s")
                    params.append(division_arg)
                if gender_arg != "both":
                    clauses.append("AND gender_program = %s")
                    params.append(gender_arg)
                cur.execute(base_sql + " ".join(clauses) + " ORDER BY name", params)
                all_null_rows = cur.fetchall()

            logger.info(
                "[ncaa-discover-urls-ncsa] %d college(s) with NULL "
                "soccer_program_url to process%s",
                len(all_null_rows), " (dry-run)" if dry_run else "",
            )

            processed = 0

            for gender in genders:
                if processed >= limit:
                    break

                dir_url = _DIRECTORY_URLS[gender]
                logger.info(
                    "[ncaa-discover-urls-ncsa] fetching %s directory: %s",
                    gender, dir_url,
                )
                html = _fetch_html(dir_url, ncsa_session)
                if not html:
                    logger.warning(
                        "[ncaa-discover-urls-ncsa] could not fetch NCSA %s "
                        "directory — skipping gender",
                        gender,
                    )
                    continue

                listings = parse_directory_page(html, gender)
                if not listings:
                    logger.warning(
                        "[ncaa-discover-urls-ncsa] no listings parsed from %s "
                        "directory — NCSA may have changed their page structure; "
                        "check %s manually",
                        gender, dir_url,
                    )
                    continue

                # Filter to colleges we actually need (NULL url + matching gender)
                target_rows = [
                    r for r in all_null_rows if r[3] == gender
                ]
                if division_arg:
                    target_rows = [r for r in target_rows if r[2] == division_arg]

                logger.info(
                    "[ncaa-discover-urls-ncsa] %d %s target(s); %d NCSA listings",
                    len(target_rows), gender, len(listings),
                )

                for listing in listings:
                    if processed >= limit:
                        break

                    # Fuzzy-match listing.name → DB row
                    match = _best_match(listing.name, target_rows, threshold=88)
                    if match is None:
                        logger.debug(
                            "[ncaa-discover-urls-ncsa] no DB match for NCSA "
                            "listing %r",
                            listing.name,
                        )
                        continue

                    college_id, db_name, db_div, db_gender = match
                    tag = f"{db_name} ({db_div} {db_gender})"

                    # Fetch NCSA profile page
                    profile_html = _fetch_html(listing.profile_url, ncsa_session)
                    _time.sleep(1.5)
                    processed += 1

                    if not profile_html:
                        logger.debug(
                            "[ncaa-discover-urls-ncsa] could not fetch profile for %s",
                            tag,
                        )
                        errors += 1
                        continue

                    raw_url = extract_program_url_from_profile(profile_html)
                    if not raw_url:
                        logger.debug(
                            "[ncaa-discover-urls-ncsa] no outbound URL found on "
                            "NCSA profile for %s",
                            tag,
                        )
                        missed += 1
                        continue

                    kind = classify_url(raw_url)
                    if kind is None:
                        logger.debug(
                            "[ncaa-discover-urls-ncsa] URL %r for %s not "
                            "classifiable — skipping",
                            raw_url, tag,
                        )
                        missed += 1
                        continue

                    if kind == "soccer_program_url":
                        if dry_run:
                            logger.info(
                                "[ncaa-discover-urls-ncsa] [dry-run] %s → "
                                "soccer_program_url=%s",
                                tag, raw_url,
                            )
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE colleges SET soccer_program_url = %s "
                                    "WHERE id = %s",
                                    (raw_url, college_id),
                                )
                            conn.commit()
                            logger.info(
                                "[ncaa-discover-urls-ncsa] %s → soccer_program_url=%s",
                                tag, raw_url,
                            )
                        resolved_direct += 1

                    elif kind == "website":
                        # Probe SIDEARM on the athletics homepage.
                        # If the root domain misses, also try athletics.{domain}
                        # (common pattern: athletics.school.edu/sports/womens-soccer).
                        probed_url: Optional[str] = None
                        _probe_urls = [raw_url]
                        try:
                            from urllib.parse import urlparse as _urlparse
                            _p = _urlparse(raw_url)
                            _host = _p.netloc.lstrip("www.")
                            _athletics_url = f"{_p.scheme}://athletics.{_host}/"
                            if _athletics_url.rstrip("/") != raw_url.rstrip("/"):
                                _probe_urls.append(_athletics_url)
                        except Exception:
                            pass
                        for _probe_url in _probe_urls:
                            try:
                                probed_url = resolve_soccer_program_url(
                                    _probe_url, gender, session=sidearm_session
                                )
                            except Exception as exc:
                                logger.debug(
                                    "[ncaa-discover-urls-ncsa] SIDEARM probe failed "
                                    "for %s (%s): %s",
                                    tag, _probe_url, exc,
                                )
                            if probed_url:
                                break

                        if probed_url:
                            if dry_run:
                                logger.info(
                                    "[ncaa-discover-urls-ncsa] [dry-run] %s → "
                                    "website=%s soccer_program_url=%s",
                                    tag, raw_url, probed_url,
                                )
                            else:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        """UPDATE colleges
                                           SET website = COALESCE(website, %s),
                                               soccer_program_url = %s
                                           WHERE id = %s""",
                                        (raw_url, probed_url, college_id),
                                    )
                                conn.commit()
                                logger.info(
                                    "[ncaa-discover-urls-ncsa] %s → "
                                    "website=%s soccer_program_url=%s",
                                    tag, raw_url, probed_url,
                                )
                            resolved_via_sidearm += 1
                        else:
                            # Fill website only; ncaa-resolve-urls can retry.
                            if dry_run:
                                logger.info(
                                    "[ncaa-discover-urls-ncsa] [dry-run] %s → "
                                    "website=%s (SIDEARM miss)",
                                    tag, raw_url,
                                )
                            else:
                                with conn.cursor() as cur:
                                    cur.execute(
                                        "UPDATE colleges "
                                        "SET website = COALESCE(website, %s) "
                                        "WHERE id = %s",
                                        (raw_url, college_id),
                                    )
                                conn.commit()
                                logger.info(
                                    "[ncaa-discover-urls-ncsa] %s → "
                                    "website=%s (SIDEARM miss — URL only)",
                                    tag, raw_url,
                                )
                            website_filled += 1

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            ncsa_session.close()
        except Exception:
            pass
        try:
            sidearm_session.close()
        except Exception:
            pass

    total_updated = resolved_direct + resolved_via_sidearm + website_filled
    logger.info(
        "[ncaa-discover-urls-ncsa] done: "
        "soccer_program_url_direct=%d soccer_program_url_via_sidearm=%d "
        "website_only=%d missed=%d errors=%d%s",
        resolved_direct, resolved_via_sidearm,
        website_filled, missed, errors,
        " (dry-run)" if dry_run else "",
    )
    if total_updated > 0:
        logger.info(
            "[ncaa-discover-urls-ncsa] total rows updated: %d "
            "(run ncaa-resolve-urls after to probe website-only fills)",
            total_updated,
        )


def _handle_naia_resolve_urls(args: argparse.Namespace) -> None:
    """Resolve NAIA ``colleges.website`` + ``soccer_program_url`` via naia.org.

    NAIA is unlike NCAA: the seed flow (``naia_directory.parse_naia_index``)
    upserts (name, state) only — there is no ``website`` column on
    naia.org's index page. So ``ncaa-resolve-urls`` (which requires
    ``website IS NOT NULL``) skips every NAIA row.

    This handler closes that gap with a two-phase per-program lookup:

      1. Fetch each gender's naia.org index ONCE to build a
         ``lower(name) -> slug`` map (``parse_naia_index_slugs``).
      2. For every NAIA college row missing ``soccer_program_url``,
         look up its slug, fetch the per-team detail page, extract the
         athletics-website outbound link, and probe SIDEARM via the
         shared ``resolve_soccer_program_url``. Backfill ``website``
         (always when extracted) and ``soccer_program_url`` (when the
         SIDEARM probe hits).

    Joining via ``lower(college.name)`` is fragile across naia.org
    name-format drift (e.g. "Wayland Baptist" vs "Wayland Baptist
    University"); misses are logged for operator review and fall back
    to manual fill — same escape hatch as ``ncaa-resolve-urls``.

    Respects ``--limit N``, ``--gender mens|womens|both``, ``--dry-run``.
    Rate-limits at 1.5s between detail-page fetches (slightly slower
    than NCAA because every row requires an HTML GET, not a HEAD).
    """
    from extractors.naia_directory import (
        _normalize_naia_name,
        discover_naia_program_url,
        fuzzy_match_naia_slug,
        parse_naia_index_slug_records,
        parse_naia_index_slugs,
        directory_url as naia_directory_url,
        USER_AGENT as _NAIA_UA,
        supported_genders as naia_supported_genders,
    )
    try:
        from config import FUZZY_THRESHOLD as _NAIA_FUZZY_THRESHOLD  # type: ignore
    except ImportError:
        _NAIA_FUZZY_THRESHOLD = 88

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[naia-resolve-urls] psycopg2 not available; cannot query DB")
        sys.exit(1)

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = list(naia_supported_genders())
    elif gender_arg in naia_supported_genders():
        genders = [gender_arg]
    else:
        logger.error(
            "--source naia-resolve-urls: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    limit = getattr(args, "limit", None)
    rate_delay = 1.5

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[naia-resolve-urls] DATABASE_URL env var not set")
        sys.exit(1)

    import requests as _requests
    import time as _time
    from utils import http as _proxy_http

    session = _requests.Session()
    session.headers.update({
        "User-Agent": _NAIA_UA,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })

    # Phase 1: fetch the naia.org index once per gender to build the
    # name → slug map. Routed through utils.http so naia.org calls go
    # through proxy_config.yaml when populated (Replit egress IPs hit
    # the WAF with HTTP 405 on direct calls). If proxies aren't
    # configured the wrapper falls back to a direct request, so the
    # same code path works in dev (empty config) and prod.
    slugs_by_gender: dict[str, dict[str, str]] = {}
    records_by_gender: dict[str, list[dict]] = {}
    for g in genders:
        idx_url = naia_directory_url(g)
        try:
            resp = _proxy_http.get(
                idx_url,
                timeout=20,
                headers={
                    "User-Agent": _NAIA_UA,
                    "Accept": "text/html,application/xhtml+xml,*/*",
                },
                allow_redirects=True,
            )
        except _requests.RequestException as exc:
            logger.warning(
                "[naia-resolve-urls] index fetch failed for %s (%s): %s",
                g, idx_url, exc,
            )
            slugs_by_gender[g] = {}
            records_by_gender[g] = []
            continue
        if resp.status_code != 200:
            logger.warning(
                "[naia-resolve-urls] index fetch %s -> HTTP %d; skipping %s",
                idx_url, resp.status_code, g,
            )
            slugs_by_gender[g] = {}
            records_by_gender[g] = []
            continue
        slugs_by_gender[g] = parse_naia_index_slugs(resp.text, g)
        records_by_gender[g] = parse_naia_index_slug_records(resp.text, g)
        logger.info(
            "[naia-resolve-urls] %s index: %d slug(s) parsed",
            g, len(slugs_by_gender[g]),
        )

    if not any(slugs_by_gender.values()):
        logger.error(
            "[naia-resolve-urls] no slugs available — aborting (likely "
            "naia.org WAF block; configure proxy_config.yaml)"
        )
        try:
            session.close()
        except Exception:
            pass
        sys.exit(1)

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key="naia-resolve-urls",
            league_name="NAIA URL resolver",
        )
        run_log.start()

    resolved = 0
    websites_only = 0
    missed_slug = 0
    missed_website = 0
    errors = 0
    fuzzy_matched = 0
    unresolved_names: list[str] = []

    try:
        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                select_sql = """
                    SELECT id, name, gender_program, state
                    FROM colleges
                    WHERE division = 'NAIA'
                      AND soccer_program_url IS NULL
                      AND gender_program = ANY(%s)
                    ORDER BY name
                """
                if limit is not None:
                    select_sql += f" LIMIT {int(limit)}"
                cur.execute(select_sql, (genders,))
                rows = cur.fetchall()

            logger.info(
                "[naia-resolve-urls] %d NAIA college(s) to resolve%s",
                len(rows), " (dry-run)" if dry_run else "",
            )

            for row in rows:
                college_id, name, gender_program, db_state = row
                # Three-pass slug join:
                #   1) exact lowercased DB name against naia.org anchor text
                #   2) suffix-normalized form (drops University/College/Institute)
                #   3) fuzzy token_sort_ratio >= FUZZY_THRESHOLD (88), gated
                #      on state when state is known on both sides — catches
                #      "St. Ambrose"/"Saint Ambrose", "Mount Marty"/"Mt. Marty",
                #      etc. that survive normalization.
                gender_slugs = slugs_by_gender.get(gender_program, {})
                lower_name = (name or "").lower()
                slug = gender_slugs.get(lower_name)
                if not slug:
                    slug = gender_slugs.get(_normalize_naia_name(name or ""))
                if slug:
                    logger.debug(
                        "[naia-resolve-urls] exact match: %s -> %s",
                        name, slug,
                    )
                else:
                    fuzzy_hit = fuzzy_match_naia_slug(
                        name or "",
                        db_state,
                        records_by_gender.get(gender_program, []),
                        threshold=_NAIA_FUZZY_THRESHOLD,
                    )
                    if fuzzy_hit is not None:
                        slug, matched_name, matched_state, score = fuzzy_hit
                        fuzzy_matched += 1
                        logger.info(
                            "[naia-resolve-urls] fuzzy match accepted: "
                            "%s (%s) -> %s (%s) score=%d slug=%s",
                            name, db_state or "?",
                            matched_name, matched_state or "?",
                            score, slug,
                        )
                if not slug:
                    missed_slug += 1
                    unresolved_names.append(f"{name} (no slug)")
                    continue

                try:
                    website, program_url = discover_naia_program_url(
                        slug, gender_program, session=session
                    )
                except Exception as exc:
                    logger.warning(
                        "[naia-resolve-urls] discover error for %s: %s",
                        name, exc,
                    )
                    errors += 1
                    _time.sleep(rate_delay)
                    continue

                if website is None:
                    missed_website += 1
                    unresolved_names.append(f"{name} (no website)")
                    _time.sleep(rate_delay)
                    continue

                if program_url is None:
                    websites_only += 1
                    if dry_run:
                        logger.info(
                            "[naia-resolve-urls] [dry-run] %s: website=%s "
                            "(no SIDEARM hit)",
                            name, website,
                        )
                    else:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE colleges SET website = "
                                "COALESCE(website, %s) WHERE id = %s",
                                (website, college_id),
                            )
                        conn.commit()
                    _time.sleep(rate_delay)
                    continue

                if dry_run:
                    logger.info(
                        "[naia-resolve-urls] [dry-run] %s: website=%s "
                        "program_url=%s",
                        name, website, program_url,
                    )
                else:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE colleges SET "
                            "website = COALESCE(website, %s), "
                            "soccer_program_url = %s "
                            "WHERE id = %s",
                            (website, program_url, college_id),
                        )
                    conn.commit()

                resolved += 1
                _time.sleep(rate_delay)

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[naia-resolve-urls] done: resolved=%d websites_only=%d "
        "missed_slug=%d missed_website=%d errors=%d "
        "fuzzy_matched=%d%s",
        resolved, websites_only, missed_slug, missed_website, errors,
        fuzzy_matched,
        " (dry-run)" if dry_run else "",
    )
    if unresolved_names:
        logger.info(
            "[naia-resolve-urls] unresolved (manual fill needed): %s",
            ", ".join(unresolved_names[:25])
            + (
                f" … ({len(unresolved_names) - 25} more)"
                if len(unresolved_names) > 25
                else ""
            ),
        )

    if run_log is not None:
        run_log.finish_ok(
            records_created=0,
            records_updated=resolved + websites_only,
            records_failed=errors + missed_slug + missed_website,
        )


def _handle_ncaa_seed_d1(args: argparse.Namespace) -> None:
    """Seed ``colleges`` from stats.ncaa.org's D1 men's + women's lists.

    Walks ``/team/inst_team_list?sport_code=MSO|WSO&division=1`` and
    upserts seed rows via ``ingest.ncaa_roster_writer.upsert_college``
    (hits ``colleges_name_division_gender_uq`` — idempotent).

    ``soccer_program_url`` is left NULL by design; PR-2 fills it.

    Optional flags:
      --gender mens|womens  (default: both)
      --dry-run             (parse only; no DB writes)
    """
    from extractors.ncaa_directory import fetch_d1_programs
    from ingest.ncaa_roster_writer import upsert_college

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source ncaa-seed-d1: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    grand = {"fetched": 0, "inserted": 0, "updated": 0, "errors": 0}

    for gender in genders:
        run_log: Optional[ScrapeRunLogger] = None
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=f"ncaa-seed-d1-{gender}",
                league_name=f"NCAA D1 {gender}",
            )
            run_log.start(source_url=f"https://stats.ncaa.org/team/inst_team_list?sport_code={'MSO' if gender == 'mens' else 'WSO'}&division=1")

        try:
            seeds = fetch_d1_programs(gender)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error("[ncaa-seed-d1] fetch failed for %s: %s", gender, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=f"ncaa-seed-d1-{gender}",
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=None,
                league_name=f"NCAA D1 {gender}",
            )
            grand["errors"] += 1
            continue

        grand["fetched"] += len(seeds)
        inserted = updated = errors = 0
        for seed in seeds:
            try:
                _cid, was_inserted = upsert_college(seed.to_upsert_row(), dry_run=dry_run)
            except Exception as exc:
                logger.warning("[ncaa-seed-d1] upsert failed for %s: %s", seed.name, exc)
                errors += 1
                continue
            if dry_run:
                continue
            if was_inserted:
                inserted += 1
            else:
                updated += 1

        logger.info(
            "[ncaa-seed-d1] %s: fetched=%d inserted=%d updated=%d errors=%d%s",
            gender, len(seeds), inserted, updated, errors,
            " (dry-run)" if dry_run else "",
        )
        grand["inserted"] += inserted
        grand["updated"] += updated
        grand["errors"] += errors

        if run_log is not None:
            run_log.finish_ok(
                records_created=inserted,
                records_updated=updated,
                records_failed=errors,
            )

    logger.info(
        "[ncaa-seed-d1] done: fetched=%d inserted=%d updated=%d errors=%d%s",
        grand["fetched"], grand["inserted"], grand["updated"], grand["errors"],
        " (dry-run)" if dry_run else "",
    )


def _handle_ncaa_seed_d2_d3(args: argparse.Namespace) -> None:
    """Seed/update ``colleges.ncaa_id`` from stats.ncaa.org's D2 and D3 lists.

    Walks ``/team/inst_team_list?sport_code=MSO|WSO&division=2|3`` and
    upserts seed rows via ``ingest.ncaa_roster_writer.upsert_college``.
    Also writes ``ncaa_id`` for existing rows that were seeded from Wikipedia
    without one (the primary value of this source for D2/D3).

    After this runs, execute ``--source ncaa-enrich-websites-ncaaid`` with
    ``--division D2`` (or D3) to fetch each team's current athletics URL from
    the stats.ncaa.org team page — the NCAA maintains these links and they
    reflect current domains, unlike the stale Wikipedia ``| website =`` field.

    Optional flags:
      --division D2|D3|both  (default: both)
      --gender mens|womens|both  (default: both)
      --dry-run
    """
    from extractors.ncaa_directory import fetch_programs
    from ingest.ncaa_roster_writer import upsert_college

    division_arg = (getattr(args, "division", None) or "both").upper()
    if division_arg in ("D2", "D3"):
        divisions = [division_arg]
    elif division_arg in ("BOTH", "ALL"):
        divisions = ["D2", "D3"]
    else:
        logger.error(
            "--source ncaa-seed-d2-d3: --division must be D2|D3|both (got %r)",
            division_arg,
        )
        sys.exit(2)

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source ncaa-seed-d2-d3: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    grand = {"fetched": 0, "inserted": 0, "updated": 0, "errors": 0}

    for division in divisions:
        for gender in genders:
            run_log: Optional[ScrapeRunLogger] = None
            sport_code = "MSO" if gender == "mens" else "WSO"
            div_num = division[1]  # "2" or "3"
            source_url = f"https://stats.ncaa.org/team/inst_team_list?sport_code={sport_code}&division={div_num}"

            if not dry_run:
                run_log = ScrapeRunLogger(
                    scraper_key=f"ncaa-seed-d2-d3-{division.lower()}-{gender}",
                    league_name=f"NCAA {division} {gender}",
                )
                run_log.start(source_url=source_url)

            try:
                seeds = fetch_programs(gender, division)
            except Exception as exc:
                kind = _classify_exception(exc)
                logger.error(
                    "[ncaa-seed-d2-d3] fetch failed for %s %s: %s",
                    division, gender, exc,
                )
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                grand["errors"] += 1
                continue

            grand["fetched"] += len(seeds)
            inserted = updated = errors = 0
            for seed in seeds:
                try:
                    _cid, was_inserted = upsert_college(seed.to_upsert_row(), dry_run=dry_run)
                except Exception as exc:
                    logger.warning(
                        "[ncaa-seed-d2-d3] upsert failed for %s: %s", seed.name, exc,
                    )
                    errors += 1
                    continue
                if dry_run:
                    continue
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1

            logger.info(
                "[ncaa-seed-d2-d3] %s %s: fetched=%d inserted=%d updated=%d errors=%d%s",
                division, gender, len(seeds), inserted, updated, errors,
                " (dry-run)" if dry_run else "",
            )
            grand["inserted"] += inserted
            grand["updated"] += updated
            grand["errors"] += errors

            if run_log is not None:
                run_log.finish_ok(
                    records_created=inserted,
                    records_updated=updated,
                    records_failed=errors,
                )

    logger.info(
        "[ncaa-seed-d2-d3] done: fetched=%d inserted=%d updated=%d errors=%d%s",
        grand["fetched"], grand["inserted"], grand["updated"], grand["errors"],
        " (dry-run)" if dry_run else "",
    )


def _handle_ncaa_seed_wikipedia(args: argparse.Namespace) -> None:
    """Seed ``colleges`` from Wikipedia's D2/D3/NAIA soccer-program lists.

    Sibling of ``_handle_ncaa_seed_d1``. Stats.ncaa.org blocks our
    scraper; Wikipedia is open and has maintained "List of ..."
    tables per division that cover the full universe of programs.

    Requires ``--division D2|D3|NAIA``. Optional flags:
      --gender mens|womens  (default: both)
      --dry-run             (parse only; no DB writes)

    NJCAA is NOT supported — Wikipedia's NJCAA coverage is fragmented
    across regional conference pages with no consolidated program
    list. Operator would need a curated CSV for that division.
    """
    from extractors.ncaa_wikipedia_directory import (
        fetch_division_programs,
        directory_url,
        supported_divisions,
    )
    from ingest.ncaa_roster_writer import upsert_college

    division = getattr(args, "division", None)
    if not division:
        logger.error(
            "--source ncaa-seed-wikipedia requires --division "
            "(one of %s)",
            supported_divisions(),
        )
        sys.exit(2)
    if division not in supported_divisions():
        logger.error(
            "--source ncaa-seed-wikipedia: --division must be one of %s (got %r)",
            supported_divisions(), division,
        )
        sys.exit(2)

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source ncaa-seed-wikipedia: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    grand = {"fetched": 0, "inserted": 0, "updated": 0, "errors": 0}

    for gender in genders:
        scraper_key = f"ncaa-seed-wikipedia-{division.lower()}-{gender}"
        run_log: Optional[ScrapeRunLogger] = None
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NCAA {division} {gender}",
            )
            run_log.start(source_url=directory_url(division, gender))

        try:
            seeds = fetch_division_programs(division, gender)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error(
                "[ncaa-seed-wikipedia] %s %s fetch failed: %s",
                division, gender, exc,
            )
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=directory_url(division, gender),
                league_name=f"NCAA {division} {gender}",
            )
            grand["errors"] += 1
            continue

        grand["fetched"] += len(seeds)
        inserted = updated = errors = 0
        for seed in seeds:
            try:
                _cid, was_inserted = upsert_college(seed.to_upsert_row(), dry_run=dry_run)
            except Exception as exc:
                logger.warning(
                    "[ncaa-seed-wikipedia] upsert failed for %s (%s %s): %s",
                    seed.name, division, gender, exc,
                )
                errors += 1
                continue
            if dry_run:
                continue
            if was_inserted:
                inserted += 1
            else:
                updated += 1

        logger.info(
            "[ncaa-seed-wikipedia] %s %s: fetched=%d inserted=%d updated=%d errors=%d%s",
            division, gender, len(seeds), inserted, updated, errors,
            " (dry-run)" if dry_run else "",
        )
        grand["inserted"] += inserted
        grand["updated"] += updated
        grand["errors"] += errors

        if run_log is not None:
            run_log.finish_ok(
                records_created=inserted,
                records_updated=updated,
                records_failed=errors,
            )

    logger.info(
        "[ncaa-seed-wikipedia] %s done: fetched=%d inserted=%d updated=%d errors=%d%s",
        division, grand["fetched"], grand["inserted"], grand["updated"], grand["errors"],
        " (dry-run)" if dry_run else "",
    )


def _handle_ncaa_seed_wikipedia_category(args: argparse.Namespace) -> None:
    """Seed ``colleges`` from Wikipedia's MediaWiki category pages.

    Sibling of ``_handle_ncaa_seed_wikipedia``. Used when the plain
    ``List_of_...`` page doesn't exist for a given division. D3 is the
    motivating case (April 2026: list pages 404; category pages still
    200). Partial coverage — only schools with their own Wikipedia
    article.

    Requires ``--division D3``. Optional flags:
      --gender mens|womens  (default: both)
      --dry-run             (parse only; no DB writes)
    """
    from extractors.ncaa_wikipedia_category_directory import (
        fetch_division_programs,
        category_title,
        supported_divisions_categories,
    )
    from ingest.ncaa_roster_writer import upsert_college

    division = getattr(args, "division", None)
    if not division:
        logger.error(
            "--source ncaa-seed-wikipedia-category requires --division "
            "(one of %s)",
            supported_divisions_categories(),
        )
        sys.exit(2)
    if division not in supported_divisions_categories():
        logger.error(
            "--source ncaa-seed-wikipedia-category: --division must be one of %s (got %r)",
            supported_divisions_categories(), division,
        )
        sys.exit(2)

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source ncaa-seed-wikipedia-category: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    grand = {"fetched": 0, "inserted": 0, "updated": 0, "errors": 0}

    for gender in genders:
        scraper_key = f"ncaa-seed-wikipedia-category-{division.lower()}-{gender}"
        run_log: Optional[ScrapeRunLogger] = None
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NCAA {division} {gender}",
            )
            run_log.start(source_url=f"mediawiki-category://{category_title(division, gender)}")

        try:
            seeds = fetch_division_programs(division, gender)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error(
                "[ncaa-seed-wikipedia-category] %s %s fetch failed: %s",
                division, gender, exc,
            )
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=f"mediawiki-category://{category_title(division, gender)}",
                league_name=f"NCAA {division} {gender}",
            )
            grand["errors"] += 1
            continue

        grand["fetched"] += len(seeds)
        inserted = updated = errors = 0
        for seed in seeds:
            try:
                _cid, was_inserted = upsert_college(seed.to_upsert_row(), dry_run=dry_run)
            except Exception as exc:
                logger.warning(
                    "[ncaa-seed-wikipedia-category] upsert failed for %s (%s %s): %s",
                    seed.name, division, gender, exc,
                )
                errors += 1
                continue
            if dry_run:
                continue
            if was_inserted:
                inserted += 1
            else:
                updated += 1

        logger.info(
            "[ncaa-seed-wikipedia-category] %s %s: fetched=%d inserted=%d updated=%d errors=%d%s",
            division, gender, len(seeds), inserted, updated, errors,
            " (dry-run)" if dry_run else "",
        )
        grand["inserted"] += inserted
        grand["updated"] += updated
        grand["errors"] += errors

        if run_log is not None:
            run_log.finish_ok(
                records_created=inserted,
                records_updated=updated,
                records_failed=errors,
            )

    logger.info(
        "[ncaa-seed-wikipedia-category] %s done: fetched=%d inserted=%d updated=%d errors=%d%s",
        division, grand["fetched"], grand["inserted"], grand["updated"], grand["errors"],
        " (dry-run)" if dry_run else "",
    )


def _handle_naia_seed_official(args: argparse.Namespace) -> None:
    """Seed ``colleges`` from naia.org's 2021-22 soccer teams index.

    Sibling of ``_handle_ncaa_seed_d1`` and ``_handle_ncaa_seed_wikipedia``.
    Wikipedia has no "List of NAIA ... soccer programs" page; naia.org
    is the authoritative source, but the current-season team index
    endpoint broke after 2021-22 (redirects to the first team detail
    page instead of rendering the listing). This handler parses the
    2021-22 index, which still renders and covers ~95% of current
    NAIA membership.

    Optional flags:
      --gender mens|womens  (default: both)
      --dry-run             (parse only; no DB writes)
    """
    from extractors.naia_directory import (
        fetch_naia_programs,
        directory_url,
    )
    from ingest.ncaa_roster_writer import upsert_college

    gender_arg = getattr(args, "gender", None) or "both"
    gender_arg = {"boys": "mens", "girls": "womens"}.get(gender_arg, gender_arg)
    if gender_arg == "both":
        genders = ["mens", "womens"]
    elif gender_arg in ("mens", "womens"):
        genders = [gender_arg]
    else:
        logger.error(
            "--source naia-seed-official: --gender must be mens|womens|both (got %r)",
            gender_arg,
        )
        sys.exit(2)

    dry_run = bool(getattr(args, "dry_run", False))
    grand = {"fetched": 0, "inserted": 0, "updated": 0, "errors": 0}

    for gender in genders:
        scraper_key = f"naia-seed-official-{gender}"
        run_log: Optional[ScrapeRunLogger] = None
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NAIA {gender}",
            )
            run_log.start(source_url=directory_url(gender))

        try:
            seeds = fetch_naia_programs(gender)
        except Exception as exc:
            kind = _classify_exception(exc)
            logger.error(
                "[naia-seed-official] %s fetch failed: %s", gender, exc,
            )
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=directory_url(gender),
                league_name=f"NAIA {gender}",
            )
            grand["errors"] += 1
            continue

        grand["fetched"] += len(seeds)
        inserted = updated = errors = 0
        for seed in seeds:
            try:
                _cid, was_inserted = upsert_college(seed.to_upsert_row(), dry_run=dry_run)
            except Exception as exc:
                logger.warning(
                    "[naia-seed-official] upsert failed for %s (NAIA %s): %s",
                    seed.name, gender, exc,
                )
                errors += 1
                continue
            if dry_run:
                continue
            if was_inserted:
                inserted += 1
            else:
                updated += 1

        logger.info(
            "[naia-seed-official] %s: fetched=%d inserted=%d updated=%d errors=%d%s",
            gender, len(seeds), inserted, updated, errors,
            " (dry-run)" if dry_run else "",
        )
        grand["inserted"] += inserted
        grand["updated"] += updated
        grand["errors"] += errors

        if run_log is not None:
            run_log.finish_ok(
                records_created=inserted,
                records_updated=updated,
                records_failed=errors,
            )

    logger.info(
        "[naia-seed-official] done: fetched=%d inserted=%d updated=%d errors=%d%s",
        grand["fetched"], grand["inserted"], grand["updated"], grand["errors"],
        " (dry-run)" if dry_run else "",
    )


def _derive_school_name(url: str) -> str:
    """Last-resort school-name fallback if --school-name is missing.

    Returns the hostname minus common athletic-site suffixes
    (``goheels.com`` → ``goheels``). Operators should always pass
    ``--school-name`` explicitly — this is just so a smoke-test run
    doesn't fail on an argparse error when the URL looks reasonable.
    """
    from urllib.parse import urlparse
    host = urlparse(url).hostname or "unknown"
    stem = host.split(".")[0]
    for prefix in ("www", "athletics"):
        if stem == prefix and "." in host:
            stem = host.split(".")[1]
            break
    return stem.capitalize()


def _handle_topdrawer_commitments(args: argparse.Namespace) -> None:
    from commitments_runner import (
        run_topdrawer_commitments,
        print_summary as _tdc_print_summary,
        DEFAULT_LIMIT as _TDC_DEFAULT_LIMIT,
    )
    outcome = run_topdrawer_commitments(
        dry_run=args.dry_run,
        limit=args.limit if args.limit is not None else _TDC_DEFAULT_LIMIT,
    )
    _tdc_print_summary(outcome)


def _handle_ncaa_transfer_portal(args: argparse.Namespace) -> None:
    from transfer_portal_runner import (
        run_ncaa_transfer_portal,
        print_summary as _tp_print_summary,
        DEFAULT_LIMIT as _TP_DEFAULT_LIMIT,
    )
    outcome = run_ncaa_transfer_portal(
        dry_run=args.dry_run,
        limit=args.limit if args.limit is not None else _TP_DEFAULT_LIMIT,
    )
    _tp_print_summary(outcome)


def _handle_hs_cif_ca(args: argparse.Namespace) -> None:
    from cif_california_runner import (
        run_cif_california,
        print_summary as _cif_print_summary,
        DEFAULT_LIMIT as _CIF_DEFAULT_LIMIT,
    )
    outcome = run_cif_california(
        dry_run=args.dry_run,
        limit=args.limit if args.limit is not None else _CIF_DEFAULT_LIMIT,
    )
    _cif_print_summary(outcome)


def _handle_ncaa_seed_ncsa(args: argparse.Namespace) -> None:
    import psycopg2  # type: ignore
    from extractors.ncaa_seed_ncsa import run_ncaa_seed_ncsa

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-seed-ncsa] DATABASE_URL env var not set")
        sys.exit(1)

    conn = psycopg2.connect(database_url)
    try:
        stats = run_ncaa_seed_ncsa(
            conn,
            division=getattr(args, "division", None),
            dry_run=bool(getattr(args, "dry_run", False)),
        )
        print(f"[ncaa-seed-ncsa] Stats: {stats}")
    finally:
        conn.close()


def _handle_ncaa_enrich_websites_conferences(args: argparse.Namespace) -> None:
    """Fill ``colleges.website`` by scraping D2/D3 conference member directories.

    For each unique conference in ``colleges.conference`` (filtered by
    division/gender), looks up the conference member-directory URL in
    ``CONFERENCE_DIRECTORY_URLS``, fetches the page, extracts
    ``(school_name, athletics_url)`` pairs, fuzzy-matches them to college
    rows, and writes ``website`` for matched rows where it is NULL.

    Fuzzy match: rapidfuzz token_set_ratio >= 88 within the same division.
    Never overwrites an existing ``website`` value.

    Optional flags:
      --division D2|D3|both  (default: both)
      --gender mens|womens|both  (default: both)
      --limit N  (default: 500)
      --dry-run
    """
    import time as _time

    try:
        from rapidfuzz import fuzz as _fuzz
        import psycopg2  # type: ignore
    except ImportError as exc:
        logger.error("[ncaa-enrich-websites-conferences] missing dependency: %s", exc)
        sys.exit(1)

    from extractors.ncaa_conference_websites import (
        make_session,
        fetch_html,
        extract_member_schools,
        conference_directory_url,
    )

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-enrich-websites-conferences] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = (getattr(args, "division", None) or "both").upper()
    if division_arg in ("D2", "D3"):
        divisions = [division_arg]
    elif division_arg in ("BOTH", "ALL"):
        divisions = ["D2", "D3"]
    else:
        logger.error(
            "[ncaa-enrich-websites-conferences] --division must be D2|D3|both (got %r)",
            division_arg,
        )
        sys.exit(2)

    gender_arg = getattr(args, "gender", None) or "both"
    genders = ["mens", "womens"] if gender_arg in ("both", None) else [gender_arg]
    dry_run = bool(getattr(args, "dry_run", False))
    limit = int(getattr(args, "limit", None) or 500)

    session = make_session()
    filled = 0
    skipped_has_website = 0
    no_match = 0

    # Cache: conference_url → list[(school_name, athletics_url)]
    _conf_cache: dict[str, list[tuple[str, str]]] = {}

    try:
        conn = psycopg2.connect(database_url)
        try:
            for division in divisions:
                for gender in genders:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT id, name, conference, website
                            FROM colleges
                            WHERE division = %s AND gender_program = %s
                              AND conference IS NOT NULL
                            ORDER BY name
                            LIMIT %s
                            """,
                            (division, gender, limit),
                        )
                        rows = cur.fetchall()

                    logger.info(
                        "[ncaa-enrich-websites-conferences] %s %s: %d rows to process%s",
                        division, gender, len(rows),
                        " (dry-run)" if dry_run else "",
                    )

                    for college_id, name, conference, existing_website in rows:
                        if existing_website:
                            skipped_has_website += 1
                            continue

                        conf_url = conference_directory_url(conference)
                        if not conf_url:
                            logger.debug(
                                "[ncaa-enrich-websites-conferences] no directory URL "
                                "for conference %r (college: %s)", conference, name,
                            )
                            no_match += 1
                            continue

                        # Fetch + cache the conference member page
                        if conf_url not in _conf_cache:
                            html = fetch_html(conf_url, session)
                            _time.sleep(1.0)
                            if not html:
                                logger.warning(
                                    "[ncaa-enrich-websites-conferences] could not "
                                    "fetch conference page %s", conf_url,
                                )
                                _conf_cache[conf_url] = []
                            else:
                                pairs = extract_member_schools(html, conf_url)
                                _conf_cache[conf_url] = pairs
                                logger.info(
                                    "[ncaa-enrich-websites-conferences] %s: "
                                    "extracted %d school entries",
                                    conf_url, len(pairs),
                                )

                        pairs = _conf_cache[conf_url]
                        if not pairs:
                            no_match += 1
                            continue

                        # Fuzzy match school name to conference entries
                        best_score = 0
                        best_url: Optional[str] = None
                        norm_name = name.lower()
                        for conf_school, conf_athletics_url in pairs:
                            score = _fuzz.token_set_ratio(
                                norm_name, conf_school.lower()
                            )
                            if score > best_score:
                                best_score = score
                                best_url = conf_athletics_url

                        if best_score < 88 or not best_url:
                            logger.debug(
                                "[ncaa-enrich-websites-conferences] no match for %s "
                                "(best score=%d)", name, best_score,
                            )
                            no_match += 1
                            continue

                        if dry_run:
                            logger.info(
                                "[ncaa-enrich-websites-conferences] [dry-run] "
                                "%s (%s %s) → website=%s (score=%d)",
                                name, division, gender, best_url, best_score,
                            )
                        else:
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE colleges SET website = %s "
                                    "WHERE id = %s AND website IS NULL",
                                    (best_url, college_id),
                                )
                            conn.commit()
                            logger.info(
                                "[ncaa-enrich-websites-conferences] %s (%s %s) "
                                "→ website=%s (score=%d conf=%s)",
                                name, division, gender, best_url, best_score, conference,
                            )
                        filled += 1

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[ncaa-enrich-websites-conferences] done: filled=%d "
        "skipped_has_website=%d no_match=%d%s",
        filled, skipped_has_website, no_match,
        " (dry-run)" if dry_run else "",
    )
    if filled > 0 and not dry_run:
        logger.info(
            "[ncaa-enrich-websites-conferences] run --source ncaa-resolve-urls "
            "to SIDEARM-probe the %d newly-filled website rows", filled,
        )


def _handle_validate_college_websites(args: argparse.Namespace) -> None:
    """HEAD-check every ``colleges.website`` and clear values that fail DNS.

    Many ``website`` values were scraped from Wikipedia months or years ago.
    Schools close (Alderson Broaddus 2023), merge (Cal U → PennWest 2022),
    or rebrand their athletics domain — leaving dead URLs that block the
    entire URL pipeline (SIDEARM probe gets 404, crawl gets DNS failure,
    Wikipedia won't overwrite an existing value).

    This handler clears those dead ``website`` values (sets them to NULL) so
    subsequent pipeline steps can re-fill them from a fresh source:

      1. python3 run.py --source validate-college-websites --division D2 --dry-run
      2. python3 run.py --source validate-college-websites --division D2
      3. python3 run.py --source ncaa-resolve-urls-wikipedia --division D2
      4. python3 run.py --source ncaa-resolve-urls --division D2

    Only DNS failures (``NameResolutionError`` / ``ConnectionError``) clear
    the value — a 403 or 429 means the site is alive but blocking us, so
    the ``website`` is kept. Timeouts are treated as transient and kept too.

    Optional flags:
      --division D1|D2|D3|NAIA  (default: all)
      --gender mens|womens|both  (default: both)
      --limit N   (default: 1000)
      --dry-run
    """
    import time as _time

    try:
        import requests as _requests
        import psycopg2  # type: ignore
    except ImportError as exc:
        logger.error("[validate-college-websites] missing dependency: %s", exc)
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[validate-college-websites] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = getattr(args, "division", None)
    gender_arg = getattr(args, "gender", None) or "both"
    dry_run = bool(getattr(args, "dry_run", False))
    limit = int(getattr(args, "limit", None) or 1000)

    genders = ["mens", "womens"] if gender_arg in ("both", None) else [gender_arg]

    _UA = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    session = _requests.Session()
    session.headers.update({"User-Agent": _UA, "Accept": "*/*"})

    cleared = 0
    alive = 0
    errors = 0  # timeouts / blocked — keep the website value

    # Track already-checked websites to avoid re-probing the same domain
    # multiple times (many schools share a website domain).
    _dns_dead: set[str] = set()
    _dns_alive: set[str] = set()

    def _is_dead(url: str) -> bool:
        """HEAD-check url; return True only on DNS failure (domain gone)."""
        from urllib.parse import urlparse as _up
        origin = _up(url).scheme + "://" + _up(url).netloc
        if origin in _dns_dead:
            return True
        if origin in _dns_alive:
            return False
        try:
            resp = session.head(url, timeout=10, allow_redirects=True)
            # Any HTTP response means DNS resolved → site is alive.
            _dns_alive.add(origin)
            return False
        except _requests.ConnectionError:
            # NameResolutionError wraps as ConnectionError — domain is gone.
            _dns_dead.add(origin)
            return True
        except _requests.Timeout:
            # Transient — keep the website.
            _dns_alive.add(origin)
            return False
        except Exception:
            _dns_alive.add(origin)
            return False

    try:
        conn = psycopg2.connect(database_url)
        try:
            sql = """
                SELECT id, name, division, gender_program, website
                FROM colleges
                WHERE website IS NOT NULL
                  AND soccer_program_url IS NULL
                  AND gender_program = ANY(%s)
            """
            params: list = [genders]
            if division_arg:
                sql += " AND division = %s"
                params.append(division_arg)
            sql += " ORDER BY name LIMIT %s"
            params.append(limit)

            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

            logger.info(
                "[validate-college-websites] checking %d college website(s)%s",
                len(rows), " (dry-run)" if dry_run else "",
            )

            for college_id, name, division, gender_program, website in rows:
                tag = f"{name} ({division} {gender_program})"
                dead = _is_dead(website)
                _time.sleep(0.3)  # light throttle — HEAD is cheap

                if dead:
                    if dry_run:
                        logger.info(
                            "[validate-college-websites] [dry-run] DEAD %s → website=%s",
                            tag, website,
                        )
                    else:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE colleges SET website = NULL WHERE id = %s",
                                (college_id,),
                            )
                        conn.commit()
                        logger.info(
                            "[validate-college-websites] cleared dead website for %s (%s)",
                            tag, website,
                        )
                    cleared += 1
                else:
                    logger.debug(
                        "[validate-college-websites] alive: %s → %s", tag, website,
                    )
                    alive += 1

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[validate-college-websites] done: cleared=%d alive=%d%s",
        cleared, alive, " (dry-run)" if dry_run else "",
    )
    if cleared > 0 and not dry_run:
        logger.info(
            "[validate-college-websites] run --source ncaa-resolve-urls-wikipedia "
            "to re-fill the %d cleared website rows from Wikipedia",
            cleared,
        )


def _handle_ncaa_crawl_athletics_pages(args: argparse.Namespace) -> None:
    """Fill ``colleges.soccer_program_url`` by crawling each school's
    athletics homepage and extracting soccer-program links from the HTML.

    For each college row with ``website IS NOT NULL AND soccer_program_url IS NULL``,
    GETs the athletics homepage, parses all ``<a href="…">`` anchors, scores
    each by soccer-keyword presence in the URL path (hard hit, score ≥ 10)
    and gender preference (+5), and writes the best match.

    Unlike ``ncaa-resolve-urls`` (which probes pre-known SIDEARM/PrestoSports
    paths via HEAD), this handler works for any CMS — BlueStar, AthleticNet,
    custom .edu/athletics pages — because it reads what the site actually links
    to rather than guessing path patterns. Ideal for D2/D3 where CMS diversity
    makes SIDEARM probing ineffective.

    Optional flags:
      --division D1|D2|D3|NAIA  (default: all)
      --gender mens|womens|both  (default: both)
      --limit N   (default: 300)
      --dry-run
    """
    import time as _time

    from extractors.ncaa_crawl_athletics import (
        make_session,
        fetch_html,
        find_soccer_url,
    )

    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.error("[ncaa-crawl-athletics] psycopg2 not available")
        sys.exit(1)

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("[ncaa-crawl-athletics] DATABASE_URL env var not set")
        sys.exit(1)

    division_arg = getattr(args, "division", None)
    gender_arg = getattr(args, "gender", None) or "both"
    dry_run = bool(getattr(args, "dry_run", False))
    limit = int(getattr(args, "limit", None) or 300)

    genders = ["mens", "womens"] if gender_arg in ("both", None) else [gender_arg]

    session = make_session()
    filled = 0
    missed = 0
    errors = 0

    try:
        conn = psycopg2.connect(database_url)
        try:
            for gender in genders:
                with conn.cursor() as cur:
                    sql = """
                        SELECT id, name, division, gender_program, website
                        FROM colleges
                        WHERE website IS NOT NULL
                          AND soccer_program_url IS NULL
                          AND gender_program = %s
                    """
                    params: list = [gender]
                    if division_arg:
                        sql += " AND division = %s"
                        params.append(division_arg)
                    sql += " ORDER BY name LIMIT %s"
                    params.append(limit)
                    cur.execute(sql, params)
                    rows = cur.fetchall()

                logger.info(
                    "[ncaa-crawl-athletics] %d %s college(s) with website but "
                    "no soccer_program_url%s",
                    len(rows), gender, " (dry-run)" if dry_run else "",
                )

                for college_id, name, division, gender_program, website in rows:
                    tag = f"{name} ({division} {gender_program})"

                    # Normalise to origin (scheme + host only) so we can
                    # construct fallback paths cleanly.
                    from urllib.parse import urlparse as _urlparse, urlunparse as _urlunparse
                    _p = _urlparse(website)
                    _origin = _urlunparse((_p.scheme, _p.netloc, "", "", "", ""))

                    # Pages to try in order.  Homepage first; then the SIDEARM
                    # static sport-index page (/sports/) which exists even when
                    # the homepage nav is JS-rendered; then /athletics/ for
                    # university custom sites.
                    _pages_to_try = [
                        website,
                        f"{_origin}/sports/",
                        f"{_origin}/athletics/",
                    ]

                    soccer_url = None
                    fetch_error = True
                    for _page_url in _pages_to_try:
                        html = fetch_html(_page_url, session)
                        _time.sleep(1.0)
                        if not html:
                            continue
                        fetch_error = False
                        soccer_url = find_soccer_url(html, _page_url, gender)
                        if soccer_url:
                            break

                    if fetch_error:
                        logger.debug(
                            "[ncaa-crawl-athletics] could not fetch any page for %s",
                            tag,
                        )
                        errors += 1
                        continue

                    if not soccer_url:
                        logger.debug(
                            "[ncaa-crawl-athletics] no soccer link found on any page for %s",
                            tag,
                        )
                        missed += 1
                        continue

                    if dry_run:
                        logger.info(
                            "[ncaa-crawl-athletics] [dry-run] %s → soccer_program_url=%s",
                            tag, soccer_url,
                        )
                    else:
                        with conn.cursor() as cur:
                            cur.execute(
                                "UPDATE colleges SET soccer_program_url = %s WHERE id = %s",
                                (soccer_url, college_id),
                            )
                        conn.commit()
                        logger.info(
                            "[ncaa-crawl-athletics] %s → soccer_program_url=%s",
                            tag, soccer_url,
                        )
                    filled += 1

        finally:
            try:
                conn.close()
            except Exception:
                pass
    finally:
        try:
            session.close()
        except Exception:
            pass

    logger.info(
        "[ncaa-crawl-athletics] done: filled=%d missed=%d errors=%d%s",
        filled, missed, errors, " (dry-run)" if dry_run else "",
    )


# Kebab-case is the canonical, documented form in the CLI help output;
# snake-case aliases exist only because early scripts sometimes passed
# them. Keep both in SOURCE_HANDLERS but ONLY kebab in SOURCE_HELP (snake
# duplicates would pollute --help).
SOURCE_HANDLERS: dict[str, Callable[[argparse.Namespace], None]] = {
    "gotsport-matches": _handle_gotsport_matches,
    "gotsport_matches": _handle_gotsport_matches,
    "ga-matches": _handle_ga_matches,
    "ga_matches": _handle_ga_matches,
    "sincsports-events": _handle_sincsports_events,
    "sincsports_events": _handle_sincsports_events,
    "link-canonical-clubs": _handle_link_canonical_clubs,
    "link_canonical_clubs": _handle_link_canonical_clubs,
    "link-canonical-schools": _handle_link_canonical_schools,
    "link_canonical_schools": _handle_link_canonical_schools,
    "nav-leaked-names-detect": _handle_nav_leaked_names_detect,
    "nav_leaked_names_detect": _handle_nav_leaked_names_detect,
    "numeric-only-names-detect": _handle_numeric_only_names_detect,
    "numeric_only_names_detect": _handle_numeric_only_names_detect,
    "coach-pollution-detect": _handle_coach_pollution_detect,
    "coach_pollution_detect": _handle_coach_pollution_detect,
    "coach-ui-fragment-detect": _handle_coach_ui_fragment_detect,
    "coach_ui_fragment_detect": _handle_coach_ui_fragment_detect,
    "maxpreps-rosters": _handle_maxpreps_rosters,
    "maxpreps_rosters": _handle_maxpreps_rosters,
    "odp-rosters": _handle_odp_rosters,
    "odp_rosters": _handle_odp_rosters,
    "replay-html": _handle_replay_html,
    "replay_html": _handle_replay_html,
    "sincsports-rosters": _handle_sincsports_rosters,
    "sincsports_rosters": _handle_sincsports_rosters,
    "gotsport-events": _handle_gotsport_events,
    "gotsport_events": _handle_gotsport_events,
    "totalglobalsports-events": _handle_totalglobalsports_events,
    "totalglobalsports_events": _handle_totalglobalsports_events,
    "tgs-events": _handle_totalglobalsports_events,
    "tgs_events": _handle_totalglobalsports_events,
    "gotsport-matches-batch": _handle_gotsport_matches_batch,
    "gotsport_matches_batch": _handle_gotsport_matches_batch,
    "sincsports-matches": _handle_sincsports_matches,
    "sincsports_matches": _handle_sincsports_matches,
    "athleteone-matches": _handle_athleteone_matches,
    "athleteone_matches": _handle_athleteone_matches,
    "tgs-matches": _handle_totalglobalsports_matches,
    "tgs_matches": _handle_totalglobalsports_matches,
    "totalglobalsports-matches": _handle_totalglobalsports_matches,
    "totalglobalsports_matches": _handle_totalglobalsports_matches,
    "mlsnext-matches": _handle_mlsnext_matches,
    "mlsnext_matches": _handle_mlsnext_matches,
    "mls-next-matches": _handle_mlsnext_matches,
    "gotsport-rosters": _handle_gotsport_rosters,
    "gotsport_rosters": _handle_gotsport_rosters,
    "tryouts-wordpress": _handle_tryouts_wordpress,
    "tryouts_wordpress": _handle_tryouts_wordpress,
    "tryouts-gotsport": _handle_tryouts_gotsport,
    "tryouts_gotsport": _handle_tryouts_gotsport,
    "tryouts": _handle_tryouts,
    "youth-coaches": _handle_youth_coaches,
    "youth_coaches": _handle_youth_coaches,
    "squarespace-clubs": _handle_squarespace_clubs,
    "squarespace_clubs": _handle_squarespace_clubs,
    "sportsengine-clubs": _handle_sportsengine_clubs,
    "sportsengine_clubs": _handle_sportsengine_clubs,
    "topdrawer-commitments": _handle_topdrawer_commitments,
    "topdrawer_commitments": _handle_topdrawer_commitments,
    "ncaa-transfer-portal": _handle_ncaa_transfer_portal,
    "ncaa_transfer_portal": _handle_ncaa_transfer_portal,
    "hs-cif-ca": _handle_hs_cif_ca,
    "hs_cif_ca": _handle_hs_cif_ca,
    "club-enrichment": _handle_club_enrichment,
    "club_enrichment": _handle_club_enrichment,
    "club-dedup": _handle_club_dedup,
    "club_dedup": _handle_club_dedup,
    "college-dedup": _handle_college_dedup,
    "college_dedup": _handle_college_dedup,
    "club-dedup-resolve": _handle_club_dedup_resolve,
    "club_dedup_resolve": _handle_club_dedup_resolve,
    "usclub-sanctioned": _handle_usclub_sanctioned,
    "usclub_sanctioned": _handle_usclub_sanctioned,
    "usclub-seeds": _handle_usclub_seeds,
    "usclub_seeds": _handle_usclub_seeds,
    "usclub-id": _handle_usclub_id,
    "usclub_id": _handle_usclub_id,
    "ussoccer-ynt": _handle_ussoccer_ynt,
    "ussoccer_ynt": _handle_ussoccer_ynt,
    "youtube-ecnl": _handle_youtube_ecnl,
    "youtube_ecnl": _handle_youtube_ecnl,
    "mlsnext-video": _handle_mlsnext_video,
    "mlsnext_video": _handle_mlsnext_video,
    "duda-360player-clubs": _handle_duda_360player_clubs,
    "duda_360player_clubs": _handle_duda_360player_clubs,
    "ncaa-rosters": _handle_ncaa_rosters,
    "ncaa_rosters": _handle_ncaa_rosters,
    "ncaa-seed-d1": _handle_ncaa_seed_d1,
    "ncaa_seed_d1": _handle_ncaa_seed_d1,
    "ncaa-seed-d2-d3": _handle_ncaa_seed_d2_d3,
    "ncaa_seed_d2_d3": _handle_ncaa_seed_d2_d3,
    "ncaa-seed-wikipedia": _handle_ncaa_seed_wikipedia,
    "ncaa_seed_wikipedia": _handle_ncaa_seed_wikipedia,
    "ncaa-seed-wikipedia-category": _handle_ncaa_seed_wikipedia_category,
    "ncaa_seed_wikipedia_category": _handle_ncaa_seed_wikipedia_category,
    "naia-seed-official": _handle_naia_seed_official,
    "naia_seed_official": _handle_naia_seed_official,
    "naia-resolve-urls": _handle_naia_resolve_urls,
    "naia_resolve_urls": _handle_naia_resolve_urls,
    "ncaa-resolve-urls": _handle_ncaa_resolve_urls,
    "ncaa_resolve_urls": _handle_ncaa_resolve_urls,
    "ncaa-resolve-urls-wikipedia": _handle_ncaa_resolve_urls_wikipedia,
    "ncaa_resolve_urls_wikipedia": _handle_ncaa_resolve_urls_wikipedia,
    "ncaa-discover-urls-google": _handle_ncaa_discover_urls_google,
    "ncaa_discover_urls_google": _handle_ncaa_discover_urls_google,
    "ncaa-enrich-websites-ncaaid": _handle_ncaa_enrich_websites_ncaaid,
    "ncaa_enrich_websites_ncaaid": _handle_ncaa_enrich_websites_ncaaid,
    "ncaa-discover-urls-ncsa": _handle_ncaa_discover_urls_ncsa,
    "ncaa_discover_urls_ncsa": _handle_ncaa_discover_urls_ncsa,
    "ncaa-seed-ncsa": _handle_ncaa_seed_ncsa,
    "ncaa_seed_ncsa": _handle_ncaa_seed_ncsa,
    "ncaa-crawl-athletics-pages": _handle_ncaa_crawl_athletics_pages,
    "ncaa_crawl_athletics_pages": _handle_ncaa_crawl_athletics_pages,
    "validate-college-websites": _handle_validate_college_websites,
    "validate_college_websites": _handle_validate_college_websites,
    "ncaa-enrich-websites-conferences": _handle_ncaa_enrich_websites_conferences,
    "ncaa_enrich_websites_conferences": _handle_ncaa_enrich_websites_conferences,
}

# One entry per UNIQUE source (kebab form only). Used to build the
# --source argparse help block. Snake-case aliases deliberately absent
# here: they're a compatibility shim, not user-facing.
SOURCE_HELP: dict[str, str] = {
    "gotsport-matches": "populates matches from GotSport schedules (requires --event-id)",
    "ga-matches": "populate Girls Academy matches from GotSport event 42137 (requires GOTSPORT_SESSION_COOKIE env var)",
    "gotsport-matches-batch": "batch matches across all GotSport events",
    "sincsports-matches": "populates tournament_matches from SincSports schedule (requires --tid)",
    "athleteone-matches": "populates matches + tournament_matches from all ECNL AthleteOne org_seasons",
    "tgs-matches": "populates matches from TGS (STXCL NPL) schedules (optional --event-id; default: all KNOWN_EVENT_IDS)",
    "mlsnext-matches": "populates matches from MLS NEXT (Modular11) schedules for all age groups U13-U19",
    "gotsport-events": "populates events + event_teams from GotSport",
    "gotsport-rosters": "populates club_roster_snapshots from GotSport rosters",
    "totalglobalsports-events": "populates events + event_teams from TotalGlobalSports (alias: tgs-events)",
    "tgs-events": "short alias for totalglobalsports-events",
    "sincsports-events": "populates events + event_teams from SincSports",
    "sincsports-rosters": "populates club_roster_snapshots + roster_diffs from SincSports",
    "tryouts-wordpress": "populates tryouts from WordPress club sites",
    "tryouts": "wordpress source + status expiry (see tryouts_runner.py for why GotSport tryout discovery is not supported)",
    "tryouts-gotsport": "removed — GotSport disallows automated event discovery",
    "topdrawer-commitments": "scrape college commitments from TopDrawerSoccer into commitments table (default --limit 20; expect 403 without proxies)",
    "ncaa-transfer-portal": "scrape NCAA transfer-portal entries from TopDrawerSoccer tracker articles into transfer_portal_entries (default --limit 20; expect 403 without proxies)",
    "hs-cif-ca": "scrape CIF California HS state-tournament brackets, results, and rankings into hs_matches + hs_state_rankings (default --limit 30; no proxy needed)",
    "youth-coaches": "scrapes youth club staff pages into coach_discoveries",
    "squarespace-clubs": "Squarespace + JSON-LD harvest: rosters, coaches, tryouts, enrichment",
    "sportsengine-clubs": "SportsEngine + JSON-LD harvest: rosters, coaches, tryouts, enrichment",
    "duda-360player-clubs": "probe Duda CMS + 360Player club sites; writes Event JSON-LD into tryouts",
    "link-canonical-clubs": "resolves event_teams.canonical_club_id / matches.home_club_id / etc.",
    "link-canonical-schools": "resolves hs_rosters.school_id via state-scoped 4-pass resolver against canonical_schools + school_aliases",
    "nav-leaked-names-detect": "scans club_roster_snapshots for nav-menu strings ('Home', 'Contact', etc.) leaking into player_name and writes roster_quality_flags rows of type 'nav_leaked_name'. Defaults to a 7-day scraped_at incremental window; pass --full-scan to re-scan every row.",
    "numeric-only-names-detect": "scans club_roster_snapshots for player_name values that are entirely digits/dates/whitespace (e.g. '14', '2024-05-15') and writes roster_quality_flags rows of type 'numeric_only_name'. Defaults to a 7-day scraped_at incremental window; pass --full-scan to re-scan every row.",
    "coach-pollution-detect": "scans coach_discoveries, runs each row's `name` through the shared looks_like_name guard, and writes coach_quality_flags rows of type 'looks_like_name_reject' for every failing row. DRY-RUN BY DEFAULT (pass --commit to actually write). Scope: FLAG ONLY — deletion is a separate follow-up PR so the audit trail survives. Supports --limit N and --window-days N.",
    "coach-ui-fragment-detect": "second-wave complement to coach-pollution-detect. Scans coach_discoveries for UI-fragment pollution that shape-wise passes looks_like_name (two-token, Title-Case, alpha-start) but semantically is a nav label / pricing tier / section heading / marketing tile (e.g. 'Where We Are', 'One Week', 'Fashion Magazine'). Exact-match gazetteer (no heuristics). Writes coach_quality_flags rows of type 'ui_fragment_as_name'. DRY-RUN BY DEFAULT (pass --commit to write). Supports --limit N and --window-days N.",
    "maxpreps-rosters": "populates hs_rosters from MaxPreps HS soccer roster pages (framework; default --limit 20; expect 403s without proxy creds)",
    "odp-rosters": "scrapes state-association Olympic Development Program rosters (top-5 states; 49 follow-ups)",
    "replay-html": "replay archived HTML from raw_html_archive through extractors (requires --run-id; defaults to dry-run, --no-dry-run to commit)",
    "club-enrichment": "enrich canonical_clubs with logo/socials/status",
    "club-dedup": "fuzzy dedup report for canonical_clubs",
    "club-dedup-resolve": "tiered: auto-merges high-confidence pairs + writes review CSV; defaults to dry-run, requires --no-dry-run to commit",
    "college-dedup": "fuzzy dedup report for colleges grouped by (division, gender_program); --persist queues pairs into college_duplicates for admin review. Primary use: collapse D1 womens duplicate rows seeded from multiple scrapers. Optional --division, --gender, --threshold.",
    "usclub-sanctioned": "discover US Club Soccer sanctioned tournaments + seed National Cup/NPL events",
    "usclub-seeds": "seed only — National Cup + NPL Finals GotSport events, skip discovery",
    "usclub-id": "discover US Club iD National Pool / Training Center articles via SoccerWire WP REST API (scaffold)",
    "ussoccer-ynt": "scrape US Soccer Youth National Team (YNT) call-ups from ussoccer.com press releases into ynt_call_ups",
    "youtube-ecnl": "scrape the ECNL YouTube channel (@TheECNL) RSS feed into video_sources",
    "mlsnext-video": "scrape the MLS NEXT video library (mlssoccer.com/mlsnext/video) Brightcove cards into video_sources (source_platform='mls_com')",
    "ncaa-rosters": "NCAA D1/D2/D3 soccer roster scrape (SIDEARM-first). Exactly one of --school-url (single) OR --all (bulk; --division + --gender required). Writes colleges + college_coaches + college_roster_history.",
    "ncaa-seed-d1": "seed colleges table from stats.ncaa.org D1 men's + women's soccer program lists. Optional --gender mens|womens (default: both); --dry-run.",
    "ncaa-seed-d2-d3": "seed/update colleges.ncaa_id from stats.ncaa.org D2 and D3 soccer program lists. Primary value: writes ncaa_id onto existing rows seeded from Wikipedia (which lack it), enabling ncaa-enrich-websites-ncaaid to fetch current athletics URLs. Optional --division D2|D3|both (default: both), --gender mens|womens|both (default: both), --dry-run. After running, execute ncaa-enrich-websites-ncaaid --division D2 (or D3).",
    "ncaa-seed-wikipedia": "seed colleges table from Wikipedia's D1/D2/D3/NAIA soccer-program lists. Requires --division {D1,D2,D3,NAIA}. Optional --gender mens|womens (default: both); --dry-run. D1 support is a FALLBACK for when stats.ncaa.org 403s the scraper — prefer --source ncaa-seed-d1 for D1 when it works (richer conference data). NAIA Wikipedia pages are deprecated; use --source naia-seed-official instead. NJCAA not supported — Wikipedia coverage too fragmented.",
    "ncaa-seed-wikipedia-category": "seed colleges table from Wikipedia's MediaWiki category pages. Use when the plain 'List of ...' page doesn't exist (D3 as of April 2026). Requires --division D3. Optional --gender mens|womens (default: both); --dry-run. Partial coverage (only schools with their own Wikipedia article — ~60-80% of the real universe); remaining long tail lands in the kid's manual-entry CSV.",
    "naia-seed-official": "seed colleges table from naia.org's 2021-22 soccer teams index (last working listing endpoint — current-season listings 302-redirect to the first team). Covers ~95% of current NAIA membership; ~5-program/year churn means the rest comes in via manual entry. Optional --gender mens|womens (default: both); --dry-run.",
    "ncaa-resolve-urls": "resolve colleges.soccer_program_url by probing the canonical SIDEARM roster path for each college.website. Scoped by --division (default D1); --limit N for smoke-tests; --dry-run.",
    "ncaa-resolve-urls-wikipedia": "resolve D1/D2 colleges.soccer_program_url by walking each program's own Wikipedia article infobox for the athletics-website URL, then probing the canonical SIDEARM roster path. Closes the gap left by ncaa-resolve-urls (which requires website IS NOT NULL) — D1/D2 seeders don't write a website column, so 76% of those rows are unreachable today. Scope is D1+D2 only (D3 has working category-seeded URL coverage; NAIA has --source naia-resolve-urls). Optional --division D1|D2 (default both); --gender mens|womens|both (default both); --limit N for smoke-tests; --dry-run.",
    "ncaa-discover-urls-google": "fill colleges.soccer_program_url (or website) for rows where URL is NULL via Google Custom Search Engine. Two-pass per school: pass 1 targets the soccer roster page directly; pass 2 finds the athletics homepage as a fallback. Requires GOOGLE_CSE_API_KEY + GOOGLE_CSE_CX env vars. Default --limit 100 (free tier cap). Optional --division, --gender mens|womens|both, --dry-run.",
    "ncaa-enrich-websites-ncaaid": "fill colleges.website by fetching each school's stats.ncaa.org team page (via stored ncaa_id) and extracting the outbound athletics-homepage link. Only targets rows with ncaa_id IS NOT NULL AND website IS NULL. After running, execute --source ncaa-resolve-urls to SIDEARM-probe the newly-filled website rows for soccer_program_url. Default --division D1; default --limit 200; --dry-run.",
    "ncaa-discover-urls-ncsa": "fill colleges.soccer_program_url (or website) via NCSA Sports college soccer directory (ncsasports.org). NOTE: ncsasports.org is a JavaScript SPA — returns 0 listings. Prefer ncaa-enrich-websites-ncaaid. Default --limit 500. Optional --division D1|D2|D3|NAIA, --gender mens|womens|both, --dry-run.",
    "naia-resolve-urls": "resolve NAIA colleges.website + soccer_program_url via naia.org per-team detail pages (closes the gap left by ncaa-resolve-urls, which requires website IS NOT NULL — NAIA seeds carry no website). Phase-1 fetches the naia.org index per gender for a name→slug map; phase-2 GETs each detail page, extracts the athletics outbound link, and probes SIDEARM. Optional --gender mens|womens|both (default both); --limit N for smoke-tests; --dry-run. Production runs require proxy_config.yaml — Replit IPs hit naia.org's WAF (HTTP 405).",
    "ncaa-seed-ncsa": "fill colleges.soccer_program_url gaps from curated NCSA/productiverecruit seed CSVs (scraper/seeds/ncaa_urls_*.csv); Jaro-Winkler >= 0.88 match within division+gender. Optional: --division D1|D2|D3|NAIA, --dry-run.",
    "ncaa-crawl-athletics-pages": "fill colleges.soccer_program_url by GETting each school's athletics homepage (colleges.website) and extracting soccer-program links from the page HTML. CMS-agnostic — works for BlueStar, AthleticNet, custom .edu sites, and any platform that links to a soccer page. Only requires a hard hit (soccer keyword in the href path). Targets rows where website IS NOT NULL AND soccer_program_url IS NULL. Optional --division D1|D2|D3|NAIA, --gender mens|womens|both (default both), --limit N (default 300), --dry-run.",
    "ncaa-enrich-websites-conferences": "fill colleges.website for D2/D3 rows by scraping conference member-school directories. Maps colleges.conference to a curated conference→URL dict, fetches each member page, extracts (school_name, athletics_url) pairs, fuzzy-matches (token_set_ratio >= 88) to college rows, and writes website where NULL. Never overwrites existing data. Optional --division D2|D3|both (default both), --gender, --limit N (default 500), --dry-run.",
    "validate-college-websites": "HEAD-check every colleges.website (where soccer_program_url IS NULL) and set website=NULL for any that fail DNS resolution (NameResolutionError = domain gone/expired). Keeps 403/429/timeout values — those sites are alive but blocking. Run before ncaa-resolve-urls-wikipedia to clear stale domains so Wikipedia can re-fill them. Optional --division D1|D2|D3|NAIA, --gender mens|womens|both (default both), --limit N (default 1000), --dry-run.",
}


def _build_source_help() -> str:
    """Render the --source argparse help block from SOURCE_HELP."""
    lines = ["Run a non-league scraper by key. Supported sources:"]
    for k in sorted(SOURCE_HELP):
        lines.append(f"  {k} — {SOURCE_HELP[k]}")
    return "\n".join(lines)


def _run_source(args: argparse.Namespace) -> None:
    """Dispatch --source KEY to the appropriate non-league scraper."""
    key = args.source
    handler = SOURCE_HANDLERS.get(key)
    if handler is None:
        raise ValueError(
            f"Unknown --source value: {key!r}. "
            f"Valid: {sorted(set(SOURCE_HELP))}"
        )
    handler(args)
    return


def _run_gotsport_matches(
    *,
    event_id: str,
    season: Optional[str],
    league_name: Optional[str],
    dry_run: bool,
    session_cookie: Optional[str] = None,
) -> None:
    from extractors.gotsport_matches import scrape_gotsport_matches, GotSportAuthError
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
            session_cookie=session_cookie,
        )
    except GotSportAuthError as exc:
        logger.error("[gotsport-matches] auth failure event=%s: %s", event_id, exc)
        if run_log is not None:
            run_log.finish_failed(FailureKind.NETWORK, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=FailureKind.NETWORK.value,
            error_message=str(exc),
            source_url=f"https://system.gotsport.com/org_event/events/{event_id}/schedules",
            league_name=league_name or f"gotsport-event-{event_id}",
        )
        return
    except Exception as exc:
        kind = _classify_exception(exc)
        logger.error("[gotsport-matches] failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
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
                run_log.finish_failed(kind, error_message=str(exc))
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
                run_log.finish_failed(kind, error_message=str(exc))
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
                run_log.finish_failed(kind, error_message=str(exc))
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

    if key == "college-dedup":
        import os
        import psycopg2
        from dedup.college_dedup import run_college_dedup

        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        try:
            run_college_dedup(conn, dry_run=args.dry_run)
        finally:
            conn.close()
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
    parser.add_argument("--gender",
                        choices=["boys", "girls", "boys_and_girls", "mens", "womens"],
                        help="Filter by gender program. Youth leagues use "
                             "boys/girls/boys_and_girls; NCAA college sources "
                             "use mens/womens.")
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
                        help=_build_source_help())
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
    parser.add_argument("--no-dry-run", action="store_true", dest="no_dry_run",
                        help="For --source club-dedup-resolve: explicitly opt in to "
                             "committing auto-merge tier merges. Without this flag the "
                             "resolver runs in dry-run mode regardless. Ignored by other "
                             "sources.")
    parser.add_argument("--persist", action="store_true",
                        help="For --source club-dedup: persist pending pairs into "
                             "club_duplicates table so the admin dedup review panel has a queue. "
                             "Without this flag club-dedup prints pairs only. Ignored by other "
                             "sources.")
    parser.add_argument("--platform-family",
                        choices=["sportsengine", "leagueapps", "wordpress", "unknown"],
                        dest="platform_family",
                        help="Platform family filter for --source youth-coaches.")
    parser.add_argument("--run-id", metavar="UUID", dest="run_id",
                        help="For --source replay-html: UUID of the scrape run "
                             "whose archived raw HTML should be replayed.")
    parser.add_argument("--school-url", metavar="URL", dest="school_url",
                        help="For --source ncaa-rosters: full roster page URL "
                             "(e.g. https://goheels.com/sports/mens-soccer/roster).")
    parser.add_argument("--school-name", metavar="NAME", dest="school_name",
                        help="For --source ncaa-rosters: display name for the "
                             "colleges row (e.g. 'North Carolina'). Falls back "
                             "to hostname stem if omitted.")
    parser.add_argument("--division", choices=["D1", "D2", "D3", "NAIA", "NJCAA"],
                        help="For --source ncaa-rosters: division (default D1).")
    parser.add_argument("--all", action="store_true", dest="all",
                        help="For --source ncaa-rosters: enumerate every colleges row "
                             "matching --division + --gender (bulk mode; mutually "
                             "exclusive with --school-url).")
    parser.add_argument("--backfill-seasons", metavar="N", type=int,
                        dest="backfill_seasons", default=0,
                        help="For --source ncaa-rosters --all: also pull rosters "
                             "for the prior N seasons (e.g. --backfill-seasons 3 "
                             "→ current + 2023-24 + 2022-23 + 2021-22). Default 0.")
    parser.add_argument("--skip-fresh-days", metavar="N", type=int,
                        dest="skip_fresh_days", default=30,
                        help="For --source ncaa-rosters --all: skip current-season "
                             "colleges whose last_scraped_at is within N days (per-season "
                             "guard inside the main loop). Default 30.")
    parser.add_argument("--force-rescrape", action="store_true", dest="force_rescrape",
                        default=False,
                        help="For --source ncaa-rosters --all: bypass all should_scrape "
                             "guards (freshness, historical-has-data, max-attempts) AND "
                             "the pre-filter skip guard.")
    parser.add_argument("--force-historical", metavar="YYYY-YY", dest="force_historical",
                        default=None,
                        help="For --source ncaa-rosters --all: bypass guards for this "
                             "specific academic year only (e.g. --force-historical 2023-24).")
    parser.add_argument("--force-covid", action="store_true", default=False,
                        dest="force_covid",
                        help="Bypass the 2020-21 COVID season skip guard in "
                             "--source ncaa-rosters --all. By default the scraper "
                             "skips 2020-21 entirely (NCAA cancelled soccer that "
                             "year) to avoid wasting Playwright retries.")
    parser.add_argument("--max-age-days", metavar="N", type=int,
                        dest="max_age_days", default=30,
                        help="For --source ncaa-rosters --all: pre-filter colleges that "
                             "already have current-season roster rows and were scraped "
                             "within this many days. Default 30. Set to 0 to disable "
                             "(same effect as --force-rescrape for the pre-filter).")
    parser.add_argument("--sport", metavar="SPORT", dest="sport", default="soccer",
                        help="Sport identifier for --source ncaa-* handlers. "
                             "Default 'soccer'. New handlers ship with required=True; "
                             "existing handlers use this optional default.")
    parser.add_argument("--rollup", choices=["club-results", "scrape-health", "retention-prune", "college-dedup"],
                        help="Run a derived-data rollup over existing DB rows.")
    parser.add_argument("--full-scan", action="store_true", dest="full_scan",
                        help="For --source nav-leaked-names-detect / numeric-only-names-detect: "
                             "skip the default 7-day scraped_at window and scan every "
                             "club_roster_snapshots row. Use for one-off re-scans after a "
                             "detector heuristic change or historical-bug investigation. "
                             "Ignored by other sources.")
    parser.add_argument("--commit", action="store_true", dest="commit",
                        help="For --source coach-pollution-detect / coach-ui-fragment-detect: "
                             "actually write flag rows. Without this flag those detectors run "
                             "dry-run and print the would-be flags. Ignored by other sources.")
    parser.add_argument("--window-days", type=int, metavar="N", dest="window_days",
                        default=None,
                        help="For --source coach-pollution-detect / coach-ui-fragment-detect: "
                             "restrict the scan to coach_discoveries rows whose first_seen_at "
                             "is within the last N days. Omit to scan every row. Ignored by "
                             "other sources.")
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
