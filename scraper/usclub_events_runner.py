"""
usclub_events_runner.py — Discover + seed US Club Soccer events.

Two responsibilities:
  1. Scrape the sanctioned tournament list page to discover ~150 events/season.
  2. Seed known National Cup, NPL Finals, and State Cup events with GotSport IDs.

For GotSport events with known IDs, this runner delegates to
gotsport_events_runner for team-level scraping.

Invoked from ``run.py --source usclub-sanctioned``.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, os.path.dirname(__file__))

_LIST_URL = "https://usclubsoccer.org/list-of-sanctioned-tournaments/"

from extractors.usclub_sanctioned import (
    DiscoveredTournament,
    scrape_usclub_sanctioned,
)
from extractors.sincsports_events import EventMeta, TeamRow
from events_writer import WriteResult, upsert_event_and_teams, _connect
from scrape_run_logger import ScrapeRunLogger, FailureKind, classify_exception
from alerts import alert_scraper_failure

logger = logging.getLogger("usclub_events_runner")


# ---------------------------------------------------------------------------
# Known US Club Soccer events with GotSport IDs (historical + upcoming)
# ---------------------------------------------------------------------------

NATIONAL_CUP_SEEDS = [
    # 2025 National Cup (historical — results available for legacy scraping)
    {"event_id": "37924", "name": "2025 National Cup South Central Regional", "league": "us-club-national-cup", "state": "OK", "city": "Tulsa", "start": "2025-06-13", "end": "2025-06-16", "season": "2024-25"},
    {"event_id": "37928", "name": "2025 National Cup Midwest Regional", "league": "us-club-national-cup", "state": "IL", "city": "Waukegan", "start": "2025-06-20", "end": "2025-06-23", "season": "2024-25"},
    {"event_id": "27174", "name": "2025 National Cup Southeast Regional", "league": "us-club-national-cup", "state": "NC", "city": "Bermuda Run", "start": "2025-06-20", "end": "2025-06-23", "season": "2024-25"},
    {"event_id": "37930", "name": "2025 National Cup West Regional", "league": "us-club-national-cup", "state": "CA", "city": "Temecula", "start": "2025-06-27", "end": "2025-06-30", "season": "2024-25"},
    {"event_id": "37932", "name": "2025 National Cup Finals", "league": "us-club-national-cup", "state": "CO", "city": "Denver", "start": "2025-07-17", "end": "2025-07-24", "season": "2024-25"},
    # 2026 National Cup (upcoming)
    {"event_id": "47498", "name": "2026 National Cup Finals", "league": "us-club-national-cup", "state": "CO", "city": "Denver", "start": "2026-07-17", "end": "2026-07-24", "season": "2025-26"},
]

NPL_FINALS_SEEDS = [
    {"event_id": "37931", "name": "2025 NPL Finals", "league": "us-club-npl-finals", "state": "CO", "city": "Denver", "start": "2025-07-07", "end": "2025-07-14", "season": "2024-25"},
    {"event_id": "47496", "name": "2026 NPL Finals", "league": "us-club-npl-finals", "state": "CO", "city": "Denver", "start": "2026-07-07", "end": "2026-07-14", "season": "2025-26"},
]

ALL_SEEDS = NATIONAL_CUP_SEEDS + NPL_FINALS_SEEDS


# ---------------------------------------------------------------------------
# Convert discovered tournaments to EventMeta for upserting
# ---------------------------------------------------------------------------

def _tournament_to_meta(t: DiscoveredTournament, season: str = "2025-26") -> EventMeta:
    """Convert a DiscoveredTournament to an EventMeta for DB upsert."""
    # Build platform_event_id from what we know
    if t.gotsport_event_id:
        platform_id = t.gotsport_event_id
        source = "gotsport"
    elif t.sincsports_tid:
        platform_id = t.sincsports_tid
        source = "sincsports"
    else:
        # For "other" platform, use a stable hash of name + URL.
        # Source must be a value accepted by the events_source_enum CHECK
        # constraint: gotsport | sincsports | manual | other | NULL.
        platform_id = f"usclub-{hash(t.name) & 0xFFFFFFFF:08x}"
        source = "other"

    slug = re.sub(r"[^a-z0-9]+", "-", t.name.lower()).strip("-")[:100]

    return EventMeta(
        tid=platform_id,
        name=t.name,
        slug=f"usclub-{season}-{slug}",
        source=source,
        platform_event_id=platform_id,
        league_name="us-club-sanctioned",
        source_url=t.url or "",
        location_city=None,
        location_state=t.state,
        start_date=t.start_date,
        end_date=t.end_date,
        season=season,
    )


def _seed_to_meta(seed: dict) -> EventMeta:
    """Convert a hardcoded seed dict to EventMeta."""
    slug = re.sub(r"[^a-z0-9]+", "-", seed["name"].lower()).strip("-")[:100]
    return EventMeta(
        tid=seed["event_id"],
        name=seed["name"],
        slug=slug,
        source="gotsport",
        platform_event_id=seed["event_id"],
        league_name=seed["league"],
        source_url=f"https://system.gotsport.com/org_event/events/{seed['event_id']}/teams",
        location_city=seed.get("city"),
        location_state=seed.get("state"),
        start_date=seed.get("start"),
        end_date=seed.get("end"),
        season=seed.get("season", "2025-26"),
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

@dataclass
class RunOutcome:
    phase: str
    events_processed: int = 0
    events_created: int = 0
    events_updated: int = 0
    gotsport_ids_found: int = 0
    sincsports_tids_found: int = 0
    error: Optional[str] = None


def run_usclub_events(
    dry_run: bool = False,
    skip_discovery: bool = False,
    skip_seeds: bool = False,
    season: str = "2025-26",
) -> List[RunOutcome]:
    """Main entry point.

    Phase 1: Seed known National Cup / NPL Finals events.
    Phase 2: Scrape sanctioned tournament list for discovery.
    """
    outcomes: List[RunOutcome] = []
    conn = None if dry_run else _connect()

    try:
        # --- Phase 1: Seed known events ---
        if not skip_seeds:
            outcome = RunOutcome(phase="seeds")
            logger.info("[usclub] Phase 1: Seeding %d known events", len(ALL_SEEDS))

            for seed in ALL_SEEDS:
                meta = _seed_to_meta(seed)
                if dry_run:
                    logger.info("  [dry-run] Would seed: %s (GotSport #%s)", meta.name, meta.platform_event_id)
                    outcome.events_created += 1
                else:
                    result = upsert_event_and_teams(meta, [], conn=conn, dry_run=False)
                    outcome.events_created += result.events_created
                    outcome.events_updated += result.events_updated

                outcome.events_processed += 1
                outcome.gotsport_ids_found += 1

            outcomes.append(outcome)
            logger.info(
                "[usclub] Phase 1 done: %d seeded (%d created, %d updated)",
                outcome.events_processed, outcome.events_created, outcome.events_updated,
            )

        # --- Phase 2: Discover sanctioned tournaments ---
        if not skip_discovery:
            outcome = RunOutcome(phase="discovery")
            scraper_key = "usclub-sanctioned-tournaments"

            run_log = None
            if not dry_run:
                run_log = ScrapeRunLogger(scraper_key=scraper_key, league_name="US Club Soccer")
                run_log.start(source_url=_LIST_URL)

            try:
                tournaments = scrape_usclub_sanctioned()
            except Exception as exc:
                kind = classify_exception(exc)
                logger.error("[usclub] Discovery scrape failed: %s", exc)
                outcome.error = str(exc)
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                alert_scraper_failure(
                    scraper_key=scraper_key,
                    failure_kind=kind.value,
                    error_message=str(exc),
                    source_url=_LIST_URL,
                    league_name="US Club Soccer",
                )
                outcomes.append(outcome)
                return outcomes

            for t in tournaments:
                meta = _tournament_to_meta(t, season=season)
                if dry_run:
                    tag = ""
                    if t.gotsport_event_id:
                        tag = f" [GotSport #{t.gotsport_event_id}]"
                    elif t.platform == "gotsport":
                        tag = " [GotSport (reg link)]"
                    elif t.sincsports_tid:
                        tag = f" [SincSports {t.sincsports_tid}]"
                    elif t.platform == "sincsports":
                        tag = " [SincSports]"
                    else:
                        tag = " [Other]"
                    logger.info("  [dry-run]%s %s | %s | %s | %s", tag, t.name, t.start_date or "?", t.state or "?", t.host_club or "?")
                    outcome.events_created += 1
                else:
                    result = upsert_event_and_teams(meta, [], conn=conn, dry_run=False)
                    outcome.events_created += result.events_created
                    outcome.events_updated += result.events_updated

                outcome.events_processed += 1
                if t.platform == "gotsport":
                    outcome.gotsport_ids_found += 1
                if t.platform == "sincsports":
                    outcome.sincsports_tids_found += 1

            if run_log is not None:
                run_log.finish_ok(
                    records_created=outcome.events_created,
                    records_updated=outcome.events_updated,
                )

            outcomes.append(outcome)
            logger.info(
                "[usclub] Phase 2 done: %d discovered (%d GotSport IDs, %d SincSports TIDs)",
                outcome.events_processed, outcome.gotsport_ids_found, outcome.sincsports_tids_found,
            )

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return outcomes


def print_summary(outcomes: List[RunOutcome]) -> None:
    print("\n" + "=" * 60)
    print("  US Club Soccer Events — run summary")
    print("=" * 60)

    for o in outcomes:
        print(f"\n  Phase: {o.phase}")
        print(f"    Events processed  : {o.events_processed}")
        print(f"    Created           : {o.events_created}")
        print(f"    Updated           : {o.events_updated}")
        if o.gotsport_ids_found:
            print(f"    GotSport IDs      : {o.gotsport_ids_found}")
        if o.sincsports_tids_found:
            print(f"    SincSports TIDs   : {o.sincsports_tids_found}")
        if o.error:
            print(f"    ERROR             : {o.error}")

    total_processed = sum(o.events_processed for o in outcomes)
    total_gs = sum(o.gotsport_ids_found for o in outcomes)
    total_sc = sum(o.sincsports_tids_found for o in outcomes)
    print(f"\n  TOTAL: {total_processed} events ({total_gs} GotSport, {total_sc} SincSports)")
    print("=" * 60)
