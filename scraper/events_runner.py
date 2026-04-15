"""
events_runner.py — Orchestrate the SincSports events scraper across the
configured tournament seeds.

Invoked from ``run.py --source sincsports-events``.

For each seeded SincSports event (leagues with source_type ``sincsports``
in ``config.get_leagues()``):
  1. Extract ``tid`` from the league URL.
  2. Fetch + parse the TTTeamList page.
  3. Upsert into ``events`` + ``event_teams``.
  4. Log a ``scrape_run_logs`` row per seed.

Fails soft: one bad URL does not stop the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

# Allow invocation as a module or script.
sys.path.insert(0, os.path.dirname(__file__))

from config import get_leagues
from extractors.sincsports_events import (
    EventMeta,
    TeamRow,
    extract_tid,
    fetch_and_parse,
)
from events_writer import WriteResult, upsert_event_and_teams, _connect
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind,
    classify_exception,
)

logger = logging.getLogger("events_runner")


@dataclass
class EventRunOutcome:
    tid: str
    league_name: str
    team_count: int
    write: WriteResult
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _sincsports_seeds() -> List[dict]:
    """Return every configured league that uses the SincSports platform."""
    seeds: List[dict] = []
    for lg in get_leagues():
        url = (lg.get("url") or "")
        st = (lg.get("source_type") or "").lower()
        if "sincsports.com" in url or st == "sincsports":
            tid = extract_tid(url)
            if tid:
                seeds.append(lg)
    return seeds


def run_sincsports_events(
    dry_run: bool = False,
    only_tid: Optional[str] = None,
) -> List[EventRunOutcome]:
    """Scrape + upsert every configured SincSports event.

    Returns per-event outcomes for reporting in the CLI summary.
    ``only_tid`` is a developer convenience — run a single event without
    having to re-filter the league CSV.
    """
    seeds = _sincsports_seeds()
    if only_tid:
        seeds = [s for s in seeds if extract_tid(s["url"]) == only_tid.upper()]
        if not seeds:
            logger.error("No SincSports seed matches tid=%s", only_tid)
            return []

    logger.info("Processing %d SincSports event(s)", len(seeds))

    # Share one connection across all events for the run.
    conn = None if dry_run else _connect()
    outcomes: List[EventRunOutcome] = []

    try:
        for lg in seeds:
            tid = extract_tid(lg["url"])
            assert tid, "filtered above"
            league_name = lg["name"]
            scraper_key = f"sincsports-events:{tid}"

            run_log = None
            if not dry_run:
                run_log = ScrapeRunLogger(scraper_key=scraper_key, league_name=league_name)
                run_log.start(source_url=lg["url"])

            try:
                meta, teams = fetch_and_parse(tid, league_name=league_name)
            except Exception as exc:
                kind = classify_exception(exc)
                logger.error("[sincsports-events] tid=%s failed: %s", tid, exc)
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                outcomes.append(EventRunOutcome(
                    tid=tid, league_name=league_name, team_count=0,
                    write=WriteResult(), failure_kind=kind, error=str(exc),
                ))
                continue

            if not teams:
                logger.warning("[sincsports-events] tid=%s returned 0 teams", tid)
                if run_log is not None:
                    run_log.finish_partial(
                        records_failed=0,
                        error_message="0 teams parsed from TTTeamList",
                    )
                outcomes.append(EventRunOutcome(
                    tid=tid, league_name=league_name, team_count=0,
                    write=WriteResult(), failure_kind=FailureKind.ZERO_RESULTS,
                    error="0 teams parsed",
                ))
                continue

            result = upsert_event_and_teams(meta, teams, conn=conn, dry_run=dry_run)
            logger.info(
                "[sincsports-events] tid=%s  teams=%d  created=%d  skipped=%d",
                tid, len(teams), result.teams_created, result.teams_skipped,
            )

            if run_log is not None:
                run_log.finish_ok(
                    records_created=result.events_created + result.teams_created,
                    records_updated=result.events_updated,
                )

            outcomes.append(EventRunOutcome(
                tid=tid, league_name=league_name, team_count=len(teams), write=result,
            ))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    return outcomes


def print_summary(outcomes: List[EventRunOutcome]) -> None:
    total_events = len(outcomes)
    succeeded = sum(1 for o in outcomes if o.failure_kind is None and o.team_count > 0)
    failed = sum(1 for o in outcomes if o.failure_kind is not None)
    total_teams = sum(o.write.teams_created for o in outcomes)

    print("\n" + "=" * 60)
    print("  SincSports Events — run summary")
    print("=" * 60)
    print(f"  Events processed  : {total_events}")
    print(f"  Succeeded         : {succeeded}")
    print(f"  Failed            : {failed}")
    print(f"  event_teams rows  : {total_teams} new")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                print(f"    • {o.tid} ({o.league_name}) — {o.failure_kind.value}: {(o.error or '')[:80]}")
    print("=" * 60)
