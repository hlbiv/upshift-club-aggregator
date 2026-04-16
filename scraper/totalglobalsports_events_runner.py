"""
totalglobalsports_events_runner.py — Orchestrate the TotalGlobalSports
events scraper across configured league seeds.

Invoked from ``run.py --source totalglobalsports-events``.

Seeds come from two sources:
  1. ``leagues_master.csv`` notes column — regex-extracted TGS event IDs
     (patterns like "TotalGlobalSports events 3979 + 3973").
  2. Explicit ``--event-id`` CLI argument(s).

For each event:
  1. Fetch + parse the event details + division + team endpoints.
  2. Upsert into ``events`` + ``event_teams`` via ``events_writer``.
  3. Log a ``scrape_run_logs`` row.

Failure handling mirrors ``gotsport_events_runner``: per-event try/except,
soft-failure alerts via ``alert_scraper_failure``, partial-success logging
for ZERO_RESULTS.
"""

from __future__ import annotations

import csv
import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.totalglobalsports_events import (
    EventMeta,
    TeamRow,
    scrape_totalglobalsports_event,
)
from events_writer import WriteResult, upsert_event_and_teams, _connect
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind,
    classify_exception,
)
from alerts import alert_scraper_failure

logger = logging.getLogger("totalglobalsports_events_runner")

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
# Match patterns like:
#   "TotalGlobalSports events 3979 + 3973"
#   "TotalGlobalSports event 3979"
_TGS_EVENT_RE = re.compile(
    r"TotalGlobalSports\s+events?\s+(\d+(?:\s*[+,\-]\s*\d+)*)",
    re.IGNORECASE,
)
_BARE_DIGITS_RE = re.compile(r"\d+")


@dataclass
class EventRunOutcome:
    event_id: str
    league_name: str
    team_count: int
    write: WriteResult
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def extract_tgs_event_ids_from_csv() -> List[dict]:
    """Parse TGS event IDs from ``leagues_master.csv`` notes column.

    Returns a list of ``{"event_id": str, "league_name": str}`` dicts.
    """
    csv_path = os.path.join(_DATA_DIR, "leagues_master.csv")
    if not os.path.exists(csv_path):
        logger.warning("leagues_master.csv not found at %s", csv_path)
        return []

    seeds: List[dict] = []
    seen: set = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            notes = row.get("notes", "")
            league_name = row.get("league_name", "").strip()
            if not notes:
                continue

            for m in _TGS_EVENT_RE.finditer(notes):
                block = m.group(1)
                for digit_match in _BARE_DIGITS_RE.finditer(block):
                    eid = digit_match.group(0)
                    if eid not in seen:
                        seen.add(eid)
                        seeds.append({
                            "event_id": eid,
                            "league_name": league_name,
                        })

    return seeds


def run_totalglobalsports_events(
    dry_run: bool = False,
    event_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
    season: str = "2025-26",
) -> List[EventRunOutcome]:
    """Scrape + upsert TGS events.

    If ``event_ids`` is provided, only those events are scraped.
    Otherwise, seeds are extracted from ``leagues_master.csv``.
    """
    if event_ids:
        seeds = [
            {"event_id": eid, "league_name": f"tgs-event-{eid}"}
            for eid in event_ids
        ]
    else:
        seeds = extract_tgs_event_ids_from_csv()

    if not seeds:
        logger.warning("No TotalGlobalSports event seeds found")
        return []

    if limit is not None and limit > 0:
        seeds = seeds[:limit]

    logger.info("Processing %d TotalGlobalSports event(s)", len(seeds))

    conn = None if dry_run else _connect()
    outcomes: List[EventRunOutcome] = []

    try:
        for seed in seeds:
            event_id = seed["event_id"]
            league_name = seed["league_name"]
            scraper_key = f"tgs-events:{event_id}"

            run_log = None
            if not dry_run:
                run_log = ScrapeRunLogger(
                    scraper_key=scraper_key, league_name=league_name,
                )
                run_log.start(
                    source_url=(
                        f"https://public.totalglobalsports.com/events/{event_id}"
                    ),
                )

            try:
                meta, teams = scrape_totalglobalsports_event(
                    event_id, league_name=league_name, season=season,
                )
            except Exception as exc:
                kind = classify_exception(exc)
                logger.error(
                    "[tgs-events] event %s failed: %s", event_id, exc,
                )
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                alert_scraper_failure(
                    scraper_key=scraper_key,
                    failure_kind=kind.value,
                    error_message=str(exc),
                    source_url=(
                        f"https://public.totalglobalsports.com/events/{event_id}"
                    ),
                    league_name=league_name,
                )
                outcomes.append(EventRunOutcome(
                    event_id=event_id, league_name=league_name,
                    team_count=0, write=WriteResult(),
                    failure_kind=kind, error=str(exc),
                ))
                continue

            if not teams:
                logger.warning(
                    "[tgs-events] event %s returned 0 teams", event_id,
                )
                if run_log is not None:
                    run_log.finish_partial(
                        records_failed=0,
                        error_message="0 teams parsed from TGS team list API",
                    )
                outcomes.append(EventRunOutcome(
                    event_id=event_id, league_name=league_name,
                    team_count=0, write=WriteResult(),
                    failure_kind=FailureKind.ZERO_RESULTS,
                    error="0 teams parsed",
                ))
                continue

            result = upsert_event_and_teams(
                meta, teams, conn=conn, dry_run=dry_run,
            )
            logger.info(
                "[tgs-events] event %s  teams=%d  created=%d  skipped=%d",
                event_id, len(teams),
                result.teams_created, result.teams_skipped,
            )

            if run_log is not None:
                run_log.finish_ok(
                    records_created=result.teams_created,
                    records_updated=(
                        result.events_updated + result.events_created
                        + result.teams_updated
                    ),
                    records_failed=result.teams_failed,
                )

            outcomes.append(EventRunOutcome(
                event_id=event_id, league_name=league_name,
                team_count=len(teams), write=result,
            ))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # Post-run reconcile.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcomes


def print_summary(outcomes: List[EventRunOutcome]) -> None:
    total_events = len(outcomes)
    succeeded = sum(
        1 for o in outcomes if o.failure_kind is None and o.team_count > 0
    )
    failed = sum(1 for o in outcomes if o.failure_kind is not None)
    total_teams = sum(o.write.teams_created for o in outcomes)

    print("\n" + "=" * 60)
    print("  TotalGlobalSports Events — run summary")
    print("=" * 60)
    print(f"  Events processed  : {total_events}")
    print(f"  Succeeded         : {succeeded}")
    print(f"  Failed            : {failed}")
    print(f"  event_teams rows  : {total_teams} new")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                err_snippet = (o.error or "")[:80]
                print(
                    f"    * {o.event_id} ({o.league_name}) "
                    f"— {o.failure_kind.value}: {err_snippet}"
                )
    print("=" * 60)
