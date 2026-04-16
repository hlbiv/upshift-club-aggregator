"""
gotsport_events_runner.py — Orchestrate the GotSport events scraper
across configured tournament seeds.

Invoked from ``run.py --source gotsport-events``.

Seeds come from two sources:
  1. ``leagues_master.csv`` notes column — regex-extracted GotSport event IDs.
  2. Explicit ``--event-id`` CLI argument(s).

For each event:
  1. Fetch + parse the teams page (division codes + team tables).
  2. Upsert into ``events`` + ``event_teams`` via ``events_writer``.
  3. Log a ``scrape_run_logs`` row.
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

from extractors.gotsport_events import (
    EventMeta,
    TeamRow,
    scrape_gotsport_event,
)
from events_writer import WriteResult, upsert_event_and_teams, _connect
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind,
    classify_exception,
)
from alerts import alert_scraper_failure

logger = logging.getLogger("gotsport_events_runner")

_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
_GOTSPORT_EVENT_RE = re.compile(r"GotSport\s+events?\s+(\d+)", re.IGNORECASE)


@dataclass
class EventRunOutcome:
    event_id: str
    league_name: str
    team_count: int
    write: WriteResult
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def extract_gotsport_event_ids_from_csv() -> List[dict]:
    """Parse GotSport event IDs from ``leagues_master.csv`` notes column.

    Returns a list of ``{"event_id": str, "league_name": str}`` dicts.
    A single notes field may contain multiple event IDs (e.g.
    "GotSport events 50731-50734" → individual IDs, or
    "GotSport event 45036 (current, 50 clubs) + 36297 (prior, 20 clubs)").
    """
    csv_path = os.path.join(_DATA_DIR, "leagues_master.csv")
    if not os.path.exists(csv_path):
        logger.warning("leagues_master.csv not found at %s", csv_path)
        return []

    seeds: List[dict] = []
    seen_ids: set = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            notes = row.get("notes", "")
            league_name = row.get("league_name", "").strip()
            if not notes:
                continue

            # Find all event IDs mentioned in notes.
            # Handles: "GotSport event 45036", "GotSport events 44015"
            for m in _GOTSPORT_EVENT_RE.finditer(notes):
                eid = m.group(1)
                if eid not in seen_ids:
                    seen_ids.add(eid)
                    seeds.append({
                        "event_id": eid,
                        "league_name": league_name,
                    })

            # Also handle comma/space-separated IDs after "GotSport events":
            # e.g. "GotSport events 50731-50734" — extract range endpoints
            # Actually, looking at real data these are separate event IDs
            # not ranges. Handle "50731 + 50987" or "50731, 50987" patterns
            # by finding all bare \d{4,6} near "GotSport event" context.
            # The regex above already handles "GotSport event(s) NNNN".
            # For IDs that appear as "... + 36297 (prior..." we need a
            # second pass:
            for extra_m in re.finditer(r"\b(\d{4,6})\b", notes):
                eid = extra_m.group(1)
                # Only include if this looks like a GotSport event context
                # (the notes mention "GotSport" somewhere)
                if "gotsport" in notes.lower() and eid not in seen_ids:
                    # Avoid matching years like "2025-26" or "2024-25"
                    start = extra_m.start()
                    if start > 0 and notes[start - 1] in ("-", "/"):
                        continue
                    end = extra_m.end()
                    if end < len(notes) and notes[end] in ("-", "/"):
                        continue
                    seen_ids.add(eid)
                    seeds.append({
                        "event_id": eid,
                        "league_name": league_name,
                    })

    return seeds


def run_gotsport_events(
    dry_run: bool = False,
    event_ids: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> List[EventRunOutcome]:
    """Scrape + upsert GotSport events.

    If ``event_ids`` is provided, only those events are scraped.
    Otherwise, seeds are extracted from ``leagues_master.csv``.
    """
    if event_ids:
        seeds = [{"event_id": eid, "league_name": f"gotsport-event-{eid}"} for eid in event_ids]
    else:
        seeds = extract_gotsport_event_ids_from_csv()

    if not seeds:
        logger.warning("No GotSport event seeds found")
        return []

    if limit is not None and limit > 0:
        seeds = seeds[:limit]

    logger.info("Processing %d GotSport event(s)", len(seeds))

    conn = None if dry_run else _connect()
    outcomes: List[EventRunOutcome] = []

    try:
        for seed in seeds:
            event_id = seed["event_id"]
            league_name = seed["league_name"]
            scraper_key = f"gotsport-events:{event_id}"

            run_log = None
            if not dry_run:
                run_log = ScrapeRunLogger(
                    scraper_key=scraper_key, league_name=league_name,
                )
                run_log.start(
                    source_url=f"https://system.gotsport.com/org_event/events/{event_id}/teams?showall=clean",
                )

            try:
                meta, teams = scrape_gotsport_event(
                    event_id, league_name=league_name,
                )
            except Exception as exc:
                kind = classify_exception(exc)
                logger.error("[gotsport-events] event %s failed: %s", event_id, exc)
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                alert_scraper_failure(
                    scraper_key=scraper_key,
                    failure_kind=kind.value,
                    error_message=str(exc),
                    source_url=f"https://system.gotsport.com/org_event/events/{event_id}/teams",
                    league_name=league_name,
                )
                outcomes.append(EventRunOutcome(
                    event_id=event_id, league_name=league_name,
                    team_count=0, write=WriteResult(),
                    failure_kind=kind, error=str(exc),
                ))
                continue

            if not teams:
                logger.warning("[gotsport-events] event %s returned 0 teams", event_id)
                if run_log is not None:
                    run_log.finish_partial(
                        records_failed=0,
                        error_message="0 teams parsed from GotSport teams page",
                    )
                outcomes.append(EventRunOutcome(
                    event_id=event_id, league_name=league_name,
                    team_count=0, write=WriteResult(),
                    failure_kind=FailureKind.ZERO_RESULTS,
                    error="0 teams parsed",
                ))
                continue

            result = upsert_event_and_teams(meta, teams, conn=conn, dry_run=dry_run)
            logger.info(
                "[gotsport-events] event %s  teams=%d  created=%d  skipped=%d",
                event_id, len(teams), result.teams_created, result.teams_skipped,
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
    succeeded = sum(1 for o in outcomes if o.failure_kind is None and o.team_count > 0)
    failed = sum(1 for o in outcomes if o.failure_kind is not None)
    total_teams = sum(o.write.teams_created for o in outcomes)

    print("\n" + "=" * 60)
    print("  GotSport Events — run summary")
    print("=" * 60)
    print(f"  Events processed  : {total_events}")
    print(f"  Succeeded         : {succeeded}")
    print(f"  Failed            : {failed}")
    print(f"  event_teams rows  : {total_teams} new")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                print(f"    * {o.event_id} ({o.league_name}) — {o.failure_kind.value}: {(o.error or '')[:80]}")
    print("=" * 60)
