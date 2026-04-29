"""
gotsport_matches_runner.py — Batch matches runner for GotSport events.

Iterates all events in the ``events`` table with ``source='gotsport'``
and ``platform_event_id IS NOT NULL``, scrapes match schedules for each,
and writes results to the ``matches`` table.

Invoked via ``run.py --source gotsport-matches-batch``.

Per event:
  1. Look up the DB ``events.id`` FK for the platform event ID.
  2. Call ``scrape_gotsport_matches()`` from the existing extractor.
  3. Stamp ``event_fk_id`` (the DB id, not the platform id) onto each row.
  4. Feed results to ``insert_matches()`` from the existing writer.
  5. Log a ``scrape_run_logs`` row.

Fails soft: one bad event does not stop the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.gotsport_matches import scrape_gotsport_matches  # noqa: E402
from ingest.matches_writer import insert_matches  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("gotsport_matches_runner")

_RATE_LIMIT_SECONDS = 2.0


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_connection():
    try:
        import psycopg2  # type: ignore
    except ImportError:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _fetch_gotsport_events(
    conn,
    *,
    event_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[dict]:
    """Query the events table for GotSport events with platform_event_id.

    Returns list of dicts with keys: id, name, platform_event_id, season, league_name.
    """
    sql = """
        SELECT id, name, platform_event_id, season, league_name
        FROM events
        WHERE source = 'gotsport'
          AND platform_event_id IS NOT NULL
          AND platform_event_id ~ '^\d+$'
    """
    params: list = []
    if event_id:
        sql += " AND platform_event_id = %s"
        params.append(str(event_id))
    sql += " ORDER BY id"
    if limit is not None and limit > 0:
        sql += " LIMIT %s"
        params.append(limit)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Outcome tracking
# ---------------------------------------------------------------------------

@dataclass
class MatchRunOutcome:
    platform_event_id: str
    event_name: str
    event_db_id: Optional[int] = None
    match_count: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_gotsport_matches_batch(
    *,
    dry_run: bool = False,
    event_id: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[MatchRunOutcome]:
    """Scrape matches for all (or filtered) GotSport events in the DB.

    Returns per-event outcomes for reporting in the CLI summary.
    """
    conn = _get_connection()
    try:
        events = _fetch_gotsport_events(conn, event_id=event_id, limit=limit)
    finally:
        conn.close()

    if not events:
        if event_id:
            logger.error("No GotSport event found with platform_event_id=%s", event_id)
        else:
            logger.warning("No GotSport events in the events table")
        return []

    logger.info("Processing %d GotSport event(s) for matches", len(events))
    outcomes: List[MatchRunOutcome] = []

    for i, ev in enumerate(events):
        platform_eid = str(ev["platform_event_id"])
        event_name = ev["name"] or f"gotsport-event-{platform_eid}"
        event_db_id = ev["id"]
        season = ev.get("season")
        league_name = ev.get("league_name")
        scraper_key = f"gotsport-matches:{platform_eid}"
        source_url = f"https://system.gotsport.com/org_event/events/{platform_eid}/schedules"

        outcome = MatchRunOutcome(
            platform_event_id=platform_eid,
            event_name=event_name,
            event_db_id=event_db_id,
        )

        run_log: Optional[ScrapeRunLogger] = None
        if not dry_run:
            run_log = ScrapeRunLogger(scraper_key=scraper_key, league_name=league_name or event_name)
            run_log.start(source_url=source_url)

        # --- Extract ---
        try:
            rows = scrape_gotsport_matches(
                platform_eid,
                default_season=season,
                default_league=league_name,
            )
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[gotsport-matches-batch] event %s failed: %s", platform_eid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=source_url,
                league_name=league_name or event_name,
            )
            outcomes.append(outcome)
            _rate_limit(i, len(events))
            continue

        outcome.match_count = len(rows)

        if not rows:
            logger.warning("[gotsport-matches-batch] event %s -> 0 matches", platform_eid)
            if run_log is not None:
                run_log.finish_partial(records_failed=0, error_message="no matches extracted")
            outcome.failure_kind = FailureKind.ZERO_RESULTS
            outcomes.append(outcome)
            _rate_limit(i, len(events))
            continue

        # Stamp the DB-level event_id FK onto each row so the writer
        # can populate matches.event_id correctly.
        for r in rows:
            r["event_fk_id"] = event_db_id

        # --- Write ---
        try:
            counts = insert_matches(rows, dry_run=dry_run)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[gotsport-matches-batch] event %s write failed: %s", platform_eid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=source_url,
                league_name=league_name or event_name,
            )
            outcomes.append(outcome)
            _rate_limit(i, len(events))
            continue

        outcome.counts = counts
        logger.info(
            "[gotsport-matches-batch] event %s -> %d matches, inserted=%d updated=%d skipped=%d",
            platform_eid, len(rows),
            counts.get("inserted", 0), counts.get("updated", 0),
            counts.get("skipped", 0),
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=counts.get("inserted", 0),
                records_updated=counts.get("updated", 0),
                records_failed=counts.get("skipped", 0),
            )
        outcomes.append(outcome)
        _rate_limit(i, len(events))

    # Post-run reconcile
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcomes


def _rate_limit(index: int, total: int) -> None:
    """Sleep between events (skip after the last one)."""
    if index < total - 1:
        time.sleep(_RATE_LIMIT_SECONDS)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(outcomes: List[MatchRunOutcome]) -> None:
    total = len(outcomes)
    succeeded = sum(1 for o in outcomes if o.failure_kind is None and o.match_count > 0)
    failed = sum(1 for o in outcomes if o.failure_kind is not None)
    total_inserted = sum(o.counts.get("inserted", 0) for o in outcomes)
    total_updated = sum(o.counts.get("updated", 0) for o in outcomes)

    print("\n" + "=" * 60)
    print("  GotSport Matches (batch) -- run summary")
    print("=" * 60)
    print(f"  Events processed  : {total}")
    print(f"  Succeeded         : {succeeded}")
    print(f"  Failed            : {failed}")
    print(f"  Matches inserted  : {total_inserted}")
    print(f"  Matches updated   : {total_updated}")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                kind_val = o.failure_kind.value if o.failure_kind else ""
                print(f"    * {o.platform_event_id} ({o.event_name}) -- {kind_val}: {(o.error or '')[:80]}")
    print("=" * 60)
