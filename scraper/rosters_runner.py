"""
rosters_runner.py — Orchestrate the SincSports rosters scraper across
the same seed list ``events_runner.py`` uses for SincSports events.

Invoked via ``run.py --source sincsports-rosters``.

Per tournament (tid):
  1. Extract the team list.
  2. Fetch each team's TTRoster page.
  3. Upsert rows into ``club_roster_snapshots`` and materialize any
     ``roster_diffs`` against the previous snapshot.
  4. Log a ``scrape_run_logs`` row per seed.

Fails soft: one bad tid does not stop the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from config import get_leagues  # noqa: E402
from extractors.sincsports_events import extract_tid  # noqa: E402
from extractors.sincsports_rosters import scrape_sincsports_rosters  # noqa: E402
from ingest.roster_snapshot_writer import insert_roster_snapshots  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("rosters_runner")


@dataclass
class RosterRunOutcome:
    tid: str
    league_name: str
    row_count: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _sincsports_seeds() -> List[dict]:
    seeds: List[dict] = []
    for lg in get_leagues():
        url = (lg.get("url") or "")
        st = (lg.get("source_type") or "").lower()
        if "sincsports.com" in url or st == "sincsports":
            tid = extract_tid(url)
            if tid:
                seeds.append(lg)
    return seeds


def run_sincsports_rosters(
    *,
    dry_run: bool = False,
    only_tid: Optional[str] = None,
) -> List[RosterRunOutcome]:
    seeds = _sincsports_seeds()
    if only_tid:
        seeds = [s for s in seeds if extract_tid(s["url"]) == only_tid.upper()]
        if not seeds:
            logger.error("No SincSports seed matches tid=%s", only_tid)
            return []

    logger.info("Processing %d SincSports rosters seed(s)", len(seeds))
    outcomes: List[RosterRunOutcome] = []

    for lg in seeds:
        tid = extract_tid(lg["url"])
        assert tid, "filtered above"
        league_name = lg["name"]
        scraper_key = f"sincsports-rosters:{tid}"

        run_log = None
        if not dry_run:
            run_log = ScrapeRunLogger(scraper_key=scraper_key, league_name=league_name)
            run_log.start(source_url=lg["url"])

        outcome = RosterRunOutcome(tid=tid, league_name=league_name)
        try:
            rows = scrape_sincsports_rosters(tid)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[sincsports-rosters] tid=%s failed: %s", tid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=lg["url"],
                league_name=league_name,
            )
            outcomes.append(outcome)
            continue

        outcome.row_count = len(rows)
        if not rows:
            logger.warning("[sincsports-rosters] tid=%s — 0 rows", tid)
            if run_log is not None:
                run_log.finish_partial(records_failed=0, error_message="0 roster rows extracted")
            outcome.failure_kind = FailureKind.ZERO_RESULTS
            outcomes.append(outcome)
            continue

        try:
            counts = insert_roster_snapshots(rows, dry_run=dry_run)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[sincsports-rosters] tid=%s write failed: %s", tid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=lg["url"],
                league_name=league_name,
            )
            outcomes.append(outcome)
            continue

        outcome.counts = counts
        logger.info(
            "[sincsports-rosters] tid=%s rows=%d inserted=%d updated=%d diffs=%d",
            tid, len(rows),
            counts.get("inserted", 0), counts.get("updated", 0),
            counts.get("diffs_written", 0),
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=counts.get("inserted", 0),
                records_updated=counts.get("updated", 0),
                records_failed=counts.get("skipped", 0),
            )
        outcomes.append(outcome)

    # Post-run scrape_health reconcile — soft failure only.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcomes


def print_summary(outcomes: List[RosterRunOutcome]) -> None:
    total = len(outcomes)
    succeeded = sum(1 for o in outcomes if o.failure_kind is None and o.row_count > 0)
    failed = sum(1 for o in outcomes if o.failure_kind is not None)
    total_inserted = sum(o.counts.get("inserted", 0) for o in outcomes)
    total_updated = sum(o.counts.get("updated", 0) for o in outcomes)
    total_diffs = sum(o.counts.get("diffs_written", 0) for o in outcomes)

    print("\n" + "=" * 60)
    print("  SincSports Rosters — run summary")
    print("=" * 60)
    print(f"  Events processed   : {total}")
    print(f"  Succeeded          : {succeeded}")
    print(f"  Failed             : {failed}")
    print(f"  Snapshots inserted : {total_inserted}")
    print(f"  Snapshots updated  : {total_updated}")
    print(f"  Diffs written      : {total_diffs}")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                kind_val = o.failure_kind.value if o.failure_kind else ""
                print(f"    • {o.tid} ({o.league_name}) — {kind_val}: {(o.error or '')[:80]}")
    print("=" * 60)
