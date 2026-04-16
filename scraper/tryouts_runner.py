"""
tryouts_runner.py — Orchestrate tryout scrapers (WordPress + GotSport).

Invoked via:
    ``run.py --source tryouts-wordpress``  — WordPress sites only
    ``run.py --source tryouts-gotsport``   — GotSport events only
    ``run.py --source tryouts``            — all sources + status expiry

Walks configured seeds for each source, upserts into ``tryouts``, logs
``scrape_run_logs`` rows per batch. At the end of the unified ``tryouts``
run, expires past-date tryouts via ``tryouts_status_updater``.

Fails soft: a single site's fetch failure never stops the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.tryouts_wordpress import scrape_tryouts_wordpress  # noqa: E402
from extractors.tryouts_wordpress_seed import TRYOUTS_WORDPRESS_SEED  # noqa: E402
from extractors.gotsport_tryouts import scrape_gotsport_tryouts  # noqa: E402
from ingest.tryouts_writer import insert_tryouts  # noqa: E402
from tryouts_status_updater import expire_past_tryouts  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("tryouts_runner")

# GotSport event IDs known to host tryout/combine events. Grow this list
# as new tryout events appear on GotSport. Shape: list of dicts with
# ``event_id`` (str) and ``league_name`` (str, for logging).
GOTSPORT_TRYOUT_SEEDS: List[Dict[str, str]] = [
    # TODO: backfill from live GotSport event discovery.
    # {"event_id": "50001", "league_name": "ECNL Boys Tryouts 2026"},
]


@dataclass
class TryoutsRunOutcome:
    site_count: int = 0
    row_count: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def run_tryouts_wordpress(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> List[TryoutsRunOutcome]:
    sites = list(TRYOUTS_WORDPRESS_SEED)
    if limit is not None:
        sites = sites[:limit]

    outcome = TryoutsRunOutcome(site_count=len(sites))
    outcomes = [outcome]

    if not sites:
        logger.info("[tryouts-wordpress] seed list is empty — nothing to do")
        return outcomes

    scraper_key = "tryouts-wordpress"
    run_log = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="WordPress tryouts",
        )
        run_log.start(source_url="seed:TRYOUTS_WORDPRESS_SEED")

    try:
        rows = scrape_tryouts_wordpress(sites)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[tryouts-wordpress] scraping failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="seed:TRYOUTS_WORDPRESS_SEED",
            league_name="WordPress tryouts",
        )
        return outcomes

    outcome.row_count = len(rows)
    if not rows:
        logger.warning("[tryouts-wordpress] 0 rows extracted from %d site(s)", len(sites))
        if run_log is not None:
            run_log.finish_partial(records_failed=0, error_message="no tryout rows extracted")
        outcome.failure_kind = FailureKind.ZERO_RESULTS
        return outcomes

    try:
        counts = insert_tryouts(rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[tryouts-wordpress] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="seed:TRYOUTS_WORDPRESS_SEED",
            league_name="WordPress tryouts",
        )
        return outcomes

    outcome.counts = counts
    logger.info(
        "[tryouts-wordpress] sites=%d rows=%d inserted=%d updated=%d skipped=%d",
        len(sites), len(rows),
        counts.get("inserted", 0), counts.get("updated", 0), counts.get("skipped", 0),
    )
    if run_log is not None:
        run_log.finish_ok(
            records_created=counts.get("inserted", 0),
            records_updated=counts.get("updated", 0),
            records_failed=counts.get("skipped", 0),
        )

    # Post-run scrape_health reconcile — soft failure only.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcomes


def run_tryouts_gotsport(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
    event_ids: Optional[List[str]] = None,
) -> List[TryoutsRunOutcome]:
    """Scrape tryout listings from GotSport events.

    ``event_ids`` overrides the seed list for ad-hoc CLI use.
    """
    seeds = (
        [{"event_id": eid, "league_name": f"gotsport-{eid}"} for eid in event_ids]
        if event_ids
        else list(GOTSPORT_TRYOUT_SEEDS)
    )
    if limit is not None:
        seeds = seeds[:limit]

    outcomes: List[TryoutsRunOutcome] = []

    if not seeds:
        logger.info("[tryouts-gotsport] seed list is empty — nothing to do")
        outcomes.append(TryoutsRunOutcome())
        return outcomes

    for seed in seeds:
        eid = seed["event_id"]
        league_name = seed.get("league_name", f"gotsport-tryout-{eid}")
        scraper_key = f"tryouts-gotsport:{eid}"

        outcome = TryoutsRunOutcome(site_count=1)

        run_log = None
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=league_name,
            )
            run_log.start(
                source_url=f"https://system.gotsport.com/org_event/events/{eid}/teams",
            )

        try:
            rows = scrape_gotsport_tryouts(eid)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[tryouts-gotsport] event %s failed: %s", eid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=f"https://system.gotsport.com/org_event/events/{eid}/teams",
                league_name=league_name,
            )
            outcomes.append(outcome)
            continue

        outcome.row_count = len(rows)
        if not rows:
            logger.warning("[tryouts-gotsport] event %s -> 0 tryout rows", eid)
            if run_log is not None:
                run_log.finish_partial(
                    records_failed=0,
                    error_message="no tryout rows extracted",
                )
            outcome.failure_kind = FailureKind.ZERO_RESULTS
            outcomes.append(outcome)
            continue

        try:
            counts = insert_tryouts(rows, dry_run=dry_run)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.failure_kind = kind
            outcome.error = str(exc)
            logger.error("[tryouts-gotsport] event %s write failed: %s", eid, exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=f"https://system.gotsport.com/org_event/events/{eid}/teams",
                league_name=league_name,
            )
            outcomes.append(outcome)
            continue

        outcome.counts = counts
        logger.info(
            "[tryouts-gotsport] event=%s rows=%d inserted=%d updated=%d skipped=%d",
            eid, len(rows),
            counts.get("inserted", 0), counts.get("updated", 0), counts.get("skipped", 0),
        )
        if run_log is not None:
            run_log.finish_ok(
                records_created=counts.get("inserted", 0),
                records_updated=counts.get("updated", 0),
                records_failed=counts.get("skipped", 0),
            )
        outcomes.append(outcome)

    return outcomes


def run_tryouts(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
    only_source: Optional[str] = None,
) -> List[TryoutsRunOutcome]:
    """Unified tryout runner — all sources + status expiry.

    ``only_source`` restricts to ``"wordpress"`` or ``"gotsport"``.
    """
    all_outcomes: List[TryoutsRunOutcome] = []

    if only_source is None or only_source == "wordpress":
        wp = run_tryouts_wordpress(dry_run=dry_run, limit=limit)
        all_outcomes.extend(wp)

    if only_source is None or only_source == "gotsport":
        gs = run_tryouts_gotsport(dry_run=dry_run, limit=limit)
        all_outcomes.extend(gs)

    # Expire past-date tryouts.
    if not dry_run:
        try:
            result = expire_past_tryouts()
            logger.info("[tryouts] expired %d past-date tryout(s)", result.get("expired", 0))
        except Exception as exc:
            logger.warning("[tryouts] status expiry failed: %s", exc)
    else:
        try:
            result = expire_past_tryouts(dry_run=True)
        except Exception as exc:
            logger.warning("[tryouts] status expiry check failed: %s", exc)

    # Post-run scrape_health reconcile — soft failure only.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return all_outcomes


def print_summary(outcomes: List[TryoutsRunOutcome]) -> None:
    if not outcomes:
        print("\n[tryouts] no outcomes to summarize.")
        return

    total_sites = sum(o.site_count for o in outcomes)
    total_rows = sum(o.row_count for o in outcomes)
    total_inserted = sum(o.counts.get("inserted", 0) for o in outcomes)
    total_updated = sum(o.counts.get("updated", 0) for o in outcomes)
    total_skipped = sum(o.counts.get("skipped", 0) for o in outcomes)
    failed = sum(1 for o in outcomes if o.failure_kind is not None)

    print("\n" + "=" * 60)
    print("  Tryouts — run summary")
    print("=" * 60)
    print(f"  Sources processed : {len(outcomes)}")
    print(f"  Sites probed      : {total_sites}")
    print(f"  Rows extracted    : {total_rows}")
    print(f"  Inserted          : {total_inserted}")
    print(f"  Updated           : {total_updated}")
    print(f"  Skipped           : {total_skipped}")
    print(f"  Failed            : {failed}")
    if failed:
        print("\n  Failures:")
        for o in outcomes:
            if o.failure_kind is not None:
                kind_val = o.failure_kind.value if o.failure_kind else ""
                print(f"    - {kind_val}: {(o.error or '')[:80]}")
    print("=" * 60)
