"""
tryouts_runner.py — Orchestrate the WordPress tryouts scraper.

Invoked via ``run.py --source tryouts-wordpress``.

Walks ``TRYOUTS_WORDPRESS_SEED`` (see ``extractors.tryouts_wordpress_seed``),
probes each site's common tryout paths, upserts into ``tryouts``, logs a
single ``scrape_run_logs`` row for the batch.

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
from ingest.tryouts_writer import insert_tryouts  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)

logger = logging.getLogger("tryouts_runner")


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
    return outcomes


def print_summary(outcomes: List[TryoutsRunOutcome]) -> None:
    if not outcomes:
        print("\n[tryouts-wordpress] no outcomes to summarize.")
        return
    o = outcomes[0]
    print("\n" + "=" * 60)
    print("  WordPress Tryouts — run summary")
    print("=" * 60)
    print(f"  Sites probed   : {o.site_count}")
    print(f"  Rows extracted : {o.row_count}")
    print(f"  Inserted       : {o.counts.get('inserted', 0)}")
    print(f"  Updated        : {o.counts.get('updated', 0)}")
    print(f"  Skipped        : {o.counts.get('skipped', 0)}")
    if o.failure_kind is not None:
        kind_val = o.failure_kind.value if o.failure_kind else ""
        print(f"  Failure        : {kind_val}: {(o.error or '')[:80]}")
    print("=" * 60)
