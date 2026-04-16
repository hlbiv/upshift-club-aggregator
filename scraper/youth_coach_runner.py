"""
youth_coach_runner.py — Orchestrate the youth club coach scraper across
canonical_clubs with websites.

Invoked from ``run.py --source youth-coaches``.

For each club with a website in ``canonical_clubs``:
  1. Detect platform family from URL.
  2. Discover + fetch staff page.
  3. Parse coaching staff via multi-strategy HTML extraction.
  4. Upsert into ``coach_discoveries`` ON CONFLICT (club_id, name, title).
  5. Log a ``scrape_run_logs`` row for the run.

Fails soft: one bad URL does not stop the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

# Allow invocation as a module or script.
sys.path.insert(0, os.path.dirname(__file__))

from extractors.youth_club_coaches import scrape_youth_club_coaches, SCRAPER_KEY
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind,
    classify_exception,
)
from alerts import alert_scraper_failure

logger = logging.getLogger("youth_coach_runner")


def run_youth_coaches(
    dry_run: bool = False,
    limit: Optional[int] = None,
    state: Optional[str] = None,
    platform_family: Optional[str] = None,
) -> dict:
    """Scrape youth club coaching staff and write to coach_discoveries.

    Returns a summary dict with keys: scraped, rows_inserted, rows_updated, errors.
    """
    logger.info(
        "Youth coach runner: dry_run=%s limit=%s state=%s platform=%s",
        dry_run, limit, state, platform_family,
    )

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=SCRAPER_KEY,
            league_name="Youth Club Coaches",
        )
        run_log.start(source_url="canonical_clubs:website")

    try:
        result = scrape_youth_club_coaches(
            limit=limit,
            state=state,
            platform_family=platform_family,
            dry_run=dry_run,
        )
    except Exception as exc:
        kind = classify_exception(exc)
        logger.error("[youth-coaches] fatal error: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=SCRAPER_KEY,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="canonical_clubs:website",
            league_name="Youth Club Coaches",
        )
        return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 1}

    if run_log is not None:
        if result["errors"] > 0 and result["scraped"] == 0:
            run_log.finish_failed(
                FailureKind.ZERO_RESULTS,
                error_message=f"{result['errors']} clubs failed with no results",
            )
        elif result["errors"] > 0:
            run_log.finish_partial(
                records_created=result["rows_inserted"],
                records_updated=result["rows_updated"],
                records_failed=result["errors"],
                error_message=f"{result['errors']} clubs had errors",
            )
        else:
            run_log.finish_ok(
                records_created=result["rows_inserted"],
                records_updated=result["rows_updated"],
                records_failed=result["errors"],
            )

    # Post-run scrape_health reconcile. Opens its own short-lived conn;
    # never raises.
    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return result


def print_summary(result: dict) -> None:
    """Print a structured summary of the youth coach scrape run."""
    print("\n" + "=" * 60)
    print("  Youth Club Coaches — run summary")
    print("=" * 60)
    print(f"  Clubs scraped     : {result['scraped']}")
    print(f"  Rows inserted     : {result['rows_inserted']}")
    print(f"  Rows updated      : {result['rows_updated']}")
    print(f"  Errors            : {result['errors']}")
    print("=" * 60)
