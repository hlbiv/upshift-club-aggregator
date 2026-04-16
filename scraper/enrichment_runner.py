"""
enrichment_runner.py — Orchestrate club website enrichment across
canonical_clubs rows.

Invoked from ``run.py --source club-enrichment``.

For each canonical_club that has a website_url but is missing enrichment
data (logo_url IS NULL or scrape_confidence IS NULL):
  1. Fetch the club's website.
  2. Extract logo, socials, website status, staff page URL.
  3. Compute a scrape_confidence score.
  4. Write results via club_enrichment_writer.

Fails soft: one bad website does not stop the whole run.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.club_website import ClubEnrichmentResult, extract_club_enrichment
from ingest.club_enrichment_writer import update_club_enrichment
from scrape_run_logger import (
    ScrapeRunLogger,
    FailureKind,
    classify_exception,
)
from alerts import alert_scraper_failure

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

logger = logging.getLogger("enrichment_runner")

# Polite rate limit — 1 request per 0.5s to avoid hammering club sites.
_RATE_LIMIT_SLEEP = 0.5

_SCRAPER_KEY = "club-enrichment"


@dataclass
class EnrichmentRunOutcome:
    total_clubs: int = 0
    enriched: int = 0
    skipped: int = 0
    failed: int = 0
    updated: int = 0


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _fetch_clubs_needing_enrichment(
    conn,
    *,
    only_club_id: Optional[int] = None,
    force: bool = False,
    limit: Optional[int] = None,
) -> List[Dict]:
    """Fetch canonical_clubs rows that need enrichment."""
    query_parts = [
        "SELECT id, club_name_canonical, website, scrape_confidence",
        "FROM canonical_clubs",
        "WHERE website IS NOT NULL AND website != ''",
    ]
    params: list = []

    if only_club_id is not None:
        query_parts.append("AND id = %s")
        params.append(only_club_id)
    elif not force:
        # Only clubs missing enrichment data
        query_parts.append(
            "AND (logo_url IS NULL OR scrape_confidence IS NULL)"
        )

    query_parts.append("ORDER BY id")

    if limit is not None:
        query_parts.append("LIMIT %s")
        params.append(limit)

    query = " ".join(query_parts)
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def run_club_enrichment(
    *,
    dry_run: bool = False,
    only_club_id: Optional[int] = None,
    force: bool = False,
    limit: Optional[int] = None,
) -> EnrichmentRunOutcome:
    """Scrape + update enrichment for clubs needing it.

    Returns an outcome summary for CLI reporting.
    """
    outcome = EnrichmentRunOutcome()

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=_SCRAPER_KEY,
            league_name="club website enrichment",
        )
        run_log.start(source_url="canonical_clubs")

    try:
        conn = _get_connection()
    except Exception as exc:
        logger.error("Cannot connect to database: %s", exc)
        if run_log is not None:
            run_log.finish_failed(
                FailureKind.NETWORK,
                error_message=f"DB connect failed: {exc}",
            )
        return outcome

    try:
        clubs = _fetch_clubs_needing_enrichment(
            conn,
            only_club_id=only_club_id,
            force=force,
            limit=limit,
        )
    except Exception as exc:
        logger.error("Failed to fetch clubs: %s", exc)
        if run_log is not None:
            run_log.finish_failed(
                classify_exception(exc),
                error_message=str(exc),
            )
        conn.close()
        return outcome

    outcome.total_clubs = len(clubs)
    logger.info("Processing %d club(s) for enrichment", len(clubs))

    if dry_run:
        for c in clubs[:20]:
            logger.info(
                "  [dry-run] id=%d  %s  →  %s",
                c["id"], c["club_name_canonical"], c["website"],
            )
        if len(clubs) > 20:
            logger.info("  ... and %d more", len(clubs) - 20)
        conn.close()
        return outcome

    enrichment_rows: List[Dict] = []

    for i, club in enumerate(clubs, 1):
        club_id = club["id"]
        website = club["website"]
        name = club["club_name_canonical"]

        try:
            result = extract_club_enrichment(club_id, website)
        except Exception as exc:
            logger.warning(
                "[%d/%d] id=%d %s — extraction error: %s",
                i, len(clubs), club_id, name, exc,
            )
            outcome.failed += 1
            continue

        if result.error:
            logger.debug(
                "[%d/%d] id=%d %s — %s (status=%s, confidence=%.0f)",
                i, len(clubs), club_id, name,
                result.error, result.website_status, result.scrape_confidence,
            )

        enrichment_rows.append({
            "club_id": result.club_id,
            "logo_url": result.logo_url,
            "instagram": result.instagram,
            "facebook": result.facebook,
            "twitter": result.twitter,
            "staff_page_url": result.staff_page_url,
            "website_status": result.website_status,
            "scrape_confidence": result.scrape_confidence,
        })
        outcome.enriched += 1

        if i % 50 == 0:
            logger.info(
                "  Progress: %d / %d (enriched=%d, failed=%d)",
                i, len(clubs), outcome.enriched, outcome.failed,
            )

        time.sleep(_RATE_LIMIT_SLEEP)

    # Batch write all enrichment rows
    if enrichment_rows:
        try:
            write_counts = update_club_enrichment(
                enrichment_rows, conn=conn, dry_run=False,
            )
            outcome.updated = write_counts["updated"]
            outcome.skipped += write_counts["skipped"]
        except Exception as exc:
            logger.error("Enrichment write failed: %s", exc)
            outcome.failed += len(enrichment_rows)
            if run_log is not None:
                run_log.finish_failed(
                    classify_exception(exc),
                    error_message=str(exc),
                )
            conn.close()
            return outcome

    # Finalize run log
    if run_log is not None:
        if outcome.failed > 0 and outcome.enriched == 0:
            run_log.finish_failed(
                FailureKind.UNKNOWN,
                error_message=f"All {outcome.failed} clubs failed",
            )
        elif outcome.failed > 0:
            run_log.finish_partial(
                records_created=0,
                records_updated=outcome.updated,
                records_failed=outcome.failed,
                error_message=f"{outcome.failed} clubs failed",
            )
        else:
            run_log.finish_ok(
                records_created=0,
                records_updated=outcome.updated,
                records_failed=outcome.skipped,
            )

    conn.close()

    # Post-run reconcile
    try:
        from reconcilers import end_of_run_reconcile
        end_of_run_reconcile()
    except Exception as exc:
        logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcome


def print_summary(outcome: EnrichmentRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  Club Enrichment — run summary")
    print("=" * 60)
    print(f"  Total clubs queried : {outcome.total_clubs}")
    print(f"  Enriched            : {outcome.enriched}")
    print(f"  DB rows updated     : {outcome.updated}")
    print(f"  Skipped             : {outcome.skipped}")
    print(f"  Failed              : {outcome.failed}")
    print("=" * 60)
