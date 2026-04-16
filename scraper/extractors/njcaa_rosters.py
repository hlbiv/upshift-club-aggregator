"""
NJCAA roster scraper — **stub**.

Scaffolding for future NJCAA roster scraping. The DB-write plumbing
(upsert, run logging, CLI flags) is real and tested, but the HTML
parsing is not yet implemented — NJCAA school sites vary widely and
no common platform pattern has been identified.

The entry function ``scrape_njcaa_rosters`` returns immediately with
zero rows.  Wire it into the scheduler once parsing is implemented.

CLI::

    python -m scraper.extractors.njcaa_rosters \\
        [--gender mens|womens] [--limit 5] [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Dict, List, Optional

# ---------------------------------------------------------------------------
# Sibling package imports
# ---------------------------------------------------------------------------

_SCRAPER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from scrape_run_logger import ScrapeRunLogger, FailureKind  # noqa: E402

# Reuse shared helpers from the NCAA scraper
from extractors.ncaa_rosters import (  # noqa: E402
    current_academic_year,
    _get_connection,
    RATE_LIMIT_DELAY,
)

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRAPER_KEY = "njcaa-rosters"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_njcaa_colleges(
    conn,
    gender: Optional[str] = None,
) -> List[Dict]:
    """Query the colleges table for NJCAA schools."""
    clauses = ["division = %s"]
    params: List = ["NJCAA"]

    if gender:
        clauses.append("gender_program = %s")
        params.append(gender)

    where = "WHERE " + " AND ".join(clauses)

    query = f"""
        SELECT id, name, slug, division, conference, state, city,
               website, soccer_program_url, gender_program,
               last_scraped_at
        FROM colleges
        {where}
        ORDER BY name
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Main scraper (stub)
# ---------------------------------------------------------------------------

def scrape_njcaa_rosters(
    gender: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict:
    """Scrape NJCAA rosters — currently a stub.

    The DB query, run-logging, and CLI plumbing are real. The actual
    HTML fetch + parse loop is not yet implemented.

    Parameters
    ----------
    gender : 'mens', 'womens', or None (all)
    limit  : max number of colleges to process (for testing)
    dry_run: if True, parse pages but skip DB writes

    Returns
    -------
    dict with keys: scraped, rows_inserted, rows_updated, errors
    """
    academic_year = current_academic_year()
    logger.info(
        "Starting NJCAA roster scrape (STUB): gender=%s limit=%s dry_run=%s academic_year=%s",
        gender, limit, dry_run, academic_year,
    )

    conn = _get_connection()
    college_count = 0
    if conn is not None:
        colleges = _fetch_njcaa_colleges(conn, gender=gender)
        if limit:
            colleges = colleges[:limit]
        college_count = len(colleges)
        conn.close()

    logger.info(
        "Found %d NJCAA colleges in DB — NJCAA parsing not yet implemented, returning empty results",
        college_count,
    )

    # Log a run so the health dashboard sees this scraper exists
    run_logger = ScrapeRunLogger(
        scraper_key=SCRAPER_KEY,
        league_name="NJCAA",
    )
    run_logger.start()
    run_logger.finish_ok(
        records_created=0,
        records_updated=0,
        records_failed=0,
    )

    summary = {
        "scraped": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
        "errors": 0,
    }
    logger.info("NJCAA roster scrape complete (stub): %s", summary)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Scrape NJCAA soccer rosters into college_roster_history (stub)",
    )
    parser.add_argument(
        "--gender",
        choices=["mens", "womens"],
        default=None,
        help="Filter to a single gender program (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of colleges to process (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse pages but skip DB writes",
    )
    args = parser.parse_args()

    result = scrape_njcaa_rosters(
        gender=args.gender,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(0)


if __name__ == "__main__":
    main()
