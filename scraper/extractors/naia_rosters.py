"""
NAIA roster scraper — writes to ``college_roster_history``.

Ported from the TypeScript scraper in the sibling player-platform repo
(``naia-roster-scraper.ts``). Key design decisions preserved:

- **DB-driven school list**: reads from ``colleges`` table where
  ``division = 'NAIA'``, not a hardcoded list.
- **Sidearm sport-name API discovery**: probes
  ``/services/sportnames.ashx`` to find the correct sport shortname,
  then falls back to common path segments.
- **Same three-strategy HTML parser** as NCAA — Sidearm, header-aware
  table, card/div layout.
- **Year/class normalization**: shared with ``ncaa_rosters.py`` via
  import.
- **Rate limiting**: >= 1 s between schools.
- **Graceful degradation**: 404s, timeouts, unparseable pages logged
  and skipped.

CLI::

    python -m scraper.extractors.naia_rosters \\
        [--gender mens|womens] [--limit 5] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Sibling package imports
# ---------------------------------------------------------------------------

_SCRAPER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from scrape_run_logger import ScrapeRunLogger, FailureKind, classify_exception  # noqa: E402
from alerts import alert_scraper_failure  # noqa: E402

# Reuse parsing + normalization from the NCAA scraper — don't duplicate
from extractors.ncaa_rosters import (  # noqa: E402
    RosterPlayer,
    normalize_year,
    parse_roster_html,
    current_academic_year,
    fetch_with_retry,
    _get_session,
    _get_connection,
    _upsert_roster_row,
    _update_last_scraped,
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

SCRAPER_KEY = "naia-rosters"

# Soccer sport-path segments for URL discovery (same as NCAA)
MENS_PATHS = ["mens-soccer", "msoc", "m-soccer", "soccer"]
WOMENS_PATHS = ["womens-soccer", "wsoc", "w-soccer", "soccer"]


# ---------------------------------------------------------------------------
# Sidearm sport-name API discovery
# ---------------------------------------------------------------------------

def _discover_sport_shortname(
    session: requests.Session,
    base_url: str,
    gender: str,
) -> Optional[str]:
    """Probe Sidearm ``/services/sportnames.ashx`` to find the soccer
    sport shortname (e.g. ``wsoc``, ``msoc``).  Returns None if the
    endpoint is missing or doesn't list soccer.
    """
    api_url = f"{base_url.rstrip('/')}/services/sportnames.ashx"
    try:
        resp = session.get(api_url, timeout=5, allow_redirects=True)
        if resp.status_code != 200:
            return None
        # Sidearm returns JSON with a ``sports`` array
        text = resp.text.strip()
        if text.startswith("<"):
            return None
        data = json.loads(text)
        sports = data.get("sports", [])
    except Exception:
        return None

    is_womens = gender == "womens"

    for entry in sports:
        info = entry.get("sportInfo", {})
        title = (info.get("sport_title") or "").lower()
        shortname = (info.get("sport_shortname") or "").lower()
        global_name = (info.get("global_sport_name") or "").lower()

        is_soccer = (
            "soccer" in title
            or "soc" in shortname
            or "soc" in global_name
        )
        if not is_soccer:
            continue

        if is_womens:
            if "women" in title or "wsoc" in shortname or global_name == "wsoc":
                return info.get("sport_shortname")
            if "men" not in title and not shortname.startswith("msoc"):
                return info.get("sport_shortname")
        else:
            if ("men" in title and "women" not in title) or shortname.startswith("msoc") or global_name == "msoc":
                return info.get("sport_shortname")

    return None


# ---------------------------------------------------------------------------
# URL discovery — enhanced for NAIA
# ---------------------------------------------------------------------------

def discover_naia_roster_url(
    session: requests.Session,
    college: Dict,
    gender: str,
) -> Optional[str]:
    """Try to find the roster page URL for an NAIA college.

    Strategy order:
    1. ``soccer_program_url`` from the DB (if it contains /roster)
    2. ``soccer_program_url`` base + ``/roster``
    3. Sidearm sport-name API discovery → ``/sports/{shortname}/roster``
    4. Common sport-path candidates from ``website`` field
    """
    program_url = college.get("soccer_program_url")
    website = college.get("website")
    paths = WOMENS_PATHS if gender == "womens" else MENS_PATHS

    # 1. program_url already points to roster
    if program_url and "/roster" in program_url.lower():
        html = fetch_with_retry(session, program_url)
        if html and len(html) > 500:
            return program_url

    # 2. program_url as base + /roster
    if program_url:
        base = program_url.rstrip("/")
        base = re.sub(r"/(roster|schedule)$", "", base, flags=re.IGNORECASE)
        url = f"{base}/roster"
        html = fetch_with_retry(session, url)
        if html and len(html) > 500:
            return url

    # 3. Sidearm sport-name API
    base = (program_url or website or "").rstrip("/")
    if base:
        # Strip trailing /sports/* path if present
        base = re.sub(r"/sports/[^/]+(?:/.*)?$", "", base, flags=re.IGNORECASE)
        shortname = _discover_sport_shortname(session, base, gender)
        if shortname:
            url = f"{base}/sports/{shortname}/roster"
            html = fetch_with_retry(session, url)
            if html and len(html) > 500:
                return url

    # 4. Common path candidates from website
    if website:
        base = website.rstrip("/")
        for path in paths:
            url = f"{base}/sports/{path}/roster"
            html = fetch_with_retry(session, url)
            if html and len(html) > 500:
                return url
            time.sleep(0.3)

    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _fetch_naia_colleges(
    conn,
    gender: Optional[str] = None,
) -> List[Dict]:
    """Query the colleges table for NAIA schools."""
    clauses = ["division = %s"]
    params: List = ["NAIA"]

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
# Main scraper
# ---------------------------------------------------------------------------

def scrape_naia_rosters(
    gender: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict:
    """Scrape NAIA rosters and write to college_roster_history.

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
        "Starting NAIA roster scrape: gender=%s limit=%s dry_run=%s academic_year=%s",
        gender, limit, dry_run, academic_year,
    )

    conn = _get_connection()
    if conn is None:
        if dry_run:
            logger.warning("No DB connection in dry-run mode; cannot fetch colleges list")
            return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 0}
        logger.error("DATABASE_URL not set or connection failed; aborting (use --dry-run for no-DB mode)")
        return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 1}

    colleges = _fetch_naia_colleges(conn, gender=gender)
    if limit:
        colleges = colleges[:limit]

    logger.info("Processing %d NAIA colleges", len(colleges))

    session = _get_session()
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    total_scraped = 0

    run_logger = ScrapeRunLogger(
        scraper_key=SCRAPER_KEY,
        league_name="NAIA",
    )
    run_logger.start()

    for i, college in enumerate(colleges):
        college_gender = college["gender_program"]
        tag = f"[{i + 1}/{len(colleges)}] {college['name']} (NAIA {college_gender})"

        try:
            roster_url = discover_naia_roster_url(session, college, college_gender)
            if not roster_url:
                logger.info("  SKIP %s - no roster URL found", tag)
                total_errors += 1
                continue

            html = fetch_with_retry(session, roster_url)
            if not html:
                logger.warning("  FAIL %s - fetch failed: %s", tag, roster_url)
                total_errors += 1
                continue

            players = parse_roster_html(html)
            if not players:
                logger.info("  SKIP %s - no players parsed from %s", tag, roster_url)
                total_errors += 1
                continue

            total_scraped += 1
            inserted = 0
            updated = 0

            if not dry_run:
                for p in players:
                    try:
                        result = _upsert_roster_row(conn.cursor(), college["id"], p, academic_year)
                        if result == "inserted":
                            inserted += 1
                        else:
                            updated += 1
                    except Exception as exc:
                        logger.warning("  DB error for %s / %s: %s", college["name"], p.player_name, exc)
                        conn.rollback()
                        continue
                conn.commit()
                _update_last_scraped(conn.cursor(), college["id"])
                conn.commit()

            total_inserted += inserted
            total_updated += updated

            logger.info(
                "  OK   %s - %d players (%d new, %d updated) from %s",
                tag, len(players), inserted, updated, roster_url,
            )

        except Exception as exc:
            logger.error("  ERROR %s - %s", tag, exc)
            total_errors += 1
            kind = classify_exception(exc)
            alert_scraper_failure(
                scraper_key=SCRAPER_KEY,
                failure_kind=kind.value,
                error_message=str(exc),
                league_name="NAIA",
            )
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    # Finish run logger
    if total_errors > 0 and total_scraped == 0:
        run_logger.finish_failed(
            FailureKind.ZERO_RESULTS,
            error_message=f"{total_errors} colleges failed with no results",
        )
    else:
        run_logger.finish_ok(
            records_created=total_inserted,
            records_updated=total_updated,
            records_failed=total_errors,
        )

    if conn:
        conn.close()

    summary = {
        "scraped": total_scraped,
        "rows_inserted": total_inserted,
        "rows_updated": total_updated,
        "errors": total_errors,
    }
    logger.info("NAIA roster scrape complete: %s", summary)
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
        description="Scrape NAIA soccer rosters into college_roster_history",
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

    result = scrape_naia_rosters(
        gender=args.gender,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(1 if result["errors"] > 0 and result["scraped"] == 0 else 0)


if __name__ == "__main__":
    main()
