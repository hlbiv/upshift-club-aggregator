"""
squarespace_clubs_runner.py — Orchestrate the Squarespace + JSON-LD club
extractor across the canonical_clubs seed list.

Invoked via ``run.py --source squarespace-clubs``.

Per club with a non-empty ``website``:
  1. HEAD/GET the homepage; bail unless ``cms_detect.detect_cms``
     returns ``'squarespace'``.
  2. Aggregate JSON-LD across the homepage + a fixed list of common
     subpaths (see ``extractors.squarespace_clubs.DEFAULT_SUBPATHS``).
  3. Route typed nodes into the four sinks:
       - ``SportsTeam.athlete[]`` → ``club_roster_snapshots``
       - ``Person`` blocks         → ``coach_discoveries``
       - tryout-keyword ``Event``  → ``tryouts``
       - ``Organization`` metadata → ``canonical_clubs`` enrichment
  4. Log a single ``scrape_run_logs`` row for the whole run.

Fails soft: one bad URL never aborts the whole run; the per-site failure
is dropped and the runner continues. The ``--limit`` flag caps how many
candidate clubs to probe per invocation (default 50) — operational
tuning happens after the first manual run shows real coverage numbers.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

from extractors.squarespace_clubs import (  # noqa: E402
    SCRAPER_KEY,
    SquarespaceClubSite,
    SquarespaceHarvest,
    harvest_squarespace_club,
    _get_session,
)
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("squarespace_clubs_runner")

# Default cap on candidates per run. Conservative — first production
# run will be a probe; the operator (CLAUDE.md: only Replit) bumps it
# once we've confirmed the JSON-LD coverage numbers.
DEFAULT_LIMIT = 50


@dataclass
class SquarespaceRunOutcome:
    sites_considered: int = 0
    sites_squarespace: int = 0
    pages_fetched: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_connection():
    """Return a psycopg2 conn from DATABASE_URL, or None on failure.

    Mirrors the soft-fail pattern in ``youth_club_coaches`` — the
    runner can be invoked in dry-run from a laptop without DB access
    and still report progress (per CLAUDE.md "scraping continues
    regardless").
    """
    if psycopg2 is None:
        return None
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        return psycopg2.connect(url)
    except Exception as exc:
        logger.warning("[squarespace-clubs] DB connect failed: %s", exc)
        return None


def _fetch_candidates(
    conn,
    *,
    limit: Optional[int] = None,
    state: Optional[str] = None,
) -> List[SquarespaceClubSite]:
    """SELECT id, club_name_canonical, website FROM canonical_clubs.

    Filters out NULL/empty websites. Optional state filter mirrors the
    ``--state`` flag used by other youth-side scrapers.
    """
    clauses = ["website IS NOT NULL", "website != ''"]
    params: List[Any] = []
    if state:
        clauses.append("state = %s")
        params.append(state.upper())
    sql = (
        "SELECT id, club_name_canonical, website, state "
        "FROM canonical_clubs "
        f"WHERE {' AND '.join(clauses)} "
        "ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"

    sites: List[SquarespaceClubSite] = []
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            sites.append(
                SquarespaceClubSite(
                    club_id=int(row["id"]),
                    club_name_canonical=row["club_name_canonical"],
                    website=row["website"],
                    state=row.get("state"),
                )
            )
    return sites


# ---------------------------------------------------------------------------
# Writer dispatch
# ---------------------------------------------------------------------------

def _write_harvest(
    harvest: SquarespaceHarvest,
    *,
    conn,
    dry_run: bool,
    counts: Dict[str, int],
) -> None:
    """Hand each sink to its writer. All writers are idempotent.

    Lazy-imports the writers so a dry-run on a machine without psycopg2
    can still exercise the extraction code without blowing up.
    """
    if dry_run:
        counts["roster_rows"] += len(harvest.roster_rows)
        counts["coach_rows"] += len(harvest.coach_rows)
        counts["tryout_rows"] += len(harvest.tryout_rows)
        if harvest.enrichment_row is not None:
            counts["enrichment_rows"] += 1
        return

    if conn is None:
        # No DB but not dry-run: log + count, don't try to write.
        logger.warning(
            "[squarespace-clubs] no DB connection; would have written "
            "%d roster, %d coach, %d tryout, %s enrichment for %s",
            len(harvest.roster_rows), len(harvest.coach_rows),
            len(harvest.tryout_rows),
            "1" if harvest.enrichment_row else "0",
            harvest.club_name,
        )
        return

    # ---- Roster snapshots ----
    if harvest.roster_rows:
        from ingest.roster_snapshot_writer import insert_roster_snapshots
        try:
            r = insert_roster_snapshots(harvest.roster_rows, conn=conn)
            counts["roster_inserted"] += r.get("inserted", 0)
            counts["roster_updated"] += r.get("updated", 0)
            counts["roster_diffs"] += r.get("diffs_written", 0)
        except Exception as exc:
            logger.warning(
                "[squarespace-clubs] roster write failed for %s: %s",
                harvest.club_name, exc,
            )
            try:
                conn.rollback()
            except Exception:
                pass

    # ---- Coach discoveries ----
    # ``coach_discoveries`` doesn't have a batch writer module yet (the
    # youth_club_coaches scraper inlines its upsert). We re-use the
    # same SQL shape here so the constraint name + WHERE-guard match.
    if harvest.coach_rows:
        try:
            with conn.cursor() as cur:
                for row in harvest.coach_rows:
                    cur.execute(
                        """
                        INSERT INTO coach_discoveries
                            (club_id, name, title, email, phone,
                             source_url, scraped_at, confidence,
                             platform_family, first_seen_at, last_seen_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s, %s,
                                NOW(), NOW())
                        ON CONFLICT ON CONSTRAINT
                            coach_discoveries_club_name_title_uq
                        DO UPDATE SET
                            email = COALESCE(EXCLUDED.email,
                                             coach_discoveries.email),
                            phone = COALESCE(EXCLUDED.phone,
                                             coach_discoveries.phone),
                            source_url = EXCLUDED.source_url,
                            scraped_at = NOW(),
                            confidence = EXCLUDED.confidence,
                            platform_family = EXCLUDED.platform_family,
                            last_seen_at = NOW()
                        RETURNING (xmax = 0) AS is_insert
                        """,
                        (
                            row["club_id"], row["name"], row["title"],
                            row.get("email"), row.get("phone"),
                            row.get("source_url"),
                            row.get("confidence"),
                            row.get("platform_family") or "unknown",
                        ),
                    )
                    res = cur.fetchone()
                    if res and res[0]:
                        counts["coach_inserted"] += 1
                    else:
                        counts["coach_updated"] += 1
            conn.commit()
        except Exception as exc:
            logger.warning(
                "[squarespace-clubs] coach write failed for %s: %s",
                harvest.club_name, exc,
            )
            try:
                conn.rollback()
            except Exception:
                pass

    # ---- Tryouts ----
    if harvest.tryout_rows:
        from ingest.tryouts_writer import insert_tryouts
        try:
            r = insert_tryouts(harvest.tryout_rows, conn=conn)
            counts["tryouts_inserted"] += r.get("inserted", 0)
            counts["tryouts_updated"] += r.get("updated", 0)
        except Exception as exc:
            logger.warning(
                "[squarespace-clubs] tryouts write failed for %s: %s",
                harvest.club_name, exc,
            )
            try:
                conn.rollback()
            except Exception:
                pass

    # ---- Enrichment ----
    if harvest.enrichment_row is not None:
        from ingest.club_enrichment_writer import update_club_enrichment
        try:
            r = update_club_enrichment([harvest.enrichment_row], conn=conn)
            counts["enrichment_updated"] += r.get("updated", 0)
        except Exception as exc:
            logger.warning(
                "[squarespace-clubs] enrichment write failed for %s: %s",
                harvest.club_name, exc,
            )
            try:
                conn.rollback()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

def run_squarespace_clubs(
    *,
    dry_run: bool = False,
    limit: Optional[int] = DEFAULT_LIMIT,
    state: Optional[str] = None,
) -> SquarespaceRunOutcome:
    """Probe canonical_clubs websites for Squarespace + JSON-LD.

    Parameters
    ----------
    dry_run
        Parse pages but skip every DB write. Counts of WOULD-write rows
        are still reported via the outcome.
    limit
        Cap on number of candidate clubs to probe per invocation.
        Defaults to ``DEFAULT_LIMIT`` (50). Pass None to disable.
    state
        Optional state filter (e.g. 'GA').
    """
    outcome = SquarespaceRunOutcome()
    counts: Dict[str, int] = {
        "roster_rows": 0, "coach_rows": 0, "tryout_rows": 0, "enrichment_rows": 0,
        "roster_inserted": 0, "roster_updated": 0, "roster_diffs": 0,
        "coach_inserted": 0, "coach_updated": 0,
        "tryouts_inserted": 0, "tryouts_updated": 0,
        "enrichment_updated": 0,
    }

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=SCRAPER_KEY,
            league_name="Squarespace JSON-LD clubs",
        )
        run_log.start(source_url="canonical_clubs:website")

    # Get the candidates. In dry-run mode without DB we just report
    # zero candidates rather than aborting — same fail-soft pattern
    # the youth-coaches runner uses.
    conn = _get_connection()
    if conn is None and not dry_run:
        logger.error(
            "[squarespace-clubs] DATABASE_URL not set; aborting (dry-run "
            "OK without DB)"
        )
        outcome.failure_kind = FailureKind.UNKNOWN
        outcome.error = "DATABASE_URL not set"
        if run_log is not None:
            run_log.finish_failed(
                FailureKind.UNKNOWN, error_message=outcome.error,
            )
        return outcome

    try:
        if conn is None:
            sites: List[SquarespaceClubSite] = []
            logger.warning(
                "[squarespace-clubs] no DB connection — dry-run with empty "
                "candidate list. Run on Replit to exercise real data."
            )
        else:
            try:
                sites = _fetch_candidates(conn, limit=limit, state=state)
            except Exception as exc:
                kind = classify_exception(exc)
                outcome.failure_kind = kind
                outcome.error = str(exc)
                logger.error(
                    "[squarespace-clubs] candidate fetch failed: %s", exc,
                )
                if run_log is not None:
                    run_log.finish_failed(kind, error_message=str(exc))
                alert_scraper_failure(
                    scraper_key=SCRAPER_KEY,
                    failure_kind=kind.value,
                    error_message=str(exc),
                    source_url="canonical_clubs:website",
                    league_name="Squarespace JSON-LD clubs",
                )
                return outcome

        outcome.sites_considered = len(sites)
        logger.info(
            "[squarespace-clubs] probing %d candidate club(s) (limit=%s, state=%s)",
            len(sites), limit, state,
        )

        session = _get_session()
        try:
            for site in sites:
                try:
                    harvest = harvest_squarespace_club(site, session=session)
                except Exception as exc:
                    # Per CLAUDE.md: fail-soft per site.
                    logger.warning(
                        "[squarespace-clubs] crashed on %s: %s",
                        site.club_name_canonical, exc,
                    )
                    continue

                outcome.pages_fetched += harvest.pages_fetched
                if not harvest.is_squarespace:
                    continue
                outcome.sites_squarespace += 1

                # Write whatever we found — each writer is idempotent
                # and shrugs off zero-row inputs internally.
                _write_harvest(harvest, conn=conn, dry_run=dry_run, counts=counts)
        finally:
            try:
                session.close()
            except Exception:
                pass

        outcome.counts = counts

        # Run-log accounting — count "records_created" as the sum of
        # net-new writes across all four sinks. Updates are reported
        # separately. A run that finds 0 Squarespace sites is partial
        # (we DID work, just didn't write); a run that finds candidates
        # but writes 0 is similarly partial.
        total_inserted = (
            counts["roster_inserted"] + counts["coach_inserted"]
            + counts["tryouts_inserted"] + counts["enrichment_updated"]
        )
        total_updated = counts["roster_updated"] + counts["coach_updated"] + counts["tryouts_updated"]

        if run_log is not None:
            if outcome.sites_squarespace == 0:
                run_log.finish_partial(
                    records_failed=0,
                    error_message=(
                        f"probed {outcome.sites_considered} sites; "
                        "0 detected as Squarespace"
                    ),
                )
                outcome.failure_kind = FailureKind.ZERO_RESULTS
            else:
                run_log.finish_ok(
                    records_created=total_inserted,
                    records_updated=total_updated,
                    records_failed=0,
                )

        logger.info(
            "[squarespace-clubs] done: candidates=%d squarespace=%d "
            "pages=%d inserted=%d updated=%d",
            outcome.sites_considered, outcome.sites_squarespace,
            outcome.pages_fetched, total_inserted, total_updated,
        )
        return outcome

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        # Post-run scrape_health reconcile — soft failure only.
        if not dry_run:
            try:
                from reconcilers import end_of_run_reconcile
                end_of_run_reconcile()
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning("end_of_run_reconcile skipped: %s", exc)


def print_summary(outcome: SquarespaceRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  Squarespace JSON-LD clubs — run summary")
    print("=" * 60)
    print(f"  Candidates considered : {outcome.sites_considered}")
    print(f"  Detected Squarespace  : {outcome.sites_squarespace}")
    print(f"  Pages fetched         : {outcome.pages_fetched}")
    c = outcome.counts
    if c.get("roster_rows") is not None and (c.get("roster_inserted", 0) == 0 and c.get("roster_updated", 0) == 0):
        # Dry-run path: report intent counts.
        print(f"  Roster rows (would)   : {c.get('roster_rows', 0)}")
        print(f"  Coach rows (would)    : {c.get('coach_rows', 0)}")
        print(f"  Tryout rows (would)   : {c.get('tryout_rows', 0)}")
        print(f"  Enrichment (would)    : {c.get('enrichment_rows', 0)}")
    else:
        print(f"  Roster ins/upd/diffs  : "
              f"{c.get('roster_inserted', 0)}/{c.get('roster_updated', 0)}"
              f"/{c.get('roster_diffs', 0)}")
        print(f"  Coach ins/upd         : "
              f"{c.get('coach_inserted', 0)}/{c.get('coach_updated', 0)}")
        print(f"  Tryouts ins/upd       : "
              f"{c.get('tryouts_inserted', 0)}/{c.get('tryouts_updated', 0)}")
        print(f"  Enrichment updated    : {c.get('enrichment_updated', 0)}")
    if outcome.failure_kind is not None:
        print(f"  Failure               : {outcome.failure_kind.value}: "
              f"{(outcome.error or '')[:80]}")
    print("=" * 60)
