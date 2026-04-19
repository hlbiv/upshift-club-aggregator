"""
duda_360player_clubs_runner.py — Probe Duda + 360Player club sites.

Two distinct platforms, one runner — see
``scraper/extractors/duda_360player_clubs.py`` for the platform-specific
detection rationale.

Seed strategy:
    1. Try the 360Player public clubs directory at
       ``https://360player.com/clubs``. If accessible, surface those as
       additional probe candidates (cross-referenced with
       ``canonical_clubs.website`` so we don't re-probe a site we've
       already seeded another way).
    2. Pull seeds from ``canonical_clubs`` whose ``website`` URL pattern
       suggests Duda (``cdn-website.com`` host) or 360Player
       (``360player.com`` host). This is the read-only path used in
       Replit; locally we no-op when ``DATABASE_URL`` is unset.

Writes:
    * ``tryouts`` rows go through the existing ``ingest.tryouts_writer``
      contract — same as ``tryouts_wordpress``.
    * ``coach_discoveries`` rows are NOT written this PR. They are
      collected and reported in the run summary so a follow-up PR can
      wire ``ingest.coach_discoveries_writer`` (or equivalent) once the
      shape is reviewed.

Fail-soft: per-site probe errors never stop the batch. Aggregate counts
are reported in the run summary.

Invoked from ``run.py --source duda-360player-clubs``.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.duda_360player_clubs import (  # noqa: E402
    discover_360player_directory,
    scrape_duda_360player_clubs,
)
from ingest.tryouts_writer import insert_tryouts  # noqa: E402
from scrape_run_logger import ScrapeRunLogger, classify_exception  # noqa: E402
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("duda_360player_clubs_runner")


_SCRAPER_KEY = "duda-360player-clubs"
_LEAGUE_NAME = "Duda CMS + 360Player clubs"
_SEED_SOURCE_LABEL = "seed:canonical_clubs+360player_directory"


# URL-substring fingerprints used when reading seeds from canonical_clubs.
# Match the same hostnames cms_detect.py keys off of.
_DUDA_HOSTS = ("cdn-website.com", "multiscreensite.com")
_360PLAYER_HOSTS = ("360player.com",)


def _fetch_canonical_seeds(limit: Optional[int] = None) -> List[Dict[str, str]]:
    """Read club seeds from ``canonical_clubs.website`` filtered to URLs
    whose host matches Duda or 360Player fingerprints.

    Read-only. Returns ``[]`` if ``DATABASE_URL`` is unset or psycopg2
    is missing — keeps local dry-runs working without a database.
    """
    try:
        import psycopg2  # type: ignore
    except ImportError:
        logger.info("[duda-360player] psycopg2 not available; canonical seed skipped")
        return []
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        logger.info("[duda-360player] DATABASE_URL unset; canonical seed skipped")
        return []

    # ILIKE pattern union — Postgres optimizer handles the OR cheaply
    # and the table is small (< 10k rows in production).
    like_clauses = [f"website ILIKE '%{h}%'" for h in (*_DUDA_HOSTS, *_360PLAYER_HOSTS)]
    where = " OR ".join(like_clauses)
    sql = (
        "SELECT club_name_canonical, website "
        "FROM canonical_clubs "
        f"WHERE website IS NOT NULL AND website <> '' AND ({where}) "
        "ORDER BY club_name_canonical ASC"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception as exc:
        logger.warning("[duda-360player] canonical seed query failed: %s", exc)
        return []

    return [
        {"club_name_raw": str(name), "website": str(site)}
        for (name, site) in rows
        if name and site
    ]


@dataclass
class RunOutcome:
    seeds_canonical: int = 0
    seeds_directory: int = 0
    sites_probed: int = 0
    duda_sites: int = 0
    _360player_sites: int = 0
    other_or_unknown: int = 0
    sites_with_jsonld: int = 0
    tryouts_extracted: int = 0
    persons_extracted: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    error: Optional[str] = None


def run_duda_360player_clubs(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
) -> RunOutcome:
    """Main entry point — discover seeds, probe each, upsert tryouts.

    ``limit`` caps the canonical-seed pull (passed straight into the
    ``LIMIT`` clause). The 360Player directory walk is unbounded
    because it self-caps to whatever the public page renders.
    """
    outcome = RunOutcome()

    # --- Seed discovery: 360Player directory + canonical_clubs ---
    directory_seeds = discover_360player_directory()
    outcome.seeds_directory = len(directory_seeds)

    canonical_seeds = _fetch_canonical_seeds(limit=limit)
    outcome.seeds_canonical = len(canonical_seeds)

    # Dedup by website (prefer canonical_clubs name when both have a site).
    by_url: Dict[str, Dict[str, str]] = {}
    for entry in directory_seeds:
        by_url[entry["website"].rstrip("/").lower()] = entry
    for entry in canonical_seeds:
        key = entry["website"].rstrip("/").lower()
        # canonical name wins — likely cleaner than the directory anchor text.
        by_url[key] = entry
    sites = list(by_url.values())
    if limit is not None:
        sites = sites[:limit]

    if not sites:
        logger.info(
            "[duda-360player] no seeds discovered (directory=%d, canonical=%d). "
            "Likely DATABASE_URL unset locally and 360Player directory "
            "is JS-rendered — both expected outcomes.",
            len(directory_seeds), len(canonical_seeds),
        )
        # Still log a run row so operators can see we tried.
        if not dry_run:
            run_log = ScrapeRunLogger(
                scraper_key=_SCRAPER_KEY, league_name=_LEAGUE_NAME,
            )
            run_log.start(source_url=_SEED_SOURCE_LABEL)
            run_log.finish_partial(
                records_failed=0,
                error_message="no seeds discovered",
            )
        return outcome

    # --- Probe + extract ---
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(scraper_key=_SCRAPER_KEY, league_name=_LEAGUE_NAME)
        run_log.start(source_url=_SEED_SOURCE_LABEL)

    try:
        result = scrape_duda_360player_clubs(sites)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.error = str(exc)
        logger.error("[duda-360player] batch probe failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=_SCRAPER_KEY,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=_SEED_SOURCE_LABEL,
            league_name=_LEAGUE_NAME,
        )
        return outcome

    stats = result["stats"]
    outcome.sites_probed = stats["sites_probed"]
    outcome.duda_sites = stats["duda_sites"]
    outcome._360player_sites = stats["_360player_sites"]
    outcome.other_or_unknown = stats["other_or_unknown"]
    outcome.sites_with_jsonld = stats["sites_with_jsonld"]
    outcome.tryouts_extracted = len(result["tryouts"])
    outcome.persons_extracted = len(result["coach_discoveries"])

    # --- Write tryouts (coach_discoveries deferred to follow-up) ---
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if result["tryouts"]:
        try:
            counts = insert_tryouts(result["tryouts"], dry_run=dry_run)
        except Exception as exc:
            kind = classify_exception(exc)
            outcome.error = str(exc)
            logger.error("[duda-360player] tryouts write failed: %s", exc)
            if run_log is not None:
                run_log.finish_failed(kind, error_message=str(exc))
            alert_scraper_failure(
                scraper_key=_SCRAPER_KEY,
                failure_kind=kind.value,
                error_message=str(exc),
                source_url=_SEED_SOURCE_LABEL,
                league_name=_LEAGUE_NAME,
            )
            return outcome
    outcome.counts = counts

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

    return outcome


def print_summary(outcome: RunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  Duda CMS + 360Player — run summary")
    print("=" * 60)
    print(f"  Seeds (360Player dir.) : {outcome.seeds_directory}")
    print(f"  Seeds (canonical_clubs): {outcome.seeds_canonical}")
    print(f"  Sites probed           : {outcome.sites_probed}")
    print(f"    Duda                 : {outcome.duda_sites}")
    print(f"    360Player            : {outcome._360player_sites}")
    print(f"    Other / unknown      : {outcome.other_or_unknown}")
    print(f"  Sites with JSON-LD     : {outcome.sites_with_jsonld}")
    print(f"  Tryouts extracted      : {outcome.tryouts_extracted}")
    print(f"  Persons extracted      : {outcome.persons_extracted} (NOT written)")
    if outcome.counts:
        print(f"  Tryouts inserted       : {outcome.counts.get('inserted', 0)}")
        print(f"  Tryouts updated        : {outcome.counts.get('updated', 0)}")
        print(f"  Tryouts skipped        : {outcome.counts.get('skipped', 0)}")
    if outcome.error:
        print(f"  ERROR                  : {outcome.error}")
    print("=" * 60)
