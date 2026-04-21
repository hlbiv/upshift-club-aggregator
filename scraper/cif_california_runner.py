"""
cif_california_runner.py — Orchestrate the CIF (California
Interscholastic Federation) state-tournament scrape.

Invoked via:
    run.py --source hs-cif-ca [--dry-run] [--limit N]

Walks CIF state-tournament bracket / results / rankings pages via
``scraper.utils.http.get``, parses each with
``extractors.cif_california.parse_cif_california_html``, and upserts
through two writers:

  * ``ingest.hs_matches_writer.insert_hs_matches`` for match rows
    (brackets + results)
  * ``ingest.hs_state_rankings_writer.insert_hs_state_rankings`` for
    ranking rows

Each seed carries implicit metadata (gender, season, role) that the
runner passes to the parser as ``default_gender`` / ``default_season``
so a page lacking a clean <title> still produces correctly-attributed
rows.

COVERAGE & LIMITATIONS (April 2026)
-----------------------------------
Recon verified CIF publishes static HTML with no bot wall and no
captcha, so ``DEFAULT_LIMIT`` is intentionally higher than the TDS
scrapers (30). If CIF rolls out a CDN WAF the runner will degrade
cleanly — per-page HTTP errors bump ``http_errors`` and the run log
is tagged ``failure_kind=network`` on 0 pages fetched.

The initial seed list is deliberately small — 3 pages covering the
three shapes. Growth is a cheap follow-up (add more CIFstate.org URLs
with the right metadata); no proxy work required.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

sys.path.insert(0, os.path.dirname(__file__))

from extractors.cif_california import parse_cif_california_html  # noqa: E402
from ingest.hs_matches_writer import insert_hs_matches  # noqa: E402
from ingest.hs_state_rankings_writer import insert_hs_state_rankings  # noqa: E402
from utils.http import get as http_get  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("cif_california_runner")


@dataclass(frozen=True)
class CifSeed:
    url: str
    gender: str     # "boys" | "girls"
    season: str    # "2025-26"
    role: str      # "bracket" | "results" | "rankings"


# Starter seed list — three pages, one per shape. Growth lands in a
# follow-up PR with the full state-tournament URL inventory.
DEFAULT_SEEDS: List[CifSeed] = [
    CifSeed(
        url=(
            "https://www.cifstate.org/sports/boys_soccer/state/bracket.html"
        ),
        gender="boys", season="2025-26", role="bracket",
    ),
    CifSeed(
        url=(
            "https://www.cifstate.org/sports/girls_soccer/state/results.html"
        ),
        gender="girls", season="2025-26", role="results",
    ),
    CifSeed(
        url=(
            "https://www.cifstate.org/sports/boys_soccer/rankings.html"
        ),
        gender="boys", season="2025-26", role="rankings",
    ),
]

DEFAULT_LIMIT = 30

_CIF_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class CifRunOutcome:
    pages_fetched: int = 0
    matches_parsed: int = 0
    rankings_parsed: int = 0
    matches_upserted: int = 0
    rankings_upserted: int = 0
    http_errors: int = 0
    match_counts: Dict[str, int] = field(default_factory=dict)
    ranking_counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _fetch_page(url: str, timeout: int = 20) -> Optional[str]:
    """Fetch one CIF page. Returns HTML string on 2xx, ``None`` otherwise."""
    try:
        resp = http_get(url, headers=_CIF_HEADERS, timeout=timeout)
    except requests.RequestException as exc:
        logger.warning("[cif-ca] fetch failed for %s: %s", url, exc)
        return None

    if resp.status_code != 200:
        logger.warning(
            "[cif-ca] non-200 status %d at %s", resp.status_code, url,
        )
        return None
    return resp.text


def run_cif_california(
    *,
    dry_run: bool = False,
    limit: Optional[int] = DEFAULT_LIMIT,
    seeds: Optional[List[CifSeed]] = None,
    **_kwargs,
) -> CifRunOutcome:
    """Fetch CIF state-tournament pages and upsert matches + rankings."""
    seed_list: List[CifSeed] = list(seeds) if seeds else list(DEFAULT_SEEDS)
    if limit is not None and limit > 0:
        seed_list = seed_list[:limit]

    outcome = CifRunOutcome()
    if not seed_list:
        logger.info("[cif-ca] seed list empty — nothing to do")
        return outcome

    scraper_key = "hs-cif-ca"
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="CIF California HS soccer",
        )
        run_log.start(source_url="https://www.cifstate.org")

    all_matches: List[Dict] = []
    all_rankings: List[Dict] = []

    try:
        for seed in seed_list:
            html = _fetch_page(seed.url)
            if html is None:
                outcome.http_errors += 1
                continue
            outcome.pages_fetched += 1
            try:
                parsed = parse_cif_california_html(
                    html,
                    source_url=seed.url,
                    default_gender=seed.gender,
                    default_season=seed.season,
                )
            except Exception as exc:
                logger.error("[cif-ca] parse failed for %s: %s", seed.url, exc)
                outcome.http_errors += 1
                continue
            all_matches.extend(parsed.get("matches", []))
            all_rankings.extend(parsed.get("rankings", []))
    except Exception as exc:  # pragma: no cover — defensive
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[cif-ca] runner crashed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.cifstate.org",
            league_name="CIF California HS soccer",
        )
        return outcome

    outcome.matches_parsed = len(all_matches)
    outcome.rankings_parsed = len(all_rankings)

    if outcome.pages_fetched == 0:
        logger.warning(
            "[cif-ca] 0 pages fetched (%d HTTP errors)", outcome.http_errors,
        )
        if run_log is not None:
            run_log.finish_partial(
                records_failed=outcome.http_errors,
                error_message=(
                    f"{outcome.http_errors} HTTP error(s); 0 pages fetched"
                ),
            )
        outcome.failure_kind = FailureKind.NETWORK
        return outcome

    if not all_matches and not all_rankings:
        logger.warning(
            "[cif-ca] fetched %d page(s) but extracted 0 rows",
            outcome.pages_fetched,
        )
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="no matches or rankings extracted",
            )
        outcome.failure_kind = FailureKind.ZERO_RESULTS
        return outcome

    try:
        match_counts = insert_hs_matches(all_matches, dry_run=dry_run)
        ranking_counts = insert_hs_state_rankings(all_rankings, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[cif-ca] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.cifstate.org",
            league_name="CIF California HS soccer",
        )
        return outcome

    outcome.match_counts = match_counts
    outcome.ranking_counts = ranking_counts
    outcome.matches_upserted = match_counts.get("inserted", 0) + match_counts.get("updated", 0)
    outcome.rankings_upserted = ranking_counts.get("inserted", 0) + ranking_counts.get("updated", 0)

    logger.info(
        "[cif-ca] pages=%d matches_parsed=%d rankings_parsed=%d "
        "m_inserted=%d m_updated=%d r_inserted=%d r_updated=%d http_errors=%d",
        outcome.pages_fetched,
        outcome.matches_parsed,
        outcome.rankings_parsed,
        match_counts.get("inserted", 0),
        match_counts.get("updated", 0),
        ranking_counts.get("inserted", 0),
        ranking_counts.get("updated", 0),
        outcome.http_errors,
    )

    if run_log is not None:
        run_log.finish_ok(
            records_created=(
                match_counts.get("inserted", 0)
                + ranking_counts.get("inserted", 0)
            ),
            records_updated=(
                match_counts.get("updated", 0)
                + ranking_counts.get("updated", 0)
            ),
            records_failed=(
                match_counts.get("skipped", 0)
                + ranking_counts.get("skipped", 0)
                + outcome.http_errors
            ),
        )

    return outcome


def print_summary(outcome: CifRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  CIF California HS soccer — run summary")
    print("=" * 60)
    print(f"  Pages fetched        : {outcome.pages_fetched}")
    print(f"  Matches parsed       : {outcome.matches_parsed}")
    print(f"  Rankings parsed      : {outcome.rankings_parsed}")
    print(f"  Matches upserted     : {outcome.matches_upserted}")
    print(f"    inserted           : {outcome.match_counts.get('inserted', 0)}")
    print(f"    updated            : {outcome.match_counts.get('updated', 0)}")
    print(f"    skipped            : {outcome.match_counts.get('skipped', 0)}")
    print(f"  Rankings upserted    : {outcome.rankings_upserted}")
    print(f"    inserted           : {outcome.ranking_counts.get('inserted', 0)}")
    print(f"    updated            : {outcome.ranking_counts.get('updated', 0)}")
    print(f"    skipped            : {outcome.ranking_counts.get('skipped', 0)}")
    print(f"  HTTP errors          : {outcome.http_errors}")
    if outcome.failure_kind is not None:
        print(f"  Failure kind         : {outcome.failure_kind.value}")
    if outcome.error:
        print(f"  Error                : {outcome.error[:120]}")
    print("=" * 60)
