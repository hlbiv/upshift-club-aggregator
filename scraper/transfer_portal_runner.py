"""
transfer_portal_runner.py — Orchestrate the TopDrawerSoccer
transfer-tracker scrape.

Invoked via:
    run.py --source ncaa-transfer-portal [--dry-run] [--limit N]

Walks one or more TDS transfer-tracker article pages via
``scraper.utils.http.get`` (proxy-aware), parses each page with
``extractors.topdrawer_transfer_portal.parse_topdrawer_transfer_portal_html``,
and upserts rows through
``ingest.transfer_portal_writer.insert_transfer_portal_entries``.

Each seed URL carries implicit metadata (year, gender, division,
tracker half) that the parser does NOT try to infer from the HTML.
Seeds are expressed as ``TrackerSeed`` records so the metadata rides
alongside the URL and attaches to every row parsed from that page.

COVERAGE & LIMITATIONS (April 2026)
-----------------------------------
  * TDS aggressively blocks bulk crawlers. Expect HTTP 403 at volume.
    ``DEFAULT_LIMIT = 20`` keeps the initial live run bounded while
    surfacing any 403 rate issue within ~30 seconds. Scaling requires
    rotating residential proxies via ``scraper/proxy_config.yaml``
    under ``topdrawersoccer.com``.

  * ``season_window`` is a composite string formed from the seed
    metadata::

        "<year>-<gender>-<division>-<half>"
        e.g. "2026-womens-di-mid-year"

    This matches the natural key on ``transfer_portal_entries`` and
    keeps cross-window re-scrapes independent (the same player entering
    the portal in mid-year and then again in summer produces two rows,
    which is correct).

SEED LIST
---------
Only D1 mid-year seeds are enabled by default — the summer tracker
articles are published late in the year (July/August) and the
article IDs rotate per season. When summer trackers are live,
add entries to ``DEFAULT_SEEDS`` with the appropriate article aid.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

sys.path.insert(0, os.path.dirname(__file__))

from extractors.topdrawer_transfer_portal import (  # noqa: E402
    parse_topdrawer_transfer_portal_html,
)
from ingest.transfer_portal_writer import insert_transfer_portal_entries  # noqa: E402
from utils.http import get as http_get  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("transfer_portal_runner")


@dataclass(frozen=True)
class TrackerSeed:
    url: str
    year: int
    gender: str       # "mens" | "womens"
    division: str     # "d1" | "d2" | "d3"
    half: str         # "mid-year" | "summer"

    @property
    def season_window(self) -> str:
        return f"{self.year}-{self.gender}-{self.division}-{self.half}"


# Default seeds — verified 2026-04-20 via WebFetch. Article IDs rotate
# per season; when a new tracker is published, add a new seed rather
# than editing the existing one (keeps history of the URL that produced
# each row).
DEFAULT_SEEDS: List[TrackerSeed] = [
    TrackerSeed(
        url=(
            "https://www.topdrawersoccer.com/college-soccer-articles/"
            "2026-womens-division-i-transfer-tracker_aid55352"
        ),
        year=2026, gender="womens", division="d1", half="mid-year",
    ),
    TrackerSeed(
        url=(
            "https://www.topdrawersoccer.com/college-soccer-articles/"
            "2026-mens-division-i-transfer-tracker_aid55358"
        ),
        year=2026, gender="mens", division="d1", half="mid-year",
    ),
]

DEFAULT_LIMIT = 20

_TDS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class TransferPortalRunOutcome:
    pages_fetched: int = 0
    entries_parsed: int = 0
    rows_upserted: int = 0
    http_errors: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _fetch_page(url: str, timeout: int = 20) -> Optional[str]:
    """Fetch one TDS page via the proxy-aware HTTP helper.

    Returns the HTML string on 2xx, ``None`` on any non-200 /
    connection error. Warns explicitly on 403 with a proxy-config hint.
    """
    try:
        resp = http_get(url, headers=_TDS_HEADERS, timeout=timeout)
    except requests.RequestException as exc:
        logger.warning("[transfer-portal] fetch failed for %s: %s", url, exc)
        return None

    if resp.status_code == 403:
        logger.warning(
            "[transfer-portal] 403 at %s — consider adding proxy "
            "credentials to scraper/proxy_config.yaml under "
            "'topdrawersoccer.com'",
            url,
        )
        return None
    if resp.status_code != 200:
        logger.warning(
            "[transfer-portal] non-200 status %d at %s",
            resp.status_code, url,
        )
        return None
    return resp.text


def run_ncaa_transfer_portal(
    *,
    dry_run: bool = False,
    limit: Optional[int] = DEFAULT_LIMIT,
    seeds: Optional[List[TrackerSeed]] = None,
    **_kwargs,
) -> TransferPortalRunOutcome:
    """Fetch TDS transfer-tracker articles and upsert rows.

    Default ``limit`` is **20** — same rationale as the commitments
    runner: TDS blocks bulk crawlers, and the small cap keeps the
    pipeline land-and-verify friendly. Raise with caution once
    ``proxy_config.yaml`` has ``topdrawersoccer.com`` configured.
    """
    seed_list: List[TrackerSeed] = list(seeds) if seeds else list(DEFAULT_SEEDS)
    if limit is not None and limit > 0:
        seed_list = seed_list[:limit]

    outcome = TransferPortalRunOutcome()
    if not seed_list:
        logger.info("[transfer-portal] seed list empty — nothing to do")
        return outcome

    scraper_key = "ncaa-transfer-portal"
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="TopDrawerSoccer transfer tracker",
        )
        run_log.start(
            source_url="https://www.topdrawersoccer.com/college-soccer-articles",
        )

    all_rows: List[Dict] = []
    try:
        for seed in seed_list:
            html = _fetch_page(seed.url)
            if html is None:
                outcome.http_errors += 1
                continue
            outcome.pages_fetched += 1
            try:
                page_rows = parse_topdrawer_transfer_portal_html(
                    html, source_url=seed.url,
                )
            except Exception as exc:
                logger.error(
                    "[transfer-portal] parse failed for %s: %s",
                    seed.url, exc,
                )
                outcome.http_errors += 1
                continue
            for r in page_rows:
                r["season_window"] = seed.season_window
                r["gender"] = seed.gender
                r["division"] = seed.division
            all_rows.extend(page_rows)
    except Exception as exc:  # pragma: no cover — defensive
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[transfer-portal] runner crashed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.topdrawersoccer.com/college-soccer-articles",
            league_name="TopDrawerSoccer transfer tracker",
        )
        return outcome

    outcome.entries_parsed = len(all_rows)

    if outcome.pages_fetched == 0:
        logger.warning(
            "[transfer-portal] 0 pages fetched (%d HTTP errors) — likely "
            "blocked; see proxy_config.yaml note",
            outcome.http_errors,
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

    if not all_rows:
        logger.warning(
            "[transfer-portal] fetched %d page(s) but extracted 0 entries",
            outcome.pages_fetched,
        )
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="no transfer entries extracted",
            )
        outcome.failure_kind = FailureKind.ZERO_RESULTS
        return outcome

    try:
        counts = insert_transfer_portal_entries(all_rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[transfer-portal] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.topdrawersoccer.com/college-soccer-articles",
            league_name="TopDrawerSoccer transfer tracker",
        )
        return outcome

    outcome.counts = counts
    outcome.rows_upserted = counts.get("inserted", 0) + counts.get("updated", 0)

    logger.info(
        "[transfer-portal] pages=%d parsed=%d inserted=%d updated=%d "
        "skipped=%d http_errors=%d",
        outcome.pages_fetched,
        outcome.entries_parsed,
        counts.get("inserted", 0),
        counts.get("updated", 0),
        counts.get("skipped", 0),
        outcome.http_errors,
    )
    if run_log is not None:
        run_log.finish_ok(
            records_created=counts.get("inserted", 0),
            records_updated=counts.get("updated", 0),
            records_failed=counts.get("skipped", 0) + outcome.http_errors,
        )

    return outcome


def print_summary(outcome: TransferPortalRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  NCAA Transfer Portal (TopDrawerSoccer) — run summary")
    print("=" * 60)
    print(f"  Pages fetched        : {outcome.pages_fetched}")
    print(f"  Entries parsed       : {outcome.entries_parsed}")
    print(f"  Rows upserted        : {outcome.rows_upserted}")
    print(f"    inserted           : {outcome.counts.get('inserted', 0)}")
    print(f"    updated            : {outcome.counts.get('updated', 0)}")
    print(f"    skipped            : {outcome.counts.get('skipped', 0)}")
    print(f"  HTTP errors          : {outcome.http_errors}")
    if outcome.failure_kind is not None:
        print(f"  Failure kind         : {outcome.failure_kind.value}")
    if outcome.error:
        print(f"  Error                : {outcome.error[:120]}")
    print("=" * 60)
