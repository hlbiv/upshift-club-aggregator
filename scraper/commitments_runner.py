"""
commitments_runner.py — Orchestrate the TopDrawerSoccer commitments scrape.

Invoked via:
    run.py --source topdrawer-commitments [--dry-run] [--limit N]

Walks one or more TDS commitments-list pages via
``scraper.utils.http.get`` (which layers in per-domain proxy rotation
when ``proxy_config.yaml`` has ``topdrawersoccer.com`` configured),
parses each page with
``extractors.topdrawer_commitments.parse_topdrawer_commitments_html``,
and upserts rows through ``ingest.commitments_writer.insert_commitments``.

COVERAGE & LIMITATIONS (April 2026)
-----------------------------------
TopDrawerSoccer actively blocks bulk crawlers. On an unproxied fetch,
expect HTTP 403 on the first or second request. For that reason:

  * The default ``--limit`` is 20 — intentionally low. Product decision:
    it's better to land a working pipeline that grabs a small slice of
    commitments behind the default IP than to ship something that
    tripwires a block on its first run and looks broken.
  * Scaling this beyond the default will require rotating residential
    proxies configured under ``scraper/proxy_config.yaml`` as::

          domains:
            topdrawersoccer.com:
              proxies:
                - http://user:pass@host1:port
                - http://user:pass@host2:port
              cooldown_seconds: 300

    Until a proxy provider is wired up, expect the runner to bail out
    early on 403 with a clear warning; the run log row is tagged
    ``failure_kind=network`` and the process exits cleanly.

SEED LIST
---------
We seed only the most valuable aggregate index pages by default. The
list is intentionally tiny — page-level pagination fan-out is disabled
pending proxy support. Individual class-year subpages can be added
once we have a stable fetch profile.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

sys.path.insert(0, os.path.dirname(__file__))

from extractors.topdrawer_commitments import parse_topdrawer_commitments_html  # noqa: E402
from ingest.commitments_writer import insert_commitments  # noqa: E402
from utils.http import get as http_get  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("commitments_runner")


# Default starting URLs. Each is a public TDS commitments index page.
# TDS paginates; we deliberately do NOT follow pagination in the
# default run until proxy support lands.
DEFAULT_TDS_SEEDS: List[str] = [
    "https://www.topdrawersoccer.com/college-soccer-commitments/girls",
    "https://www.topdrawersoccer.com/college-soccer-commitments/boys",
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
class CommitmentsRunOutcome:
    pages_fetched: int = 0
    commitments_parsed: int = 0
    rows_upserted: int = 0
    http_errors: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _fetch_page(url: str, timeout: int = 20) -> Optional[str]:
    """Fetch one TDS page via the proxy-aware HTTP helper.

    Returns the HTML string on 2xx, ``None`` on any non-200 /
    connection error. Logs a warning for 403 explicitly calling out
    the proxy-config remediation.
    """
    try:
        resp = http_get(url, headers=_TDS_HEADERS, timeout=timeout)
    except requests.RequestException as exc:
        logger.warning("[topdrawer] fetch failed for %s: %s", url, exc)
        return None

    if resp.status_code == 403:
        logger.warning(
            "[topdrawer] 403 at %s — consider adding proxy credentials to "
            "scraper/proxy_config.yaml under 'topdrawersoccer.com'",
            url,
        )
        return None
    if resp.status_code != 200:
        logger.warning(
            "[topdrawer] non-200 status %d at %s", resp.status_code, url,
        )
        return None
    return resp.text


def run_topdrawer_commitments(
    *,
    dry_run: bool = False,
    limit: Optional[int] = DEFAULT_LIMIT,
    seeds: Optional[List[str]] = None,
    **_kwargs,
) -> CommitmentsRunOutcome:
    """Fetch TopDrawerSoccer commitments index pages and upsert rows.

    Default ``limit`` is **20** for a reason: TopDrawerSoccer blocks
    bulk crawlers aggressively. Until rotating residential proxies are
    wired up for ``topdrawersoccer.com`` in ``proxy_config.yaml``, any
    larger run will trip 403s and produce no data. The small cap keeps
    the pipeline land-and-verify friendly.

    Returns a ``CommitmentsRunOutcome`` summary; ``.counts`` carries
    the inserted/updated/skipped breakdown from the writer.
    """
    seed_list = list(seeds) if seeds else list(DEFAULT_TDS_SEEDS)
    if limit is not None and limit > 0:
        seed_list = seed_list[:limit]

    outcome = CommitmentsRunOutcome()
    if not seed_list:
        logger.info("[topdrawer] seed list empty — nothing to do")
        return outcome

    scraper_key = "topdrawer-commitments"
    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="TopDrawerSoccer commitments",
        )
        run_log.start(source_url="https://www.topdrawersoccer.com/college-soccer-commitments")

    all_rows: List[Dict] = []
    try:
        for url in seed_list:
            html = _fetch_page(url)
            if html is None:
                outcome.http_errors += 1
                continue
            outcome.pages_fetched += 1
            try:
                page_rows = parse_topdrawer_commitments_html(
                    html, source_url=url,
                )
            except Exception as exc:
                logger.error("[topdrawer] parse failed for %s: %s", url, exc)
                outcome.http_errors += 1
                continue
            all_rows.extend(page_rows)
    except Exception as exc:  # pragma: no cover — defensive
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[topdrawer] runner crashed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.topdrawersoccer.com/college-soccer-commitments",
            league_name="TopDrawerSoccer commitments",
        )
        return outcome

    outcome.commitments_parsed = len(all_rows)

    # Edge: every seed returned a 403 / non-200. Log a partial run
    # and exit cleanly.
    if outcome.pages_fetched == 0:
        logger.warning(
            "[topdrawer] 0 pages fetched (%d HTTP errors) — likely blocked; "
            "see proxy_config.yaml note in the runner docstring",
            outcome.http_errors,
        )
        if run_log is not None:
            run_log.finish_partial(
                records_failed=outcome.http_errors,
                error_message=f"{outcome.http_errors} HTTP error(s); 0 pages fetched",
            )
        outcome.failure_kind = FailureKind.NETWORK
        return outcome

    if not all_rows:
        logger.warning(
            "[topdrawer] fetched %d page(s) but extracted 0 commitments",
            outcome.pages_fetched,
        )
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="no commitments extracted",
            )
        outcome.failure_kind = FailureKind.ZERO_RESULTS
        return outcome

    try:
        counts = insert_commitments(all_rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        outcome.failure_kind = kind
        outcome.error = str(exc)
        logger.error("[topdrawer] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="https://www.topdrawersoccer.com/college-soccer-commitments",
            league_name="TopDrawerSoccer commitments",
        )
        return outcome

    outcome.counts = counts
    outcome.rows_upserted = counts.get("inserted", 0) + counts.get("updated", 0)

    logger.info(
        "[topdrawer] pages=%d parsed=%d inserted=%d updated=%d skipped=%d "
        "http_errors=%d",
        outcome.pages_fetched,
        outcome.commitments_parsed,
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

    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return outcome


def print_summary(outcome: CommitmentsRunOutcome) -> None:
    print("\n" + "=" * 60)
    print("  TopDrawerSoccer commitments — run summary")
    print("=" * 60)
    print(f"  Pages fetched        : {outcome.pages_fetched}")
    print(f"  Commitments parsed   : {outcome.commitments_parsed}")
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
