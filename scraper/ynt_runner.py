"""
ynt_runner.py — US Soccer YNT call-up scraper.

Invoked via ``run.py --source ussoccer-ynt``.

Press-release URLs are hardcoded here as a small static seed list.
Automated discovery of new article URLs (scanning ussoccer.com/news
for YNT-tagged posts) is explicitly out of scope for this PR — the
expected pattern is to add new article URLs to ``USSOCCER_YNT_SEED``
as US Soccer publishes them, or follow up with a discovery extractor
in a later PR.

US Soccer's public marketing site is friendly to scrapers (no
Cloudflare challenge, no proxy needed today). This makes YNT a
useful end-to-end validator for the Wave 2 pipeline scaffold
(``utils.http.get`` + writer + SOURCE_HANDLERS) without requiring
residential-proxy infrastructure.

If a seed URL 404s or its HTML layout drifts so the extractor emits
zero rows, the runner logs a warning and continues — other articles
in the batch are still processed. Per-run telemetry goes to
``scrape_run_logs`` under ``scraper_key='ussoccer-ynt'``.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.ussoccer_ynt import parse_article_html  # noqa: E402
from ingest.ynt_writer import insert_ynt_call_ups  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402
from utils.http import get as http_get  # noqa: E402
from utils.retry import retry_with_backoff  # noqa: E402

logger = logging.getLogger("ynt_runner")

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; "
        "+https://upshift.club)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "en-US,en;q=0.8",
}


# ---------------------------------------------------------------------------
# Seed list — US Soccer YNT article index pages.
#
# Hardcoded on purpose. Discovery of new URLs is out of scope for this
# PR (see module docstring). Adding a new URL = one line here.
#
# Format: (url, age_group_hint | None, gender_hint | None). Hints help
# when the URL slug is unambiguous; the extractor also sniffs from the
# article's <h1>.
# ---------------------------------------------------------------------------
USSOCCER_YNT_SEED: List[Dict[str, Optional[str]]] = [
    # Each entry is kept loose — if a URL 404s the runner logs and
    # continues. Over time this list will grow / churn as US Soccer
    # publishes new rosters and archives old ones.
    {
        "url": (
            "https://www.ussoccer.com/stories/2025/12/"
            "u17-bnt-announces-january-2026-training-camp-roster"
        ),
        "age_group_hint": "U-17",
        "gender_hint": "boys",
    },
    {
        "url": (
            "https://www.ussoccer.com/stories/2025/11/"
            "u19-bnt-roster-november-2025-international-camp"
        ),
        "age_group_hint": "U-19",
        "gender_hint": "boys",
    },
    {
        "url": (
            "https://www.ussoccer.com/stories/2025/12/"
            "u17-gnt-announces-january-2026-training-camp-roster"
        ),
        "age_group_hint": "U-17",
        "gender_hint": "girls",
    },
    {
        "url": (
            "https://www.ussoccer.com/stories/2025/11/"
            "u19-gnt-roster-november-2025-international-camp"
        ),
        "age_group_hint": "U-19",
        "gender_hint": "girls",
    },
]


SCRAPER_KEY = "ussoccer-ynt"


@dataclass
class YntRunSummary:
    articles_fetched: int = 0
    call_ups_parsed: int = 0
    rows_upserted: int = 0
    http_errors: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def _fetch(url: str, timeout: int = 20) -> Optional[str]:
    """Fetch an article URL through ``utils.http.get`` with retry.

    Returns the response body on 200, or None on error/non-200. Never
    raises — errors are logged and swallowed so a single bad URL
    doesn't kill the batch.
    """

    def _do() -> Optional[str]:
        resp = http_get(url, timeout=timeout, headers=_HEADERS)
        if resp.status_code != 200:
            logger.warning(
                "[ussoccer-ynt] %s → HTTP %s", url, resp.status_code,
            )
            return None
        return resp.text

    try:
        return retry_with_backoff(
            _do,
            max_retries=2,
            base_delay=2.0,
            label=f"ussoccer-ynt:{url[:60]}",
        )
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("[ussoccer-ynt] fetch failed for %s: %s", url, exc)
        return None


def run_ussoccer_ynt(
    *,
    dry_run: bool = False,
    limit: Optional[int] = None,
    **kwargs: Any,
) -> YntRunSummary:
    """Fetch and parse US Soccer YNT press-release articles, upsert rows.

    Extra ``**kwargs`` accepted so call-sites (run.py handlers, tests)
    can pass future flags without breaking the signature.
    """
    seeds = list(USSOCCER_YNT_SEED)
    if limit is not None:
        seeds = seeds[:limit]

    summary = YntRunSummary()

    if not seeds:
        logger.info("[ussoccer-ynt] seed list empty — nothing to do")
        return summary

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=SCRAPER_KEY,
            league_name="US Soccer YNT call-ups",
        )
        run_log.start(source_url="seed:USSOCCER_YNT_SEED")

    all_rows: List[Dict[str, Any]] = []

    for seed in seeds:
        url = seed.get("url") or ""
        if not url:
            continue

        html = _fetch(url)
        if not html:
            summary.http_errors += 1
            continue
        summary.articles_fetched += 1

        try:
            rows = parse_article_html(
                html,
                source_url=url,
                age_group_hint=seed.get("age_group_hint"),
                gender_hint=seed.get("gender_hint"),
            )
        except Exception as exc:
            logger.warning(
                "[ussoccer-ynt] parse failed for %s: %s", url, exc,
            )
            continue

        if not rows:
            logger.warning(
                "[ussoccer-ynt] %s parsed 0 rows (layout drift?)", url,
            )
            continue

        logger.info("[ussoccer-ynt] %s → %d call-ups", url, len(rows))
        summary.call_ups_parsed += len(rows)
        all_rows.extend(rows)

    if not all_rows:
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="no call-up rows extracted",
            )
        summary.failure_kind = FailureKind.ZERO_RESULTS
        return summary

    try:
        counts = insert_ynt_call_ups(all_rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[ussoccer-ynt] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=SCRAPER_KEY,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url="seed:USSOCCER_YNT_SEED",
            league_name="US Soccer YNT call-ups",
        )
        return summary

    summary.counts = counts
    summary.rows_upserted = (
        counts.get("inserted", 0) + counts.get("updated", 0)
    )

    logger.info(
        "[ussoccer-ynt] articles=%d call_ups=%d inserted=%d updated=%d "
        "skipped=%d http_errors=%d",
        summary.articles_fetched,
        summary.call_ups_parsed,
        counts.get("inserted", 0),
        counts.get("updated", 0),
        counts.get("skipped", 0),
        summary.http_errors,
    )
    if run_log is not None:
        run_log.finish_ok(
            records_created=counts.get("inserted", 0),
            records_updated=counts.get("updated", 0),
            records_failed=counts.get("skipped", 0),
        )

    if not dry_run:
        try:
            from reconcilers import end_of_run_reconcile
            end_of_run_reconcile()
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("end_of_run_reconcile skipped: %s", exc)

    return summary


def print_summary(summary: YntRunSummary) -> None:
    print("\n" + "=" * 60)
    print("  US Soccer YNT — run summary")
    print("=" * 60)
    print(f"  Articles fetched  : {summary.articles_fetched}")
    print(f"  Call-ups parsed   : {summary.call_ups_parsed}")
    print(f"  Rows upserted     : {summary.rows_upserted}")
    print(f"  HTTP errors       : {summary.http_errors}")
    if summary.counts:
        print(f"    inserted        : {summary.counts.get('inserted', 0)}")
        print(f"    updated         : {summary.counts.get('updated', 0)}")
        print(f"    skipped         : {summary.counts.get('skipped', 0)}")
    if summary.failure_kind is not None:
        print(f"  Failure kind      : {summary.failure_kind.value}")
        if summary.error:
            print(f"  Error             : {summary.error[:120]}")
    print("=" * 60)
