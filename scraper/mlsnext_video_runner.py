"""
mlsnext_video_runner.py — MLS NEXT video-library scraper.

Invoked via ``run.py --source mlsnext-video``. Steps:

  1. GET ``https://www.mlssoccer.com/mlsnext/video/`` (server-rendered
     HTML; no auth, no SPA shell).
  2. Parse each Brightcove-backed video card into a ``video_sources``
     row. Cards land with ``source_platform='mls_com'`` and
     ``video_type='unknown'``; classification is a follow-up.
  3. Upsert via ``ingest.video_sources_writer.insert_video_sources``.

Scope:
  - No YouTube Data API usage; MLS NEXT videos are Brightcove-hosted.
  - ``club_id`` and ``club_name_raw`` stay NULL — MLS NEXT is a
    league-level source, not a single club. The canonical-club
    linker can wire club associations in a follow-up via title
    parsing (teams in card titles like "LA Galaxy vs. Boca Juniors").
  - ``published_at`` is NULL — not exposed on the listing page.
  - No classification (``video_type`` defaults to ``'unknown'``).
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.mlsnext_video import fetch_mlsnext_videos  # noqa: E402
from ingest.video_sources_writer import insert_video_sources  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("mlsnext_video_runner")


_DEFAULT_LIST_URL = "https://www.mlssoccer.com/mlsnext/video/"


@dataclass
class MlsnextVideoRunSummary:
    list_url: str = _DEFAULT_LIST_URL
    videos_parsed: int = 0
    rows_upserted: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def run_mlsnext_video(
    *,
    list_url: str = _DEFAULT_LIST_URL,
    dry_run: bool = False,
    **kwargs: Any,
) -> MlsnextVideoRunSummary:
    """Fetch the MLS NEXT video-library page and upsert ``video_sources``.

    Returns a summary with the parsed/upserted counts + failure info.
    """
    summary = MlsnextVideoRunSummary(list_url=list_url)
    scraper_key = "mlsnext-video"

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name="MLS NEXT",
        )
        run_log.start(source_url=list_url)

    try:
        videos = fetch_mlsnext_videos(list_url)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[mlsnext-video-runner] fetch failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=list_url,
            league_name="MLS NEXT",
        )
        return summary

    summary.videos_parsed = len(videos)
    logger.info(
        "[mlsnext-video-runner] %s → %d videos parsed",
        list_url, len(videos),
    )

    if not videos:
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="page returned zero video cards",
            )
        summary.failure_kind = FailureKind.ZERO_RESULTS
        return summary

    rows: List[Dict[str, Any]] = []
    for v in videos:
        rows.append({
            "source_platform": "mls_com",
            "video_type": "unknown",
            "external_id": v.get("external_id"),
            "source_url": v.get("source_url"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "duration_seconds": v.get("duration_seconds"),
            "thumbnail_url": v.get("thumbnail_url"),
            "metadata": v.get("metadata") or {},
            "club_name_raw": None,
        })

    try:
        counts = insert_video_sources(rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[mlsnext-video-runner] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=list_url,
            league_name="MLS NEXT",
        )
        return summary

    summary.counts = counts
    summary.rows_upserted = (
        counts.get("inserted", 0) + counts.get("updated", 0)
    )

    if run_log is not None:
        run_log.finish_ok(
            records_created=counts.get("inserted", 0),
            records_updated=counts.get("updated", 0),
            records_failed=counts.get("skipped", 0),
        )

    return summary


def print_summary(summary: MlsnextVideoRunSummary) -> None:
    print("\n" + "=" * 60)
    print("  MLS NEXT video library — run summary")
    print("=" * 60)
    print(f"  List URL          : {summary.list_url}")
    print(f"  Videos parsed     : {summary.videos_parsed}")
    print(f"  Rows upserted     : {summary.rows_upserted}")
    if summary.counts:
        print(f"    inserted        : {summary.counts.get('inserted', 0)}")
        print(f"    updated         : {summary.counts.get('updated', 0)}")
        print(f"    skipped         : {summary.counts.get('skipped', 0)}")
    if summary.failure_kind is not None:
        print(f"  Failure kind      : {summary.failure_kind.value}")
        if summary.error:
            print(f"  Error             : {summary.error[:120]}")
    print("=" * 60)
