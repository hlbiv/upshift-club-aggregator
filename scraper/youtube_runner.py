"""
youtube_runner.py — YouTube channel RSS scraper.

Invoked via ``run.py --source youtube-<channel-key>`` (e.g.
``youtube-ecnl``). For each configured channel:

  1. Resolve the ``@handle`` (or legacy username) to a ``UC...``
     channel id by fetching the channel HTML and extracting the
     ``channelId`` meta tag.
  2. GET the channel's zero-auth Atom feed at
     ``https://www.youtube.com/feeds/videos.xml?channel_id=<UC...>``.
  3. Parse each ``<entry>`` into a ``video_sources`` row. YouTube
     feed items land with ``video_type='unknown'`` —
     highlight/documentary/promo classification is a follow-up PR.
  4. Upsert via ``ingest.video_sources_writer.insert_video_sources``.

Scope:
  - No YouTube Data API usage — RSS only. ``duration_seconds`` stays
    NULL until a follow-up PR wires the Data API for contentDetails.
  - ``club_id`` stays NULL. For channels tied to a single club, the
    canonical-club linker can resolve it later via ``club_name_raw``;
    for league-level channels (ECNL) we leave ``club_name_raw`` NULL
    because the channel does not represent a specific club.
  - No classification (``video_type`` defaults to ``'unknown'``).
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(__file__))

from extractors.youtube_channel import (  # noqa: E402
    fetch_channel_videos,
    resolve_channel_id,
)
from ingest.video_sources_writer import insert_video_sources  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)
from alerts import alert_scraper_failure  # noqa: E402

logger = logging.getLogger("youtube_runner")


@dataclass
class YoutubeRunSummary:
    channel_id: Optional[str] = None
    videos_parsed: int = 0
    rows_upserted: int = 0
    counts: Dict[str, int] = field(default_factory=dict)
    failure_kind: Optional[FailureKind] = None
    error: Optional[str] = None


def run_youtube_channel(
    handle: str,
    league_name: str,
    source_platform: str = "youtube",
    *,
    dry_run: bool = False,
    club_name_raw: Optional[str] = None,
    **kwargs: Any,
) -> YoutubeRunSummary:
    """Fetch the RSS feed for ``handle`` and upsert ``video_sources`` rows.

    ``league_name`` is used only for ``scrape_run_logs`` labeling. The
    YouTube channel itself is identified by the resolved ``UC`` id.

    ``club_name_raw`` is intentionally surfaced as a kwarg but defaults
    to None — for league channels (ECNL) we want NULL here because the
    channel is not a single club. Callers that scrape club-specific
    channels should pass the club's display name.
    """
    summary = YoutubeRunSummary()
    scraper_key = f"youtube:{handle}"

    run_log: Optional[ScrapeRunLogger] = None
    if not dry_run:
        run_log = ScrapeRunLogger(
            scraper_key=scraper_key,
            league_name=league_name,
        )
        run_log.start(source_url=f"youtube:{handle}")

    try:
        channel_id = resolve_channel_id(handle)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[youtube-runner] resolve_channel_id failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=f"youtube:{handle}",
            league_name=league_name,
        )
        return summary

    if not channel_id:
        summary.failure_kind = FailureKind.PARSE_ERROR
        summary.error = f"could not resolve channel id for handle={handle!r}"
        logger.warning("[youtube-runner] %s", summary.error)
        if run_log is not None:
            run_log.finish_failed(
                FailureKind.PARSE_ERROR,
                error_message=summary.error,
            )
        return summary

    summary.channel_id = channel_id
    logger.info("[youtube-runner] %s → channel_id=%s", handle, channel_id)

    try:
        videos = fetch_channel_videos(channel_id)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[youtube-runner] fetch_channel_videos failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=f"youtube:{channel_id}",
            league_name=league_name,
        )
        return summary

    summary.videos_parsed = len(videos)
    logger.info(
        "[youtube-runner] %s (UC %s) → %d videos parsed",
        handle, channel_id, len(videos),
    )

    if not videos:
        if run_log is not None:
            run_log.finish_partial(
                records_failed=0,
                error_message="feed returned zero entries",
            )
        summary.failure_kind = FailureKind.ZERO_RESULTS
        return summary

    rows: List[Dict[str, Any]] = []
    for v in videos:
        rows.append({
            "source_platform": source_platform,
            "video_type": "unknown",
            "external_id": v.get("external_id"),
            "source_url": v.get("source_url"),
            "title": v.get("title"),
            "published_at": v.get("published_at"),
            "duration_seconds": None,
            "thumbnail_url": v.get("thumbnail_url"),
            "metadata": v.get("metadata") or {},
            "club_name_raw": club_name_raw,
        })

    try:
        counts = insert_video_sources(rows, dry_run=dry_run)
    except Exception as exc:
        kind = classify_exception(exc)
        summary.failure_kind = kind
        summary.error = str(exc)
        logger.error("[youtube-runner] write failed: %s", exc)
        if run_log is not None:
            run_log.finish_failed(kind, error_message=str(exc))
        alert_scraper_failure(
            scraper_key=scraper_key,
            failure_kind=kind.value,
            error_message=str(exc),
            source_url=f"youtube:{channel_id}",
            league_name=league_name,
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


def print_summary(summary: YoutubeRunSummary) -> None:
    print("\n" + "=" * 60)
    print("  YouTube channel — run summary")
    print("=" * 60)
    print(f"  Channel id        : {summary.channel_id or '<unresolved>'}")
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
