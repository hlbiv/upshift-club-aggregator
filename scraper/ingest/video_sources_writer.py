"""
video_sources_writer.py — Idempotent upsert of ``video_sources`` rows.

See ``lib/db/src/schema/video-sources.ts`` for the table shape. The
natural-key unique index is::

    video_sources_platform_external_id_uq
      UNIQUE (source_platform, external_id)

On conflict we refresh ``last_seen_at`` and re-sync the mutable display
fields (``title``, ``thumbnail_url``, ``metadata``). ``club_id`` is
intentionally left NULL — the canonical-club linker resolves it in a
follow-up pass (channel-to-league mapping lives there, not here).

``duration_seconds`` is NULL for YouTube feed items — the feed
doesn't expose duration, and fetching it requires a YouTube Data
API key. Deferred to a follow-up PR.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg2  # type: ignore
    from psycopg2.extras import Json  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore
    Json = None  # type: ignore

log = logging.getLogger("video_sources_writer")


_INSERT_SQL = """
INSERT INTO video_sources (
    club_id, club_name_raw,
    source_platform, video_type,
    external_id, source_url,
    title, published_at, duration_seconds, thumbnail_url,
    metadata,
    first_seen_at, last_seen_at
)
VALUES (
    NULL, %(club_name_raw)s,
    %(source_platform)s, %(video_type)s,
    %(external_id)s, %(source_url)s,
    %(title)s, %(published_at)s, %(duration_seconds)s, %(thumbnail_url)s,
    %(metadata)s,
    now(), now()
)
ON CONFLICT ON CONSTRAINT video_sources_platform_external_id_uq
DO UPDATE SET
    last_seen_at   = now(),
    title          = COALESCE(EXCLUDED.title, video_sources.title),
    thumbnail_url  = COALESCE(EXCLUDED.thumbnail_url, video_sources.thumbnail_url),
    metadata       = COALESCE(EXCLUDED.metadata, video_sources.metadata)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_ALLOWED_PLATFORMS = {
    "youtube", "hudl_fan_recap", "hudl_fan_full_game",
    "hudl_broadcast", "mls_com",
}
_ALLOWED_VIDEO_TYPES = {"highlight", "full_game", "documentary", "promo", "unknown"}


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row.get("source_platform"):
        raise ValueError("video_sources row missing source_platform")
    if not row.get("external_id"):
        raise ValueError("video_sources row missing external_id")
    if not row.get("source_url"):
        raise ValueError("video_sources row missing source_url")

    platform = row["source_platform"]
    if platform not in _ALLOWED_PLATFORMS:
        raise ValueError(
            f"video_sources row source_platform={platform!r} not in allowed set"
        )

    video_type = row.get("video_type") or "unknown"
    if video_type not in _ALLOWED_VIDEO_TYPES:
        raise ValueError(
            f"video_sources row video_type={video_type!r} not in allowed set"
        )

    metadata = row.get("metadata")
    if metadata is not None and not isinstance(metadata, (dict, list)):
        raise ValueError("video_sources row metadata must be dict/list or None")

    # psycopg2 needs ``Json(...)`` to serialize dict → jsonb. Handle
    # None by passing None through so SQL sees NULL.
    if metadata is not None and Json is not None:
        metadata_param: Any = Json(metadata)
    else:
        metadata_param = None

    return {
        "club_name_raw": row.get("club_name_raw"),
        "source_platform": platform,
        "video_type": video_type,
        "external_id": row["external_id"],
        "source_url": row["source_url"],
        "title": row.get("title"),
        "published_at": row.get("published_at"),
        "duration_seconds": row.get("duration_seconds"),
        "thumbnail_url": row.get("thumbnail_url"),
        "metadata": metadata_param,
    }


def insert_video_sources(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of ``video_sources`` rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[video-sources-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[video-sources-writer] skipping bad row: %s", exc)
            counts["skipped"] += 1

    if not normalized:
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            for row in normalized:
                try:
                    cur.execute(_INSERT_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[video-sources-writer] upsert failed for %s/%s: %s",
                        row.get("source_platform"),
                        row.get("external_id"),
                        exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()
                    continue
                if result is None:
                    continue
                inserted = bool(result[0])
                if inserted:
                    counts["inserted"] += 1
                else:
                    counts["updated"] += 1
        if own_conn:
            conn.commit()
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    log.info(json.dumps({
        "event": "video-sources-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
