"""
rankings_writer.py — Idempotent upsert of ``club_rankings`` rows.

See lib/db/src/schema/rankings.ts for the table shape. The natural-key
unique constraint is:

    club_rankings_natural_uq
      UNIQUE (platform, club_name_raw, age_group, gender, season, division)

On conflict, ``rank_value``, ``rating_value``, and ``scraped_at`` are
refreshed (ranking position and rating can legitimately change between
scrape runs).

``canonical_club_id`` is intentionally left NULL at write time. The
canonical-club linker (``python3 run.py --source link-canonical-clubs``)
resolves it in a subsequent pass.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger("rankings_writer")

_INSERT_RANKING_SQL = """
INSERT INTO club_rankings (
    canonical_club_id,
    club_name_raw,
    platform,
    rank_value,
    rating_value,
    age_group,
    gender,
    season,
    division,
    source_url,
    scraped_at
)
VALUES (
    NULL,
    %(club_name_raw)s,
    %(platform)s,
    %(rank_value)s,
    %(rating_value)s,
    %(age_group)s,
    %(gender)s,
    %(season)s,
    %(division)s,
    %(source_url)s,
    NOW()
)
ON CONFLICT ON CONSTRAINT club_rankings_natural_uq
DO UPDATE SET
    rank_value   = EXCLUDED.rank_value,
    rating_value = EXCLUDED.rating_value,
    scraped_at   = NOW()
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_REQUIRED = ("club_name_raw", "platform")


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and normalise a ranking dict before DB write."""
    for key in _REQUIRED:
        value = row.get(key)
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError(f"club_rankings row missing required field: {key!r}")

    club_name_raw = row["club_name_raw"].strip()
    platform = row["platform"].strip()

    rank_value = row.get("rank_value")
    if rank_value is not None:
        try:
            rank_value = int(rank_value)
        except (ValueError, TypeError):
            rank_value = None

    return {
        "club_name_raw": club_name_raw,
        "platform": platform,
        "rank_value": rank_value,
        "rating_value": row.get("rating_value") or None,
        "age_group": row.get("age_group") or None,
        "gender": row.get("gender") or None,
        "season": row.get("season") or None,
        "division": row.get("division") or None,
        "source_url": row.get("source_url") or None,
    }


def insert_rankings(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of club ranking rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[rankings-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[rankings-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_RANKING_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[rankings-writer] upsert failed for %s (%s %s): %s",
                        row.get("club_name_raw"),
                        row.get("age_group"),
                        row.get("gender"),
                        exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()  # noqa: writer-rollback
                    continue
                if result is None:
                    # DO UPDATE WHERE short-circuited — nothing changed.
                    continue
                if bool(result[0]):
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
        "event": "rankings-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
