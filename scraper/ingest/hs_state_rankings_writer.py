"""
hs_state_rankings_writer.py — Idempotent upsert of
``hs_state_rankings`` rows.

See lib/db/src/schema/hs_state_rankings.ts for the table shape. Natural
key is a unique INDEX:

    hs_state_rankings_natural_key_uq
      UNIQUE (state, gender, season, school_name_raw, rank)

On conflict:
  - ``last_seen_at`` refreshes to ``now()``
  - COALESCE-updates fill in ``record``, ``points``, ``section``
    only where the stored value is NULL (first non-NULL wins).

``school_id`` is NOT written by the scraper — a linker pass resolves
it later.
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

log = logging.getLogger("hs_state_rankings_writer")


_INSERT_HS_RANKING_SQL = """
INSERT INTO hs_state_rankings (
    state,
    gender,
    season,
    rank,
    school_id,
    school_name_raw,
    record,
    points,
    section,
    source_url,
    first_seen_at,
    last_seen_at
)
VALUES (
    %(state)s,
    %(gender)s,
    %(season)s,
    %(rank)s,
    NULL,
    %(school_name_raw)s,
    %(record)s,
    %(points)s,
    %(section)s,
    %(source_url)s,
    now(),
    now()
)
ON CONFLICT (state, gender, season, school_name_raw, rank)
DO UPDATE SET
    last_seen_at = now(),
    record       = COALESCE(hs_state_rankings.record,  EXCLUDED.record),
    points       = COALESCE(hs_state_rankings.points,  EXCLUDED.points),
    section      = COALESCE(hs_state_rankings.section, EXCLUDED.section)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_REQUIRED = ("state", "gender", "season", "school_name_raw", "source_url")


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in _REQUIRED:
        value = row.get(key)
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError(f"hs_state_rankings row missing {key}")
        out[key] = value
    rank = row.get("rank")
    if not isinstance(rank, int):
        raise ValueError("hs_state_rankings row missing integer rank")
    out["rank"] = rank
    out["record"] = (row.get("record") or None)
    out["points"] = row.get("points")
    out["section"] = (row.get("section") or None)
    return out


def insert_hs_state_rankings(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of ``hs_state_rankings`` rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info(
            "[hs-state-rankings-writer] dry-run: would upsert %d rows", len(rows),
        )
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[hs-state-rankings-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_HS_RANKING_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[hs-state-rankings-writer] upsert failed for %s #%s: %s",
                        row.get("school_name_raw"),
                        row.get("rank"),
                        exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()
                    continue
                if result is None:
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
        "event": "hs-state-rankings-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
