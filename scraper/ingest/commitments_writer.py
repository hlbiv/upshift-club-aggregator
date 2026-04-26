"""
commitments_writer.py — Idempotent upsert of ``commitments`` rows.

See lib/db/src/schema/commitments.ts for the table shape. The natural
key is a unique INDEX (not a table constraint):

    commitments_natural_key_uq
      UNIQUE (player_name, graduation_year, college_name_raw)

Postgres accepts ``ON CONFLICT (cols)`` against a unique index whose
columns match. We use the column list form (rather than a named
``ON CONSTRAINT`` reference) because Drizzle emits this as an index,
not a constraint.

On conflict:
  - ``last_seen_at`` refreshes to ``now()`` (carries first-seen untouched)
  - ``club_name_raw`` fills in only if the stored value is NULL
  - ``commitment_date`` fills in only if the new value is non-NULL and
    the stored value is NULL (first non-NULL wins)

``club_id`` is intentionally NOT written by the scraper. The canonical-
club linker resolves it in a later pass.
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

log = logging.getLogger("commitments_writer")


_INSERT_COMMITMENT_SQL = """
INSERT INTO commitments (
    player_name,
    graduation_year,
    position,
    club_id,
    club_name_raw,
    college_id,
    college_name_raw,
    commitment_date,
    source_url,
    first_seen_at,
    last_seen_at
)
VALUES (
    %(player_name)s,
    %(graduation_year)s,
    %(position)s,
    NULL,
    %(club_name_raw)s,
    %(college_id)s,
    %(college_name_raw)s,
    %(commitment_date)s,
    %(source_url)s,
    now(),
    now()
)
ON CONFLICT (player_name, graduation_year, college_name_raw)
DO UPDATE SET
    last_seen_at    = now(),
    club_name_raw   = COALESCE(commitments.club_name_raw, EXCLUDED.club_name_raw),
    commitment_date = COALESCE(commitments.commitment_date, EXCLUDED.commitment_date),
    position        = COALESCE(commitments.position, EXCLUDED.position),
    college_id      = COALESCE(commitments.college_id, EXCLUDED.college_id)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    player_name = (row.get("player_name") or "").strip()
    college_name_raw = (row.get("college_name_raw") or "").strip()
    if not player_name:
        raise ValueError("commitment row missing player_name")
    if not college_name_raw:
        raise ValueError("commitment row missing college_name_raw")
    source_url = (row.get("source_url") or "").strip()
    if not source_url:
        raise ValueError("commitment row missing source_url")
    return {
        "player_name": player_name,
        "graduation_year": row.get("graduation_year"),
        "position": (row.get("position") or None),
        "club_name_raw": (row.get("club_name_raw") or None),
        "college_id": row.get("college_id"),
        "college_name_raw": college_name_raw,
        "commitment_date": row.get("commitment_date"),
        "source_url": source_url,
    }


def insert_commitments(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of commitment rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    ``updated`` counts conflict-hits (the natural key was already
    present); the SET list always refreshes ``last_seen_at`` so every
    conflict is a real update.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[commitments-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[commitments-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_COMMITMENT_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[commitments-writer] upsert failed for %s / %s: %s",
                        row.get("player_name"), row.get("college_name_raw"), exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()  # noqa: writer-rollback
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
        "event": "commitments-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
