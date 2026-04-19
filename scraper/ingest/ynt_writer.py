"""
ynt_writer.py — Idempotent upsert of ``ynt_call_ups`` rows.

See ``lib/db/src/schema/ynt.ts`` for the table shape. The natural-key
unique index is::

    ynt_call_ups_natural_key_uq
      UNIQUE (player_name, age_group, gender, camp_event)

``club_id`` is intentionally left NULL — the canonical-club linker
resolves it in a follow-up pass.

On conflict we refresh ``last_seen_at`` and opportunistically fill in
``club_name_raw`` if the existing row's value is NULL (the linker can
then resolve the new hint). Other mutable fields (``position``,
``graduation_year``, camp dates, source URL) are left untouched once
the row exists — re-ingesting the same press release should be a no-op.
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

log = logging.getLogger("ynt_writer")


_INSERT_SQL = """
INSERT INTO ynt_call_ups (
    player_name, graduation_year, position,
    club_id, club_name_raw,
    age_group, gender,
    camp_event, camp_start_date, camp_end_date,
    source_url, first_seen_at, last_seen_at
)
VALUES (
    %(player_name)s, %(graduation_year)s, %(position)s,
    NULL, %(club_name_raw)s,
    %(age_group)s, %(gender)s,
    %(camp_event)s, %(camp_start_date)s, %(camp_end_date)s,
    %(source_url)s, now(), now()
)
ON CONFLICT (player_name, age_group, gender, camp_event)
DO UPDATE SET
    last_seen_at  = EXCLUDED.last_seen_at,
    club_name_raw = COALESCE(ynt_call_ups.club_name_raw, EXCLUDED.club_name_raw)
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
    if not row.get("player_name"):
        raise ValueError("ynt row missing player_name")
    if not row.get("age_group"):
        raise ValueError("ynt row missing age_group")
    if not row.get("gender"):
        raise ValueError("ynt row missing gender")
    if not row.get("source_url"):
        raise ValueError("ynt row missing source_url")
    return {
        "player_name": row["player_name"],
        "graduation_year": row.get("graduation_year"),
        "position": row.get("position"),
        "club_name_raw": row.get("club_name_raw"),
        "age_group": row["age_group"],
        "gender": row["gender"],
        "camp_event": row.get("camp_event"),
        "camp_start_date": row.get("camp_start_date"),
        "camp_end_date": row.get("camp_end_date"),
        "source_url": row["source_url"],
    }


def insert_ynt_call_ups(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of YNT call-up rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``. ``skipped``
    counts rows that failed validation or threw at ``execute`` time.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[ynt-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[ynt-writer] skipping bad row: %s", exc)
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
                        "[ynt-writer] upsert failed for %s / %s / %s: %s",
                        row.get("player_name"),
                        row.get("age_group"),
                        row.get("camp_event"),
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
        "event": "ynt-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
