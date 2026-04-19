"""
odp_writer.py — Idempotent upsert of ``odp_roster_entries`` rows.

See lib/db/src/schema/odp.ts for the table shape. The natural-key
unique index is:

    odp_roster_entries_natural_key_uq
      UNIQUE (player_name, state, program_year, age_group, gender)

Re-running the same scrape is a no-op for the natural key — the
upsert refreshes ``last_seen_at`` and back-fills ``club_name_raw``
when the first scrape was missing it, but does not overwrite a
populated ``club_name_raw`` with a different source's raw name.

``club_id`` is intentionally left NULL. The canonical-club linker
resolves it after the scraper writes — see
``scraper/canonical_club_linker.py``.
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

log = logging.getLogger("odp_writer")


_INSERT_ODP_SQL = """
INSERT INTO odp_roster_entries (
    player_name, graduation_year, position,
    state, program_year, age_group, gender,
    club_id, club_name_raw, source_url,
    first_seen_at, last_seen_at
)
VALUES (
    %(player_name)s, %(graduation_year)s, %(position)s,
    %(state)s, %(program_year)s, %(age_group)s, %(gender)s,
    NULL, %(club_name_raw)s, %(source_url)s,
    now(), now()
)
ON CONFLICT (player_name, state, program_year, age_group, gender)
DO UPDATE SET
    last_seen_at  = EXCLUDED.last_seen_at,
    club_name_raw = COALESCE(odp_roster_entries.club_name_raw, EXCLUDED.club_name_raw)
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
    """Validate required fields and drop anything the writer doesn't know."""
    required = ("player_name", "state", "program_year", "age_group", "gender", "source_url")
    for k in required:
        if not row.get(k):
            raise ValueError(f"odp row missing required field: {k}")
    return {
        "player_name": row["player_name"],
        "graduation_year": row.get("graduation_year"),
        "position": row.get("position"),
        "state": row["state"],
        "program_year": row["program_year"],
        "age_group": row["age_group"],
        "gender": row["gender"],
        "club_name_raw": row.get("club_name_raw"),
        "source_url": row["source_url"],
    }


def insert_odp_entries(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of ODP roster entries.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``. Mirrors
    the shape the other writers return so the runner's logging can
    stay uniform.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[odp-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[odp-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_ODP_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[odp-writer] upsert failed for %s (%s/%s/%s): %s",
                        row.get("player_name"),
                        row.get("state"),
                        row.get("age_group"),
                        row.get("gender"),
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
        "event": "odp-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
