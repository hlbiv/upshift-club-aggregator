"""
hs_rosters_writer.py — Idempotent upsert of ``hs_rosters`` rows.

See lib/db/src/schema/hs.ts. Natural key:

    hs_rosters_natural_key_uq
      UNIQUE (school_name_raw, school_state, team_level, season, gender, player_name)

Conflict behaviour (per PR spec):

    ON CONFLICT (cols) DO UPDATE SET
      last_seen_at   = EXCLUDED.last_seen_at,
      jersey_number  = COALESCE(EXCLUDED.jersey_number, hs_rosters.jersey_number),
      position       = COALESCE(EXCLUDED.position, hs_rosters.position)

``first_seen_at`` is preserved (historical — set on insert, never
overwritten). ``jersey_number`` and ``position`` are COALESCE'd so a
follow-up scrape that happens to drop one of those fields never
blanks out existing data.

``school_name_raw`` / ``school_state`` are written verbatim. A canonical
HS-school linker is a follow-up PR.
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

log = logging.getLogger("hs_rosters_writer")


_INSERT_HS_ROSTER_SQL = """
INSERT INTO hs_rosters (
    school_name_raw, school_state, school_city,
    team_level, season, gender,
    player_name, jersey_number, graduation_year,
    position, height, source_url,
    first_seen_at, last_seen_at
)
VALUES (
    %(school_name_raw)s, %(school_state)s, %(school_city)s,
    %(team_level)s, %(season)s, %(gender)s,
    %(player_name)s, %(jersey_number)s, %(graduation_year)s,
    %(position)s, %(height)s, %(source_url)s,
    now(), now()
)
ON CONFLICT ON CONSTRAINT hs_rosters_natural_key_uq
DO UPDATE SET
    last_seen_at  = EXCLUDED.last_seen_at,
    jersey_number = COALESCE(EXCLUDED.jersey_number, hs_rosters.jersey_number),
    position      = COALESCE(EXCLUDED.position, hs_rosters.position)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_REQUIRED = ("school_name_raw", "school_state", "gender", "player_name", "source_url")


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    for k in _REQUIRED:
        if not row.get(k):
            raise ValueError(f"hs_rosters row missing required field: {k}")
    state = str(row["school_state"]).strip().upper()
    if len(state) != 2:
        raise ValueError(f"school_state must be 2-letter code, got {state!r}")
    return {
        "school_name_raw": row["school_name_raw"],
        "school_state": state,
        "school_city": row.get("school_city"),
        "team_level": row.get("team_level"),
        "season": row.get("season"),
        "gender": row["gender"],
        "player_name": row["player_name"],
        "jersey_number": row.get("jersey_number"),
        "graduation_year": row.get("graduation_year"),
        "position": row.get("position"),
        "height": row.get("height"),
        "source_url": row["source_url"],
    }


def insert_hs_rosters(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of hs_rosters rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``. Per-row
    errors are swallowed (rolled back) with a warning so one bad row
    doesn't kill the whole batch.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[hs-rosters-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[hs-rosters-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_HS_ROSTER_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[hs-rosters-writer] upsert failed for %s / %s: %s",
                        row.get("school_name_raw"), row.get("player_name"), exc,
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
        "event": "hs-rosters-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
