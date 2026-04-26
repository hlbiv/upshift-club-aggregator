"""
id_selection_writer.py — Idempotent upsert of ``player_id_selections``.

See lib/db/src/schema/player-id-selections.ts for the table shape.

Natural key (named constraint):

    player_id_selections_player_year_birth_gender_tier_uq
      UNIQUE (player_name, selection_year, birth_year, gender, pool_tier)

Selections are append-only by year — there is no "diff materialization"
pass like roster_snapshot_writer.py. Re-running the same scrape is a
no-op for previously-captured rows: the WHERE predicate on the DO UPDATE
guards against pointless writes when nothing mutable changed.

``club_id`` is intentionally left NULL on insert. The canonical-club
linker (``scraper/canonical_club_linker.py``) resolves it post-hoc.

psycopg2 is imported lazily so this module stays importable without
DATABASE_URL (tests exercising the pure shape skip DB altogether).
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

log = logging.getLogger("id_selection_writer")


_INSERT_SELECTION_SQL = """
INSERT INTO player_id_selections (
    player_name, selection_year, birth_year, gender, pool_tier,
    region, club_name_raw, club_id, state, position,
    source_url, source, announced_at, scraped_at
)
VALUES (
    %(player_name)s, %(selection_year)s, %(birth_year)s, %(gender)s, %(pool_tier)s,
    %(region)s, %(club_name_raw)s, NULL, %(state)s, %(position)s,
    %(source_url)s, %(source)s, %(announced_at)s, now()
)
ON CONFLICT ON CONSTRAINT player_id_selections_player_year_birth_gender_tier_uq
DO UPDATE SET
    region        = COALESCE(EXCLUDED.region, player_id_selections.region),
    club_name_raw = COALESCE(EXCLUDED.club_name_raw, player_id_selections.club_name_raw),
    state         = COALESCE(EXCLUDED.state, player_id_selections.state),
    position      = COALESCE(EXCLUDED.position, player_id_selections.position),
    source_url    = COALESCE(EXCLUDED.source_url, player_id_selections.source_url),
    announced_at  = COALESCE(EXCLUDED.announced_at, player_id_selections.announced_at),
    scraped_at    = now()
WHERE player_id_selections.region        IS DISTINCT FROM EXCLUDED.region
   OR player_id_selections.club_name_raw IS DISTINCT FROM EXCLUDED.club_name_raw
   OR player_id_selections.state         IS DISTINCT FROM EXCLUDED.state
   OR player_id_selections.position      IS DISTINCT FROM EXCLUDED.position
   OR player_id_selections.source_url    IS DISTINCT FROM EXCLUDED.source_url
   OR player_id_selections.announced_at  IS DISTINCT FROM EXCLUDED.announced_at
RETURNING (xmax = 0) AS inserted
"""


_REQUIRED_FIELDS = ("player_name", "selection_year", "gender", "pool_tier", "source")


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every expected key exists, with None for optional fields."""
    for key in _REQUIRED_FIELDS:
        if not row.get(key):
            raise ValueError(f"player_id_selection row missing {key}")
    return {
        "player_name": row["player_name"],
        "selection_year": int(row["selection_year"]),
        "birth_year": int(row["birth_year"]) if row.get("birth_year") else None,
        "gender": row["gender"],
        "pool_tier": row["pool_tier"],
        "region": row.get("region"),
        "club_name_raw": row.get("club_name_raw"),
        "state": row.get("state"),
        "position": row.get("position"),
        "source_url": row.get("source_url"),
        "source": row["source"],
        "announced_at": row.get("announced_at"),
    }


def insert_player_id_selections(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of player_id_selections rows.

    Mirrors :func:`ingest.roster_snapshot_writer.insert_roster_snapshots`
    in shape (psycopg2, named ON CONFLICT, dry-run support, returns
    inserted/updated/skipped counts) but skips diff materialization —
    selections are append-only by year.

    Parameters
    ----------
    rows
        Iterable of row dicts. Required keys: ``player_name``,
        ``selection_year``, ``gender``, ``pool_tier``, ``source``.
        Optional: ``birth_year``, ``region``, ``club_name_raw``,
        ``state``, ``position``, ``source_url``, ``announced_at``.
    conn
        Optional existing psycopg2 connection. If omitted, a new one is
        opened, used, and closed (autocommit off, explicit commit).
    dry_run
        When True, no DB I/O occurs; returns zero counts.

    Returns
    -------
    ``{"inserted": N, "updated": N, "skipped": N}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info(
            "[id-selection-writer] dry-run: would upsert %d rows", len(rows)
        )
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[id-selection-writer] skipping bad row: %s", exc)
            counts["skipped"] += 1
    if not normalized:
        log.info(json.dumps({
            "event": "id-selection-writer",
            "inserted": 0, "updated": 0, "skipped": counts["skipped"],
        }))
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            for row in normalized:
                try:
                    cur.execute(_INSERT_SELECTION_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[id-selection-writer] upsert failed for %s / %s / %s: %s",
                        row.get("player_name"),
                        row.get("selection_year"),
                        row.get("pool_tier"),
                        exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()  # noqa: writer-rollback
                    continue
                if result is None:
                    # WHERE predicate short-circuited — no change.
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
        "event": "id-selection-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
