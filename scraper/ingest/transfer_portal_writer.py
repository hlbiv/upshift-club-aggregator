"""
transfer_portal_writer.py — Idempotent upsert of
``transfer_portal_entries`` rows.

See lib/db/src/schema/transfer_portal.ts for the table shape. Natural
key is a unique INDEX::

    transfer_portal_entries_natural_key_uq
      UNIQUE (player_name, from_college_name_raw, season_window)

On conflict:
  - ``last_seen_at`` refreshes to ``now()``
  - ``position`` fills in only if the stored value is NULL
  - ``to_college_name_raw`` fills in only if the stored value is NULL
    (first non-NULL wins; in practice this never changes on the TDS
    tracker since destination is part of the row contract, but the
    COALESCE guards against a mid-window page edit overwriting a
    previously-populated value)
  - ``from_college_id`` / ``to_college_id`` are NOT written by the
    scraper — a linker resolves them in a later pass.
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

log = logging.getLogger("transfer_portal_writer")


_INSERT_TRANSFER_PORTAL_SQL = """
INSERT INTO transfer_portal_entries (
    player_name,
    position,
    from_college_id,
    from_college_name_raw,
    to_college_id,
    to_college_name_raw,
    season_window,
    gender,
    division,
    source_url,
    first_seen_at,
    last_seen_at
)
VALUES (
    %(player_name)s,
    %(position)s,
    NULL,
    %(from_college_name_raw)s,
    NULL,
    %(to_college_name_raw)s,
    %(season_window)s,
    %(gender)s,
    %(division)s,
    %(source_url)s,
    now(),
    now()
)
ON CONFLICT (player_name, from_college_name_raw, season_window)
DO UPDATE SET
    last_seen_at         = now(),
    position             = COALESCE(transfer_portal_entries.position,
                                    EXCLUDED.position),
    to_college_name_raw  = COALESCE(transfer_portal_entries.to_college_name_raw,
                                    EXCLUDED.to_college_name_raw)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_REQUIRED_FIELDS = (
    "player_name",
    "from_college_name_raw",
    "to_college_name_raw",
    "season_window",
    "gender",
    "division",
    "source_url",
)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in _REQUIRED_FIELDS:
        value = (row.get(key) or "")
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError(f"transfer-portal row missing {key}")
        out[key] = value
    out["position"] = (row.get("position") or None)
    return out


def insert_transfer_portal_entries(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of transfer-portal rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``.
    ``updated`` counts conflict-hits; the SET list always refreshes
    ``last_seen_at`` so every conflict is a real update.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info(
            "[transfer-portal-writer] dry-run: would upsert %d rows", len(rows),
        )
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[transfer-portal-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_TRANSFER_PORTAL_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[transfer-portal-writer] upsert failed for %s / %s: %s",
                        row.get("player_name"),
                        row.get("from_college_name_raw"),
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
        "event": "transfer-portal-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
