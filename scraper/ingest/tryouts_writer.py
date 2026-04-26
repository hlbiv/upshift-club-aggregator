"""
tryouts_writer.py — Idempotent upsert of ``tryouts`` rows.

See lib/db/src/schema/rosters-and-tryouts.ts for the table shape. The
natural-key unique index is:

    tryouts_name_date_bracket_uq
      UNIQUE (
          club_name_raw,
          COALESCE(tryout_date, 'epoch'::timestamp),
          COALESCE(age_group, ''),
          COALESCE(gender, ''),
          COALESCE(season, '')
      )

The index is created by hand-rolled SQL (migration 0001); it's a bare
``CREATE UNIQUE INDEX`` not a table constraint, but Postgres accepts
``ON CONFLICT ON CONSTRAINT <index_name>`` against unique indexes — see
the matches_writer.py doc block for the rationale.

Mutable columns updated on conflict: ``location_name``, ``source_url``,
``notes``. ``detected_at`` is historical first-seen; ``scraped_at``
is refreshed on every write. The DO UPDATE guard only fires when a
mutable field actually changed so re-running the same scrape is a no-op.

``club_id`` is intentionally left NULL. The canonical-club linker
resolves it after the scraper writes.
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

log = logging.getLogger("tryouts_writer")


_INSERT_TRYOUT_SQL = """
INSERT INTO tryouts (
    club_id, club_name_raw,
    age_group, gender, division, season,
    tryout_date, registration_deadline,
    location_name, location_address, location_city, location_state,
    cost, url, notes,
    source, status, detected_at, scraped_at
)
VALUES (
    NULL, %(club_name_raw)s,
    %(age_group)s, %(gender)s, %(division)s, %(season)s,
    %(tryout_date)s, %(registration_deadline)s,
    %(location_name)s, %(location_address)s, %(location_city)s, %(location_state)s,
    %(cost)s, %(url)s, %(notes)s,
    %(source)s, %(status)s, now(), now()
)
ON CONFLICT ON CONSTRAINT tryouts_name_date_bracket_uq
DO UPDATE SET
    location_name = COALESCE(EXCLUDED.location_name, tryouts.location_name),
    url           = COALESCE(EXCLUDED.url, tryouts.url),
    notes         = COALESCE(EXCLUDED.notes, tryouts.notes),
    season        = COALESCE(EXCLUDED.season, tryouts.season),
    scraped_at    = now()
WHERE tryouts.location_name IS DISTINCT FROM EXCLUDED.location_name
   OR tryouts.url           IS DISTINCT FROM EXCLUDED.url
   OR tryouts.notes         IS DISTINCT FROM EXCLUDED.notes
   OR tryouts.season        IS DISTINCT FROM EXCLUDED.season
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
    """Accept either a flat ``location`` string or the schema-split
    ``location_name``/``location_address``/``location_city``/``location_state``.

    The tryouts schema has four separate location columns; WordPress
    site scraping is best-effort and typically only produces a single
    free-text field. We map ``location`` → ``location_name``.
    """
    if not row.get("club_name_raw"):
        raise ValueError("tryout row missing club_name_raw")
    loc = row.get("location")
    if loc and not row.get("location_name"):
        row = {**row, "location_name": loc}
    return {
        "club_name_raw": row["club_name_raw"],
        "age_group": row.get("age_group"),
        "gender": row.get("gender"),
        "division": row.get("division"),
        "season": row.get("season"),
        "tryout_date": row.get("tryout_date"),
        "registration_deadline": row.get("registration_deadline"),
        "location_name": row.get("location_name"),
        "location_address": row.get("location_address"),
        "location_city": row.get("location_city"),
        "location_state": row.get("location_state"),
        "cost": row.get("cost"),
        "url": row.get("url") or row.get("source_url"),
        "notes": row.get("notes"),
        "source": row.get("source") or "site_monitor",
        "status": row.get("status") or "upcoming",
    }


def insert_tryouts(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of tryout rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``. The
    ``updated`` count only reflects real mutable-field drift — the
    DO UPDATE WHERE predicate short-circuits when nothing changed.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[tryouts-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[tryouts-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_TRYOUT_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[tryouts-writer] upsert failed for %s (%s): %s",
                        row.get("club_name_raw"), row.get("tryout_date"), exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()
                    continue
                if result is None:
                    # WHERE predicate short-circuited — nothing to count.
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
        "event": "tryouts-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
