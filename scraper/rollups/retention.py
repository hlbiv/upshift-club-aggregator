"""
retention rollup — prune historical rows from append-only telemetry tables.

Two tables are pruned today:

1. ``scrape_run_logs`` — keep the last 90 days. The table is per-run
   telemetry; older rows have no consumer (the ``scrape_health`` rollup
   only reads recent runs to decide freshness/streak status). Keyed off
   ``started_at`` (see ``lib/db/src/schema/scrape-health.ts``).

2. ``coach_scrape_snapshots`` — keep the last 5 snapshots per
   ``club_id``. Snapshots are dense (one per scrape per club) and the
   only downstream consumer is the diff/trend reconstruction job, which
   only needs a short tail.

   NOTE: the original plan called for partitioning by
   ``(club_id, coach_id)``. The schema at
   ``lib/db/src/schema/coaches.ts`` only has ``club_id`` + ``scraped_at``
   + ``raw_staff`` JSONB — no ``coach_id`` column. We partition by
   ``(club_id)`` only. If/when ``coach_id`` is added, extend the
   PARTITION BY clause and bump the keep-count if appropriate.

3. ``coach_movement_events`` — **intentionally never touched**. This
   table is an append-only audit feed used for backfilled trend
   analysis (coach-movement reconstruction across seasons / clubs).
   Pruning it would corrupt historical movement reconstruction. If you
   ever need to truncate it, do so explicitly out-of-band — never from
   this rollup.

Idempotency
-----------
Each table is pruned in its own transaction. Re-running the rollup is
safe: the second run will delete zero rows (or only newly-eligible
rows). DELETE-then-commit is the standard pattern; no upsert required
since these tables are append-only.

Dry-run
-------
``--dry-run`` swaps each DELETE for a ``SELECT count(*)`` over the same
filter and logs ``[retention][dry-run] would delete N rows from
<table>``. No DB writes occur.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

# scrape_run_logs — 90-day rolling window, keyed off started_at.
_DELETE_SCRAPE_RUN_LOGS_SQL = """
DELETE FROM scrape_run_logs
WHERE started_at < NOW() - INTERVAL '90 days'
"""

_COUNT_SCRAPE_RUN_LOGS_SQL = """
SELECT COUNT(*)::int FROM scrape_run_logs
WHERE started_at < NOW() - INTERVAL '90 days'
"""

# coach_scrape_snapshots — keep last 5 per (club_id) ordered by
# scraped_at DESC, id DESC (id breaks ties for snapshots taken in the
# same second).
#
# DECISION (PR #51 follow-up): partition by `(club_id)` only — NOT
# `(club_id, coach_id)`. The schema in lib/db/src/schema/coaches.ts
# does not have a `coach_id` column on `coach_scrape_snapshots`; the
# table stores `raw_staff` JSONB which carries per-coach detail
# embedded inside the snapshot. Because the only consumer is
# diff/trend reconstruction over the snapshot-as-a-whole, partitioning
# by `(club_id)` is the correct grain. If a future schema migration
# adds a top-level `coach_id`, extend this PARTITION BY and bump the
# keep-count as appropriate.
_DELETE_COACH_SCRAPE_SNAPSHOTS_SQL = """
DELETE FROM coach_scrape_snapshots
WHERE id IN (
    SELECT id FROM (
        SELECT id, ROW_NUMBER() OVER (
            PARTITION BY club_id ORDER BY scraped_at DESC, id DESC
        ) AS rn
        FROM coach_scrape_snapshots
    ) ranked
    WHERE rn > 5
)
"""

_COUNT_COACH_SCRAPE_SNAPSHOTS_SQL = """
SELECT COUNT(*)::int FROM (
    SELECT id, ROW_NUMBER() OVER (
        PARTITION BY club_id ORDER BY scraped_at DESC, id DESC
    ) AS rn
    FROM coach_scrape_snapshots
) ranked
WHERE rn > 5
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _prune_table(
    *,
    conn: Any,
    own_conn: bool,
    delete_sql: str,
    count_sql: str,
    table_label: str,
    dry_run: bool,
) -> int:
    """Run one prune step in its own transaction.

    Returns the number of rows deleted (or rows that would be deleted in
    dry-run mode). Each step commits/rolls back independently so a
    failure pruning one table does not block the other.
    """
    try:
        with conn.cursor() as cur:
            if dry_run:
                cur.execute(count_sql)
                count = cur.fetchone()[0]
                log.info(
                    "[retention][dry-run] would delete %d rows from %s",
                    count, table_label,
                )
                return int(count)
            cur.execute(delete_sql)
            deleted = cur.rowcount or 0
        if own_conn:
            conn.commit()
        log.info("[retention] deleted %d rows from %s", deleted, table_label)
        return int(deleted)
    except Exception:
        if own_conn:
            conn.rollback()
        raise


def prune_retention(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Prune retention-bounded telemetry tables.

    Touches ``scrape_run_logs`` (90-day window) and
    ``coach_scrape_snapshots`` (last 5 per ``club_id``). Does NOT touch
    ``coach_movement_events`` — see module docstring.

    Returns
    -------
    dict
        ``{"scrape_run_logs_deleted": int, "coach_scrape_snapshots_deleted": int}``.
        In dry-run mode the values are the counts that *would* have been
        deleted.
    """
    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        scrape_run_logs_deleted = _prune_table(
            conn=conn,
            own_conn=own_conn,
            delete_sql=_DELETE_SCRAPE_RUN_LOGS_SQL,
            count_sql=_COUNT_SCRAPE_RUN_LOGS_SQL,
            table_label="scrape_run_logs",
            dry_run=dry_run,
        )
        coach_scrape_snapshots_deleted = _prune_table(
            conn=conn,
            own_conn=own_conn,
            delete_sql=_DELETE_COACH_SCRAPE_SNAPSHOTS_SQL,
            count_sql=_COUNT_COACH_SCRAPE_SNAPSHOTS_SQL,
            table_label="coach_scrape_snapshots",
            dry_run=dry_run,
        )
    finally:
        if own_conn and conn is not None:
            conn.close()

    log.info(
        "[retention] summary: scrape_run_logs_deleted=%d "
        "coach_scrape_snapshots_deleted=%d (coach_movement_events untouched)",
        scrape_run_logs_deleted,
        coach_scrape_snapshots_deleted,
    )
    return {
        "scrape_run_logs_deleted": scrape_run_logs_deleted,
        "coach_scrape_snapshots_deleted": coach_scrape_snapshots_deleted,
    }
