"""
scrape_health reconciler.

`scrape_run_logs` is written by each scraper invocation. `scrape_health`
is the polymorphic current-state rollup — one row per (entity_type,
entity_id). Nothing writes to `scrape_health` at scrape time; this
reconciler fills it from each entity table's freshness timestamp after
the scrape run completes.

Design:
  - For each configured entity_type, pick the best freshness column from
    a small allowlist and UPSERT a `scrape_health` row per row in the
    source table that was refreshed within ``window_hours``.
  - After the per-type upserts, DEMOTE any `status='ok'` row whose
    ``last_scraped_at`` is older than the window to `status='stale'`.
    This lets freshness degrade automatically without any SLA config
    (that lives in D8.5).

Tables with no freshness column (``leagues_master``) are skipped with a
log warning — reference data that doesn't go stale.

Tables that don't exist yet (``colleges`` may not be deployed) are
skipped with a log.info so the reconciler stays forward-compatible with
partial deployments.

Idempotent — safe to call multiple times per run.
"""

from __future__ import annotations

import logging
from typing import Optional

from config.freshness_sla import FRESHNESS_SLA_HOURS, DEFAULT_SLA_HOURS

log = logging.getLogger("reconcilers.scrape_health")


# ---------------------------------------------------------------------------
# Entity configuration
# ---------------------------------------------------------------------------
#
# `freshness_cols` is probed in order; the first column that exists on the
# table is used as both `last_scraped_at` and `last_success_at` on the
# `scrape_health` row. If none exist, the entity_type is skipped with a
# warning (reference data — e.g. `leagues_master`).

_ENTITY_CONFIG = [
    # (entity_type, source_table, freshness_cols_in_preference_order)
    ("club",    "canonical_clubs",   ("last_scraped_at",)),
    ("league",  "leagues_master",    ()),  # no freshness column — skipped
    ("college", "colleges",          ("last_scraped_at",)),
    ("coach",   "coach_discoveries", ("last_seen_at", "scraped_at")),
    ("event",   "events",            ("last_scraped_at",)),
    ("match",   "matches",           ("scraped_at",)),
    # Schema table is `tryouts` (not `tryout_listings`). The scrape_health
    # entity_type enum value is `tryout`.
    ("tryout",  "tryouts",           ("scraped_at",)),
]


_UPSERT_SQL = """
    INSERT INTO scrape_health (
        entity_type, entity_id,
        last_scraped_at, last_success_at,
        status, consecutive_failures, last_error
    )
    SELECT %(entity_type)s, t.id, t.{freshness_col}, t.{freshness_col},
           'ok', 0, NULL
    FROM {table} t
    WHERE t.{freshness_col} IS NOT NULL
      AND t.{freshness_col} >= now() - make_interval(hours => %(window_hours)s)
    ON CONFLICT ON CONSTRAINT scrape_health_entity_uq DO UPDATE
    SET last_scraped_at      = EXCLUDED.last_scraped_at,
        last_success_at      = EXCLUDED.last_success_at,
        status               = 'ok',
        consecutive_failures = 0,
        last_error           = NULL
"""


_DEMOTE_SQL = """
    UPDATE scrape_health
    SET status = 'stale'
    WHERE status = 'ok'
      AND last_scraped_at < now() - make_interval(hours => %(window_hours)s)
"""


def _table_exists(cur, table: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return cur.fetchone() is not None


def _first_present_column(cur, table: str, candidates: tuple) -> Optional[str]:
    """Return the first column in ``candidates`` that exists on ``table``."""
    if not candidates:
        return None
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    present = {row[0] for row in cur.fetchall()}
    for c in candidates:
        if c in present:
            return c
    return None


def reconcile_scrape_health(conn, window_hours: int = 168) -> dict:
    """Reconcile `scrape_health` from entity tables' freshness columns.

    Args:
        conn: psycopg2 connection. Caller owns the lifecycle.
        window_hours: Rows in each source table whose freshness timestamp
            is within this window are marked `status='ok'`. The same
            window is used to DEMOTE existing `ok` rows to `stale`.
            Default: 168h (7 days).

    Returns:
        A dict keyed by entity_type with ``{"refreshed": N, "demoted_stale": M}``.
        Skipped entity types map to ``{"skipped": "<reason>"}``.
    """
    summary: dict = {}

    # Whether the connection autocommits or not, each statement below is
    # independently idempotent — no multi-statement invariant to guard.
    with conn.cursor() as cur:
        for entity_type, table, freshness_cols in _ENTITY_CONFIG:
            if not _table_exists(cur, table):
                log.info(
                    "scrape_health: table %s does not exist — skipping entity_type=%s",
                    table, entity_type,
                )
                summary[entity_type] = {"skipped": f"table {table} missing"}
                continue

            freshness_col = _first_present_column(cur, table, freshness_cols)
            if freshness_col is None:
                log.warning(
                    "scrape_health: table %s has no freshness column "
                    "(tried %s) — skipping entity_type=%s",
                    table, list(freshness_cols) or ["(none configured)"],
                    entity_type,
                )
                summary[entity_type] = {"skipped": "no freshness column"}
                continue

            sql = _UPSERT_SQL.format(table=table, freshness_col=freshness_col)
            cur.execute(
                sql,
                {"entity_type": entity_type, "window_hours": window_hours},
            )
            refreshed = cur.rowcount or 0

            # Demote stale rows using per-entity-type SLA thresholds
            # from freshness_sla.py. Falls back to ``window_hours`` for
            # entity types not in the config.
            sla_hours = FRESHNESS_SLA_HOURS.get(entity_type, window_hours)
            cur.execute(
                _DEMOTE_SQL + " AND entity_type = %(entity_type)s",
                {"entity_type": entity_type, "window_hours": sla_hours},
            )
            demoted = cur.rowcount or 0

            summary[entity_type] = {
                "refreshed": refreshed,
                "demoted_stale": demoted,
            }
            log.info(
                "scrape_health: entity_type=%s table=%s refreshed=%d demoted_stale=%d",
                entity_type, table, refreshed, demoted,
            )

    # Commit explicitly for non-autocommit connections. Safe on autocommit
    # connections (psycopg2 no-ops commit outside an explicit transaction).
    try:
        conn.commit()
    except Exception:
        pass

    return summary
