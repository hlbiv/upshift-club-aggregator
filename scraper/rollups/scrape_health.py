"""
scrape_health rollup — recompute ``scrape_health`` from ``scrape_run_logs``.

``scrape_health`` is the polymorphic per-entity current-state rollup
(one row per ``(entity_type, entity_id)``). It is **not** written by
scrapers directly. Instead, this rollup reads the append-only
``scrape_run_logs`` and computes:

  * ``last_scraped_at``       — MAX(started_at) per entity
  * ``last_success_at``       — MAX(started_at) FILTER (status='ok')
  * ``consecutive_failures``  — leading streak of ``failed`` rows from
    most-recent backwards (window function)
  * ``last_error``            — error_message of the most recent run
  * ``status``                — ``ok`` | ``stale`` | ``failed`` | ``never``

Status thresholds
-----------------
``STALE_THRESHOLDS_DAYS`` defines the "freshness" SLO per
``entity_type``. A run is ``ok`` if the last successful scrape
happened within the threshold window; otherwise ``stale``.

A run is ``failed`` if there are 3 or more consecutive failures
(``FAILURE_STREAK_THRESHOLD``), or if there has never been a
successful run for that entity but at least one failed attempt
exists.

scraper_key → (entity_type, entity_id) mapping
----------------------------------------------
Per-entity scraper_keys follow ``<source>:<entity_id>`` where
``entity_id`` is an integer. Examples observed in the codebase:

  * ``gotsport-events:<event_id>``       → entity_type='event'
  * ``gotsport-rosters:<event_id>``      → entity_type='club'  (roster freshness mapped onto club; enum has no 'roster')
  * ``gotsport-matches:<event_id>``      → entity_type='event'
  * ``sincsports-events:<tid>``          → entity_type='event'
  * ``sincsports-rosters:<tid>``         → entity_type='club'
  * ``tgs-events:<event_id>``            → entity_type='event'

Pure-source/rollup keys (no numeric suffix) are EXCLUDED from this
rollup via the ``WHERE scraper_key ~ ':[0-9]+$'`` filter. Examples
that are intentionally skipped:

  * ``link-canonical-clubs``
  * ``rollup:club-results``
  * ``rollup:scrape-health``
  * ``tryouts-wordpress`` (single-shot probe, no per-entity id)
  * ``youth-club-coaches``
  * ``naia-rosters`` / ``njcaa-rosters`` / ``ncaa-d{1,2,3}-rosters``
  * ``ncaa-d{1,2,3}-coaches``
  * ``club-enrichment``
  * ``usclub-sanctioned-tournaments``
  * Per-league keys (``ecnl-boys``, etc.) — these scrape many clubs
    each, so they don't map to a single entity_id.

Idempotency
-----------
Each run is a full UPSERT — no DELETE. We rely on
``scrape_health_entity_uq (entity_type, entity_id)`` to drive
``ON CONFLICT DO UPDATE``. Stale rows naturally age into the ``stale``
status next run as ``last_success_at`` falls behind ``NOW() -
threshold``. Deleting first would lose history for entities whose
scraper_key disappears (e.g. an event_id rolled off the schedule).

Roster mapping
--------------
The DB ``entity_type`` enum is
``('club','league','college','coach','event','match','tryout')`` — no
``'roster'``. Roster scrapers (``gotsport-rosters:<event_id>``,
``sincsports-rosters:<tid>``) are mapped onto ``entity_type='club'``
even though the suffix is an event/tournament id, because the
roster-freshness signal logically belongs to the club whose roster is
being scraped. This is an imperfect mapping — the suffix isn't
actually a club_id — but matches the plan and keeps the schema
unchanged. A future iteration can either (a) add ``'roster'`` to the
enum or (b) join through a roster→club map.
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


# --- Config -----------------------------------------------------------------

# Per-entity-type freshness SLO. A successful run within this many days
# keeps the entity in ``ok`` status; otherwise ``stale``.
STALE_THRESHOLDS_DAYS: Dict[str, int] = {
    "club": 90,
    "coach": 30,
    "tryout": 7,
    "event": 14,
}

# 3+ consecutive failed runs flips the status to ``failed`` regardless
# of when the last success was.
FAILURE_STREAK_THRESHOLD = 3


# --- SQL --------------------------------------------------------------------

# Recompute SQL: window function over scrape_run_logs to derive
# consecutive_failures + last_error per scraper_key, then map to
# (entity_type, entity_id) via prefix matching, then classify status.
#
# The CASE for entity_type is centralised here. Order matters — the
# first matching prefix wins. Roster scrapers map to 'club' (see module
# docstring); tryouts/events/coaches/matches map to themselves.
_ROLLUP_SQL = """
WITH ranked AS (
    SELECT
        s.scraper_key,
        s.started_at,
        s.status,
        s.error_message,
        ROW_NUMBER() OVER (
            PARTITION BY s.scraper_key
            ORDER BY s.started_at DESC, s.id DESC
        ) AS rn
    FROM scrape_run_logs s
    WHERE s.scraper_key ~ ':[0-9]+$'
),
streaks AS (
    -- Leading streak of consecutive 'failed' rows from most recent
    -- backwards. We sum failed=1 rows in the leading prefix until the
    -- first non-failed row, by counting failed rows whose rn is less
    -- than the rn of the first non-failed row per scraper_key.
    SELECT
        scraper_key,
        COALESCE((
            SELECT MIN(rn)
            FROM ranked r2
            WHERE r2.scraper_key = ranked.scraper_key
              AND r2.status <> 'failed'
        ), 1 + (SELECT MAX(rn) FROM ranked r3 WHERE r3.scraper_key = ranked.scraper_key))
            AS first_non_failed_rn
    FROM ranked
    GROUP BY scraper_key
),
per_key AS (
    SELECT
        s.scraper_key,
        MAX(s.started_at) AS last_scraped_at,
        MAX(s.started_at) FILTER (WHERE s.status = 'ok') AS last_success_at,
        (SELECT first_non_failed_rn - 1 FROM streaks WHERE streaks.scraper_key = s.scraper_key)
            AS consecutive_failures,
        (SELECT error_message FROM ranked r4
            WHERE r4.scraper_key = s.scraper_key AND r4.rn = 1)
            AS last_error
    FROM scrape_run_logs s
    WHERE s.scraper_key ~ ':[0-9]+$'
    GROUP BY s.scraper_key
),
per_entity AS (
    SELECT
        -- DECISION (PR #53 follow-up): roster scrapers map onto
        -- entity_type='club'. The Postgres CHECK constraint
        -- `scrape_health_entity_type_enum` (see
        -- lib/db/src/schema/scrape-health.ts:92) does not include a
        -- 'roster' value — its enum is
        -- ('club','league','college','coach','event','match','tryout').
        -- Roster freshness logically belongs to the club whose roster
        -- is being scraped, so 'club' is the closest fit without a
        -- schema migration. The mapping is imperfect because the
        -- scraper_key suffix is an event/tournament id, not a
        -- canonical_clubs.id; it just preserves freshness signal in
        -- the polymorphic table. A future iteration can either add
        -- 'roster' to the enum or join through a roster→club map.
        CASE
            WHEN scraper_key LIKE 'gotsport-rosters:%'   THEN 'club'
            WHEN scraper_key LIKE 'sincsports-rosters:%' THEN 'club'
            WHEN scraper_key LIKE 'gotsport-events:%'    THEN 'event'
            WHEN scraper_key LIKE 'sincsports-events:%'  THEN 'event'
            WHEN scraper_key LIKE 'tgs-events:%'         THEN 'event'
            WHEN scraper_key LIKE 'gotsport-matches:%'   THEN 'event'
            WHEN scraper_key LIKE 'youth-coaches:%'      THEN 'coach'
            WHEN scraper_key LIKE 'tryouts-%:%'          THEN 'tryout'
        END AS entity_type,
        CASE
            WHEN scraper_key ~ ':[0-9]+$'
                THEN NULLIF(split_part(scraper_key, ':', 2), '')::int
        END AS entity_id,
        last_scraped_at,
        last_success_at,
        COALESCE(consecutive_failures, 0)::int AS consecutive_failures,
        last_error
    FROM per_key
),
classified AS (
    SELECT
        entity_type,
        entity_id,
        last_scraped_at,
        last_success_at,
        consecutive_failures,
        last_error,
        CASE
            WHEN last_scraped_at IS NULL THEN 'never'
            WHEN consecutive_failures >= %(streak)s THEN 'failed'
            WHEN last_success_at IS NULL THEN 'failed'
            WHEN last_success_at >= NOW() - (
                CASE entity_type
                    WHEN 'club'   THEN make_interval(days => %(club_days)s)
                    WHEN 'coach'  THEN make_interval(days => %(coach_days)s)
                    WHEN 'tryout' THEN make_interval(days => %(tryout_days)s)
                    WHEN 'event'  THEN make_interval(days => %(event_days)s)
                END
            ) THEN 'ok'
            ELSE 'stale'
        END AS status
    FROM per_entity
    WHERE entity_type IS NOT NULL AND entity_id IS NOT NULL
)
INSERT INTO scrape_health (
    entity_type, entity_id,
    last_scraped_at, last_success_at,
    status, consecutive_failures, last_error
)
SELECT
    entity_type, entity_id,
    last_scraped_at, last_success_at,
    status, consecutive_failures, last_error
FROM classified
ON CONFLICT (entity_type, entity_id) DO UPDATE
SET
    last_scraped_at      = EXCLUDED.last_scraped_at,
    last_success_at      = EXCLUDED.last_success_at,
    status               = EXCLUDED.status,
    consecutive_failures = EXCLUDED.consecutive_failures,
    last_error           = EXCLUDED.last_error
"""


_BY_STATUS_SQL = """
SELECT status, COUNT(*)::int
FROM scrape_health
GROUP BY status
"""


_TOTAL_SQL = "SELECT COUNT(*)::int FROM scrape_health"


# --- Connection helper ------------------------------------------------------

def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


# --- Public API -------------------------------------------------------------

def recompute_scrape_health(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Recompute ``scrape_health`` from ``scrape_run_logs`` (full upsert).

    Returns
    -------
    dict
        ``{"rows_written": int, "by_status": {"ok": N, "stale": N, "failed": N, "never": N}}``.
    """
    if dry_run:
        log.info("[scrape-health] dry-run: would recompute scrape_health from scrape_run_logs")
        return {
            "rows_written": 0,
            "by_status": {"ok": 0, "stale": 0, "failed": 0, "never": 0},
        }

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    params = {
        "streak": FAILURE_STREAK_THRESHOLD,
        "club_days": STALE_THRESHOLDS_DAYS["club"],
        "coach_days": STALE_THRESHOLDS_DAYS["coach"],
        "tryout_days": STALE_THRESHOLDS_DAYS["tryout"],
        "event_days": STALE_THRESHOLDS_DAYS["event"],
    }

    try:
        with conn.cursor() as cur:
            cur.execute(_ROLLUP_SQL, params)
            cur.execute(_TOTAL_SQL)
            rows_written = cur.fetchone()[0]
            cur.execute(_BY_STATUS_SQL)
            by_status: Dict[str, int] = {"ok": 0, "stale": 0, "failed": 0, "never": 0}
            for status, count in cur.fetchall():
                by_status[status] = count
        if own_conn:
            conn.commit()
    except Exception:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn and conn is not None:
            conn.close()

    log.info(
        "[scrape-health] rollup: rows_written=%d by_status=%s",
        rows_written,
        by_status,
    )
    return {"rows_written": rows_written, "by_status": by_status}
