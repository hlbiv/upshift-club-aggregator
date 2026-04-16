"""
coach_career_builder — build/refresh ``coach_career_history`` from
``coach_discoveries`` + ``college_coaches``.

For each coach in the ``coaches`` master table:
  1. Find all ``coach_discoveries`` with matching ``coach_id``
     -> create career_history entries for clubs (entity_type='club')
  2. Find all ``college_coaches`` with matching ``coach_id``
     -> create career_history entries for colleges (entity_type='college')

Movement detection: if a coach was at Club A in the previous snapshot
but is no longer there, insert a ``coach_movement_events`` row.

Idempotency
-----------
Uses ON CONFLICT (coach_id, entity_type, entity_id, role, start_year)
DO UPDATE on coach_career_history for upsert behavior.

Movement events use ON CONFLICT DO NOTHING on their unique constraint.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger(__name__)


# Normalize discovery titles to career-history role enum values.
_ROLE_MAP = {
    "head coach": "head_coach",
    "head_coach": "head_coach",
    "assistant coach": "assistant",
    "assistant": "assistant",
    "asst coach": "assistant",
    "goalkeeper coach": "gk_coach",
    "gk coach": "gk_coach",
    "director of coaching": "club_director",
    "doc": "doc",
    "fitness": "fitness",
    "fitness coach": "fitness",
}

_VALID_ROLES = frozenset(
    ["head_coach", "assistant", "doc", "gk_coach", "fitness", "club_director", "other"]
)


def _normalize_role(title: str | None) -> str:
    if not title:
        return "other"
    key = title.strip().lower()
    mapped = _ROLE_MAP.get(key)
    if mapped:
        return mapped
    # Heuristic fallback
    if "head" in key:
        return "head_coach"
    if "assistant" in key or "asst" in key:
        return "assistant"
    if "director" in key or "doc" in key:
        return "doc"
    if "goalkeep" in key or "gk" in key:
        return "gk_coach"
    if "fitness" in key or "strength" in key or "conditioning" in key:
        return "fitness"
    return "other"


# Step 1: Build career history from coach_discoveries (clubs)
_UPSERT_CLUB_CAREERS_SQL = """
INSERT INTO coach_career_history (
    coach_id, entity_type, entity_id, role, start_year, end_year,
    is_current, source, source_url, confidence
)
SELECT
    cd.coach_id,
    'club',
    cd.club_id,
    CASE
        WHEN LOWER(cd.title) LIKE '%%head%%' THEN 'head_coach'
        WHEN LOWER(cd.title) LIKE '%%assistant%%' OR LOWER(cd.title) LIKE '%%asst%%' THEN 'assistant'
        WHEN LOWER(cd.title) LIKE '%%director%%' OR LOWER(cd.title) = 'doc' THEN 'doc'
        WHEN LOWER(cd.title) LIKE '%%goalkeep%%' OR LOWER(cd.title) LIKE '%%gk%%' THEN 'gk_coach'
        WHEN LOWER(cd.title) LIKE '%%fitness%%' OR LOWER(cd.title) LIKE '%%strength%%' THEN 'fitness'
        ELSE 'other'
    END AS role,
    EXTRACT(YEAR FROM cd.first_seen_at)::int AS start_year,
    NULL AS end_year,
    TRUE AS is_current,
    'coach_discovery' AS source,
    cd.source_url,
    cd.confidence
FROM coach_discoveries cd
WHERE cd.coach_id IS NOT NULL
  AND cd.club_id IS NOT NULL
ON CONFLICT ON CONSTRAINT coach_career_history_unique
DO UPDATE SET
    is_current = TRUE,
    confidence = EXCLUDED.confidence,
    source_url = COALESCE(EXCLUDED.source_url, coach_career_history.source_url)
"""

# Step 2: Build career history from college_coaches
_UPSERT_COLLEGE_CAREERS_SQL = """
INSERT INTO coach_career_history (
    coach_id, entity_type, entity_id, role, start_year, end_year,
    is_current, source, source_url, confidence
)
SELECT
    cc.coach_id,
    'college',
    cc.college_id,
    CASE
        WHEN cc.is_head_coach = TRUE THEN 'head_coach'
        WHEN LOWER(cc.title) LIKE '%%assistant%%' OR LOWER(cc.title) LIKE '%%asst%%' THEN 'assistant'
        WHEN LOWER(cc.title) LIKE '%%director%%' THEN 'doc'
        WHEN LOWER(cc.title) LIKE '%%goalkeep%%' OR LOWER(cc.title) LIKE '%%gk%%' THEN 'gk_coach'
        ELSE 'other'
    END AS role,
    EXTRACT(YEAR FROM cc.first_seen_at)::int AS start_year,
    NULL AS end_year,
    TRUE AS is_current,
    cc.source,
    cc.source_url,
    cc.confidence
FROM college_coaches cc
WHERE cc.coach_id IS NOT NULL
ON CONFLICT ON CONSTRAINT coach_career_history_unique
DO UPDATE SET
    is_current = TRUE,
    confidence = EXCLUDED.confidence,
    source_url = COALESCE(EXCLUDED.source_url, coach_career_history.source_url)
"""

# Step 3: Mark careers as not current if the discovery/college_coach row
# is no longer present (vanished detection). A coach_discovery is
# considered stale if last_seen_at is older than 90 days.
_MARK_STALE_CLUB_CAREERS_SQL = """
UPDATE coach_career_history cch
SET is_current = FALSE,
    end_year = EXTRACT(YEAR FROM NOW())::int
WHERE cch.entity_type = 'club'
  AND cch.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM coach_discoveries cd
      WHERE cd.coach_id = cch.coach_id
        AND cd.club_id = cch.entity_id
        AND cd.last_seen_at > NOW() - INTERVAL '90 days'
  )
"""

_MARK_STALE_COLLEGE_CAREERS_SQL = """
UPDATE coach_career_history cch
SET is_current = FALSE,
    end_year = EXTRACT(YEAR FROM NOW())::int
WHERE cch.entity_type = 'college'
  AND cch.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM college_coaches cc
      WHERE cc.coach_id = cch.coach_id
        AND cc.college_id = cch.entity_id
        AND cc.last_seen_at > NOW() - INTERVAL '90 days'
  )
"""

# Step 4: Detect movements — new arrivals that weren't there before.
# Insert 'joined' events for careers that were just created (no
# prior movement event of type 'joined' for this coach+entity).
_DETECT_JOINED_SQL = """
INSERT INTO coach_movement_events (
    coach_id, event_type, to_entity_type, to_entity_id, to_role,
    detected_at, confidence
)
SELECT
    cch.coach_id,
    'joined',
    cch.entity_type,
    cch.entity_id,
    cch.role,
    NOW(),
    cch.confidence
FROM coach_career_history cch
WHERE cch.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM coach_movement_events cme
      WHERE cme.coach_id = cch.coach_id
        AND cme.event_type = 'joined'
        AND cme.to_entity_type = cch.entity_type
        AND cme.to_entity_id = cch.entity_id
  )
ON CONFLICT ON CONSTRAINT coach_movement_events_unique
DO NOTHING
"""

# Detect departures — careers that just went stale.
_DETECT_DEPARTED_SQL = """
INSERT INTO coach_movement_events (
    coach_id, event_type, from_entity_type, from_entity_id, from_role,
    detected_at, confidence
)
SELECT
    cch.coach_id,
    'departed',
    cch.entity_type,
    cch.entity_id,
    cch.role,
    NOW(),
    cch.confidence
FROM coach_career_history cch
WHERE cch.is_current = FALSE
  AND cch.end_year = EXTRACT(YEAR FROM NOW())::int
  AND NOT EXISTS (
      SELECT 1 FROM coach_movement_events cme
      WHERE cme.coach_id = cch.coach_id
        AND cme.event_type = 'departed'
        AND cme.from_entity_type = cch.entity_type
        AND cme.from_entity_id = cch.entity_id
  )
ON CONFLICT ON CONSTRAINT coach_movement_events_unique
DO NOTHING
"""

_CAREER_COUNT_SQL = "SELECT COUNT(*)::int FROM coach_career_history"
_MOVEMENT_COUNT_SQL = "SELECT COUNT(*)::int FROM coach_movement_events"


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def build_coach_careers(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Build/refresh coach_career_history and detect movements.

    Returns
    -------
    dict
        ``{"career_rows": int, "movement_rows": int}``.
    """
    if dry_run:
        log.info("[coach-career-builder] dry-run: would refresh career history")
        return {"career_rows": 0, "movement_rows": 0}

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            # Step 1: Upsert club careers from discoveries
            cur.execute(_UPSERT_CLUB_CAREERS_SQL)
            club_upserted = cur.rowcount or 0
            log.info("[coach-career-builder] club careers upserted: %d", club_upserted)

            # Step 2: Upsert college careers
            cur.execute(_UPSERT_COLLEGE_CAREERS_SQL)
            college_upserted = cur.rowcount or 0
            log.info("[coach-career-builder] college careers upserted: %d", college_upserted)

            # Step 3: Mark stale careers
            cur.execute(_MARK_STALE_CLUB_CAREERS_SQL)
            stale_clubs = cur.rowcount or 0
            cur.execute(_MARK_STALE_COLLEGE_CAREERS_SQL)
            stale_colleges = cur.rowcount or 0
            log.info(
                "[coach-career-builder] stale: clubs=%d colleges=%d",
                stale_clubs,
                stale_colleges,
            )

            # Step 4: Detect movements
            cur.execute(_DETECT_JOINED_SQL)
            joined = cur.rowcount or 0
            cur.execute(_DETECT_DEPARTED_SQL)
            departed = cur.rowcount or 0
            log.info(
                "[coach-career-builder] movements: joined=%d departed=%d",
                joined,
                departed,
            )

            # Final counts
            cur.execute(_CAREER_COUNT_SQL)
            career_rows = cur.fetchone()[0]
            cur.execute(_MOVEMENT_COUNT_SQL)
            movement_rows = cur.fetchone()[0]

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
        "[coach-career-builder] done: career_rows=%d movement_rows=%d",
        career_rows,
        movement_rows,
    )
    return {"career_rows": career_rows, "movement_rows": movement_rows}
