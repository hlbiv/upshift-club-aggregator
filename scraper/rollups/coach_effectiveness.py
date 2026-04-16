"""
coach_effectiveness rollup — recompute ``coach_effectiveness`` from
``coach_career_history`` + ``college_roster_history``.

The core insight: a youth coach's effectiveness is measured by how many
of their club's players went on to play college soccer. We compute this
by joining:

  coaches -> coach_career_history (entity_type='club')
           -> canonical_clubs
           -> event_teams (clubs that participated in events)
           -> college_roster_history.prev_club (fuzzy name match)
           -> colleges.division

``prev_club`` in college_roster_history records the youth club a player
came from. We match that against the canonical club name and its aliases.

Idempotency
-----------
Full recompute: DELETE FROM coach_effectiveness, then INSERT aggregated
rows. This matches the club_results.py pattern. Safe because the table
is derived data.

Linker dependency
-----------------
Requires coach_career_history to be populated (run coach_career_builder
first). Also requires college_roster_history to have prev_club values.
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


_PRECHECK_SQL = """
SELECT COUNT(*)::int
FROM coach_career_history
WHERE entity_type = 'club'
"""

# The effectiveness rollup:
#
# 1. For each coach, find clubs they coached at (coach_career_history)
# 2. For each club, find the canonical name + all aliases
# 3. Match college_roster_history.prev_club against those names (ILIKE)
# 4. Count distinct placements per division
# 5. Also count clubs_coached and seasons_tracked from career history
#
# The CTE approach:
# - coach_clubs: all (coach_id, club_id, club names) from career history
# - club_names: union of canonical names + aliases for matched clubs
# - placements: college roster rows whose prev_club matches a club name
# - per_division: grouped counts by coach + division
_ROLLUP_SQL = """
DELETE FROM coach_effectiveness;

WITH coach_clubs AS (
    SELECT DISTINCT
        cch.coach_id,
        cch.entity_id AS club_id
    FROM coach_career_history cch
    WHERE cch.entity_type = 'club'
),
club_names AS (
    SELECT cc.club_id, LOWER(c.club_name_canonical) AS name
    FROM coach_clubs cc
    JOIN canonical_clubs c ON c.id = cc.club_id
    UNION
    SELECT cc.club_id, LOWER(ca.alias_name) AS name
    FROM coach_clubs cc
    JOIN club_aliases ca ON ca.club_id = cc.club_id
),
placements AS (
    SELECT DISTINCT
        cc.coach_id,
        crh.id AS roster_id,
        col.division
    FROM coach_clubs cc
    JOIN club_names cn ON cn.club_id = cc.club_id
    JOIN college_roster_history crh
        ON LOWER(crh.prev_club) LIKE '%%' || cn.name || '%%'
        AND crh.prev_club IS NOT NULL
        AND crh.prev_club != ''
    JOIN colleges col ON col.id = crh.college_id
),
career_stats AS (
    SELECT
        cch.coach_id,
        COUNT(DISTINCT CASE WHEN cch.entity_type = 'club' THEN cch.entity_id END)::int AS clubs_coached,
        COUNT(DISTINCT cch.start_year)::int AS seasons_tracked
    FROM coach_career_history cch
    GROUP BY cch.coach_id
)
INSERT INTO coach_effectiveness (
    coach_id,
    players_placed_d1,
    players_placed_d2,
    players_placed_d3,
    players_placed_naia,
    players_placed_njcaa,
    players_placed_total,
    clubs_coached,
    seasons_tracked,
    last_calculated_at
)
SELECT
    c.id AS coach_id,
    COALESCE(SUM(CASE WHEN p.division = 'D1' THEN 1 ELSE 0 END), 0)::int AS players_placed_d1,
    COALESCE(SUM(CASE WHEN p.division = 'D2' THEN 1 ELSE 0 END), 0)::int AS players_placed_d2,
    COALESCE(SUM(CASE WHEN p.division = 'D3' THEN 1 ELSE 0 END), 0)::int AS players_placed_d3,
    COALESCE(SUM(CASE WHEN p.division = 'NAIA' THEN 1 ELSE 0 END), 0)::int AS players_placed_naia,
    COALESCE(SUM(CASE WHEN p.division = 'NJCAA' THEN 1 ELSE 0 END), 0)::int AS players_placed_njcaa,
    COUNT(DISTINCT p.roster_id)::int AS players_placed_total,
    COALESCE(cs.clubs_coached, 0)::int,
    COALESCE(cs.seasons_tracked, 0)::smallint,
    NOW()
FROM coaches c
LEFT JOIN (
    SELECT DISTINCT coach_id, roster_id, division
    FROM placements
) p ON p.coach_id = c.id
LEFT JOIN career_stats cs ON cs.coach_id = c.id
GROUP BY c.id, cs.clubs_coached, cs.seasons_tracked
HAVING COUNT(DISTINCT p.roster_id) > 0
    OR cs.clubs_coached > 0
"""

_INSERTED_COUNT_SQL = "SELECT COUNT(*)::int FROM coach_effectiveness"


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def recompute_coach_effectiveness(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Recompute the ``coach_effectiveness`` table from scratch.

    Returns
    -------
    dict
        ``{"rows_written": int}``.
    """
    if dry_run:
        log.info("[coach-effectiveness] dry-run: would recompute coach_effectiveness")
        return {"rows_written": 0}

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            # Guard: if no career history exists, the rollup would produce
            # zero rows AND wipe existing effectiveness data.
            cur.execute(_PRECHECK_SQL)
            career_count = cur.fetchone()[0]
            if career_count == 0:
                raise RuntimeError(
                    "coach_effectiveness rollup aborted: no coach_career_history "
                    "rows with entity_type='club'. Run coach_career_builder first."
                )

            cur.execute(_ROLLUP_SQL)
            cur.execute(_INSERTED_COUNT_SQL)
            rows_written = cur.fetchone()[0]

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
        "[coach-effectiveness] rollup: rows_written=%d",
        rows_written,
    )
    return {"rows_written": rows_written}
