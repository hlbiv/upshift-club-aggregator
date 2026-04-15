"""
club_results rollup — recompute ``club_results`` from ``matches``.

``club_results`` is a materialized per-club standings table. It is
**not** written by scrapers directly. Instead, a rollup job reads all
final matches with resolved ``home_club_id`` / ``away_club_id``,
groups by ``(club_id, season, league, division, age_group, gender)``,
and writes aggregated W/L/D + GF/GA counts.

Idempotency
-----------
Each run is a full recompute within a single transaction:

    DELETE FROM club_results;
    INSERT INTO club_results (...) SELECT ... FROM matches ...;

This is safe because the table is derived data. Two successive runs
produce identical counts. If you want incremental rollups later,
partition the DELETE by (season) or (season, league). The full wipe
is fine while the dataset is small.

Linker dependency
-----------------
We skip any match row where ``home_club_id`` or ``away_club_id`` is
NULL. The scraper inserts matches with raw team names only; a
separate linker job populates the FKs. Until the linker runs,
``club_results`` will be empty — that is the expected state
immediately after the first ``gotsport-matches`` scrape.
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


# The rollup aggregates each match twice — once from the home club's
# perspective and once from the away club's. A UNION ALL of the two
# projections, then a GROUP BY the club + grouping columns.
_LINKER_PRECHECK_SQL = """
SELECT COUNT(*)::int
FROM matches
WHERE home_club_id IS NOT NULL OR away_club_id IS NOT NULL
"""


# TODO: partition DELETE by (club_id, event_id, season) scope once we roll
# up incrementally. A blanket DELETE is fine while the dataset is small
# and every run recomputes the full table, but once we start ingesting
# future-season league data the blanket wipe will nuke rows that the
# current run has no matches for. Partition as soon as there's >1 season
# of live data in matches.
_ROLLUP_SQL = """
DELETE FROM club_results;

INSERT INTO club_results (
    club_id, season, league, division, age_group, gender,
    wins, losses, draws, goals_for, goals_against, matches_played,
    last_calculated_at
)
SELECT
    club_id,
    season,
    league,
    division,
    age_group,
    gender,
    SUM(win)::int  AS wins,
    SUM(loss)::int AS losses,
    SUM(draw)::int AS draws,
    SUM(gf)::int   AS goals_for,
    SUM(ga)::int   AS goals_against,
    COUNT(*)::int  AS matches_played,
    NOW()
FROM (
    SELECT
        home_club_id AS club_id,
        season, league, division, age_group, gender,
        CASE WHEN home_score > away_score THEN 1 ELSE 0 END AS win,
        CASE WHEN home_score < away_score THEN 1 ELSE 0 END AS loss,
        CASE WHEN home_score = away_score THEN 1 ELSE 0 END AS draw,
        home_score AS gf,
        away_score AS ga
    FROM matches
    WHERE status = 'final'
      AND home_club_id IS NOT NULL
      AND away_club_id IS NOT NULL
      AND home_score IS NOT NULL
      AND away_score IS NOT NULL
      AND season IS NOT NULL

    UNION ALL

    SELECT
        away_club_id AS club_id,
        season, league, division, age_group, gender,
        CASE WHEN away_score > home_score THEN 1 ELSE 0 END AS win,
        CASE WHEN away_score < home_score THEN 1 ELSE 0 END AS loss,
        CASE WHEN away_score = home_score THEN 1 ELSE 0 END AS draw,
        away_score AS gf,
        home_score AS ga
    FROM matches
    WHERE status = 'final'
      AND home_club_id IS NOT NULL
      AND away_club_id IS NOT NULL
      AND home_score IS NOT NULL
      AND away_score IS NOT NULL
      AND season IS NOT NULL
) per_side
GROUP BY club_id, season, league, division, age_group, gender
"""


# Count linker-blocked matches so the caller can report how many rows
# were skipped for lack of an FK resolution.
_SKIPPED_COUNT_SQL = """
SELECT COUNT(*)::int
FROM matches
WHERE status = 'final'
  AND (home_club_id IS NULL OR away_club_id IS NULL)
  AND home_score IS NOT NULL
  AND away_score IS NOT NULL
"""


_INSERTED_COUNT_SQL = "SELECT COUNT(*)::int FROM club_results"


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def recompute_club_results(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Recompute the ``club_results`` table from scratch.

    Returns
    -------
    dict
        ``{"rows_written": int, "skipped_linker_pending": int}``.
    """
    if dry_run:
        log.info("[club-results] dry-run: would recompute club_results from matches")
        return {"rows_written": 0, "skipped_linker_pending": 0}

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            # Linker guard: if no matches have any club FK resolved, the
            # rollup would produce zero rows AND wipe existing
            # club_results. Abort loudly so the operator runs the linker
            # first. This is the common failure mode the first time the
            # matches scraper runs in a fresh environment.
            cur.execute(_LINKER_PRECHECK_SQL)
            linked_count = cur.fetchone()[0]
            if linked_count == 0:
                raise RuntimeError(
                    "club_results rollup aborted: no matches rows have "
                    "home_club_id or away_club_id populated. The linker "
                    "hasn't run yet — running this rollup would produce "
                    "zero rows AND wipe any existing club_results data. "
                    "Run the canonical-club linker first (see "
                    "claude/canonical-club-linker branch), then re-run "
                    "this rollup."
                )

            cur.execute(_SKIPPED_COUNT_SQL)
            skipped = cur.fetchone()[0]
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
        "[club-results] rollup: rows_written=%d skipped_linker_pending=%d",
        rows_written,
        skipped,
    )
    return {"rows_written": rows_written, "skipped_linker_pending": skipped}
