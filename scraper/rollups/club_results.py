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

    DELETE FROM club_results [WHERE season = ... [AND league = ...]];
    INSERT INTO club_results (...) SELECT ... FROM matches [...];

This is safe because the table is derived data. Two successive runs
produce identical counts.

Scope
-----
The rollup can be partitioned by ``(season)`` or ``(season, league)``:

* No flags (default) — full wipe + recompute. Backwards compatible
  with the original behavior; safe while datasets are small but
  expensive once multiple seasons of league data exist.
* ``season=...`` — DELETE + INSERT scoped to that season. Other
  seasons' rows (and their ``last_calculated_at`` timestamps) are
  untouched.
* ``season=..., league=...`` — DELETE + INSERT scoped to a single
  ``(season, league)`` partition. Useful for a per-league nightly
  refresh that doesn't churn the whole table.

The scope WHERE fragment is appended to the ``DELETE``, both INSERT
inner ``SELECT`` projections, the linker precheck, and the skipped-
count query so each scoped invocation only inspects matches in its
partition.

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
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger(__name__)


def _scope_clause(
    season: Optional[str],
    league: Optional[str],
    *,
    prefix: str = "AND",
) -> Tuple[str, List[Any]]:
    """Build an optional ``(season[, league])`` WHERE fragment.

    Returns ``(sql_fragment, params)``. ``sql_fragment`` is empty when
    both filters are None — callers paste it directly into the SQL
    template, so the unscoped path emits exactly the original SQL.

    ``prefix`` is the keyword to lead with: ``"AND"`` when the
    surrounding query already has a ``WHERE``, ``"WHERE"`` for the
    bare DELETE.
    """
    parts: List[str] = []
    params: List[Any] = []
    if season is not None:
        parts.append("season = %s")
        params.append(season)
    if league is not None:
        # IS NOT DISTINCT FROM lets a NULL league filter match NULL
        # rows; harmless when league is a real string.
        parts.append("league IS NOT DISTINCT FROM %s")
        params.append(league)
    if not parts:
        return "", params
    return f" {prefix} " + " AND ".join(parts), params


def _linker_precheck_sql(scope_sql: str) -> str:
    return f"""
SELECT COUNT(*)::int
FROM matches
WHERE (home_club_id IS NOT NULL OR away_club_id IS NOT NULL)
{scope_sql}
"""


def _delete_sql(season: Optional[str], league: Optional[str]) -> Tuple[str, List[Any]]:
    where_sql, params = _scope_clause(season, league, prefix="WHERE")
    return f"DELETE FROM club_results{where_sql}", params


def _insert_sql(scope_sql: str) -> str:
    """Build the INSERT ... SELECT, with ``scope_sql`` appended to
    BOTH inner SELECT WHERE clauses (home + away projections).

    The two ``%s`` placeholders for season/league appear once per
    inner SELECT, so the parameter list passed to ``cur.execute`` is
    ``params * 2`` (see :func:`recompute_club_results`).
    """
    return f"""
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
      {scope_sql}

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
      {scope_sql}
) per_side
GROUP BY club_id, season, league, division, age_group, gender
"""


def _skipped_count_sql(scope_sql: str) -> str:
    return f"""
SELECT COUNT(*)::int
FROM matches
WHERE status = 'final'
  AND (home_club_id IS NULL OR away_club_id IS NULL)
  AND home_score IS NOT NULL
  AND away_score IS NOT NULL
  {scope_sql}
"""


def _inserted_count_sql(season: Optional[str], league: Optional[str]) -> Tuple[str, List[Any]]:
    where_sql, params = _scope_clause(season, league, prefix="WHERE")
    return f"SELECT COUNT(*)::int FROM club_results{where_sql}", params


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _format_scope(season: Optional[str], league: Optional[str]) -> str:
    if season is None and league is None:
        return "all"
    parts = []
    if season is not None:
        parts.append(f"season={season}")
    if league is not None:
        parts.append(f"league={league}")
    return " ".join(parts)


def recompute_club_results(
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
    season: Optional[str] = None,
    league: Optional[str] = None,
) -> Dict[str, int]:
    """Recompute the ``club_results`` table.

    Parameters
    ----------
    conn
        Optional psycopg2 connection. If omitted, one is opened from
        ``DATABASE_URL``.
    dry_run
        If True, log the intent and return zeros without touching the DB.
    season, league
        Optional partition. With both None (default), the full table
        is wiped + rebuilt. With ``season`` set, only rows for that
        season are touched. With both set, only the
        ``(season, league)`` partition is touched. ``last_calculated_at``
        on rows outside the partition is not updated.

    Returns
    -------
    dict
        ``{"rows_written": int, "skipped_linker_pending": int}``.
    """
    scope_label = _format_scope(season, league)
    log.info("[club-results] rollup scope=%s", scope_label)

    if dry_run:
        log.info(
            "[club-results] dry-run: would recompute club_results from matches "
            "(scope=%s)",
            scope_label,
        )
        return {"rows_written": 0, "skipped_linker_pending": 0}

    scope_sql, scope_params = _scope_clause(season, league, prefix="AND")

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            # Linker guard: if no matches in the requested scope have
            # any club FK resolved, the rollup would produce zero rows
            # AND wipe the existing rows for that scope. Abort loudly
            # so the operator runs the linker first. This is the common
            # failure mode the first time the matches scraper runs in
            # a fresh environment.
            cur.execute(_linker_precheck_sql(scope_sql), scope_params)
            linked_count = cur.fetchone()[0]
            if linked_count == 0:
                raise RuntimeError(
                    "club_results rollup aborted: no matches rows have "
                    "home_club_id or away_club_id populated "
                    f"(scope={scope_label}). The linker hasn't run yet — "
                    "running this rollup would produce zero rows AND wipe "
                    "any existing club_results data in scope. Run the "
                    "canonical-club linker first (see "
                    "claude/canonical-club-linker branch), then re-run "
                    "this rollup."
                )

            cur.execute(_skipped_count_sql(scope_sql), scope_params)
            skipped = cur.fetchone()[0]

            delete_sql, delete_params = _delete_sql(season, league)
            cur.execute(delete_sql, delete_params)

            # Each inner SELECT in the INSERT consumes one copy of the
            # scope params; the UNION ALL projection has two SELECTs.
            cur.execute(_insert_sql(scope_sql), scope_params + scope_params)

            inserted_sql, inserted_params = _inserted_count_sql(season, league)
            cur.execute(inserted_sql, inserted_params)
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
        "[club-results] rollup: scope=%s rows_written=%d skipped_linker_pending=%d",
        scope_label,
        rows_written,
        skipped,
    )
    return {"rows_written": rows_written, "skipped_linker_pending": skipped}
