"""
matches_writer.py — Idempotent Postgres upsert for ``matches`` rows.

Two unique indexes on ``matches`` that we must target explicitly:

    matches_source_platform_id_uq
        UNIQUE (source, platform_match_id) WHERE platform_match_id IS NOT NULL

    matches_natural_key_uq
        UNIQUE (
            home_team_name,
            away_team_name,
            COALESCE(match_date, 'epoch'::timestamp),
            COALESCE(age_group, ''),
            COALESCE(gender, '')
        ) WHERE platform_match_id IS NULL

Postgres' ``ON CONFLICT`` inference does **text-exact matching** against
``pg_index.indexprs``. Drizzle's generated index expressions can differ in
parenthesization / whitespace / casts from what a handwritten
``ON CONFLICT (col, (COALESCE(x, '')), ...)`` statement emits, which
makes inference brittle and produces the dreaded
``no unique or exclusion constraint matching the ON CONFLICT`` error
at runtime even though the index exists.

To bypass the text-match rule entirely, we use
``ON CONFLICT ON CONSTRAINT <index_name>`` and reference the Drizzle
index names directly (they're stable: see
``lib/db/src/schema/matches.ts``). This is resilient to Drizzle
reformatting the stored expression text.

Split-brain guard
-----------------
A match can be ingested first via the natural key (no ``platform_match_id``)
and later re-ingested with an id. The natural-key ON CONFLICT won't see
the new id, and the (source, platform_match_id) ON CONFLICT won't see
the natural-key row — net result: duplicate row.

Before every INSERT that carries a ``platform_match_id`` we run a
pre-sweep UPDATE that stamps the id onto any existing natural-key row
that matches. After the sweep the row is reachable by the
(source, platform_match_id) partial unique index, so the INSERT ... ON
CONFLICT always resolves to the right row.

Sibling repos use the Drizzle ``onConflictDoUpdate`` API which cannot
emit the ``WHERE ...`` predicate — psycopg2 can. If you're ever
tempted to "fix" this by dropping the partial index, don't; read the
comment in ``lib/db/src/schema/matches.ts`` first.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger(__name__)


_INSERT_WITH_PLATFORM_ID = """
INSERT INTO matches (
    event_id, home_club_id, away_club_id,
    home_team_name, away_team_name,
    home_score, away_score,
    match_date, age_group, gender, division, season, league,
    status, source, source_url, platform_match_id
) VALUES (
    %(event_id)s, %(home_club_id)s, %(away_club_id)s,
    %(home_team_name)s, %(away_team_name)s,
    %(home_score)s, %(away_score)s,
    %(match_date)s, %(age_group)s, %(gender)s, %(division)s, %(season)s, %(league)s,
    %(status)s, %(source)s, %(source_url)s, %(platform_match_id)s
)
ON CONFLICT ON CONSTRAINT matches_source_platform_id_uq
DO UPDATE SET
    home_team_name = EXCLUDED.home_team_name,
    away_team_name = EXCLUDED.away_team_name,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    match_date = COALESCE(EXCLUDED.match_date, matches.match_date),
    age_group = COALESCE(EXCLUDED.age_group, matches.age_group),
    gender = COALESCE(EXCLUDED.gender, matches.gender),
    division = COALESCE(EXCLUDED.division, matches.division),
    season = COALESCE(EXCLUDED.season, matches.season),
    league = COALESCE(EXCLUDED.league, matches.league),
    status = EXCLUDED.status,
    source_url = EXCLUDED.source_url,
    event_id = COALESCE(EXCLUDED.event_id, matches.event_id),
    scraped_at = NOW()
RETURNING id, (xmax = 0) AS inserted
"""


# Natural-key upsert uses the partial index. Note that Postgres requires
# the conflict_target columns to literally match the index definition,
# including the COALESCE wrappers on the nullable cols.
_INSERT_NATURAL_KEY = """
INSERT INTO matches (
    event_id, home_club_id, away_club_id,
    home_team_name, away_team_name,
    home_score, away_score,
    match_date, age_group, gender, division, season, league,
    status, source, source_url, platform_match_id
) VALUES (
    %(event_id)s, %(home_club_id)s, %(away_club_id)s,
    %(home_team_name)s, %(away_team_name)s,
    %(home_score)s, %(away_score)s,
    %(match_date)s, %(age_group)s, %(gender)s, %(division)s, %(season)s, %(league)s,
    %(status)s, %(source)s, %(source_url)s, %(platform_match_id)s
)
ON CONFLICT ON CONSTRAINT matches_natural_key_uq
DO UPDATE SET
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    division = COALESCE(EXCLUDED.division, matches.division),
    season = COALESCE(EXCLUDED.season, matches.season),
    league = COALESCE(EXCLUDED.league, matches.league),
    status = EXCLUDED.status,
    source_url = EXCLUDED.source_url,
    source = COALESCE(EXCLUDED.source, matches.source),
    event_id = COALESCE(EXCLUDED.event_id, matches.event_id),
    scraped_at = NOW()
RETURNING id, (xmax = 0) AS inserted
"""


# Split-brain pre-sweep: stamp the incoming platform_match_id onto any
# pre-existing natural-key row that matches. Returns the number of rows
# upgraded (so the caller can log the count per run).
_PRESWEEP_PLATFORM_ID = """
UPDATE matches
SET platform_match_id = %(platform_match_id)s,
    source = COALESCE(matches.source, %(source)s)
WHERE platform_match_id IS NULL
  AND home_team_name = %(home_team_name)s
  AND away_team_name = %(away_team_name)s
  AND COALESCE(match_date, 'epoch'::timestamp)
      = COALESCE(%(match_date)s::timestamp, 'epoch'::timestamp)
  AND COALESCE(age_group, '') = COALESCE(%(age_group)s, '')
  AND COALESCE(gender, '')    = COALESCE(%(gender)s, '')
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Fill in DB-expected keys with None for optional ones."""
    return {
        "event_id": row.get("event_fk_id") or row.get("event_db_id"),  # FK into events.id, NOT GotSport platform id
        "home_club_id": row.get("home_club_id"),
        "away_club_id": row.get("away_club_id"),
        "home_team_name": row["home_team_name"],
        "away_team_name": row["away_team_name"],
        "home_score": row.get("home_score"),
        "away_score": row.get("away_score"),
        "match_date": row.get("match_date"),
        "age_group": row.get("age_group"),
        "gender": row.get("gender"),
        "division": row.get("division"),
        "season": row.get("season"),
        "league": row.get("league"),
        "status": row.get("status") or "scheduled",
        "source": row.get("source"),
        "source_url": row.get("source_url"),
        "platform_match_id": row.get("platform_match_id"),
    }


def insert_matches(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Insert or update a batch of match rows.

    Returns
    -------
    dict
        ``{"inserted": int, "updated": int, "skipped": int}``.

    Rows are grouped by whether they have a ``platform_match_id``; each
    group is sent through the appropriate partial-index ON CONFLICT
    path. Caller owns the connection if ``conn`` is passed; otherwise
    we open a new one and commit.

    Dry-run returns zero counts and does not open a connection.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0, "presweep_upgraded": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[matches-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            for raw in rows:
                row = _normalize_row(raw)
                has_pid = row["platform_match_id"] is not None
                sql = _INSERT_WITH_PLATFORM_ID if has_pid else _INSERT_NATURAL_KEY
                # Per-row SAVEPOINT isolates a single bad row from the
                # batch. Previously this used ``conn.rollback()`` which
                # rolls back the WHOLE transaction on any single-row
                # failure, including the presweep UPDATE that already
                # ran for this row and any successful prior rows in the
                # batch — defeating the split-brain guard for the
                # entire group.
                cur.execute("SAVEPOINT match_row")
                presweep_count = 0
                try:
                    # Split-brain sweep: upgrade any prior natural-key
                    # row to carry this platform_match_id so the INSERT
                    # below resolves via (source, platform_match_id).
                    if has_pid:
                        cur.execute(_PRESWEEP_PLATFORM_ID, row)
                        if cur.rowcount and cur.rowcount > 0:
                            presweep_count = cur.rowcount
                    cur.execute(sql, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[matches-writer] upsert failed for %s vs %s: %s",
                        row.get("home_team_name"),
                        row.get("away_team_name"),
                        exc,
                    )
                    counts["skipped"] += 1
                    cur.execute("ROLLBACK TO SAVEPOINT match_row")
                    continue
                cur.execute("RELEASE SAVEPOINT match_row")
                # Only count the presweep upgrade after the INSERT
                # commits at the savepoint level — otherwise a failing
                # INSERT would leave the counter inflated even though
                # the presweep UPDATE was rolled back.
                counts["presweep_upgraded"] += presweep_count
                if result is None:
                    counts["skipped"] += 1
                    continue
                _id, inserted = result
                if inserted:
                    counts["inserted"] += 1
                else:
                    counts["updated"] += 1
        if own_conn:
            conn.commit()
    finally:
        if own_conn and conn is not None:
            conn.close()

    if counts["presweep_upgraded"]:
        log.info(
            "[matches-writer] split-brain sweep: upgraded %d prior natural-key row(s) with platform_match_id",
            counts["presweep_upgraded"],
        )
    return counts
