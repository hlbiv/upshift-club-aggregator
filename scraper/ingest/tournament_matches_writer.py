"""
tournament_matches_writer.py — Idempotent Postgres upsert for ``tournament_matches`` rows.

Two unique indexes on ``tournament_matches`` that we target explicitly:

    tournament_matches_source_platform_id_uq
        UNIQUE (source, platform_match_id) WHERE platform_match_id IS NOT NULL

    tournament_matches_natural_key_uq
        UNIQUE (
            home_team_name,
            away_team_name,
            COALESCE(match_date, 'epoch'::timestamp),
            COALESCE(age_group, ''),
            COALESCE(gender, ''),
            COALESCE(tournament_name, '')
        ) WHERE platform_match_id IS NULL

Same split-brain guard as matches_writer: before every INSERT that carries a
``platform_match_id`` we pre-sweep any existing natural-key row to stamp the id
onto it so the INSERT ... ON CONFLICT resolves correctly.

Per-row SAVEPOINT isolation means a single bad row cannot roll back the batch.
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
INSERT INTO tournament_matches (
    event_id, home_club_id, away_club_id,
    home_team_name, away_team_name,
    home_score, away_score,
    match_date, age_group, gender, division, season,
    tournament_name, flight, group_name, bracket_round, match_type,
    status, source, source_url, platform_match_id
) VALUES (
    %(event_id)s, %(home_club_id)s, %(away_club_id)s,
    %(home_team_name)s, %(away_team_name)s,
    %(home_score)s, %(away_score)s,
    %(match_date)s, %(age_group)s, %(gender)s, %(division)s, %(season)s,
    %(tournament_name)s, %(flight)s, %(group_name)s, %(bracket_round)s, %(match_type)s,
    %(status)s, %(source)s, %(source_url)s, %(platform_match_id)s
)
ON CONFLICT ON CONSTRAINT tournament_matches_source_platform_id_uq
DO UPDATE SET
    home_team_name  = EXCLUDED.home_team_name,
    away_team_name  = EXCLUDED.away_team_name,
    home_score      = EXCLUDED.home_score,
    away_score      = EXCLUDED.away_score,
    match_date      = COALESCE(EXCLUDED.match_date, tournament_matches.match_date),
    age_group       = COALESCE(EXCLUDED.age_group, tournament_matches.age_group),
    gender          = COALESCE(EXCLUDED.gender, tournament_matches.gender),
    division        = COALESCE(EXCLUDED.division, tournament_matches.division),
    season          = COALESCE(EXCLUDED.season, tournament_matches.season),
    tournament_name = COALESCE(EXCLUDED.tournament_name, tournament_matches.tournament_name),
    flight          = COALESCE(EXCLUDED.flight, tournament_matches.flight),
    group_name      = COALESCE(EXCLUDED.group_name, tournament_matches.group_name),
    bracket_round   = COALESCE(EXCLUDED.bracket_round, tournament_matches.bracket_round),
    match_type      = COALESCE(EXCLUDED.match_type, tournament_matches.match_type),
    status          = EXCLUDED.status,
    source_url      = EXCLUDED.source_url,
    event_id        = COALESCE(EXCLUDED.event_id, tournament_matches.event_id),
    scraped_at      = NOW()
RETURNING id, (xmax = 0) AS inserted
"""

_INSERT_NATURAL_KEY = """
INSERT INTO tournament_matches (
    event_id, home_club_id, away_club_id,
    home_team_name, away_team_name,
    home_score, away_score,
    match_date, age_group, gender, division, season,
    tournament_name, flight, group_name, bracket_round, match_type,
    status, source, source_url, platform_match_id
) VALUES (
    %(event_id)s, %(home_club_id)s, %(away_club_id)s,
    %(home_team_name)s, %(away_team_name)s,
    %(home_score)s, %(away_score)s,
    %(match_date)s, %(age_group)s, %(gender)s, %(division)s, %(season)s,
    %(tournament_name)s, %(flight)s, %(group_name)s, %(bracket_round)s, %(match_type)s,
    %(status)s, %(source)s, %(source_url)s, %(platform_match_id)s
)
ON CONFLICT ON CONSTRAINT tournament_matches_natural_key_uq
DO UPDATE SET
    home_score      = EXCLUDED.home_score,
    away_score      = EXCLUDED.away_score,
    division        = COALESCE(EXCLUDED.division, tournament_matches.division),
    season          = COALESCE(EXCLUDED.season, tournament_matches.season),
    tournament_name = COALESCE(EXCLUDED.tournament_name, tournament_matches.tournament_name),
    flight          = COALESCE(EXCLUDED.flight, tournament_matches.flight),
    group_name      = COALESCE(EXCLUDED.group_name, tournament_matches.group_name),
    bracket_round   = COALESCE(EXCLUDED.bracket_round, tournament_matches.bracket_round),
    match_type      = COALESCE(EXCLUDED.match_type, tournament_matches.match_type),
    status          = EXCLUDED.status,
    source_url      = EXCLUDED.source_url,
    source          = COALESCE(EXCLUDED.source, tournament_matches.source),
    event_id        = COALESCE(EXCLUDED.event_id, tournament_matches.event_id),
    scraped_at      = NOW()
RETURNING id, (xmax = 0) AS inserted
"""

_PRESWEEP_PLATFORM_ID = """
UPDATE tournament_matches
SET platform_match_id = %(platform_match_id)s,
    source = COALESCE(tournament_matches.source, %(source)s)
WHERE platform_match_id IS NULL
  AND home_team_name = %(home_team_name)s
  AND away_team_name = %(away_team_name)s
  AND COALESCE(match_date, 'epoch'::timestamp)
      = COALESCE(%(match_date)s, 'epoch'::timestamp)
  AND COALESCE(age_group, '')      = COALESCE(%(age_group)s, '')
  AND COALESCE(gender, '')         = COALESCE(%(gender)s, '')
  AND COALESCE(tournament_name, '') = COALESCE(%(tournament_name)s, '')
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce empty strings to None for nullable key columns."""
    def _none_if_empty(val: Any) -> Any:
        return val if val not in (None, "") else None

    return {
        "event_id":         row.get("event_fk_id") or row.get("event_db_id"),
        "home_club_id":     row.get("home_club_id"),
        "away_club_id":     row.get("away_club_id"),
        "home_team_name":   row["home_team_name"],
        "away_team_name":   row["away_team_name"],
        "home_score":       row.get("home_score"),
        "away_score":       row.get("away_score"),
        "match_date":       row.get("match_date"),
        "age_group":        _none_if_empty(row.get("age_group")),
        "gender":           _none_if_empty(row.get("gender")),
        "division":         row.get("division"),
        "season":           row.get("season"),
        "tournament_name":  _none_if_empty(row.get("tournament_name")),
        "flight":           row.get("flight"),
        "group_name":       row.get("group_name"),
        "bracket_round":    row.get("bracket_round"),
        "match_type":       row.get("match_type"),
        "status":           row.get("status") or "scheduled",
        "source":           row.get("source"),
        "source_url":       row.get("source_url"),
        "platform_match_id": row.get("platform_match_id"),
    }


def insert_tournament_matches(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    Insert or update a batch of tournament match rows.

    Returns ``{"inserted": int, "updated": int, "skipped": int}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0, "presweep_upgraded": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[tournament-matches-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            for raw in rows:
                row = _normalize_row(raw)
                has_pid = row["platform_match_id"] is not None
                sql_stmt = _INSERT_WITH_PLATFORM_ID if has_pid else _INSERT_NATURAL_KEY
                cur.execute("SAVEPOINT tournament_match_row")
                presweep_count = 0
                try:
                    if has_pid:
                        cur.execute(_PRESWEEP_PLATFORM_ID, row)
                        if cur.rowcount and cur.rowcount > 0:
                            presweep_count = cur.rowcount
                    cur.execute(sql_stmt, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[tournament-matches-writer] upsert failed for %s vs %s: %s",
                        row.get("home_team_name"),
                        row.get("away_team_name"),
                        exc,
                    )
                    counts["skipped"] += 1
                    cur.execute("ROLLBACK TO SAVEPOINT tournament_match_row")
                    continue
                cur.execute("RELEASE SAVEPOINT tournament_match_row")
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
            "[tournament-matches-writer] split-brain sweep: upgraded %d row(s) with platform_match_id",
            counts["presweep_upgraded"],
        )
    return counts
