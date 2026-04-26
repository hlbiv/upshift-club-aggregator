"""
hs_matches_writer.py — Idempotent upsert of ``hs_matches`` rows.

See lib/db/src/schema/hs_matches.ts for the table shape. Natural key
is a unique INDEX:

    hs_matches_natural_key_uq
      UNIQUE (school_name_raw, school_state, opponent_raw, match_date, gender)

On conflict:
  - ``last_seen_at`` refreshes to ``now()``
  - COALESCE-updates fill in fields only where the stored value is
    NULL: ``result``, ``score_for``, ``score_against``, ``team_level``,
    ``tournament``, ``round``, ``season`` — this way the bracket pass
    (empty scores) and the later results pass (filled scores) can run
    in either order without clobbering data.

``school_id`` and ``opponent_school_id`` are NOT written by the
scraper. The canonical-schools linker resolves them in a later pass
(``python3 run.py --source link-canonical-schools``).
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

log = logging.getLogger("hs_matches_writer")


_INSERT_HS_MATCH_SQL = """
INSERT INTO hs_matches (
    school_id,
    school_name_raw,
    school_state,
    opponent_school_id,
    opponent_raw,
    match_date,
    gender,
    team_level,
    result,
    score_for,
    score_against,
    tournament,
    round,
    season,
    source_url,
    first_seen_at,
    last_seen_at
)
VALUES (
    NULL,
    %(school_name_raw)s,
    %(school_state)s,
    NULL,
    %(opponent_raw)s,
    %(match_date)s,
    %(gender)s,
    %(team_level)s,
    %(result)s,
    %(score_for)s,
    %(score_against)s,
    %(tournament)s,
    %(round)s,
    %(season)s,
    %(source_url)s,
    now(),
    now()
)
ON CONFLICT (school_name_raw, school_state, opponent_raw, match_date, gender)
DO UPDATE SET
    last_seen_at   = now(),
    result         = COALESCE(hs_matches.result,         EXCLUDED.result),
    score_for      = COALESCE(hs_matches.score_for,      EXCLUDED.score_for),
    score_against  = COALESCE(hs_matches.score_against,  EXCLUDED.score_against),
    team_level     = COALESCE(hs_matches.team_level,     EXCLUDED.team_level),
    tournament     = COALESCE(hs_matches.tournament,     EXCLUDED.tournament),
    round          = COALESCE(hs_matches.round,          EXCLUDED.round),
    season         = COALESCE(hs_matches.season,         EXCLUDED.season)
RETURNING (xmax = 0) AS inserted
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


_REQUIRED = ("school_name_raw", "school_state", "opponent_raw", "gender", "source_url")


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in _REQUIRED:
        value = row.get(key)
        if isinstance(value, str):
            value = value.strip()
        if not value:
            raise ValueError(f"hs_matches row missing {key}")
        out[key] = value
    out["match_date"] = row.get("match_date")  # may be None on brackets
    out["team_level"] = (row.get("team_level") or None)
    out["result"] = (row.get("result") or None)
    out["score_for"] = row.get("score_for")
    out["score_against"] = row.get("score_against")
    out["tournament"] = (row.get("tournament") or None)
    out["round"] = (row.get("round") or None)
    out["season"] = (row.get("season") or None)
    return out


def insert_hs_matches(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Insert or update a batch of ``hs_matches`` rows.

    Returns ``{"inserted": N, "updated": N, "skipped": N}``. Every
    conflict refreshes ``last_seen_at`` so conflict-hits count as
    updates.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[hs-matches-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[hs-matches-writer] skipping bad row: %s", exc)
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
                    cur.execute(_INSERT_HS_MATCH_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning(
                        "[hs-matches-writer] upsert failed for %s vs %s: %s",
                        row.get("school_name_raw"),
                        row.get("opponent_raw"),
                        exc,
                    )
                    counts["skipped"] += 1
                    conn.rollback()  # noqa: writer-rollback
                    continue
                if result is None:
                    continue
                if bool(result[0]):
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
        "event": "hs-matches-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
