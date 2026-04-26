"""
roster_snapshot_writer.py — Idempotent upsert of ``club_roster_snapshots``
plus per-player event materialization into ``roster_diffs``.

See lib/db/src/schema/clubs-extended.ts for the snapshot table and
lib/db/src/schema/rosters-and-tryouts.ts for the diffs table.

Snapshot upsert
---------------
Name-keyed natural key via the Drizzle-named constraint
``club_roster_snapshots_name_season_age_gender_player_uq``:

    ON CONFLICT ON CONSTRAINT
      club_roster_snapshots_name_season_age_gender_player_uq
    DO UPDATE SET
      jersey_number, position, snapshot_date, source_url, scraped_at
    WHERE any of (jersey_number, position, source_url) IS DISTINCT FROM EXCLUDED
       OR snapshot_date differs

The WHERE predicate makes re-running the same scrape a no-op for
previously-captured rows — inserted counts reflect real roster churn
and updates reflect real mutable-field drift, not noise.

``club_id`` is intentionally left NULL. The canonical-club linker
(`scraper/canonical_club_linker.py`) resolves it after the scraper writes.

Diff materialization
--------------------
After inserting a batch for a given
``(club_name_raw, season, age_group, gender)`` group, this module
compares the current snapshot against the previous ``snapshot_date``
for the same group. Per-player event rows are emitted into
``roster_diffs`` via the named unique index
``roster_diffs_name_season_age_gender_player_type_uq`` with
``ON CONFLICT DO NOTHING`` — diffs are append-only history.

Diff types emitted:
  added             — player present now, absent in prior snapshot
  removed           — player present in prior, absent now
  jersey_changed    — jersey_number differs between prior and now
  position_changed  — position differs between prior and now

psycopg2 is imported lazily so this module stays importable without
DATABASE_URL (tests that only exercise extraction skip DB altogether).
"""

from __future__ import annotations

import json
import logging
import os
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger("roster_snapshot_writer")


_INSERT_SNAPSHOT_SQL = """
INSERT INTO club_roster_snapshots (
    club_id, club_name_raw, source_url, snapshot_date,
    season, age_group, gender, division,
    player_name, jersey_number, position,
    grad_year, hometown, state, country, nationality,
    college_commitment, academic_year, prev_club, league,
    event_id, scraped_at
)
VALUES (
    NULL, %(club_name_raw)s, %(source_url)s, %(snapshot_date)s,
    %(season)s, %(age_group)s, %(gender)s, %(division)s,
    %(player_name)s, %(jersey_number)s, %(position)s,
    %(grad_year)s, %(hometown)s, %(state)s, %(country)s, %(nationality)s,
    %(college_commitment)s, %(academic_year)s, %(prev_club)s, %(league)s,
    %(event_id)s, now()
)
ON CONFLICT ON CONSTRAINT club_roster_snapshots_name_season_age_gender_player_uq
DO UPDATE SET
    jersey_number      = EXCLUDED.jersey_number,
    position           = EXCLUDED.position,
    snapshot_date      = EXCLUDED.snapshot_date,
    source_url         = COALESCE(EXCLUDED.source_url, club_roster_snapshots.source_url),
    division           = COALESCE(EXCLUDED.division, club_roster_snapshots.division),
    grad_year          = COALESCE(EXCLUDED.grad_year, club_roster_snapshots.grad_year),
    hometown           = COALESCE(EXCLUDED.hometown, club_roster_snapshots.hometown),
    state              = COALESCE(EXCLUDED.state, club_roster_snapshots.state),
    country            = COALESCE(EXCLUDED.country, club_roster_snapshots.country),
    nationality        = COALESCE(EXCLUDED.nationality, club_roster_snapshots.nationality),
    college_commitment = COALESCE(EXCLUDED.college_commitment, club_roster_snapshots.college_commitment),
    academic_year      = COALESCE(EXCLUDED.academic_year, club_roster_snapshots.academic_year),
    prev_club          = COALESCE(EXCLUDED.prev_club, club_roster_snapshots.prev_club),
    league             = COALESCE(EXCLUDED.league, club_roster_snapshots.league),
    event_id           = COALESCE(EXCLUDED.event_id, club_roster_snapshots.event_id),
    scraped_at         = now()
WHERE club_roster_snapshots.jersey_number      IS DISTINCT FROM EXCLUDED.jersey_number
   OR club_roster_snapshots.position           IS DISTINCT FROM EXCLUDED.position
   OR club_roster_snapshots.source_url         IS DISTINCT FROM EXCLUDED.source_url
   OR club_roster_snapshots.division           IS DISTINCT FROM EXCLUDED.division
   OR club_roster_snapshots.grad_year          IS DISTINCT FROM EXCLUDED.grad_year
   OR club_roster_snapshots.hometown           IS DISTINCT FROM EXCLUDED.hometown
   OR club_roster_snapshots.state              IS DISTINCT FROM EXCLUDED.state
   OR club_roster_snapshots.country            IS DISTINCT FROM EXCLUDED.country
   OR club_roster_snapshots.nationality        IS DISTINCT FROM EXCLUDED.nationality
   OR club_roster_snapshots.college_commitment IS DISTINCT FROM EXCLUDED.college_commitment
   OR club_roster_snapshots.academic_year      IS DISTINCT FROM EXCLUDED.academic_year
   OR club_roster_snapshots.prev_club          IS DISTINCT FROM EXCLUDED.prev_club
   OR club_roster_snapshots.league             IS DISTINCT FROM EXCLUDED.league
RETURNING (xmax = 0) AS inserted
"""


_SELECT_PRIOR_SNAPSHOT_SQL = """
SELECT player_name, jersey_number, position
FROM   club_roster_snapshots
WHERE  club_name_raw        = %(club_name_raw)s
  AND  COALESCE(season, '') = COALESCE(%(season)s, '')
  AND  COALESCE(age_group, '') = COALESCE(%(age_group)s, '')
  AND  COALESCE(gender, '') = COALESCE(%(gender)s, '')
  AND  snapshot_date < %(snapshot_date)s
  AND  snapshot_date = (
        SELECT MAX(snapshot_date) FROM club_roster_snapshots
        WHERE club_name_raw        = %(club_name_raw)s
          AND COALESCE(season, '') = COALESCE(%(season)s, '')
          AND COALESCE(age_group, '') = COALESCE(%(age_group)s, '')
          AND COALESCE(gender, '') = COALESCE(%(gender)s, '')
          AND snapshot_date < %(snapshot_date)s
  )
"""


# roster_diffs is guarded by a UNIQUE INDEX (not a table constraint) —
# `ON CONFLICT ON CONSTRAINT <index_name>` works against index names too
# (see scraper/ingest/matches_writer.py doc block).
_INSERT_DIFF_SQL = """
INSERT INTO roster_diffs (
    club_id, club_name_raw, season, age_group, gender,
    player_name, diff_type,
    from_jersey_number, to_jersey_number,
    from_position, to_position,
    detected_at
)
VALUES (
    NULL, %(club_name_raw)s, %(season)s, %(age_group)s, %(gender)s,
    %(player_name)s, %(diff_type)s,
    %(from_jersey_number)s, %(to_jersey_number)s,
    %(from_position)s, %(to_position)s,
    now()
)
ON CONFLICT ON CONSTRAINT roster_diffs_name_season_age_gender_player_type_uq
DO NOTHING
RETURNING id
"""


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def _normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure every expected key exists, with None for optional fields."""
    if not row.get("club_name_raw"):
        raise ValueError("roster snapshot row missing club_name_raw")
    if not row.get("player_name"):
        raise ValueError("roster snapshot row missing player_name")
    if row.get("snapshot_date") is None:
        raise ValueError("roster snapshot row missing snapshot_date")
    return {
        "club_name_raw": row["club_name_raw"],
        "source_url": row.get("source_url"),
        "snapshot_date": row["snapshot_date"],
        "season": row.get("season"),
        "age_group": row.get("age_group"),
        "gender": row.get("gender"),
        "division": row.get("division"),
        "player_name": row["player_name"],
        "jersey_number": row.get("jersey_number"),
        "position": row.get("position"),
        "grad_year": row.get("grad_year"),
        "hometown": row.get("hometown"),
        "state": row.get("state"),
        "country": row.get("country"),
        "nationality": row.get("nationality"),
        "college_commitment": row.get("college_commitment"),
        "academic_year": row.get("academic_year"),
        "prev_club": row.get("prev_club"),
        "league": row.get("league"),
        "event_id": row.get("event_id"),
    }


def _group_key(r: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str], Optional[str]]:
    return (r["club_name_raw"], r.get("season"), r.get("age_group"), r.get("gender"))


def _compute_diff_rows(
    group_key: Tuple[str, Optional[str], Optional[str], Optional[str]],
    current: List[Dict[str, Any]],
    prior: List[Tuple[str, Optional[str], Optional[str]]],
) -> List[Dict[str, Any]]:
    """Compare prior vs current player lists and emit roster_diffs rows.

    Pure function — unit-testable without a DB.
    """
    club_name_raw, season, age_group, gender = group_key
    prior_by_name: Dict[str, Tuple[Optional[str], Optional[str]]] = {
        p_name: (p_jersey, p_pos) for (p_name, p_jersey, p_pos) in prior
    }
    current_by_name: Dict[str, Tuple[Optional[str], Optional[str]]] = {
        r["player_name"]: (r.get("jersey_number"), r.get("position")) for r in current
    }

    diffs: List[Dict[str, Any]] = []
    # added / jersey_changed / position_changed
    for name, (c_jersey, c_pos) in current_by_name.items():
        if name not in prior_by_name:
            diffs.append(dict(
                club_name_raw=club_name_raw, season=season, age_group=age_group, gender=gender,
                player_name=name, diff_type="added",
                from_jersey_number=None, to_jersey_number=c_jersey,
                from_position=None, to_position=c_pos,
            ))
            continue
        p_jersey, p_pos = prior_by_name[name]
        if (p_jersey or None) != (c_jersey or None):
            diffs.append(dict(
                club_name_raw=club_name_raw, season=season, age_group=age_group, gender=gender,
                player_name=name, diff_type="jersey_changed",
                from_jersey_number=p_jersey, to_jersey_number=c_jersey,
                from_position=None, to_position=None,
            ))
        if (p_pos or None) != (c_pos or None):
            diffs.append(dict(
                club_name_raw=club_name_raw, season=season, age_group=age_group, gender=gender,
                player_name=name, diff_type="position_changed",
                from_jersey_number=None, to_jersey_number=None,
                from_position=p_pos, to_position=c_pos,
            ))
    # removed
    for name, (p_jersey, p_pos) in prior_by_name.items():
        if name not in current_by_name:
            diffs.append(dict(
                club_name_raw=club_name_raw, season=season, age_group=age_group, gender=gender,
                player_name=name, diff_type="removed",
                from_jersey_number=p_jersey, to_jersey_number=None,
                from_position=p_pos, to_position=None,
            ))
    return diffs


def insert_roster_snapshots(
    rows: Sequence[Dict[str, Any]],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of roster snapshot rows and materialize diffs.

    Parameters
    ----------
    rows
        Iterable of row dicts (see module docstring for shape).
    conn
        Optional existing psycopg2 connection. If omitted, a new one is
        opened, used, and closed (autocommit off, explicit commit).
    dry_run
        When True, no DB I/O occurs; returns zero counts.

    Returns
    -------
    ``{"inserted": N, "updated": N, "skipped": N, "diffs_written": N}``.
    """
    counts = {"inserted": 0, "updated": 0, "skipped": 0, "diffs_written": 0}
    if not rows:
        return counts
    if dry_run:
        log.info("[roster-snapshot-writer] dry-run: would upsert %d rows", len(rows))
        return counts

    # Normalize + fail-loudly on missing required fields rather than
    # silently writing a partial row.
    normalized: List[Dict[str, Any]] = []
    for raw in rows:
        try:
            normalized.append(_normalize_row(raw))
        except ValueError as exc:
            log.warning("[roster-snapshot-writer] skipping bad row: %s", exc)
            counts["skipped"] += 1
    if not normalized:
        log.info(json.dumps({
            "event": "roster-snapshot-writer",
            "inserted": 0, "updated": 0, "skipped": counts["skipped"], "diffs_written": 0,
        }))
        return counts

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()

    try:
        with conn.cursor() as cur:
            # Group rows by (club_name_raw, season, age_group, gender) so we
            # can compute diffs per-group against the prior snapshot.
            groups: Dict[Tuple[str, Optional[str], Optional[str], Optional[str]], List[Dict[str, Any]]] = defaultdict(list)
            for r in normalized:
                groups[_group_key(r)].append(r)

            for gk, group_rows in groups.items():
                # Fetch prior snapshot BEFORE inserting the new batch.
                # FAIL-LOUD: previously this was wrapped in try/except
                # that on failure logged a warning, called conn.rollback,
                # and set prior=[]. That collapsed two distinct outcomes
                # — "no prior snapshot exists" (legitimate first scrape)
                # and "prior-lookup query errored" (transient DB blip)
                # — into the same code path: no diffs emitted. The
                # group then looked like a fresh first scrape forever
                # after, silently corrupting `roster_diffs` history.
                # Now: any exception propagates, the outer txn rolls
                # back on close, and the caller sees the real error.
                sample = group_rows[0]
                cur.execute(_SELECT_PRIOR_SNAPSHOT_SQL, {
                    "club_name_raw": sample["club_name_raw"],
                    "season": sample.get("season"),
                    "age_group": sample.get("age_group"),
                    "gender": sample.get("gender"),
                    "snapshot_date": sample["snapshot_date"],
                })
                prior = [(r[0], r[1], r[2]) for r in cur.fetchall()]

                # Insert/upsert every player row in the group. Per-row
                # SAVEPOINT isolates a bad row from the batch — a single
                # FK violation no longer rolls back the whole txn (which
                # would lose every prior successful insert in this group
                # AND every prior group's work).
                for row in group_rows:
                    cur.execute("SAVEPOINT snapshot_row")
                    try:
                        cur.execute(_INSERT_SNAPSHOT_SQL, row)
                        result = cur.fetchone()
                    except Exception as exc:
                        log.warning(
                            "[roster-snapshot-writer] upsert failed for %s / %s: %s",
                            row.get("club_name_raw"), row.get("player_name"), exc,
                        )
                        counts["skipped"] += 1
                        cur.execute("ROLLBACK TO SAVEPOINT snapshot_row")
                        continue
                    cur.execute("RELEASE SAVEPOINT snapshot_row")
                    if result is None:
                        # WHERE predicate short-circuited on re-scrape (no
                        # change). Count as neither insert nor update.
                        continue
                    inserted = bool(result[0])
                    if inserted:
                        counts["inserted"] += 1
                    else:
                        counts["updated"] += 1

                # Materialize diffs. Skip entirely if there was no prior
                # snapshot — the very first scrape is not a diff event.
                if not prior:
                    continue
                diff_rows = _compute_diff_rows(gk, group_rows, prior)
                for d in diff_rows:
                    cur.execute("SAVEPOINT diff_row")
                    try:
                        cur.execute(_INSERT_DIFF_SQL, d)
                        written = cur.fetchone()
                    except Exception as exc:
                        log.warning(
                            "[roster-snapshot-writer] diff insert failed for %s / %s: %s",
                            d.get("club_name_raw"), d.get("player_name"), exc,
                        )
                        cur.execute("ROLLBACK TO SAVEPOINT diff_row")
                        continue
                    cur.execute("RELEASE SAVEPOINT diff_row")
                    if written is not None:
                        counts["diffs_written"] += 1

        if own_conn:
            conn.commit()
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    log.info(json.dumps({
        "event": "roster-snapshot-writer",
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
        "diffs_written": counts["diffs_written"],
    }))
    return counts
