"""
ncaa_roster_writer.py — Idempotent upserts for NCAA D1 single-school runs.

Writes to three tables in ``lib/db/src/schema/colleges.ts``:

- ``colleges``            — one row per (name, division, gender_program),
                            upserted on the natural-key unique
                            ``colleges_name_division_gender_uq``.
- ``college_coaches``     — one row per (college_id, name, title),
                            upserted on ``college_coaches_college_name_title_uq``.
- ``college_roster_history`` — one row per (college_id, player_name, academic_year),
                               upserted on ``college_roster_history_college_player_year_uq``.

Single-school MVP design
------------------------
The sibling player-platform ``routes/schools.ts`` needs row-level reads
of these three tables. This writer is deliberately narrow: the caller
hands in one program at a time (URL + parsed output) and gets three
counts back. Enumeration across D1 is a follow-up PR.

psycopg2 is imported lazily so this module stays importable without
DATABASE_URL set (mirrors the pattern in ``ynt_writer`` and
``roster_snapshot_writer``).
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger("ncaa_roster_writer")


# ---------------------------------------------------------------------------
# SQL — one statement per table, all upserts hit an existing named
# unique index with ON CONFLICT. Following the house rule used by
# matches_writer + ynt_writer: hand-roll named-conflict upserts in
# psycopg2 rather than going through Drizzle.
# ---------------------------------------------------------------------------

_UPSERT_COLLEGE_SQL = """
INSERT INTO colleges (
    name, slug, division, gender_program,
    conference, state, city, website,
    soccer_program_url, ncaa_id,
    last_scraped_at, scrape_confidence
)
VALUES (
    %(name)s, %(slug)s, %(division)s, %(gender_program)s,
    %(conference)s, %(state)s, %(city)s, %(website)s,
    %(soccer_program_url)s, %(ncaa_id)s,
    now(), %(scrape_confidence)s
)
ON CONFLICT ON CONSTRAINT colleges_name_division_gender_uq
DO UPDATE SET
    conference         = COALESCE(EXCLUDED.conference, colleges.conference),
    state              = COALESCE(EXCLUDED.state, colleges.state),
    city               = COALESCE(EXCLUDED.city, colleges.city),
    website            = COALESCE(EXCLUDED.website, colleges.website),
    soccer_program_url = COALESCE(EXCLUDED.soccer_program_url, colleges.soccer_program_url),
    ncaa_id            = COALESCE(EXCLUDED.ncaa_id, colleges.ncaa_id),
    last_scraped_at    = now(),
    scrape_confidence  = EXCLUDED.scrape_confidence
RETURNING id, (xmax = 0) AS inserted
"""


_UPSERT_COACH_SQL = """
INSERT INTO college_coaches (
    college_id, name, title, email, phone,
    is_head_coach, source, source_url,
    scraped_at, confidence,
    first_seen_at, last_seen_at
)
VALUES (
    %(college_id)s, %(name)s, %(title)s, %(email)s, %(phone)s,
    %(is_head_coach)s, %(source)s, %(source_url)s,
    now(), %(confidence)s,
    now(), now()
)
ON CONFLICT ON CONSTRAINT college_coaches_college_name_title_uq
DO UPDATE SET
    email         = COALESCE(EXCLUDED.email, college_coaches.email),
    phone         = COALESCE(EXCLUDED.phone, college_coaches.phone),
    is_head_coach = EXCLUDED.is_head_coach,
    source        = EXCLUDED.source,
    source_url    = EXCLUDED.source_url,
    scraped_at    = now(),
    confidence    = EXCLUDED.confidence,
    last_seen_at  = now()
RETURNING (xmax = 0) AS inserted
"""


_UPSERT_ROSTER_SQL = """
INSERT INTO college_roster_history (
    college_id, player_name, position, year,
    academic_year, hometown, prev_club, jersey_number,
    scraped_at
)
VALUES (
    %(college_id)s, %(player_name)s, %(position)s, %(year)s,
    %(academic_year)s, %(hometown)s, %(prev_club)s, %(jersey_number)s,
    now()
)
ON CONFLICT ON CONSTRAINT college_roster_history_college_player_year_uq
DO UPDATE SET
    position      = EXCLUDED.position,
    year          = EXCLUDED.year,
    hometown      = COALESCE(EXCLUDED.hometown, college_roster_history.hometown),
    prev_club     = COALESCE(EXCLUDED.prev_club, college_roster_history.prev_club),
    jersey_number = EXCLUDED.jersey_number,
    scraped_at    = now()
RETURNING (xmax = 0) AS inserted
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_connection():
    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(dsn)


def slugify(text: str) -> str:
    """Lowercase ASCII slug. Collapses whitespace + punctuation to '-'."""
    t = text.strip().lower()
    t = re.sub(r"[^a-z0-9]+", "-", t)
    return t.strip("-")


def _normalize_college(row: Dict[str, Any]) -> Dict[str, Any]:
    if not row.get("name"):
        raise ValueError("college row missing name")
    division = row.get("division")
    if division not in ("D1", "D2", "D3", "NAIA", "NJCAA"):
        raise ValueError(f"college row invalid division: {division!r}")
    gender = row.get("gender_program")
    if gender not in ("mens", "womens", "both"):
        raise ValueError(f"college row invalid gender_program: {gender!r}")

    slug = row.get("slug") or f"{slugify(row['name'])}-{division.lower()}-{gender}"
    return {
        "name": row["name"],
        "slug": slug,
        "division": division,
        "gender_program": gender,
        "conference": row.get("conference"),
        "state": row.get("state"),
        "city": row.get("city"),
        "website": row.get("website"),
        "soccer_program_url": row.get("soccer_program_url"),
        "ncaa_id": row.get("ncaa_id"),
        "scrape_confidence": row.get("scrape_confidence", 0.9),
    }


def _normalize_coach(row: Dict[str, Any], *, college_id: int) -> Dict[str, Any]:
    if not row.get("name"):
        raise ValueError("coach row missing name")
    return {
        "college_id": college_id,
        "name": row["name"],
        "title": row.get("title"),
        "email": row.get("email"),
        "phone": row.get("phone"),
        "is_head_coach": bool(row.get("is_head_coach", False)),
        "source": row.get("source", "ncaa_roster_page"),
        "source_url": row.get("source_url"),
        "confidence": row.get("confidence", 0.9),
    }


def _normalize_player(
    row: Dict[str, Any], *, college_id: int, academic_year: str
) -> Dict[str, Any]:
    if not row.get("player_name"):
        raise ValueError("roster row missing player_name")
    return {
        "college_id": college_id,
        "player_name": row["player_name"],
        "position": row.get("position"),
        "year": row.get("year"),
        "academic_year": academic_year,
        "hometown": row.get("hometown"),
        "prev_club": row.get("prev_club"),
        "jersey_number": row.get("jersey_number"),
    }


# ---------------------------------------------------------------------------
# Public writer API
# ---------------------------------------------------------------------------


def upsert_college(
    college: Dict[str, Any],
    *,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Tuple[Optional[int], bool]:
    """Upsert a single ``colleges`` row.

    Returns ``(college_id, inserted)``. In dry-run mode returns
    ``(None, False)`` without touching the DB.
    """
    normalized = _normalize_college(college)
    if dry_run:
        log.info("[ncaa-roster-writer] dry-run: would upsert college %s", normalized["name"])
        return None, False

    own_conn = conn is None
    if own_conn:
        conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_UPSERT_COLLEGE_SQL, normalized)
            row = cur.fetchone()
        if own_conn:
            conn.commit()
        if row is None:
            return None, False
        return int(row[0]), bool(row[1])
    finally:
        if own_conn and conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def upsert_coaches(
    coaches: Sequence[Dict[str, Any]],
    *,
    college_id: int,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of ``college_coaches`` rows for a given college."""
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not coaches:
        return counts
    if dry_run:
        log.info("[ncaa-roster-writer] dry-run: would upsert %d coaches", len(coaches))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in coaches:
        try:
            normalized.append(_normalize_coach(raw, college_id=college_id))
        except ValueError as exc:
            log.warning("[ncaa-roster-writer] bad coach row: %s", exc)
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
                    cur.execute(_UPSERT_COACH_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning("[ncaa-roster-writer] coach upsert failed for %s: %s",
                                row.get("name"), exc)
                    counts["skipped"] += 1
                    conn.rollback()
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
    return counts


def upsert_roster_players(
    players: Sequence[Dict[str, Any]],
    *,
    college_id: int,
    academic_year: str,
    conn: Optional[Any] = None,
    dry_run: bool = False,
) -> Dict[str, int]:
    """Upsert a batch of ``college_roster_history`` rows for a given college+season."""
    counts = {"inserted": 0, "updated": 0, "skipped": 0}
    if not players:
        return counts
    if dry_run:
        log.info("[ncaa-roster-writer] dry-run: would upsert %d roster rows", len(players))
        return counts

    normalized: List[Dict[str, Any]] = []
    for raw in players:
        try:
            normalized.append(_normalize_player(
                raw, college_id=college_id, academic_year=academic_year,
            ))
        except ValueError as exc:
            log.warning("[ncaa-roster-writer] bad roster row: %s", exc)
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
                    cur.execute(_UPSERT_ROSTER_SQL, row)
                    result = cur.fetchone()
                except Exception as exc:
                    log.warning("[ncaa-roster-writer] roster upsert failed for %s: %s",
                                row.get("player_name"), exc)
                    counts["skipped"] += 1
                    conn.rollback()
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
        "event": "ncaa-roster-writer",
        "college_id": college_id,
        "academic_year": academic_year,
        "inserted": counts["inserted"],
        "updated": counts["updated"],
        "skipped": counts["skipped"],
    }))
    return counts
