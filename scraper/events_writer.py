"""
events_writer.py — Upsert scraped events + event_teams to Postgres.

Path A ``events`` and ``event_teams`` tables (lib/db/src/schema/events.ts).

Idempotency contract:
  events
    ON CONFLICT (source, platform_event_id) DO UPDATE
      SET name, slug, league_name, source_url, last_scraped_at
    (``events_source_platform_id_uq``)

  event_teams
    ON CONFLICT (event_id, team_name_raw) DO NOTHING
    (``event_teams_event_team_name_uq``)

    Matches the "race policy" docstring on the schema — duplicate raw
    names across runs are tolerated and the linker job collapses them
    later via ``canonical_club_id``.

This module never throws to the caller on DB errors. A failed upsert
returns ``(0, 0)`` and logs a warning; the scrape run logger records
``partial`` in that case.

psycopg2 is imported lazily so the module stays importable in tests
that don't need the DB.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

from extractors.sincsports_events import EventMeta, TeamRow

log = logging.getLogger("events_writer")


@dataclass
class WriteResult:
    events_created: int = 0
    events_updated: int = 0
    teams_created: int = 0
    teams_skipped: int = 0  # existing (event_id, team_name_raw)


def _connect(dsn: Optional[str] = None):
    """Open a one-shot connection. Callers should close it."""
    if psycopg2 is None:
        log.warning("psycopg2 not installed — skipping DB writes")
        return None
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        log.warning("DATABASE_URL not set — skipping DB writes")
        return None
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        return conn
    except Exception as exc:
        log.warning("events_writer: connect failed — %s", exc)
        return None


_UPSERT_EVENT_SQL = """
    INSERT INTO events (
        name, slug, league_name, season, source, platform_event_id,
        source_url, location_city, location_state, start_date, end_date,
        last_scraped_at
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
    ON CONFLICT ON CONSTRAINT events_source_platform_id_uq DO UPDATE
    SET name = EXCLUDED.name,
        slug = EXCLUDED.slug,
        league_name = COALESCE(EXCLUDED.league_name, events.league_name),
        source_url = EXCLUDED.source_url,
        location_city = COALESCE(EXCLUDED.location_city, events.location_city),
        location_state = COALESCE(EXCLUDED.location_state, events.location_state),
        start_date = COALESCE(EXCLUDED.start_date, events.start_date),
        end_date = COALESCE(EXCLUDED.end_date, events.end_date),
        last_scraped_at = now()
    RETURNING id, (xmax = 0) AS inserted
"""

_UPSERT_TEAM_SQL = """
    INSERT INTO event_teams (
        event_id, team_name_raw, team_name_canonical,
        age_group, gender, division_code, source_url, source
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT event_teams_event_team_name_uq DO NOTHING
"""


def upsert_event_and_teams(
    meta: EventMeta,
    teams: List[TeamRow],
    conn=None,
    dry_run: bool = False,
) -> WriteResult:
    """Upsert one event and all its teams in a single transaction.

    Pass an existing ``conn`` to batch multiple events in one process;
    otherwise a connection is opened and closed here.
    """
    result = WriteResult()

    if dry_run:
        log.info(
            "[dry-run] Would upsert event=%s slug=%s teams=%d",
            meta.platform_event_id, meta.slug, len(teams),
        )
        # Treat every team as a would-be insert for reporting.
        result.events_created = 1
        result.teams_created = len(teams)
        return result

    owns_conn = conn is None
    if conn is None:
        conn = _connect()
    if conn is None:
        return result

    try:
        with conn.cursor() as cur:
            cur.execute(
                _UPSERT_EVENT_SQL,
                (
                    meta.name,
                    meta.slug,
                    meta.league_name,
                    meta.season,
                    meta.source,
                    meta.platform_event_id,
                    meta.source_url,
                    meta.location_city,
                    meta.location_state,
                    meta.start_date,
                    meta.end_date,
                ),
            )
            row = cur.fetchone()
            if row is None:
                # Shouldn't happen — upsert always returns a row.
                conn.rollback()
                log.warning("events_writer: upsert returned no row for tid=%s", meta.platform_event_id)
                return result
            event_id, inserted = row[0], bool(row[1])
            if inserted:
                result.events_created += 1
            else:
                result.events_updated += 1

            for t in teams:
                cur.execute(
                    _UPSERT_TEAM_SQL,
                    (
                        event_id,
                        t.team_name_raw,
                        _canonical_for_team(t),
                        t.age_group,
                        t.gender,
                        t.division_code,
                        meta.source_url,
                        meta.source,
                    ),
                )
                if cur.rowcount == 1:
                    result.teams_created += 1
                else:
                    result.teams_skipped += 1

        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.warning(
            "events_writer: upsert failed for tid=%s — %s",
            meta.platform_event_id, exc,
        )
    finally:
        if owns_conn:
            try:
                conn.close()
            except Exception:
                pass

    return result


def _canonical_for_team(t: TeamRow) -> str:
    """Compute the canonical club name string stored on ``event_teams``.

    We use the ``club_name`` column from SincSports (which is typically
    cleaner than the "Team" column), lowercased and whitespace-collapsed.
    The definitive canonicalization happens in TS via ``toCanonicalName``
    when the nightly linker job resolves ``canonical_club_id``; this
    early value is only used as an indexing hint.
    """
    from extractors.sincsports_events import _canonicalize_name
    return _canonicalize_name(t.club_name)
