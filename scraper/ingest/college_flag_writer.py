"""
college_flag_writer.py — Idempotent writes to ``college_roster_quality_flags``.

Three flag_types (enforced by a DB CHECK constraint):
  historical_no_data  — URL works for current season but not historical path
  partial_parse       — URL returned 200 but < threshold players parsed
  url_needs_review    — URL-level failure; reason stored in metadata

url_needs_review reason values (stored in metadata['reason']):
  no_url_at_all          — colleges.soccer_program_url IS NULL
  static_404             — URL exists but 404'd (school migrated CMS)
  playwright_exhausted   — Static + Playwright both failed
  partial_parse          — URL works but < threshold players
  historical_no_data     — Works for current but not historical season path
  current_zero_parse     — Current-season 200 but 0 players parsed

Design
------
write_college_flag() is intentionally narrow: the caller provides one
flag at a time. Dedup is enforced at the DB level via:

  ON CONFLICT ON CONSTRAINT college_roster_quality_flags_college_year_type_uq
  DO UPDATE SET metadata = ...

This makes repeated scraper runs idempotent — re-running the same failure
just refreshes the metadata timestamp; it does NOT create duplicate rows.

If the ``college_roster_quality_flags`` table doesn't exist yet (pre-PR-24
DB push), the function catches ``psycopg2.errors.UndefinedTable`` and logs
a warning, so PR-23 can import and call this safely before PR-24 ships.

psycopg2 is imported lazily so this module is importable without
DATABASE_URL set.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional

try:
    import psycopg2  # type: ignore
    import psycopg2.errors  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

log = logging.getLogger("college_flag_writer")

_VALID_FLAG_TYPES = frozenset({"historical_no_data", "partial_parse", "url_needs_review"})
_VALID_REASONS = frozenset({
    "no_url_at_all",
    "static_404",
    "playwright_exhausted",
    "partial_parse",
    "historical_no_data",
    "current_zero_parse",
})

_UPSERT_FLAG_SQL = """
INSERT INTO college_roster_quality_flags (
    college_id, academic_year, flag_type, metadata, created_at
)
VALUES (
    %(college_id)s, %(academic_year)s, %(flag_type)s,
    %(metadata)s::jsonb,
    NOW()
)
ON CONFLICT ON CONSTRAINT college_roster_quality_flags_college_year_type_uq
DO UPDATE SET
    metadata   = EXCLUDED.metadata,
    created_at = NOW()
WHERE college_roster_quality_flags.resolved_at IS NULL
RETURNING id, (xmax = 0) AS inserted
"""


def write_college_flag(
    *,
    college_id: int,
    academic_year: str,
    flag_type: str,
    metadata: Optional[Dict[str, Any]] = None,
    conn=None,
    dry_run: bool = False,
) -> Optional[Dict[str, Any]]:
    """Write one flag to ``college_roster_quality_flags``.

    Parameters
    ----------
    college_id    : FK to ``colleges.id``
    academic_year : e.g. ``"2025-26"``
    flag_type     : one of ``historical_no_data``, ``partial_parse``,
                    ``url_needs_review``
    metadata      : optional jsonb payload; for ``url_needs_review``,
                    include ``{"reason": "<reason_value>"}``
    conn          : existing psycopg2 connection; if None, opens its own
    dry_run       : if True, validate args and return without writing

    Returns
    -------
    dict with ``{"id": int, "inserted": bool}`` on success, or None on
    dry-run / table-missing.

    Raises
    ------
    ValueError   : invalid flag_type or url_needs_review reason
    RuntimeError : psycopg2 not available or DATABASE_URL not set
    """
    if flag_type not in _VALID_FLAG_TYPES:
        raise ValueError(
            f"invalid flag_type {flag_type!r}; must be one of {sorted(_VALID_FLAG_TYPES)}"
        )
    if flag_type == "url_needs_review":
        reason = (metadata or {}).get("reason")
        if reason not in _VALID_REASONS:
            raise ValueError(
                f"url_needs_review requires metadata['reason'] in {sorted(_VALID_REASONS)}"
                f" (got {reason!r})"
            )

    if dry_run:
        log.debug(
            "[college-flag][dry-run] college_id=%d year=%s flag=%s meta=%s",
            college_id, academic_year, flag_type, metadata,
        )
        return None

    if psycopg2 is None:
        raise RuntimeError("psycopg2 not available")

    own_conn = conn is None
    if own_conn:
        dsn = os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL not set")
        conn = psycopg2.connect(dsn)

    params = {
        "college_id": college_id,
        "academic_year": academic_year,
        "flag_type": flag_type,
        "metadata": json.dumps(metadata or {}),
    }

    try:
        with conn.cursor() as cur:
            try:
                cur.execute(_UPSERT_FLAG_SQL, params)
                row = cur.fetchone()
            except psycopg2.errors.UndefinedTable:
                log.warning(
                    "[college-flag] college_roster_quality_flags table does not exist yet "
                    "(pre-PR-24 db push); skipping flag write for college_id=%d",
                    college_id,
                )
                conn.rollback()
                return None
        conn.commit()
        if row is None:
            # ON CONFLICT DO UPDATE skipped because the flag is already resolved —
            # the WHERE clause (resolved_at IS NULL) excluded it.
            return None
        flag_id, inserted = row
        log.debug(
            "[college-flag] %s college_id=%d year=%s flag=%s id=%d",
            "inserted" if inserted else "refreshed",
            college_id, academic_year, flag_type, flag_id,
        )
        return {"id": flag_id, "inserted": bool(inserted)}
    except Exception:
        if own_conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if own_conn:
            try:
                conn.close()
            except Exception:
                pass
