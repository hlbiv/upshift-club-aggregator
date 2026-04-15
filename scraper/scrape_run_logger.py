"""
scrape_run_logger.py — Persist scraper invocation outcomes to
`scrape_run_logs` in Postgres.

Fits under Domain 8 of the Path A data model. The enum values in
`FailureKind` below MUST stay in sync with the check constraint on
`scrape_run_logs.failure_kind` — see lib/db/src/schema/scrape-health.ts.

The logger is defensive by design:
  - If `DATABASE_URL` is not set, it no-ops silently. Local dev runs
    that don't touch the DB still work.
  - If a write fails (network blip, transient pg error, schema missing
    because drizzle push hasn't run yet), it logs a WARNING and
    continues. Scraping must never be blocked by the log writer.
  - `records_touched` is NOT written — it is a STORED generated column.

Usage (in run.py):

    from scrape_run_logger import (
        ScrapeRunLogger, FailureKind, classify_exception,
    )

    logger_ctx = ScrapeRunLogger(scraper_key="ecnl-boys",
                                 league_name="ECNL Boys National")
    logger_ctx.start(source_url=url)
    try:
        ... do work ...
        logger_ctx.finish_ok(records_created=n)
    except Exception as exc:
        kind = classify_exception(exc)
        logger_ctx.finish_failed(kind, str(exc))
        raise
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover — tested envs have psycopg2
    psycopg2 = None  # type: ignore

log = logging.getLogger("scrape_run_logger")


class FailureKind(str, Enum):
    """
    Matches the Postgres check constraint
    `scrape_run_logs_failure_kind_enum` exactly. Keep in sync with
    lib/db/src/schema/scrape-health.ts.
    """

    TIMEOUT = "timeout"
    NETWORK = "network"
    PARSE_ERROR = "parse_error"
    ZERO_RESULTS = "zero_results"
    UNKNOWN = "unknown"


class Status(str, Enum):
    RUNNING = "running"
    OK = "ok"
    PARTIAL = "partial"
    FAILED = "failed"


_TIMEOUT_MARKERS = ("timeout", "timed out", "timeouterror")
_NETWORK_MARKERS = (
    "connectionerror", "connection", "network", "dns",
    "err_name_not_resolved", "err_connection", "err_internet",
    "transient",
)
_PARSE_MARKERS = (
    "beautifulsoup", "parseerror", "parse", "valueerror",
    "keyerror", "attributeerror", "indexerror",
)


def classify_exception(exc: BaseException) -> FailureKind:
    """Map an exception to a FailureKind using message content and type."""
    msg = str(exc).lower()
    exc_type = type(exc).__name__.lower()

    if any(m in msg or m in exc_type for m in _TIMEOUT_MARKERS):
        return FailureKind.TIMEOUT
    if any(m in msg or m in exc_type for m in _NETWORK_MARKERS):
        return FailureKind.NETWORK
    if isinstance(exc, (ValueError, KeyError, AttributeError, IndexError)):
        return FailureKind.PARSE_ERROR
    if any(m in msg or m in exc_type for m in _PARSE_MARKERS):
        return FailureKind.PARSE_ERROR
    return FailureKind.UNKNOWN


# Module-level lazy singleton connection. `scrape_run_logs` writes are
# tiny (one INSERT + one UPDATE per scrape), so a single autocommit
# connection amortised across the whole run is the right shape — opening
# per-call cost two connections per league × ~hundreds of leagues.
_CONN = None
_CONN_FAILED = False


def _conn():
    global _CONN, _CONN_FAILED
    if psycopg2 is None or _CONN_FAILED:
        return None
    if _CONN is not None:
        # Cheap liveness check: psycopg2 sets `.closed` > 0 if the conn
        # dropped. Reconnect silently rather than spraying warnings.
        try:
            if getattr(_CONN, "closed", 0) == 0:
                return _CONN
        except Exception:
            pass
        _CONN = None
    url = os.environ.get("DATABASE_URL")
    if not url:
        _CONN_FAILED = True
        return None
    try:
        c = psycopg2.connect(url)
        c.autocommit = True
        _CONN = c
        return _CONN
    except Exception as exc:
        log.warning("scrape_run_logger: connect failed — %s", exc)
        _CONN_FAILED = True
        return None


def close_connection() -> None:
    """Close the module-level connection. Call at end of process."""
    global _CONN
    if _CONN is not None:
        try:
            _CONN.close()
        except Exception:
            pass
        _CONN = None


@dataclass
class ScrapeRunLogger:
    """
    One instance per scraper invocation.

    Lifecycle:
        logger = ScrapeRunLogger(scraper_key="ecnl-boys", league_name=...)
        logger.start(source_url=url)
        ...
        logger.finish_ok(records_created=N) | finish_failed(kind, msg)

    If the INSERT on start() fails, the logger silently degrades —
    finish_*() becomes a no-op. Scraping always wins.
    """

    scraper_key: str
    league_name: Optional[str] = None
    run_id: Optional[int] = field(default=None, init=False)

    def start(self, source_url: Optional[str] = None) -> None:
        conn = _conn()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scrape_run_logs
                        (scraper_key, league_name, status, source_url)
                    VALUES (%s, %s, 'running', %s)
                    RETURNING id
                    """,
                    (self.scraper_key, self.league_name, source_url),
                )
                row = cur.fetchone()
                if row is not None:
                    self.run_id = row[0]
        except Exception as exc:
            log.warning("scrape_run_logger: start failed — %s", exc)
            # Drop the cached conn so the next call reconnects. Covers
            # server-side kills (pg idle timeout, admin terminate,
            # network RST) which don't set `.closed`.
            global _CONN
            _CONN = None

    def _finish(
        self,
        status: Status,
        failure_kind: Optional[FailureKind] = None,
        records_created: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        if self.run_id is None:
            return
        conn = _conn()
        if conn is None:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE scrape_run_logs
                    SET completed_at    = now(),
                        status          = %s,
                        failure_kind    = %s,
                        records_created = %s,
                        records_updated = %s,
                        records_failed  = %s,
                        error_message   = %s
                    WHERE id = %s
                    """,
                    (
                        status.value,
                        failure_kind.value if failure_kind else None,
                        records_created,
                        records_updated,
                        records_failed,
                        (error_message or "")[:4000] or None,
                        self.run_id,
                    ),
                )
        except Exception as exc:
            log.warning("scrape_run_logger: finish failed — %s", exc)
            # Drop the cached conn so the next call reconnects.
            global _CONN
            _CONN = None

    def finish_ok(
        self,
        records_created: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
    ) -> None:
        self._finish(
            Status.OK,
            records_created=records_created,
            records_updated=records_updated,
            records_failed=records_failed,
        )

    def finish_partial(
        self,
        records_created: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        self._finish(
            Status.PARTIAL,
            records_created=records_created,
            records_updated=records_updated,
            records_failed=records_failed,
            error_message=error_message,
        )

    def finish_failed(
        self,
        kind: FailureKind,
        error_message: Optional[str] = None,
    ) -> None:
        self._finish(
            Status.FAILED,
            failure_kind=kind,
            error_message=error_message,
        )
