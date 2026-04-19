"""
scrape_run_logger.py — Persist scraper invocation outcomes to
`scrape_run_logs` in Postgres.

Fits under Domain 8 of the Path A data model. The enum values in
`FailureKind` below MUST stay in sync with the check constraint on
`scrape_run_logs.failure_kind` — see lib/db/src/schema/scrape-health.ts.

The logger is defensive by design:
  - If `DATABASE_URL` is not set, it no-ops silently. Local dev runs
    that don't touch the DB still work.
  - If the DB is unreachable or a write fails mid-run, the logger
    FALLS BACK to an append-only JSONL file under `scraper/logs/`.
    Previously this code latched a `_CONN_FAILED` flag and silently
    no-opped the rest of the session — that lost every log row for the
    scrape session, invisibly, exactly when the logs would have been
    most useful. The JSONL fallback is drained back into the DB on the
    next successful connection attempt.
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

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

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

# Process-wide UUID stamped into every fallback JSONL line. Lets the
# drain loop correlate records with the specific process instance that
# wrote them (useful when several scrape processes share a host and one
# of them crashes with a partially-drained file).
_PROCESS_FALLBACK_RUN_ID = str(uuid.uuid4())

# One-shot warning guard so we don't spam stderr per log call when the
# DB is down for the whole run.
_FALLBACK_WARNED = False


def _logs_dir() -> str:
    """Directory where the JSONL fallback files live.

    Colocated with the scraper module so it "just works" on Replit and
    locally without touching env vars. Override by setting
    `SCRAPE_RUN_LOGGER_FALLBACK_DIR` for tests.
    """
    override = os.environ.get("SCRAPE_RUN_LOGGER_FALLBACK_DIR")
    if override:
        return override
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def _fallback_path() -> str:
    """Today's JSONL fallback path, date-stamped in UTC."""
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return os.path.join(_logs_dir(), f"scrape_run_logs.{day}.jsonl")


def _iter_pending_fallback_files() -> List[str]:
    """All undrained JSONL files, in lexicographic (≈ chronological) order."""
    d = _logs_dir()
    if not os.path.isdir(d):
        return []
    out: List[str] = []
    for name in sorted(os.listdir(d)):
        if name.startswith("scrape_run_logs.") and name.endswith(".jsonl"):
            # Skip already-drained rotations.
            if name.endswith(".drained.jsonl"):
                continue
            out.append(os.path.join(d, name))
    return out


def _write_fallback(event: Dict[str, Any]) -> str:
    """Append one JSONL line to today's fallback file. Returns the path."""
    global _FALLBACK_WARNED
    path = _fallback_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    event = dict(event)
    event.setdefault("_fallback_run_id", _PROCESS_FALLBACK_RUN_ID)
    event["_logged_at"] = datetime.now(timezone.utc).isoformat()
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, default=str) + "\n")
    if not _FALLBACK_WARNED:
        logging.warning(
            "[scrape_run_logger] DB unreachable; logging to %s", path,
        )
        _FALLBACK_WARNED = True
    return path


def _db_configured() -> bool:
    """True if the operator has asked us to talk to Postgres at all.

    `DATABASE_URL` unset is the "local dev, no DB" signal — we silent
    no-op in that case rather than spilling JSONL files into the repo.
    The JSONL fallback only activates when the caller has declared a
    DB is expected (URL set) but the connection can't be established
    or a write fails.
    """
    return psycopg2 is not None and bool(os.environ.get("DATABASE_URL"))


def _try_connect() -> Optional[Any]:
    """Attempt a raw psycopg2 connect. Returns None on failure."""
    if not _db_configured():
        return None
    try:
        c = psycopg2.connect(os.environ["DATABASE_URL"])
        c.autocommit = True
        return c
    except Exception as exc:
        log.warning("scrape_run_logger: connect failed — %s", exc)
        return None


def _conn():
    """Return a live psycopg2 connection, or None if unreachable.

    No-op latching has been removed — callers that get None AND have a
    DB configured MUST route their payload through the JSONL fallback
    instead (see `_db_configured()`). On every call we try to
    reconnect if the cached conn is dead, which means the *next* call
    after a DB outage will transparently resume DB writes (and trigger
    a drain of the fallback file).
    """
    global _CONN
    if _CONN is not None:
        try:
            if getattr(_CONN, "closed", 0) == 0:
                return _CONN
        except Exception:
            pass
        _CONN = None
    _CONN = _try_connect()
    return _CONN


def close_connection() -> None:
    """Close the module-level connection. Call at end of process.

    Also attempts a final drain of any pending JSONL fallback files so
    a short mid-run outage that recovered doesn't leave rows stranded
    on disk when the process exits.
    """
    global _CONN
    # Best-effort drain before tearing down. If it fails we swallow;
    # the JSONL file stays on disk for the next process to pick up.
    try:
        drain_fallback_if_any()
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("scrape_run_logger: pre-close drain failed — %s", exc)
    if _CONN is not None:
        try:
            _CONN.close()
        except Exception:
            pass
        _CONN = None


# ---------------------------------------------------------------------------
# JSONL drain
# ---------------------------------------------------------------------------

def _parse_fallback_file(path: str) -> List[Dict[str, Any]]:
    """Load and JSON-decode every line of a JSONL file.

    Malformed lines are logged and skipped rather than failing the whole
    drain — a half-written trailing line from a kill -9 shouldn't
    quarantine the rest of the file.
    """
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError as exc:
                log.warning(
                    "scrape_run_logger: skipping malformed fallback line "
                    "%s:%d — %s", path, lineno, exc,
                )
    return out


def _consolidate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collapse (start, finish) pairs keyed by client_run_id into one row.

    Each ScrapeRunLogger instance emits at most one start event and one
    finish event, both stamped with the same `client_run_id`. The final
    DB row should reflect the finish state (or stay at status='running'
    if only the start made it to disk — e.g. if the process was killed
    between start and finish).
    """
    by_key: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for ev in events:
        key = ev.get("client_run_id") or ev.get("_logged_at") or str(len(order))
        if key not in by_key:
            by_key[key] = dict(ev)
            order.append(key)
            continue
        merged = by_key[key]
        # Finish events override start fields. Preserve the original
        # started_at from the start event.
        started_at = merged.get("started_at") or ev.get("started_at")
        merged.update({k: v for k, v in ev.items() if v is not None})
        if started_at:
            merged["started_at"] = started_at
    return [by_key[k] for k in order]


def _drain_events_to_db(conn, events: List[Dict[str, Any]]) -> int:
    """Insert drained events into scrape_run_logs, skipping dupes.

    Dedup strategy: we can't add a schema migration in this PR, so we
    use an in-app SELECT against `(scraper_key, started_at)` — that
    combination is effectively unique for a single process because
    `started_at` carries microsecond precision from Python's datetime.
    We also tag `error_message` with the client_run_id for forensic
    audit so operators can tie a row back to the original JSONL event.

    Returns number of rows inserted.
    """
    if not events:
        return 0
    inserted = 0
    with conn.cursor() as cur:
        for ev in events:
            scraper_key = ev.get("scraper_key")
            started_at = ev.get("started_at")
            if not scraper_key or not started_at:
                log.warning(
                    "scrape_run_logger: skipping fallback event with "
                    "missing scraper_key/started_at: %s",
                    {k: ev.get(k) for k in ("scraper_key", "started_at", "client_run_id")},
                )
                continue
            # Dedup: same (scraper_key, started_at) already present?
            cur.execute(
                "SELECT 1 FROM scrape_run_logs "
                "WHERE scraper_key = %s AND started_at = %s LIMIT 1",
                (scraper_key, started_at),
            )
            if cur.fetchone() is not None:
                continue
            client_run_id = ev.get("client_run_id") or ""
            error_message = ev.get("error_message")
            # Tag the client_run_id into error_message for audit. Keep
            # within the 4000-char cap enforced by _finish().
            if client_run_id:
                tag = f"[client_run_id={client_run_id}]"
                if error_message:
                    error_message = f"{tag} {error_message}"[:4000]
                else:
                    error_message = tag
            cur.execute(
                """
                INSERT INTO scrape_run_logs
                    (scraper_key, league_name, started_at, completed_at,
                     status, failure_kind, records_created, records_updated,
                     records_failed, error_message, source_url,
                     triggered_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    scraper_key,
                    ev.get("league_name"),
                    started_at,
                    ev.get("completed_at"),
                    ev.get("status") or "running",
                    ev.get("failure_kind"),
                    int(ev.get("records_created") or 0),
                    int(ev.get("records_updated") or 0),
                    int(ev.get("records_failed") or 0),
                    error_message,
                    ev.get("source_url"),
                    # Fall back to 'manual' if an older JSONL row (pre
                    # this PR) is drained — matches the column default.
                    ev.get("triggered_by") or "manual",
                ),
            )
            inserted += 1
    return inserted


def drain_fallback_if_any(conn=None) -> int:
    """Drain every pending JSONL file into `scrape_run_logs`.

    Called opportunistically at the top of each log call (cheap: the
    directory listing short-circuits when nothing's pending). Also
    called on process exit via `close_connection()`.

    If the drain of a file fails mid-way, the file is left in place
    (not renamed) so the next attempt retries. The in-app dedup makes
    the retry idempotent. Errors are logged, never swallowed — they'd
    go into the very log table we're trying to fill, so we only have
    stderr.
    """
    if not _db_configured() and conn is None:
        return 0
    pending = _iter_pending_fallback_files()
    if not pending:
        return 0
    owned_conn = False
    if conn is None:
        conn = _try_connect()
        owned_conn = conn is not None
        if conn is None:
            return 0
    total_inserted = 0
    try:
        for path in pending:
            try:
                events = _parse_fallback_file(path)
                if not events:
                    _rename_drained(path)
                    continue
                consolidated = _consolidate_events(events)
                inserted = _drain_events_to_db(conn, consolidated)
                total_inserted += inserted
                _rename_drained(path)
            except Exception as exc:
                log.error(
                    "scrape_run_logger: drain failed for %s — %s "
                    "(leaving file in place for retry)",
                    path, exc,
                )
                # Don't rename; keep the file so the next connection
                # attempt can retry. Don't swallow silently.
                raise
    finally:
        if owned_conn:
            try:
                conn.close()
            except Exception:
                pass
    return total_inserted


def _rename_drained(path: str) -> None:
    """Rename a fully-drained JSONL file to `.drained.jsonl`.

    We don't delete — keep the file as an append-only audit trail.
    Operators (or a daily logrotate job) can prune `*.drained.jsonl`
    on whatever cadence they want.
    """
    root, ext = os.path.splitext(path)
    target = f"{root}.drained{ext}"
    # Collision handling — if a previous partial drain already produced
    # a .drained file for today, suffix with a short UUID so we never
    # clobber audit history.
    if os.path.exists(target):
        target = f"{root}.drained.{uuid.uuid4().hex[:8]}{ext}"
    os.replace(path, target)


# ---------------------------------------------------------------------------
# Public logger
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _triggered_by() -> str:
    """Return the trigger source for this process.

    Read from `SCRAPE_TRIGGERED_BY` at log time (not import time) so
    per-invocation env vars set by wrapper scripts like
    `scraper/scheduled/*.sh` are honoured. Unset / empty string falls
    back to `manual`, which matches the DB column default on
    `scrape_run_logs` — any operator-invoked run without the wrapper
    gets stamped `manual`. Keep in sync with the
    `scrape_run_logs.triggered_by` column in
    lib/db/src/schema/scrape-health.ts.
    """
    return os.environ.get("SCRAPE_TRIGGERED_BY") or "manual"


@dataclass
class ScrapeRunLogger:
    """
    One instance per scraper invocation.

    Lifecycle:
        logger = ScrapeRunLogger(scraper_key="ecnl-boys", league_name=...)
        logger.start(source_url=url)
        ...
        logger.finish_ok(records_created=N) | finish_failed(kind, msg)

    If the DB is unreachable on `start()`, we generate a synthetic
    `run_id` (a UUID) locally so `finish_*()` still has something to
    hang on to. The synthetic id is serialised into the JSONL fallback
    rather than the DB. When the drain runs, the start+finish events
    are consolidated back into a single `scrape_run_logs` row.
    """

    scraper_key: str
    league_name: Optional[str] = None
    run_id: Optional[int] = field(default=None, init=False)
    # client_run_id is always present; ties start+finish together even
    # when the DB never issues a real `run_id`. Stored into JSONL.
    client_run_id: str = field(
        default_factory=lambda: str(uuid.uuid4()), init=False,
    )
    _started_at_iso: Optional[str] = field(default=None, init=False)
    _source_url: Optional[str] = field(default=None, init=False)
    # Set from SCRAPE_TRIGGERED_BY env var inside start(). Declared here
    # so accessors (fallback writers, finish()) don't trip AttributeError
    # if they fire before start() runs. Matches the DB column default.
    _triggered_by: str = field(default="manual", init=False)

    def start(self, source_url: Optional[str] = None) -> None:
        # No DB configured → silent no-op. Matches pre-PR behaviour for
        # local dev runs that deliberately don't set DATABASE_URL.
        if not _db_configured():
            return

        # Opportunistic drain of any JSONL left by previous process.
        try:
            drain_fallback_if_any()
        except Exception as exc:  # pragma: no cover — defensive
            log.warning("scrape_run_logger: start-time drain failed — %s", exc)

        self._source_url = source_url
        self._started_at_iso = _now_iso()
        # Capture at start-time so start/finish/drain all see the same
        # value even if the env var is mutated mid-run.
        self._triggered_by = _triggered_by()

        conn = _conn()
        if conn is None:
            self._spill_start_to_fallback()
            return
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO scrape_run_logs
                        (scraper_key, league_name, status, source_url,
                         triggered_by)
                    VALUES (%s, %s, 'running', %s, %s)
                    RETURNING id, started_at
                    """,
                    (
                        self.scraper_key,
                        self.league_name,
                        source_url,
                        self._triggered_by,
                    ),
                )
                row = cur.fetchone()
                if row is not None:
                    self.run_id = row[0]
                    # Replace the optimistic client-stamped started_at
                    # with the DB's authoritative value so a later
                    # finish_* UPDATE doesn't mis-correlate.
                    if row[1] is not None:
                        self._started_at_iso = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
        except Exception as exc:
            log.warning(
                "scrape_run_logger: start failed — %s; "
                "falling back to JSONL", exc,
            )
            # Drop the cached conn so the next call reconnects.
            global _CONN
            _CONN = None
            self._spill_start_to_fallback()

    def _spill_start_to_fallback(self) -> None:
        _write_fallback({
            "event": "start",
            "client_run_id": self.client_run_id,
            "scraper_key": self.scraper_key,
            "league_name": self.league_name,
            "started_at": self._started_at_iso,
            "status": "running",
            "source_url": self._source_url,
            "triggered_by": self._triggered_by,
        })

    def _finish(
        self,
        status: Status,
        failure_kind: Optional[FailureKind] = None,
        records_created: int = 0,
        records_updated: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        # Mirror start(): if the caller opted out of DB logging entirely
        # by leaving DATABASE_URL unset, finish_*() is a silent no-op.
        if not _db_configured():
            return

        completed_at = _now_iso()
        payload = {
            "event": "finish",
            "client_run_id": self.client_run_id,
            "scraper_key": self.scraper_key,
            "league_name": self.league_name,
            "started_at": self._started_at_iso,
            "completed_at": completed_at,
            "status": status.value,
            "failure_kind": failure_kind.value if failure_kind else None,
            "records_created": records_created,
            "records_updated": records_updated,
            "records_failed": records_failed,
            "error_message": (error_message or "")[:4000] or None,
            "source_url": self._source_url,
            "triggered_by": self._triggered_by,
        }

        conn = _conn()
        if conn is None:
            _write_fallback(payload)
            return

        # If start() never got a real run_id (because start() itself
        # spilled to JSONL), we can't UPDATE — there's no row yet.
        # Send the whole payload to the fallback so the drain loop
        # inserts a consolidated row.
        if self.run_id is None:
            _write_fallback(payload)
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
            log.warning(
                "scrape_run_logger: finish failed — %s; "
                "falling back to JSONL", exc,
            )
            # Drop the cached conn so the next call reconnects.
            global _CONN
            _CONN = None
            _write_fallback(payload)

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
