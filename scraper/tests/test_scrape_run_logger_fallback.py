"""
Tests for the JSONL fallback + auto-drain behaviour in
`scrape_run_logger`. Guards the fix for the silent-log-loss bug — when
the DB goes down mid-scrape, we must NOT latch into no-op mode and
lose the session's entire log trail. Instead, entries spill to a
date-stamped JSONL file and drain back into `scrape_run_logs` the
moment the DB is reachable again.

Run:
    python -m pytest scraper/tests/test_scrape_run_logger_fallback.py -v

No real DB is required — psycopg2 is stubbed throughout.
"""

from __future__ import annotations

import json
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scrape_run_logger  # noqa: E402
from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    close_connection,
    drain_fallback_if_any,
)


# ---------------------------------------------------------------------------
# Fixtures — isolate every test in a temp logs dir + reset module state
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_module_state(tmp_path, monkeypatch):
    """
    Redirect the fallback directory to a per-test tmp path and reset
    module-level singletons so tests don't bleed into each other.
    """
    monkeypatch.setenv("SCRAPE_RUN_LOGGER_FALLBACK_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("DATABASE_URL", "postgres://fake/fake")

    # Reset module singletons so state from a previous test can't leak
    # into this one.
    scrape_run_logger._CONN = None
    scrape_run_logger._FALLBACK_WARNED = False
    # Stamp a fresh process-wide fallback id so assertions on it are
    # stable per-test.
    scrape_run_logger._PROCESS_FALLBACK_RUN_ID = "test-process-id"

    yield

    scrape_run_logger._CONN = None
    scrape_run_logger._FALLBACK_WARNED = False


def _make_fake_connect_raising():
    """Return a callable that mimics psycopg2.connect but always raises."""

    def _fail(_url):
        raise ConnectionError("simulated DB outage")

    return _fail


class _FakeCursor:
    """Minimal psycopg2 cursor double with .execute / .fetchone / context."""

    def __init__(self, existing_rows=None, insert_returning_id=None):
        # `existing_rows` is a set of (scraper_key, started_at) pairs
        # that should be treated as already present (for dedup tests).
        self._existing = existing_rows or set()
        # If set, INSERT ... RETURNING will return this row id.
        self._insert_returning_id = insert_returning_id
        self.executed = []
        self._last_result = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=()):
        self.executed.append((sql, params))
        norm = " ".join(sql.split()).upper()
        if norm.startswith("SELECT 1 FROM SCRAPE_RUN_LOGS"):
            key = (params[0], params[1])
            self._last_result = (1,) if key in self._existing else None
        elif "RETURNING ID" in norm or "RETURNING ID, STARTED_AT" in norm:
            # Start() path — emulate a DB-assigned id + started_at.
            if self._insert_returning_id is not None:
                self._last_result = (self._insert_returning_id, None)
            else:
                self._last_result = None
        else:
            self._last_result = None

    def fetchone(self):
        return self._last_result


class _FakeConn:
    """Minimal psycopg2 connection double."""

    def __init__(self, cursor_factory):
        self._cursor_factory = cursor_factory
        self.closed = 0
        self.autocommit = False

    def cursor(self):
        return self._cursor_factory()

    def close(self):
        self.closed = 1


def _install_failing_connect(monkeypatch):
    """Force every psycopg2.connect() call to raise."""
    fake_mod = MagicMock()
    fake_mod.connect.side_effect = ConnectionError("simulated DB outage")
    monkeypatch.setattr(scrape_run_logger, "psycopg2", fake_mod)
    return fake_mod


def _install_working_connect(monkeypatch, cursor_factory):
    """Replace psycopg2.connect with one that returns a _FakeConn."""
    fake_mod = MagicMock()
    fake_mod.connect.side_effect = lambda _url: _FakeConn(cursor_factory)
    monkeypatch.setattr(scrape_run_logger, "psycopg2", fake_mod)
    return fake_mod


# ---------------------------------------------------------------------------
# Test 1 — DB unreachable → JSONL fallback file grows per log call
# ---------------------------------------------------------------------------

def test_db_unreachable_writes_to_jsonl(monkeypatch):
    _install_failing_connect(monkeypatch)

    # Three separate log calls across three logger instances. Each call
    # should spill both a start and a finish event to the JSONL file
    # because the DB is unreachable for both.
    for i in range(3):
        lg = ScrapeRunLogger(scraper_key=f"key-{i}", league_name=f"L{i}")
        lg.start(source_url=f"https://example.test/{i}")
        lg.finish_ok(records_created=i)

    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]
    files = [f for f in os.listdir(logs_dir) if f.endswith(".jsonl")]
    assert len(files) == 1, f"expected one JSONL file, got {files}"

    path = os.path.join(logs_dir, files[0])
    with open(path, "r", encoding="utf-8") as f:
        lines = [json.loads(l) for l in f if l.strip()]

    # 3 logger instances × (start + finish) = 6 events.
    assert len(lines) == 6
    events = [(l["scraper_key"], l["event"]) for l in lines]
    assert ("key-0", "start") in events
    assert ("key-0", "finish") in events
    assert ("key-2", "finish") in events

    # Every line carries the process-wide fallback id + an iso timestamp.
    for l in lines:
        assert l["_fallback_run_id"] == "test-process-id"
        assert "_logged_at" in l
        assert "client_run_id" in l


def test_fallback_warning_is_emitted_once(monkeypatch, caplog):
    import logging as py_logging

    _install_failing_connect(monkeypatch)
    caplog.set_level(py_logging.WARNING)

    for i in range(4):
        lg = ScrapeRunLogger(scraper_key=f"warn-{i}")
        lg.start(source_url="x")
        lg.finish_ok()

    fallback_warnings = [
        r for r in caplog.records
        if "DB unreachable; logging to" in r.getMessage()
    ]
    assert len(fallback_warnings) == 1


# ---------------------------------------------------------------------------
# Test 2 — DB recovery triggers drain + file rename
# ---------------------------------------------------------------------------

def test_drain_on_recovery_inserts_rows_and_renames_file(monkeypatch):
    # Phase 1: DB down — spill 3 runs to JSONL.
    _install_failing_connect(monkeypatch)
    for i in range(3):
        lg = ScrapeRunLogger(scraper_key=f"rec-{i}")
        lg.start(source_url="x")
        lg.finish_ok(records_created=i + 1)

    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]
    pre = [f for f in os.listdir(logs_dir) if f.endswith(".jsonl")]
    assert len(pre) == 1
    assert not pre[0].endswith(".drained.jsonl")

    # Phase 2: DB recovers. Swap in a working fake. A fresh logger
    # call should trigger the drain, insert the 3 pending rows, and
    # rename the file.
    cursor = _FakeCursor(insert_returning_id=99)
    _install_working_connect(monkeypatch, lambda: cursor)
    # Reset the cached (dead) conn so the next _conn() reconnects.
    scrape_run_logger._CONN = None

    lg = ScrapeRunLogger(scraper_key="healthy")
    lg.start(source_url="x")
    lg.finish_ok(records_created=42)

    # The drain should have inserted 3 rows — one per spilled run.
    insert_sqls = [
        sql for sql, _ in cursor.executed
        if "INSERT INTO scrape_run_logs" in sql and "RETURNING" not in sql
    ]
    assert len(insert_sqls) == 3, (
        f"expected 3 drain inserts, got {len(insert_sqls)}: {insert_sqls}"
    )

    # The fallback file should now be renamed to *.drained.jsonl.
    post = sorted(os.listdir(logs_dir))
    drained = [f for f in post if f.endswith(".drained.jsonl")]
    assert len(drained) == 1, f"expected one drained file, got {post}"
    raw_pending = [
        f for f in post
        if f.endswith(".jsonl") and not f.endswith(".drained.jsonl")
    ]
    assert raw_pending == []


# ---------------------------------------------------------------------------
# Test 3 — drain is idempotent (second drain inserts zero rows)
# ---------------------------------------------------------------------------

def test_drain_is_idempotent(monkeypatch):
    # Spill one event via JSONL with the DB down.
    _install_failing_connect(monkeypatch)
    lg = ScrapeRunLogger(scraper_key="idem-key")
    lg.start(source_url="x")
    lg.finish_ok(records_created=7)

    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]

    # Read the JSONL to learn the started_at that got stamped, so our
    # fake cursor can claim the dedup row "already exists" on pass 2.
    files = [f for f in os.listdir(logs_dir) if f.endswith(".jsonl")]
    assert len(files) == 1
    with open(os.path.join(logs_dir, files[0])) as f:
        events = [json.loads(l) for l in f if l.strip()]
    start_ev = next(e for e in events if e["event"] == "start")
    key = ("idem-key", start_ev["started_at"])

    # Pass 1: DB recovers; drain should insert exactly 1 row.
    cursor_1 = _FakeCursor()
    _install_working_connect(monkeypatch, lambda: cursor_1)
    scrape_run_logger._CONN = None
    inserted_1 = drain_fallback_if_any()
    assert inserted_1 == 1

    # Simulate someone placing the SAME JSONL back on disk (we renamed
    # the first one to .drained.jsonl, so copy it back with the live
    # name). This models: a re-drain attempt on the same content — it
    # must be a no-op.
    drained_files = [f for f in os.listdir(logs_dir) if f.endswith(".drained.jsonl")]
    assert len(drained_files) == 1
    src = os.path.join(logs_dir, drained_files[0])
    dst_name = drained_files[0].replace(".drained.jsonl", ".jsonl")
    dst = os.path.join(logs_dir, dst_name)
    with open(src) as fin, open(dst, "w") as fout:
        fout.write(fin.read())

    # Pass 2: cursor preloaded with the dedup key so SELECT 1 hits.
    cursor_2 = _FakeCursor(existing_rows={key})
    _install_working_connect(monkeypatch, lambda: cursor_2)
    scrape_run_logger._CONN = None

    inserted_2 = drain_fallback_if_any()
    assert inserted_2 == 0, (
        "Second drain must be a no-op; cursor.executed="
        f"{cursor_2.executed}"
    )

    # Any INSERT sql is a bug — dedup should have short-circuited.
    insert_calls = [
        sql for sql, _ in cursor_2.executed
        if sql.strip().upper().startswith("INSERT")
    ]
    assert insert_calls == [], f"unexpected inserts on redrain: {insert_calls}"


# ---------------------------------------------------------------------------
# Test 4 — close_connection drains any pending JSONL before exiting
# ---------------------------------------------------------------------------

def test_close_connection_drains_pending(monkeypatch):
    _install_failing_connect(monkeypatch)
    lg = ScrapeRunLogger(scraper_key="pre-close")
    lg.start(source_url="x")
    lg.finish_ok(records_created=1)

    # DB recovers. close_connection() should drain opportunistically.
    cursor = _FakeCursor()
    _install_working_connect(monkeypatch, lambda: cursor)
    scrape_run_logger._CONN = None

    close_connection()

    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]
    post = os.listdir(logs_dir)
    drained = [f for f in post if f.endswith(".drained.jsonl")]
    assert len(drained) == 1
    inserts = [
        sql for sql, _ in cursor.executed
        if "INSERT INTO scrape_run_logs" in sql and "RETURNING" not in sql
    ]
    assert len(inserts) == 1
