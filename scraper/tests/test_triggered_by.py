"""
Tests for the `triggered_by` stamping in `scrape_run_logger`.

The logger reads `SCRAPE_TRIGGERED_BY` from the env at start() time and
writes it through to every downstream path:
  - The primary DB INSERT in `ScrapeRunLogger.start()`.
  - The JSONL fallback payload (both start and finish events).
  - The consolidated INSERT in the JSONL drain loop.

Wrapper scripts in scraper/scheduled/*.sh set the env to "scheduler";
operator-invoked runs leave it unset and get stamped "manual" (matches
the DB column default in lib/db/src/schema/scrape-health.ts).

Run:
    python -m pytest scraper/tests/test_triggered_by.py -v

No real DB is required — psycopg2 is stubbed throughout, following the
same pattern as test_scrape_run_logger_fallback.py.
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
    ScrapeRunLogger,
    drain_fallback_if_any,
)


# ---------------------------------------------------------------------------
# Fixtures — mirror the isolation pattern from test_scrape_run_logger_fallback
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_module_state(tmp_path, monkeypatch):
    monkeypatch.setenv("SCRAPE_RUN_LOGGER_FALLBACK_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("DATABASE_URL", "postgres://fake/fake")
    # Make sure neither test inherits a stale SCRAPE_TRIGGERED_BY from
    # the host shell — every test sets it (or explicitly unsets it)
    # below.
    monkeypatch.delenv("SCRAPE_TRIGGERED_BY", raising=False)

    scrape_run_logger._CONN = None
    scrape_run_logger._FALLBACK_WARNED = False
    scrape_run_logger._PROCESS_FALLBACK_RUN_ID = "test-process-id"

    yield

    scrape_run_logger._CONN = None
    scrape_run_logger._FALLBACK_WARNED = False


class _FakeCursor:
    """Minimal psycopg2 cursor double — captures execute() calls.

    Emulates the RETURNING id, started_at response so `start()` can
    stash a real run_id and the later `finish_*()` UPDATE path is
    exercised.
    """

    def __init__(self, insert_returning_id=1):
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
            self._last_result = None
        elif "RETURNING ID" in norm:
            self._last_result = (self._insert_returning_id, None)
        else:
            self._last_result = None

    def fetchone(self):
        return self._last_result


class _FakeConn:
    def __init__(self, cursor_factory):
        self._cursor_factory = cursor_factory
        self.closed = 0
        self.autocommit = False

    def cursor(self):
        return self._cursor_factory()

    def close(self):
        self.closed = 1


def _install_working_connect(monkeypatch, cursor_factory):
    fake_mod = MagicMock()
    fake_mod.connect.side_effect = lambda _url: _FakeConn(cursor_factory)
    monkeypatch.setattr(scrape_run_logger, "psycopg2", fake_mod)
    return fake_mod


def _install_failing_connect(monkeypatch):
    fake_mod = MagicMock()
    fake_mod.connect.side_effect = ConnectionError("simulated DB outage")
    monkeypatch.setattr(scrape_run_logger, "psycopg2", fake_mod)
    return fake_mod


def _extract_insert_params(cursor):
    """Return (sql, params) for the INSERT INTO scrape_run_logs call
    on `start()` — the one with RETURNING id."""
    for sql, params in cursor.executed:
        norm = " ".join(sql.split()).upper()
        if "INSERT INTO SCRAPE_RUN_LOGS" in norm and "RETURNING ID" in norm:
            return sql, params
    raise AssertionError(
        f"no start INSERT found in cursor.executed: {cursor.executed}"
    )


# ---------------------------------------------------------------------------
# Test 1 — env set to 'scheduler' → stamped on the primary INSERT
# ---------------------------------------------------------------------------

def test_start_stamps_triggered_by_scheduler(monkeypatch):
    monkeypatch.setenv("SCRAPE_TRIGGERED_BY", "scheduler")
    cursor = _FakeCursor(insert_returning_id=42)
    _install_working_connect(monkeypatch, lambda: cursor)

    lg = ScrapeRunLogger(scraper_key="nightly-tier1", league_name="ECNL")
    lg.start(source_url="https://example.test/ecnl")

    sql, params = _extract_insert_params(cursor)
    # Column order in start()'s INSERT:
    #   (scraper_key, league_name, status, source_url, triggered_by)
    # (status is the literal 'running' in SQL; not a placeholder.)
    assert "triggered_by" in sql
    # Last bound param is triggered_by.
    assert params[-1] == "scheduler"
    # And it was captured onto the instance for downstream use.
    assert lg._triggered_by == "scheduler"


# ---------------------------------------------------------------------------
# Test 2 — env unset → defaults to 'manual'
# ---------------------------------------------------------------------------

def test_start_defaults_to_manual_when_env_unset(monkeypatch):
    monkeypatch.delenv("SCRAPE_TRIGGERED_BY", raising=False)
    cursor = _FakeCursor(insert_returning_id=7)
    _install_working_connect(monkeypatch, lambda: cursor)

    lg = ScrapeRunLogger(scraper_key="adhoc-run")
    lg.start(source_url="https://example.test/adhoc")

    _sql, params = _extract_insert_params(cursor)
    assert params[-1] == "manual"
    assert lg._triggered_by == "manual"


# ---------------------------------------------------------------------------
# Test 3 — env empty string also falls back to 'manual'
# ---------------------------------------------------------------------------

def test_start_empty_env_treated_as_manual(monkeypatch):
    # A wrapper script that accidentally exports an empty string should
    # be indistinguishable from "unset" for audit purposes.
    monkeypatch.setenv("SCRAPE_TRIGGERED_BY", "")
    cursor = _FakeCursor(insert_returning_id=1)
    _install_working_connect(monkeypatch, lambda: cursor)

    lg = ScrapeRunLogger(scraper_key="empty-env")
    lg.start(source_url="x")

    _sql, params = _extract_insert_params(cursor)
    assert params[-1] == "manual"


# ---------------------------------------------------------------------------
# Test 4 — JSONL fallback payloads carry triggered_by through
# ---------------------------------------------------------------------------

def test_jsonl_fallback_payload_includes_triggered_by(monkeypatch):
    monkeypatch.setenv("SCRAPE_TRIGGERED_BY", "scheduler")
    _install_failing_connect(monkeypatch)

    lg = ScrapeRunLogger(scraper_key="fallback-key", league_name="Fallback L")
    lg.start(source_url="https://example.test/fallback")
    lg.finish_ok(records_created=3)

    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]
    files = [f for f in os.listdir(logs_dir) if f.endswith(".jsonl")]
    assert len(files) == 1

    with open(os.path.join(logs_dir, files[0])) as f:
        events = [json.loads(l) for l in f if l.strip()]

    # Both start and finish events must carry triggered_by so the drain
    # loop can reconstruct the row regardless of which event wins the
    # consolidation merge.
    assert len(events) == 2
    for ev in events:
        assert ev["triggered_by"] == "scheduler", ev


# ---------------------------------------------------------------------------
# Test 5 — drained JSONL inserts propagate triggered_by into the INSERT
# ---------------------------------------------------------------------------

def test_drain_propagates_triggered_by_into_insert(monkeypatch):
    # Phase 1: DB down → spill with triggered_by=scheduler.
    monkeypatch.setenv("SCRAPE_TRIGGERED_BY", "scheduler")
    _install_failing_connect(monkeypatch)
    lg = ScrapeRunLogger(scraper_key="drain-key")
    lg.start(source_url="x")
    lg.finish_ok(records_created=1)

    # Phase 2: DB recovers — drain.
    cursor = _FakeCursor()
    _install_working_connect(monkeypatch, lambda: cursor)
    scrape_run_logger._CONN = None

    inserted = drain_fallback_if_any()
    assert inserted == 1

    # Find the drain INSERT (the one without RETURNING) and check its
    # last bound param is 'scheduler'.
    drain_inserts = [
        (sql, params) for sql, params in cursor.executed
        if "INSERT INTO scrape_run_logs" in sql
        and "RETURNING" not in sql.upper()
    ]
    assert len(drain_inserts) == 1
    _sql, params = drain_inserts[0]
    assert params[-1] == "scheduler"


# ---------------------------------------------------------------------------
# Test 6 — older JSONL rows without triggered_by drain as 'manual'
# ---------------------------------------------------------------------------

def test_drain_backfills_manual_for_legacy_jsonl(monkeypatch, tmp_path):
    # Hand-craft a JSONL file that predates this PR (no triggered_by
    # field on either event). The drain must not crash — it fills in
    # 'manual' to match the column default.
    logs_dir = os.environ["SCRAPE_RUN_LOGGER_FALLBACK_DIR"]
    os.makedirs(logs_dir, exist_ok=True)
    legacy_path = os.path.join(logs_dir, "scrape_run_logs.2026-01-01.jsonl")
    legacy_events = [
        {
            "event": "start",
            "client_run_id": "legacy-run",
            "scraper_key": "legacy-key",
            "league_name": "Legacy L",
            "started_at": "2026-01-01T00:00:00+00:00",
            "status": "running",
            "source_url": "x",
        },
        {
            "event": "finish",
            "client_run_id": "legacy-run",
            "scraper_key": "legacy-key",
            "league_name": "Legacy L",
            "started_at": "2026-01-01T00:00:00+00:00",
            "completed_at": "2026-01-01T00:00:10+00:00",
            "status": "ok",
            "records_created": 1,
            "records_updated": 0,
            "records_failed": 0,
            "source_url": "x",
        },
    ]
    with open(legacy_path, "w") as f:
        for ev in legacy_events:
            f.write(json.dumps(ev) + "\n")

    cursor = _FakeCursor()
    _install_working_connect(monkeypatch, lambda: cursor)

    inserted = drain_fallback_if_any()
    assert inserted == 1

    drain_inserts = [
        (sql, params) for sql, params in cursor.executed
        if "INSERT INTO scrape_run_logs" in sql
        and "RETURNING" not in sql.upper()
    ]
    assert len(drain_inserts) == 1
    _sql, params = drain_inserts[0]
    assert params[-1] == "manual"
