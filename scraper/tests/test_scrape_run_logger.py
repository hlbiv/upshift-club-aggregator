"""
Tests for scrape_run_logger — classification behavior + no-op safety
when DATABASE_URL is unset.

Run:
    python -m pytest scraper/tests/test_scrape_run_logger.py -v
    # or with the repo's normal test runner.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scrape_run_logger import (  # noqa: E402
    FailureKind,
    ScrapeRunLogger,
    classify_exception,
)


class _MyTimeout(Exception):
    pass


class _MyConnectionError(Exception):
    pass


def test_classify_timeout_by_message():
    assert classify_exception(Exception("request timed out")) == FailureKind.TIMEOUT


def test_classify_timeout_by_type_name():
    assert classify_exception(_MyTimeout("x")) == FailureKind.TIMEOUT


def test_classify_network_by_type_name():
    assert classify_exception(_MyConnectionError("boom")) == FailureKind.NETWORK


def test_classify_network_by_message():
    assert classify_exception(Exception("DNS failure")) == FailureKind.NETWORK


def test_classify_parse_by_builtin_type():
    assert classify_exception(ValueError("bad int")) == FailureKind.PARSE_ERROR
    assert classify_exception(KeyError("missing")) == FailureKind.PARSE_ERROR
    assert classify_exception(AttributeError("none")) == FailureKind.PARSE_ERROR
    assert classify_exception(IndexError("oob")) == FailureKind.PARSE_ERROR


def test_classify_unknown_fallback():
    # A generic runtime error without parse/timeout/network markers.
    assert classify_exception(RuntimeError("something weird")) == FailureKind.UNKNOWN


DB_ENUM_VALUES = {"timeout", "network", "parse_error", "zero_results", "unknown"}


def test_failure_kind_values_match_db_enum():
    """
    These strings MUST match the Postgres check constraint
    `scrape_run_logs_failure_kind_enum` exactly. If this test fails, the
    Drizzle schema in lib/db/src/schema/scrape-health.ts is out of sync.
    """
    assert {k.value for k in FailureKind} == DB_ENUM_VALUES


def test_run_py_failure_kind_matches_logger_and_db_enum():
    """
    run.py defines its OWN FailureKind enum (historical: pre-dates the
    scrape_run_logger module). Their values are mapped 1:1 inside
    scrape_league() via `DbFailureKind(kind.value)`. If the two enums
    drift, every failure in the drifted category raises ValueError at
    map time. Lock the contract with a test.
    """
    # Some environments don't have pandas etc. installed; skip cleanly.
    try:
        from run import FailureKind as RunFailureKind  # type: ignore
    except Exception:
        import pytest  # type: ignore

        pytest.skip("run.py imports unavailable in this environment")
        return
    assert {k.value for k in RunFailureKind} == DB_ENUM_VALUES
    assert {k.value for k in RunFailureKind} == {k.value for k in FailureKind}


def test_logger_noops_without_database_url(monkeypatch):
    """
    When DATABASE_URL is not set, the logger must silently no-op.
    Scraping must never be blocked by the log writer.
    """
    monkeypatch.delenv("DATABASE_URL", raising=False)
    log = ScrapeRunLogger(scraper_key="test-key", league_name="Test League")
    # Must not raise.
    log.start(source_url="https://example.com")
    assert log.run_id is None
    log.finish_ok(records_created=5)
    log.finish_failed(FailureKind.TIMEOUT, "boom")
    log.finish_partial(records_failed=1)


# ---------------------------------------------------------------------------
# Edge-case fixes (PR 17): triggered_by capture order + records_raw/deduped.
# ---------------------------------------------------------------------------


import json  # noqa: E402
import scrape_run_logger as srl_module  # noqa: E402


def _read_jsonl_lines(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _force_db_path_with_failed_connect(monkeypatch, tmp_path):
    """
    Shared setup: pretend the DB is configured + reachable enough to
    reach the connect call, but force every connect attempt to fail so
    we deterministically land in the JSONL fallback. Redirects the
    fallback dir to `tmp_path` for inspection.
    """
    monkeypatch.setenv("DATABASE_URL", "postgres://example/none")
    # psycopg2 may not be installed in the test env; force
    # _db_configured() to True so we exercise the connect-then-fallback
    # path rather than the silent-no-op-without-DB path.
    monkeypatch.setattr(srl_module, "_db_configured", lambda: True)
    monkeypatch.setattr(srl_module, "_try_connect", lambda: None)
    # Reset module-level state so other tests' connection / warning
    # latch don't bleed in.
    monkeypatch.setattr(srl_module, "_CONN", None)
    monkeypatch.setattr(srl_module, "_FALLBACK_WARNED", False)
    monkeypatch.setenv("SCRAPE_RUN_LOGGER_FALLBACK_DIR", str(tmp_path))


def test_triggered_by_captured_before_db_init(monkeypatch, tmp_path):
    """
    The `triggered_by` value must be captured at the very top of start(),
    BEFORE any DB-touching code runs. If we let the DB init fail and
    spill to the JSONL fallback, the spilled record must still carry the
    operator-supplied SCRAPE_TRIGGERED_BY value rather than the
    dataclass default ("manual"). Regression guard for the case where
    the capture used to live AFTER the drain / connect path.
    """
    _force_db_path_with_failed_connect(monkeypatch, tmp_path)
    monkeypatch.setenv("SCRAPE_TRIGGERED_BY", "scheduler")

    logger = ScrapeRunLogger(scraper_key="trig-test", league_name="Trig League")
    logger.start(source_url="https://example.com/trig")

    fallback_path = srl_module._fallback_path()
    assert os.path.isfile(fallback_path), "expected JSONL fallback to be written"
    rows = _read_jsonl_lines(fallback_path)
    start_rows = [r for r in rows if r.get("event") == "start"]
    assert start_rows, "expected at least one start event in JSONL"
    assert start_rows[-1]["triggered_by"] == "scheduler"


def test_finish_ok_records_raw_and_deduped_when_provided(monkeypatch, tmp_path):
    """
    `finish_ok(records_created=20, records_raw=100, records_deduped=20)`
    should land both new fields in the JSONL fallback payload (we can't
    cheaply assert log lines here, so the JSONL is the inspectable
    proxy for the persisted payload).
    """
    _force_db_path_with_failed_connect(monkeypatch, tmp_path)

    logger = ScrapeRunLogger(scraper_key="dedup-test", league_name="Dedup League")
    logger.start(source_url="https://example.com/dedup")
    logger.finish_ok(records_created=20, records_raw=100, records_deduped=20)

    rows = _read_jsonl_lines(srl_module._fallback_path())
    finish_rows = [r for r in rows if r.get("event") == "finish"]
    assert finish_rows, "expected a finish event in JSONL"
    last = finish_rows[-1]
    assert last["records_created"] == 20
    assert last["records_raw"] == 100
    assert last["records_deduped"] == 20


def test_finish_ok_backwards_compatible(monkeypatch, tmp_path):
    """
    Old call sites pass only `records_created` (and friends). They must
    keep working unchanged: no exceptions, and the JSONL payload must
    NOT carry the new optional fields when they weren't supplied.
    """
    _force_db_path_with_failed_connect(monkeypatch, tmp_path)

    logger = ScrapeRunLogger(scraper_key="bc-test", league_name="BC League")
    logger.start(source_url="https://example.com/bc")
    # Must not raise — exact same call shape as pre-PR callers.
    logger.finish_ok(records_created=20)

    rows = _read_jsonl_lines(srl_module._fallback_path())
    finish_rows = [r for r in rows if r.get("event") == "finish"]
    assert finish_rows, "expected a finish event in JSONL"
    last = finish_rows[-1]
    assert last["records_created"] == 20
    assert "records_raw" not in last
    assert "records_deduped" not in last
