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
