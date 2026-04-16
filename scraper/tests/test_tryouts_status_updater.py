"""
Tests for tryouts_status_updater.expire_past_tryouts().

Uses fake cursor/connection stubs — no real DB needed.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tryouts_status_updater import expire_past_tryouts  # noqa: E402


class FakeCursor:
    """Minimal cursor stub for testing."""

    def __init__(self, rowcount: int = 0, fetchone_val=None):
        self._rowcount = rowcount
        self._fetchone_val = fetchone_val
        self.executed: list = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    @property
    def rowcount(self):
        return self._rowcount

    def fetchone(self):
        return self._fetchone_val

    def close(self):
        pass


class FakeConn:
    """Minimal connection stub."""

    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True


class TestExpirePastTryouts:
    def test_dry_run_returns_count(self):
        cursor = FakeCursor(fetchone_val=(7,))
        conn = FakeConn(cursor)
        result = expire_past_tryouts(conn=conn, dry_run=True)
        assert result["expired"] == 7
        assert not conn.committed

    def test_live_update(self):
        cursor = FakeCursor(rowcount=3)
        conn = FakeConn(cursor)
        result = expire_past_tryouts(conn=conn, dry_run=False)
        assert result["expired"] == 3
        assert conn.committed

    def test_zero_expired(self):
        cursor = FakeCursor(rowcount=0)
        conn = FakeConn(cursor)
        result = expire_past_tryouts(conn=conn, dry_run=False)
        assert result["expired"] == 0

    def test_dry_run_null_fetchone(self):
        cursor = FakeCursor(fetchone_val=None)
        conn = FakeConn(cursor)
        result = expire_past_tryouts(conn=conn, dry_run=True)
        assert result["expired"] == 0
