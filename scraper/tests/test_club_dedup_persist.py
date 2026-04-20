"""
Unit tests for club_dedup persistence path.

Covers:
  - `upsert_pending_duplicate` queues exactly one row for a new pair.
  - Re-calling with the same pair doesn't duplicate (ON CONFLICT DO NOTHING
    hits the ordered-pair unique index).
  - Calling with the FLIPPED pair (same two ids, swapped) also doesn't
    duplicate — LEAST/GREATEST collapse (a, b) and (b, a) to the same slot.

The DB cursor is stubbed with a small in-memory fake that parses the INSERT
+ ON CONFLICT and enforces the ordered-pair uniqueness rule the real
Postgres index will enforce in production.
"""

from __future__ import annotations

import json
import os
import sys
from unittest import mock

import pytest

# Stub psycopg2 before import so tests don't need a live DB.
sys.modules.setdefault("psycopg2", mock.MagicMock())
sys.modules.setdefault("psycopg2.extras", mock.MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dedup.club_dedup import (  # noqa: E402
    DEFAULT_METHOD,
    upsert_pending_duplicate,
)


class FakeCursor:
    """Minimal cursor stub that enforces LEAST/GREATEST ordered-pair
    uniqueness on club_duplicates. Only understands the INSERT emitted by
    upsert_pending_duplicate — nothing else.
    """

    def __init__(self):
        # Keyed by frozenset({left_id, right_id}) to mirror the ordered-pair
        # index's collision semantics (a,b) == (b,a).
        self.rows: dict = {}
        self.calls: list = []

    def execute(self, sql: str, params):
        self.calls.append((sql, params))
        assert "INSERT INTO club_duplicates" in sql
        assert "ON CONFLICT" in sql
        assert "LEAST" in sql and "GREATEST" in sql
        assert "DO NOTHING" in sql

        left_id, right_id, score, method, left_json, right_json = params
        key = frozenset({left_id, right_id})
        if key in self.rows:
            # ON CONFLICT DO NOTHING — no-op.
            return
        self.rows[key] = {
            "left_club_id": left_id,
            "right_club_id": right_id,
            "score": score,
            "method": method,
            "status": "pending",
            "left_snapshot": json.loads(left_json),
            "right_snapshot": json.loads(right_json),
        }

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


# ---------------------------------------------------------------------------
# Sample fixtures
# ---------------------------------------------------------------------------

LEFT = {"id": 101, "name": "Atlanta Fire United", "state": "GA"}
RIGHT = {"id": 102, "name": "Atlanta Fire Utd", "state": "GA"}


def _insert(cur, left, right, score=0.94):
    upsert_pending_duplicate(
        cur,
        left_id=left["id"],
        right_id=right["id"],
        score=score,
        method=DEFAULT_METHOD,
        left_snapshot=left,
        right_snapshot=right,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_persist_single_pair_queues_one_row():
    cur = FakeCursor()
    _insert(cur, LEFT, RIGHT)

    assert len(cur.rows) == 1
    row = next(iter(cur.rows.values()))
    assert row["left_club_id"] == 101
    assert row["right_club_id"] == 102
    assert row["status"] == "pending"
    assert row["method"] == DEFAULT_METHOD
    assert 0.0 <= row["score"] <= 1.0
    assert row["left_snapshot"] == LEFT
    assert row["right_snapshot"] == RIGHT


def test_persist_same_pair_is_idempotent():
    cur = FakeCursor()
    _insert(cur, LEFT, RIGHT)
    _insert(cur, LEFT, RIGHT)
    _insert(cur, LEFT, RIGHT)

    # Three INSERT calls dispatched, but ON CONFLICT DO NOTHING means
    # only one row persists.
    assert len(cur.calls) == 3
    assert len(cur.rows) == 1


def test_persist_flipped_pair_is_idempotent():
    """LEAST/GREATEST collapse (a,b) and (b,a) to the same ordered-pair
    slot, so flipping the insert order must not create a second row."""
    cur = FakeCursor()
    _insert(cur, LEFT, RIGHT)
    # Flip the pair: right-first then left. Same two canonical_clubs, so
    # the unique index must collide.
    _insert(cur, RIGHT, LEFT)

    assert len(cur.calls) == 2
    assert len(cur.rows) == 1


def test_persist_distinct_pairs_both_queue():
    cur = FakeCursor()
    third = {"id": 103, "name": "Atlanta Fire SC", "state": "GA"}
    _insert(cur, LEFT, RIGHT)
    _insert(cur, LEFT, third)

    assert len(cur.rows) == 2


def test_upsert_sql_uses_jsonb_cast():
    """Guard the JSONB cast so the writer keeps working even if a future
    refactor drops the string-to-jsonb conversion and the index signature
    changes accordingly."""
    cur = FakeCursor()
    _insert(cur, LEFT, RIGHT)
    sql, _params = cur.calls[0]
    assert "::jsonb" in sql
