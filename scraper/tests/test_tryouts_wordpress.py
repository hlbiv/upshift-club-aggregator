"""
Tests for the WordPress tryouts extractor + tryouts writer.

Extraction tests use fixture HTML. Writer tests stub psycopg2 with a
fake cursor so pytest runs without Postgres.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.tryouts_wordpress import (  # noqa: E402
    parse_date,
    parse_tryouts_page_html,
)
from ingest.tryouts_writer import insert_tryouts  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "wordpress"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- date parsing variants


def test_parse_date_month_name_format():
    assert parse_date("Aug 5, 2026") == datetime(2026, 8, 5)
    assert parse_date("August 5, 2026") == datetime(2026, 8, 5)


def test_parse_date_numeric_slash_format():
    # 2-digit year must expand to 2000s.
    assert parse_date("8/5/26") == datetime(2026, 8, 5)
    assert parse_date("08/05/2026") == datetime(2026, 8, 5)


def test_parse_date_range_picks_first_day():
    assert parse_date("August 5-7, 2026") == datetime(2026, 8, 5)


def test_parse_date_unparseable_returns_none():
    assert parse_date("every summer") is None
    assert parse_date("") is None


# --------------------------------------------------------------------------- page-level parse


def test_parse_page_happy_path_extracts_all_fields():
    html = _read("foley_fc_tryouts.html")
    rows = parse_tryouts_page_html(
        html,
        club_name_raw="Foley FC",
        source_url="https://foleyfc.example.com/tryouts/",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["club_name_raw"] == "Foley FC"
    assert row["tryout_date"] == datetime(2026, 8, 5)
    assert row["age_group"] == "U12"
    assert row["gender"] == "M"
    assert "Foley" in (row["location"] or "")


def test_parse_page_no_date_skips_and_warns(caplog):
    """A page without a parseable date must produce zero rows and
    emit a warning. Nothing is written downstream."""
    html = _read("no_date_tryouts.html")
    with caplog.at_level("WARNING"):
        rows = parse_tryouts_page_html(
            html,
            club_name_raw="Ghost FC",
            source_url="https://ghost.example.com/tryouts/",
        )
    assert rows == []
    assert any("no date parsed" in rec.message for rec in caplog.records)


# --------------------------------------------------------------------------- writer with stubbed cursor


class _FakeCursor:
    def __init__(self, script: List[Tuple[Any, int]]):
        self.script = list(script)
        self.executed: List[Tuple[str, Dict[str, Any]]] = []
        self._last: Any = None
        self.rowcount: int = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params: Dict[str, Any]):
        op = sql.strip().split()[0].upper()
        self.executed.append((op, dict(params)))
        if self.script:
            row, rc = self.script.pop(0)
            self._last = row
            self.rowcount = rc
        else:
            self._last = None
            self.rowcount = 0

    def fetchone(self):
        return self._last


class _FakeConn:
    def __init__(self, cur: _FakeCursor):
        self._cur = cur
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def _make_row(**overrides):
    base = {
        "club_name_raw": "Foley FC",
        "tryout_date": datetime(2026, 8, 5),
        "age_group": "U12",
        "gender": "M",
        "location": "Foley, AL",
        "source_url": "https://foleyfc.example.com/tryouts/",
        "notes": None,
    }
    base.update(overrides)
    return base


def test_writer_inserts_then_reinsert_is_idempotent():
    """Same (club, date, age, gender) twice with zero mutable-field
    change → 1 insert + 1 no-op (neither counted)."""
    row = _make_row()
    script: List[Tuple[Any, int]] = [
        ((True,), 1),   # first run: inserted
        (None, 0),      # second run: WHERE predicate short-circuits
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    c1 = insert_tryouts([row], conn=conn)
    assert c1["inserted"] == 1
    c2 = insert_tryouts([row], conn=conn)
    assert c2["inserted"] == 0
    assert c2["updated"] == 0


def test_writer_counts_update_when_location_changes():
    first = _make_row(location="Old Park")
    second = _make_row(location="New Park")
    script: List[Tuple[Any, int]] = [
        ((True,), 1),   # inserted
        ((False,), 1),  # updated (location changed)
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    c1 = insert_tryouts([first], conn=conn)
    c2 = insert_tryouts([second], conn=conn)
    assert c1["inserted"] == 1
    assert c2["updated"] == 1


def test_writer_skips_row_missing_club_name_raw():
    bad = _make_row()
    del bad["club_name_raw"]
    # No cursor activity expected.
    cur = _FakeCursor([])
    conn = _FakeConn(cur)
    counts = insert_tryouts([bad], conn=conn)
    assert counts["skipped"] == 1
    assert counts["inserted"] == 0


def test_writer_sends_null_club_id_literal():
    from ingest.tryouts_writer import _INSERT_TRYOUT_SQL
    assert "club_id" in _INSERT_TRYOUT_SQL
    assert "%(club_id)s" not in _INSERT_TRYOUT_SQL
