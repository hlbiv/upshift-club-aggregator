"""
Tests for the SincSports rosters extractor + snapshot writer.

Extraction tests run against fixture HTML. Writer tests stub psycopg2
with a fake cursor (same pattern as ``test_sincsports_events.py``) so
pytest runs without Postgres.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.sincsports_rosters import (  # noqa: E402
    current_season_tag,
    parse_roster_html,
    parse_team_descriptors,
)
from ingest.roster_snapshot_writer import (  # noqa: E402
    _compute_diff_rows,
    insert_roster_snapshots,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sincsports"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- extractor: team descriptors


def test_team_descriptors_captures_teamid_and_metadata():
    html = _read("teamlist_ROSTR.html")
    descriptors = parse_team_descriptors(html, tid="ROSTR")
    # TBD placeholder and teams without a teamid link must be skipped.
    assert len(descriptors) == 2
    d = descriptors[0]
    assert d.tid == "ROSTR"
    assert d.teamid == "1001"
    assert d.team_name_raw == "Foley FC 14B"
    assert d.club_name == "Foley FC"
    assert d.state == "AL"
    assert d.age_group == "U12"
    assert d.gender == "M"
    assert d.division_code == "Silver"
    assert d.birth_year == 2014


# --------------------------------------------------------------------------- extractor: roster parsing


def test_parse_roster_happy_path_captures_name_and_jersey():
    html = _read("roster_ROSTR_team_1001.html")
    rows = parse_roster_html(html)
    assert rows == [
        ("Alex Rivera", "7"),
        ("Jordan Kim", "10"),
        ("Sam Patel", "1"),
    ]


def test_parse_roster_empty_returns_empty_list():
    html = _read("roster_ROSTR_team_1002_empty.html")
    rows = parse_roster_html(html)
    assert rows == []


def test_parse_roster_missing_jersey_column_returns_none_jersey():
    html = _read("roster_ROSTR_team_1003_no_jersey.html")
    rows = parse_roster_html(html)
    assert rows == [
        ("Chris Boyd", None),
        ("Dana Yu", None),
    ]


def test_current_season_tag_august_rolls_over():
    # August 2026 → '2026-27'
    assert current_season_tag(datetime(2026, 8, 15)) == "2026-27"
    # March 2026 → '2025-26'
    assert current_season_tag(datetime(2026, 3, 15)) == "2025-26"


# --------------------------------------------------------------------------- diff computation (pure)


def test_diff_compute_adds_new_players_removes_missing_flags_jersey_change():
    group_key = ("Foley FC", "2026-27", "U12", "M")
    current = [
        {"player_name": "Alex Rivera", "jersey_number": "7", "position": None},
        {"player_name": "Jordan Kim", "jersey_number": "9", "position": None},  # changed jersey
        {"player_name": "New Kid", "jersey_number": "22", "position": None},    # added
    ]
    prior = [
        ("Alex Rivera", "7", None),
        ("Jordan Kim", "10", None),   # old jersey
        ("Departed Kid", "3", None),  # removed
    ]
    diffs = _compute_diff_rows(group_key, current, prior)
    by_type = {d["diff_type"]: d for d in diffs}
    assert "added" in by_type and by_type["added"]["player_name"] == "New Kid"
    assert "removed" in by_type and by_type["removed"]["player_name"] == "Departed Kid"
    jc = [d for d in diffs if d["diff_type"] == "jersey_changed"]
    assert len(jc) == 1
    assert jc[0]["from_jersey_number"] == "10"
    assert jc[0]["to_jersey_number"] == "9"


# --------------------------------------------------------------------------- writer with stubbed cursor


class _FakeCursor:
    """Scriptable cursor stand-in. Each entry is ``(row, rowcount)``."""

    def __init__(self, script: List[Tuple[Any, int]]):
        self.script = list(script)
        self.executed: List[Tuple[str, Dict[str, Any]]] = []
        self._last_rows: Any = None
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
            self._last_rows = row
            self.rowcount = rc
        else:
            self._last_rows = None
            self.rowcount = 0

    def fetchone(self):
        r = self._last_rows
        # fetchone on a list-returning script consumes the first element.
        if isinstance(r, list):
            return r[0] if r else None
        return r

    def fetchall(self):
        r = self._last_rows
        if r is None:
            return []
        if isinstance(r, list):
            return r
        return [r]


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


def _make_row(
    player_name: str,
    jersey: str,
    *,
    club: str = "Foley FC",
    snap: datetime = datetime(2026, 4, 1),
):
    return {
        "club_name_raw": club,
        "source_url": "https://x/roster?teamid=1001",
        "snapshot_date": snap,
        "season": "2025-26",
        "age_group": "U12",
        "gender": "M",
        "division": "Silver",
        "player_name": player_name,
        "jersey_number": jersey,
        "position": None,
        "event_id": None,
    }


def test_writer_first_run_inserts_and_writes_no_diffs():
    """Very first snapshot has no prior → inserts players, no diffs."""
    rows = [_make_row("Alex", "7"), _make_row("Jordan", "10")]
    script: List[Tuple[Any, int]] = [
        # prior snapshot lookup → fetchall returns empty list
        ([], 0),
    ] + [
        # two INSERT ... RETURNING (inserted=True)
        ((True,), 1),
        ((True,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["inserted"] == 2
    assert counts["updated"] == 0
    assert counts["diffs_written"] == 0
    # Caller owns the connection → writer does NOT commit.
    assert conn.commits == 0


def test_writer_idempotent_reinsert_reports_no_updates():
    """Re-scrape with no changes: DO UPDATE ... WHERE returns no row,
    rowcount=0. Neither inserted nor updated is incremented."""
    rows = [_make_row("Alex", "7"), _make_row("Jordan", "10")]
    # prior snapshot has the same two rows, so zero diffs.
    prior_rows = [("Alex", "7", None), ("Jordan", "10", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, len(prior_rows)),
        (None, 0),   # unchanged row → no fetchone result
        (None, 0),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["inserted"] == 0
    assert counts["updated"] == 0
    assert counts["diffs_written"] == 0


def test_writer_emits_removed_diff_when_prior_player_not_in_current():
    """Prior had 2 players, current has 1 → one `removed` diff."""
    rows = [_make_row("Alex", "7")]
    prior_rows = [("Alex", "7", None), ("Departed", "99", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, len(prior_rows)),
        # Alex: unchanged → rowcount 0
        (None, 0),
        # one roster_diffs INSERT ... RETURNING id → (123,), rowcount 1
        ((123,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["diffs_written"] == 1
    # Verify the diff insert was a `removed` type.
    diff_inserts = [p for op, p in cur.executed if op == "INSERT" and "diff_type" in p]
    assert len(diff_inserts) == 1
    assert diff_inserts[0]["diff_type"] == "removed"
    assert diff_inserts[0]["player_name"] == "Departed"


def test_writer_emits_jersey_changed_diff():
    rows = [_make_row("Alex", "9")]   # new jersey
    prior_rows = [("Alex", "7", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, 1),
        # Alex upsert → WHERE predicate fires: rowcount=1, inserted=False
        ((False,), 1),
        # jersey_changed diff insert
        ((123,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["updated"] == 1
    assert counts["diffs_written"] == 1
    diff_inserts = [p for op, p in cur.executed if op == "INSERT" and "diff_type" in p]
    assert diff_inserts[0]["diff_type"] == "jersey_changed"
    assert diff_inserts[0]["from_jersey_number"] == "7"
    assert diff_inserts[0]["to_jersey_number"] == "9"


def test_writer_sends_null_club_id_on_every_insert():
    """The SQL template hard-codes club_id=NULL so the linker owns it.
    Assert the compiled SQL never mentions a %(club_id)s placeholder."""
    from ingest.roster_snapshot_writer import _INSERT_SNAPSHOT_SQL
    assert "club_id" in _INSERT_SNAPSHOT_SQL
    # Must be NULL literal, never a parameter placeholder.
    assert "%(club_id)s" not in _INSERT_SNAPSHOT_SQL
