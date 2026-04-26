"""
Tests for `scraper.ingest.matches_writer.insert_matches`.

Locks the SAVEPOINT-per-row isolation contract: a single bad row in a
batch must NOT roll back successful sibling rows. Previously the writer
called `conn.rollback()` on per-row failure, which discarded the entire
transaction (including the split-brain pre-sweep UPDATE that had
already run for the failing row). This test fails CI if that pattern
regresses.

Hand-rolled fake conn/cursor mirrors the shape used in
`test_nav_leaked_names_detector.py` etc. — no real Postgres, no
psycopg2 dependency at test time.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from ingest import matches_writer  # noqa: E402


class FakeCursor:
    """Tracks SAVEPOINT/RELEASE/ROLLBACK state and per-row INSERT outcomes.

    `fail_on_player`: an `(home_team_name, away_team_name)` tuple — when
    the writer executes an INSERT for that row, the fake raises. The
    writer is expected to ROLLBACK TO SAVEPOINT and continue.
    """

    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn
        self._last_singleton: Optional[Tuple[Any, ...]] = None
        self.rowcount: int = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        sql_norm = " ".join(sql.split()).strip()
        head = sql_norm.split(" ", 1)[0].upper()

        # Savepoint plumbing.
        if head == "SAVEPOINT":
            self._conn.savepoints.append(sql_norm)
            self._conn.savepoint_stack.append(list(self._conn.uncommitted_inserts))
            return
        if sql_norm.upper().startswith("ROLLBACK TO SAVEPOINT"):
            # Restore uncommitted insert log to the pre-savepoint state.
            self._conn.rolled_back_savepoints += 1
            if self._conn.savepoint_stack:
                self._conn.uncommitted_inserts = self._conn.savepoint_stack.pop()
            return
        if sql_norm.upper().startswith("RELEASE SAVEPOINT"):
            self._conn.released_savepoints += 1
            if self._conn.savepoint_stack:
                self._conn.savepoint_stack.pop()
            return

        # Pre-sweep UPDATE.
        if sql_norm.upper().startswith("UPDATE MATCHES"):
            assert params is not None
            home = params.get("home_team_name")
            away = params.get("away_team_name")
            if (home, away) in self._conn.presweep_hits:
                self.rowcount = 1
                self._conn.presweep_seen.append((home, away))
            else:
                self.rowcount = 0
            return

        # INSERT INTO matches ...
        if sql_norm.upper().startswith("INSERT INTO MATCHES"):
            assert params is not None
            home = params.get("home_team_name")
            away = params.get("away_team_name")
            if (home, away) == self._conn.fail_on_pair:
                raise RuntimeError("simulated FK / unique violation")
            self._conn.uncommitted_inserts.append((home, away))
            # Return RETURNING (id, inserted=True). New ids are 1, 2, 3...
            new_id = len(self._conn.uncommitted_inserts)
            self._last_singleton = (new_id, True)
            return

        raise AssertionError(f"unexpected SQL in fake: {sql_norm[:120]}")

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._last_singleton


class FakeConn:
    def __init__(
        self,
        *,
        fail_on_pair: Optional[Tuple[str, str]] = None,
        presweep_hits: Optional[set] = None,
    ) -> None:
        self.fail_on_pair = fail_on_pair
        self.presweep_hits = presweep_hits or set()
        self.uncommitted_inserts: List[Tuple[str, str]] = []
        self.committed_inserts: List[Tuple[str, str]] = []
        self.savepoints: List[str] = []
        self.savepoint_stack: List[List[Tuple[str, str]]] = []
        self.released_savepoints: int = 0
        self.rolled_back_savepoints: int = 0
        self.commits: int = 0
        self.full_rollbacks: int = 0
        self.presweep_seen: List[Tuple[str, str]] = []
        self.closed: bool = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.committed_inserts.extend(self.uncommitted_inserts)
        self.uncommitted_inserts = []

    def rollback(self) -> None:
        # If the writer ever calls this directly, the savepoint isolation
        # contract is broken. Tests assert this counter stays at 0.
        self.full_rollbacks += 1
        self.uncommitted_inserts = []

    def close(self) -> None:
        self.closed = True


def _row(home: str, away: str, *, platform_match_id: Optional[str] = None) -> Dict[str, Any]:
    return {
        "home_team_name": home,
        "away_team_name": away,
        "match_date": None,
        "age_group": "U15",
        "gender": "Boys",
        "season": "2025-26",
        "platform_match_id": platform_match_id,
        "source": "gotsport",
        "source_url": "http://example.test/match",
        "status": "scheduled",
        "home_score": None,
        "away_score": None,
        "division": None,
        "league": None,
        "event_id": None,
        "home_club_id": None,
        "away_club_id": None,
    }


def test_one_bad_row_does_not_poison_sibling_rows() -> None:
    """The whole point of the savepoint refactor."""
    rows = [
        _row("Alpha FC", "Bravo SC"),
        _row("Charlie United", "Delta Athletic"),  # this one fails
        _row("Echo Rovers", "Foxtrot City"),
    ]
    conn = FakeConn(fail_on_pair=("Charlie United", "Delta Athletic"))

    counts = matches_writer.insert_matches(rows, conn=conn)
    # Caller owns commit when conn is passed in. Mirror that here.
    conn.commit()

    assert counts["inserted"] == 2
    assert counts["skipped"] == 1
    assert counts["updated"] == 0
    # Sibling rows survive — and end up committed.
    assert ("Alpha FC", "Bravo SC") in conn.committed_inserts
    assert ("Echo Rovers", "Foxtrot City") in conn.committed_inserts
    # The failing row is NOT in committed.
    assert ("Charlie United", "Delta Athletic") not in conn.committed_inserts


def test_writer_never_calls_conn_rollback_on_row_failure() -> None:
    """Regression guard for the original bug.

    Before the savepoint refactor the writer called `conn.rollback()`
    on every per-row failure. That nuked uncommitted sibling rows AND
    the presweep UPDATE that had already run. This test asserts the
    writer does not regress to that pattern.
    """
    rows = [
        _row("Alpha FC", "Bravo SC"),
        _row("Charlie United", "Delta Athletic"),  # fails
    ]
    conn = FakeConn(fail_on_pair=("Charlie United", "Delta Athletic"))

    matches_writer.insert_matches(rows, conn=conn)

    assert conn.full_rollbacks == 0, (
        "writer called conn.rollback() on per-row failure — regression "
        "to the pre-savepoint pattern. Use ROLLBACK TO SAVEPOINT."
    )
    # The savepoint should have been rolled back exactly once (for the
    # one failing row). Successful rows release.
    assert conn.rolled_back_savepoints == 1
    assert conn.released_savepoints == 1


def test_presweep_counter_only_advances_when_insert_commits() -> None:
    """If the INSERT fails after a successful presweep, the presweep
    counter must NOT increment — the savepoint rollback discards the
    presweep UPDATE."""
    rows = [
        # Both rows have a platform_match_id so the presweep runs.
        _row("Alpha FC", "Bravo SC", platform_match_id="A1"),
        _row("Charlie United", "Delta Athletic", platform_match_id="C1"),
    ]
    conn = FakeConn(
        fail_on_pair=("Charlie United", "Delta Athletic"),
        # Both rows have a presweep hit; the failing row's presweep
        # MUST be discarded.
        presweep_hits={
            ("Alpha FC", "Bravo SC"),
            ("Charlie United", "Delta Athletic"),
        },
    )

    counts = matches_writer.insert_matches(rows, conn=conn)

    # Only the surviving row's presweep counts.
    assert counts["presweep_upgraded"] == 1
    assert counts["inserted"] == 1
    assert counts["skipped"] == 1
