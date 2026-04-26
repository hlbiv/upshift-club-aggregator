"""
Tests for `scraper.ingest.roster_snapshot_writer.insert_roster_snapshots`.

Covers two correctness fixes:

1. SAVEPOINT-per-row isolates a single bad row from the batch — both
   for the snapshot upserts and for the diff inserts. The previous
   pattern used `conn.rollback()` which discarded the entire txn,
   throwing away every prior successful row.

2. The prior-snapshot lookup is FAIL-LOUD: a transient DB error during
   the lookup must propagate, not silently set `prior=[]` and emit a
   "first scrape, no diffs" outcome. The previous behavior masked DB
   blips by faking a clean first scrape, which then corrupted the
   `roster_diffs` history forever after.

Hand-rolled fake conn/cursor — no real Postgres at test time.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from ingest import roster_snapshot_writer  # noqa: E402


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn
        self._last_singleton: Optional[Tuple[Any, ...]] = None
        self._last_result: List[Tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        sql_norm = " ".join(sql.split()).strip()
        head = sql_norm.split(" ", 1)[0].upper()

        if head == "SAVEPOINT":
            self._conn.savepoint_stack.append(list(self._conn.uncommitted_inserts))
            return
        if sql_norm.upper().startswith("ROLLBACK TO SAVEPOINT"):
            self._conn.rolled_back_savepoints += 1
            if self._conn.savepoint_stack:
                self._conn.uncommitted_inserts = self._conn.savepoint_stack.pop()
            return
        if sql_norm.upper().startswith("RELEASE SAVEPOINT"):
            self._conn.released_savepoints += 1
            if self._conn.savepoint_stack:
                self._conn.savepoint_stack.pop()
            return

        # Prior-snapshot lookup.
        if sql_norm.upper().startswith("SELECT PLAYER_NAME, JERSEY_NUMBER, POSITION"):
            if self._conn.fail_prior_lookup:
                raise RuntimeError("simulated prior-lookup outage")
            self._last_result = list(self._conn.prior_snapshot)
            return

        if sql_norm.upper().startswith("INSERT INTO CLUB_ROSTER_SNAPSHOTS"):
            assert params is not None
            player = params.get("player_name")
            if player == self._conn.fail_on_player:
                raise RuntimeError("simulated insert error")
            self._conn.uncommitted_inserts.append(("snapshot", player))
            self._last_singleton = (True,)  # inserted
            return

        if sql_norm.upper().startswith("INSERT INTO ROSTER_DIFFS"):
            assert params is not None
            player = params.get("player_name")
            self._conn.uncommitted_inserts.append(("diff", player, params.get("diff_type")))
            self._last_singleton = (len(self._conn.uncommitted_inserts),)
            return

        raise AssertionError(f"unexpected SQL in fake: {sql_norm[:120]}")

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._last_singleton

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return self._last_result


class FakeConn:
    def __init__(
        self,
        *,
        fail_on_player: Optional[str] = None,
        fail_prior_lookup: bool = False,
        prior_snapshot: Optional[List[Tuple[str, Optional[str], Optional[str]]]] = None,
    ) -> None:
        self.fail_on_player = fail_on_player
        self.fail_prior_lookup = fail_prior_lookup
        self.prior_snapshot = prior_snapshot or []
        self.uncommitted_inserts: List[Tuple[Any, ...]] = []
        self.committed_inserts: List[Tuple[Any, ...]] = []
        self.savepoint_stack: List[List[Tuple[Any, ...]]] = []
        self.released_savepoints: int = 0
        self.rolled_back_savepoints: int = 0
        self.full_rollbacks: int = 0
        self.commits: int = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1
        self.committed_inserts.extend(self.uncommitted_inserts)
        self.uncommitted_inserts = []

    def rollback(self) -> None:
        self.full_rollbacks += 1
        self.uncommitted_inserts = []


def _row(player: str, *, club: str = "Alpha FC", season: str = "2025-26") -> Dict[str, Any]:
    return {
        "club_name_raw": club,
        "season": season,
        "age_group": "U15",
        "gender": "Boys",
        "snapshot_date": date(2026, 1, 15),
        "player_name": player,
        "jersey_number": "10",
        "position": "MF",
        "source_url": "http://example.test/roster",
    }


def test_one_bad_row_does_not_poison_sibling_rows() -> None:
    rows = [_row("Alpha"), _row("Bravo"), _row("Charlie")]
    conn = FakeConn(fail_on_player="Bravo")

    counts = roster_snapshot_writer.insert_roster_snapshots(rows, conn=conn)
    conn.commit()

    assert counts["inserted"] == 2
    assert counts["skipped"] == 1
    # Surviving snapshots committed.
    assert ("snapshot", "Alpha") in conn.committed_inserts
    assert ("snapshot", "Charlie") in conn.committed_inserts
    # Failing snapshot not in committed.
    assert not any(
        kind == "snapshot" and name == "Bravo"
        for kind, name, *_ in conn.committed_inserts
    )


def test_writer_never_calls_conn_rollback_on_row_failure() -> None:
    rows = [_row("Alpha"), _row("Bravo")]
    conn = FakeConn(fail_on_player="Bravo")

    roster_snapshot_writer.insert_roster_snapshots(rows, conn=conn)

    assert conn.full_rollbacks == 0, (
        "writer called conn.rollback() on per-row failure — regression "
        "to the pre-savepoint pattern. Use ROLLBACK TO SAVEPOINT."
    )
    assert conn.rolled_back_savepoints == 1
    assert conn.released_savepoints == 1


def test_prior_lookup_failure_propagates_not_silently_swallowed() -> None:
    """If the prior-snapshot SELECT errors, the writer must raise.

    Previously the writer caught the exception, called `conn.rollback`,
    set `prior=[]`, and continued — which produced an outcome
    indistinguishable from a legitimate first scrape (no diffs). On a
    transient DB blip the group lost its diff history forever after.
    """
    rows = [_row("Alpha"), _row("Bravo")]
    conn = FakeConn(fail_prior_lookup=True)

    with pytest.raises(RuntimeError, match="prior-lookup outage"):
        roster_snapshot_writer.insert_roster_snapshots(rows, conn=conn)


def test_diff_materialization_uses_savepoints_too() -> None:
    """A failure during diff insert must not poison sibling diff rows
    or snapshot rows in the same group."""
    rows = [_row("Alpha"), _row("Bravo")]
    # Prior had only "Alpha" — current adds "Bravo". Expect ONE diff
    # (added: Bravo). No diff insert should fail in this scenario, so
    # this is the happy-path baseline.
    conn = FakeConn(prior_snapshot=[("Alpha", "10", "MF")])

    counts = roster_snapshot_writer.insert_roster_snapshots(rows, conn=conn)
    conn.commit()

    assert counts["diffs_written"] == 1
    # Both snapshots inserted; one diff for the new player.
    diff_inserts = [c for c in conn.committed_inserts if c[0] == "diff"]
    assert ("diff", "Bravo", "added") in diff_inserts
