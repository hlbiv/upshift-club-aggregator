"""
Tests for `numeric_only_name_detector.detect_all`.

Structural clone of `test_nav_leaked_names_detector.py`. Uses a
hand-rolled in-memory `FakeConn` / `FakeCursor` rather than spinning up
a real Postgres. We exercise:

* `is_numeric_only_name` full-string match semantics (digits, separators,
  whitespace only; name-like strings must NOT match).
* A clean snapshot group produces zero flags.
* A snapshot group with numeric-only names produces exactly one upserted
  flag with the expected `metadata` payload.
* Re-running on unchanged data is idempotent (no duplicate insert; the
  conflict-target's WHERE filter prevents a metadata-equal update).
* Re-running after the offending set changes refreshes the metadata.
* Default incremental window excludes ancient rows.
* `--full-scan` bypasses the window.
* Dry-run writes nothing and rolls back.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Make the scraper/ package root importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

import numeric_only_name_detector as detector  # noqa: E402


# ---------------------------------------------------------------------------
# Fake DB
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn
        self._last_result: List[Tuple[Any, ...]] = []
        self._last_singleton: Optional[Tuple[Any, ...]] = None

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        sql_norm = " ".join(sql.split())
        if sql_norm.startswith(
            "SELECT id, club_name_raw, season, age_group, gender, player_name "
            "FROM club_roster_snapshots"
        ):
            windowed = "WHERE scraped_at" in sql_norm
            if windowed:
                assert params is not None and len(params) == 1
                window_days = int(params[0])
                cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
                matched = [
                    r
                    for r in self._conn.snapshots
                    if _scraped_at(r) >= cutoff
                ]
            else:
                matched = list(self._conn.snapshots)
            self._last_result = [
                (
                    r["id"],
                    r["club_name_raw"],
                    r["season"],
                    r["age_group"],
                    r["gender"],
                    r["player_name"],
                )
                for r in matched
            ]
            return

        if sql_norm.startswith("INSERT INTO roster_quality_flags"):
            assert params is not None
            snapshot_id, metadata_wrapped = params
            # Unwrap psycopg2.extras.Json if present, else use as-is.
            metadata = (
                metadata_wrapped.adapted
                if hasattr(metadata_wrapped, "adapted")
                else metadata_wrapped
            )
            existing = self._conn.flags_by_snapshot.get(snapshot_id)
            if existing is None:
                self._conn.flags_by_snapshot[snapshot_id] = {
                    "metadata": metadata,
                    "resolved_at": None,
                }
                self._last_singleton = (True,)  # inserted
            else:
                if existing["metadata"] == metadata:
                    # ON CONFLICT ... WHERE filter: no row returned.
                    self._last_singleton = None
                else:
                    existing["metadata"] = metadata
                    self._last_singleton = (False,)  # updated
            return

        raise AssertionError(f"unexpected SQL: {sql_norm[:120]}")

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return self._last_result

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._last_singleton


class FakeConn:
    def __init__(self, snapshots: List[Dict[str, Any]]) -> None:
        self.snapshots = snapshots
        self.flags_by_snapshot: Dict[int, Dict[str, Any]] = {}
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _row(
    snap_id: int,
    club: str,
    season: str,
    age: str,
    gender: str,
    player: str,
    scraped_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    return {
        "id": snap_id,
        "club_name_raw": club,
        "season": season,
        "age_group": age,
        "gender": gender,
        "player_name": player,
        "scraped_at": scraped_at if scraped_at is not None else datetime.now(timezone.utc),
    }


def _scraped_at(row: Dict[str, Any]) -> datetime:
    return row["scraped_at"]


# ---------------------------------------------------------------------------
# is_numeric_only_name
# ---------------------------------------------------------------------------

def test_is_numeric_only_name_positive_bare_numbers() -> None:
    assert detector.is_numeric_only_name("14")
    assert detector.is_numeric_only_name("007")
    assert detector.is_numeric_only_name("  17  ")  # trims


def test_is_numeric_only_name_positive_dates() -> None:
    # ISO-style.
    assert detector.is_numeric_only_name("2024-05-15")
    # US slash-style, both full and partial.
    assert detector.is_numeric_only_name("5/15/2024")
    assert detector.is_numeric_only_name("5/15")


def test_is_numeric_only_name_positive_misc_separators() -> None:
    # Decimals / ranges / internal whitespace.
    assert detector.is_numeric_only_name("12.5")
    assert detector.is_numeric_only_name("1 2 3")
    assert detector.is_numeric_only_name("10-12")


def test_is_numeric_only_name_positive_empty_and_whitespace() -> None:
    # Empty strings and whitespace-only strings match defensively.
    assert detector.is_numeric_only_name("")
    assert detector.is_numeric_only_name("   ")
    assert detector.is_numeric_only_name("\t")


def test_is_numeric_only_name_negative_names_with_digits() -> None:
    # A real name with a jersey number should NOT match — the alphabetic
    # characters take it out of the numeric-only set.
    assert not detector.is_numeric_only_name("Tom 14")
    assert not detector.is_numeric_only_name("Jane Smith")
    assert not detector.is_numeric_only_name("Lionel Messi")
    # The `#` sigil is deliberately NOT in the regex charset — a common
    # "# 14" jersey formatting should survive as a non-match so it can be
    # handled by a future heuristic.
    assert not detector.is_numeric_only_name("# 14")
    # Other punctuation outside the allowed set also excludes the match.
    assert not detector.is_numeric_only_name("14,15")
    assert not detector.is_numeric_only_name("5:15")


def test_is_numeric_only_name_non_string_is_false() -> None:
    # Defensive: the matcher accepts any input and returns False for
    # non-strings rather than raising.
    assert not detector.is_numeric_only_name(None)  # type: ignore[arg-type]
    assert not detector.is_numeric_only_name(14)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# detect_all
# ---------------------------------------------------------------------------

def test_clean_snapshot_group_produces_no_flag() -> None:
    snapshots = [
        _row(1, "Atlanta United", "2024-25", "U15", "Boys", "Lionel Messi"),
        _row(2, "Atlanta United", "2024-25", "U15", "Boys", "Cristiano Ronaldo"),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False)

    assert stats.snapshot_groups_scanned == 1
    assert stats.snapshot_groups_flagged == 0
    assert stats.flags_inserted == 0
    assert stats.flags_updated == 0
    assert conn.flags_by_snapshot == {}
    assert conn.commits == 1


def test_numeric_group_writes_one_flag_with_expected_metadata() -> None:
    snapshots = [
        _row(10, "Bay Oaks SC", "2024-25", "U17", "Girls", "14"),
        _row(11, "Bay Oaks SC", "2024-25", "U17", "Girls", "2024-05-15"),
        _row(12, "Bay Oaks SC", "2024-25", "U17", "Girls", "Sophia Smith"),
        # Different group: clean
        _row(13, "Other Club", "2024-25", "U17", "Girls", "Alex Morgan"),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False)

    assert stats.snapshot_groups_scanned == 2
    assert stats.snapshot_groups_flagged == 1
    assert stats.flags_inserted == 1
    assert stats.flags_updated == 0

    # Representative snapshot_id is the smallest in the offending group.
    assert 10 in conn.flags_by_snapshot
    flag = conn.flags_by_snapshot[10]
    md = flag["metadata"]
    assert sorted(md["numeric_strings"]) == ["14", "2024-05-15"]
    assert md["snapshot_roster_size"] == 3


def test_dry_run_writes_nothing_and_rolls_back() -> None:
    snapshots = [
        _row(20, "X", "2024-25", "U13", "Boys", "14"),
        _row(21, "X", "2024-25", "U13", "Boys", "John Doe"),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=True)

    assert stats.snapshot_groups_flagged == 1
    assert conn.flags_by_snapshot == {}
    assert conn.rollbacks == 1
    assert conn.commits == 0


def test_idempotent_second_run_no_duplicate_no_update() -> None:
    snapshots = [
        _row(30, "Y", "2024-25", "U14", "Girls", "14"),
        _row(31, "Y", "2024-25", "U14", "Girls", "5/15"),
        _row(32, "Y", "2024-25", "U14", "Girls", "Real Player"),
    ]
    conn = FakeConn(snapshots)

    detector.detect_all(conn, dry_run=False)
    snapshot_after_first = dict(conn.flags_by_snapshot[30])

    stats2 = detector.detect_all(conn, dry_run=False)

    # No new insert, no update (metadata unchanged).
    assert stats2.flags_inserted == 0
    assert stats2.flags_updated == 0
    assert conn.flags_by_snapshot[30]["metadata"] == snapshot_after_first["metadata"]


def test_incremental_window_skips_old_snapshots() -> None:
    """Default window (7d) excludes rows whose scraped_at is outside it."""
    now = datetime.now(timezone.utc)
    recent1 = now - timedelta(hours=6)
    recent2 = now - timedelta(days=3)
    ancient = now - timedelta(days=90)

    snapshots = [
        _row(100, "A", "2024-25", "U15", "Boys", "Pete Recent", scraped_at=recent1),
        _row(101, "A", "2024-25", "U15", "Boys", "14", scraped_at=recent2),
        # Old row — outside the default 7d window; detector should not see it.
        _row(200, "B", "2024-25", "U15", "Boys", "14", scraped_at=ancient),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False)

    # Only the two recent rows are scanned; the ancient row is filtered
    # out by the scraped_at window.
    assert stats.rows_scanned == 2
    assert stats.snapshot_groups_flagged == 1
    assert 100 in conn.flags_by_snapshot
    assert 200 not in conn.flags_by_snapshot


def test_full_scan_flag_ignores_window() -> None:
    """--full-scan (full_scan=True) bypasses the window filter entirely."""
    now = datetime.now(timezone.utc)
    recent1 = now - timedelta(hours=6)
    recent2 = now - timedelta(days=3)
    ancient = now - timedelta(days=90)

    snapshots = [
        _row(300, "A", "2024-25", "U15", "Boys", "Pete Recent", scraped_at=recent1),
        _row(301, "A", "2024-25", "U15", "Boys", "14", scraped_at=recent2),
        _row(400, "B", "2024-25", "U15", "Boys", "14", scraped_at=ancient),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False, full_scan=True)

    # Every row is scanned, including the ancient one.
    assert stats.rows_scanned == 3
    assert stats.snapshot_groups_flagged == 2
    assert 300 in conn.flags_by_snapshot
    assert 400 in conn.flags_by_snapshot


def test_re_run_after_offending_set_change_updates_metadata() -> None:
    snapshots = [
        _row(40, "Z", "2024-25", "U15", "Boys", "14"),
        _row(41, "Z", "2024-25", "U15", "Boys", "Real Player"),
    ]
    conn = FakeConn(snapshots)
    detector.detect_all(conn, dry_run=False)
    first_md = dict(conn.flags_by_snapshot[40]["metadata"])
    assert first_md["numeric_strings"] == ["14"]
    assert first_md["snapshot_roster_size"] == 2

    # Add another numeric-only row to the same group.
    snapshots.append(_row(42, "Z", "2024-25", "U15", "Boys", "2024-05-15"))
    stats2 = detector.detect_all(conn, dry_run=False)

    assert stats2.flags_inserted == 0
    assert stats2.flags_updated == 1
    md2 = conn.flags_by_snapshot[40]["metadata"]
    assert sorted(md2["numeric_strings"]) == ["14", "2024-05-15"]
    assert md2["snapshot_roster_size"] == 3
