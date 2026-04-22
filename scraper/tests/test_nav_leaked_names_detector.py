"""
Tests for `nav_leaked_names_detector.detect_all`.

Uses a hand-rolled in-memory `FakeConn` / `FakeCursor` matching the
shape used elsewhere in this suite (see `test_canonical_club_linker.py`)
rather than spinning up a real Postgres. We exercise:

* `is_nav_word` exact-match semantics (case-insensitive, NO substring).
* A clean snapshot group produces zero flags.
* A leaked snapshot group produces exactly one upserted flag with the
  expected `metadata` payload.
* Re-running on unchanged data is idempotent (no duplicate insert; the
  conflict-target's WHERE filter prevents a metadata-equal update).
* Re-running after the leak set changes refreshes the metadata.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Make the scraper/ package root importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

import nav_leaked_names_detector as detector  # noqa: E402


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
            # detector._iter_snapshot_rows uses keyset pagination:
            #   WHERE [scraped_at >= NOW() - interval AND] id > %s
            #   ORDER BY id LIMIT %s
            # The trailing two params are always (last_id, page_size);
            # an optional leading window_days_str precedes them when
            # the windowed branch is in use.
            windowed = "WHERE scraped_at" in sql_norm
            assert "ORDER BY id" in sql_norm
            assert "LIMIT" in sql_norm
            assert "id > %s" in sql_norm
            assert params is not None
            if windowed:
                assert len(params) == 3
                window_days = int(params[0])
                last_id = int(params[1])
                page_size = int(params[2])
                cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
                matched = [
                    r
                    for r in self._conn.snapshots
                    if _scraped_at(r) >= cutoff and r["id"] > last_id
                ]
            else:
                assert len(params) == 2
                last_id = int(params[0])
                page_size = int(params[1])
                matched = [
                    r for r in self._conn.snapshots if r["id"] > last_id
                ]
            matched.sort(key=lambda r: r["id"])
            matched = matched[:page_size]
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
        # Default = NOW() so existing tests (which don't pass
        # scraped_at) all fall inside the incremental window.
        "scraped_at": scraped_at if scraped_at is not None else datetime.now(timezone.utc),
    }


def _scraped_at(row: Dict[str, Any]) -> datetime:
    """Read back the timestamp injected by `_row`."""
    return row["scraped_at"]


# ---------------------------------------------------------------------------
# is_nav_word
# ---------------------------------------------------------------------------

def test_is_nav_word_exact_case_insensitive() -> None:
    assert detector.is_nav_word("Home")
    assert detector.is_nav_word("home")
    assert detector.is_nav_word("HOME")
    assert detector.is_nav_word("  Contact  ")  # trims
    assert detector.is_nav_word("About Us")
    assert detector.is_nav_word("about us")


def test_is_nav_word_full_match_only_no_substring() -> None:
    # Real player names that contain a nav word as a substring must NOT trip.
    assert not detector.is_nav_word("Tom Sitemap")
    assert not detector.is_nav_word("Homer Simpson")
    assert not detector.is_nav_word("Jane Newsome")
    assert not detector.is_nav_word("Logan Newhouse")
    assert not detector.is_nav_word("")
    assert not detector.is_nav_word("not-a-nav-word")


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


def test_leaked_group_writes_one_flag_with_expected_metadata() -> None:
    snapshots = [
        _row(10, "Bay Oaks SC", "2024-25", "U17", "Girls", "Home"),
        _row(11, "Bay Oaks SC", "2024-25", "U17", "Girls", "Contact"),
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

    # Representative snapshot_id is the smallest in the leaked group.
    assert 10 in conn.flags_by_snapshot
    flag = conn.flags_by_snapshot[10]
    md = flag["metadata"]
    assert sorted(md["leaked_strings"]) == ["Contact", "Home"]
    assert md["snapshot_roster_size"] == 3


def test_dry_run_writes_nothing_and_rolls_back() -> None:
    snapshots = [
        _row(20, "X", "2024-25", "U13", "Boys", "Home"),
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
        _row(30, "Y", "2024-25", "U14", "Girls", "Home"),
        _row(31, "Y", "2024-25", "U14", "Girls", "Sitemap"),
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
        _row(101, "A", "2024-25", "U15", "Boys", "Home", scraped_at=recent2),
        # Old row — outside the default 7d window; detector should not see it.
        _row(200, "B", "2024-25", "U15", "Boys", "Home", scraped_at=ancient),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False)

    # Only the two recent rows are scanned; the ancient row is filtered
    # out by the scraped_at window.
    assert stats.rows_scanned == 2
    # Only the recent leaked group gets flagged. The flag points at the
    # smallest snapshot_id in the group (100) — see detect_all's
    # representative_snapshot_id = min(...).
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
        _row(301, "A", "2024-25", "U15", "Boys", "Home", scraped_at=recent2),
        _row(400, "B", "2024-25", "U15", "Boys", "Home", scraped_at=ancient),
    ]
    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False, full_scan=True)

    # Every row is scanned, including the ancient one.
    assert stats.rows_scanned == 3
    # Both leaked groups get flagged. Representative snapshot_ids are
    # the smallest in each group (300 and 400).
    assert stats.snapshot_groups_flagged == 2
    assert 300 in conn.flags_by_snapshot
    assert 400 in conn.flags_by_snapshot


def test_group_split_across_batch_boundary_handled_correctly() -> None:
    """
    A snapshot group can have rows interleaved with other groups by
    `id`, so keyset pagination will frequently split a group across
    two (or more) batches. Force that scenario with batch_size=2 and
    verify the per-group accumulator (leaked_set, roster_size,
    representative min snapshot_id) is unchanged vs. a single-batch
    scan.
    """
    # Two groups (Z, W) interleaved by id. With batch_size=2 the page
    # boundaries fall mid-group:
    #   batch 1: ids 50, 51   (Z=Home, W=clean)
    #   batch 2: ids 52, 53   (Z=clean, W=Contact)
    #   batch 3: ids 54, 55   (Z=Sitemap, W=clean)
    #   batch 4: ids 56       (W=clean)
    snapshots = [
        _row(50, "Z", "2024-25", "U15", "Boys", "Home"),
        _row(51, "W", "2024-25", "U15", "Boys", "Alex Real"),
        _row(52, "Z", "2024-25", "U15", "Boys", "Real Player"),
        _row(53, "W", "2024-25", "U15", "Boys", "Contact"),
        _row(54, "Z", "2024-25", "U15", "Boys", "Sitemap"),
        _row(55, "W", "2024-25", "U15", "Boys", "Pat Real"),
        _row(56, "W", "2024-25", "U15", "Boys", "Sam Real"),
    ]

    conn = FakeConn(snapshots)
    stats = detector.detect_all(conn, dry_run=False, batch_size=2)

    assert stats.rows_scanned == 7
    assert stats.snapshot_groups_scanned == 2
    assert stats.snapshot_groups_flagged == 2

    # Z group: representative = min id = 50, two leaked strings, three rows.
    assert 50 in conn.flags_by_snapshot
    z_md = conn.flags_by_snapshot[50]["metadata"]
    assert sorted(z_md["leaked_strings"]) == ["Home", "Sitemap"]
    assert z_md["snapshot_roster_size"] == 3

    # W group: representative = min id = 51, one leaked string, four rows.
    assert 51 in conn.flags_by_snapshot
    w_md = conn.flags_by_snapshot[51]["metadata"]
    assert w_md["leaked_strings"] == ["Contact"]
    assert w_md["snapshot_roster_size"] == 4

    # Sanity check: the streaming result must equal the single-batch
    # result. Re-run with a batch large enough to fit everything in
    # one page and compare.
    conn_one_batch = FakeConn(snapshots)
    detector.detect_all(conn_one_batch, dry_run=False, batch_size=1000)
    assert (
        conn_one_batch.flags_by_snapshot[50]["metadata"]
        == conn.flags_by_snapshot[50]["metadata"]
    )
    assert (
        conn_one_batch.flags_by_snapshot[51]["metadata"]
        == conn.flags_by_snapshot[51]["metadata"]
    )


def test_re_run_after_leak_change_updates_metadata() -> None:
    snapshots = [
        _row(40, "Z", "2024-25", "U15", "Boys", "Home"),
        _row(41, "Z", "2024-25", "U15", "Boys", "Real Player"),
    ]
    conn = FakeConn(snapshots)
    detector.detect_all(conn, dry_run=False)
    first_md = dict(conn.flags_by_snapshot[40]["metadata"])
    assert first_md["leaked_strings"] == ["Home"]
    assert first_md["snapshot_roster_size"] == 2

    # Add another leaked row to the same group.
    snapshots.append(_row(42, "Z", "2024-25", "U15", "Boys", "Contact"))
    stats2 = detector.detect_all(conn, dry_run=False)

    assert stats2.flags_inserted == 0
    assert stats2.flags_updated == 1
    md2 = conn.flags_by_snapshot[40]["metadata"]
    assert sorted(md2["leaked_strings"]) == ["Contact", "Home"]
    assert md2["snapshot_roster_size"] == 3
