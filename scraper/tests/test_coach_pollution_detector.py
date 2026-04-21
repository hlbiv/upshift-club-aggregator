"""
Tests for `coach_pollution_detector.detect_all`.

Structural clone of `test_nav_leaked_names_detector.py`. Uses a
hand-rolled in-memory `FakeConn` / `FakeCursor` rather than spinning up
a real Postgres. Covers:

* Pure-function `_classify_reject` over a representative set of
  observed-pollution strings.
* Dry-run (commit=False) produces stats but NO writes and rolls back.
* Commit mode inserts one flag per rejected row with the documented
  metadata shape.
* Idempotency: a second run against the same data writes 0 additional
  rows (ON CONFLICT DO NOTHING).
* Missing `coach_quality_flags` table → no-op + no exception.
* `--window-days` filter restricts the scan to recent rows.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Make the scraper/ package root importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

import coach_pollution_detector as detector  # noqa: E402


# ---------------------------------------------------------------------------
# Fake DB
# ---------------------------------------------------------------------------

class FakeCursor:
    """Minimal psycopg2-cursor stand-in.

    Recognizes exactly the three SQL shapes the detector emits:
      1. `SELECT to_regclass(...)` — table-existence probe.
      2. `SELECT id, name, email FROM coach_discoveries ... ORDER BY id`
         with optional `WHERE first_seen_at >= NOW() - (%s || ' days')::interval`
         and optional `LIMIT N` (inlined).
      3. `INSERT INTO coach_quality_flags ... ON CONFLICT ... DO NOTHING RETURNING id`.
    """

    def __init__(self, conn: "FakeConn") -> None:
        self._conn = conn
        self._last_result: List[Tuple[Any, ...]] = []
        self._last_singleton: Optional[Tuple[Any, ...]] = None
        self._fetchmany_cursor = 0

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        sql_norm = " ".join(sql.split())

        # Reproduce psycopg2 client-side-cursor semantics: every
        # `execute()` discards the previously-buffered result set. This
        # matters when production code mixes a paging SELECT
        # (fetchmany-over-a-shared-cursor) with writes on the same
        # cursor — the INSERT silently wipes the SELECT's remaining
        # rows. The detector regression guard
        # `test_commit_mode_scans_all_rows_across_multiple_pages`
        # depends on this simulation being faithful.
        self._last_result = []
        self._last_singleton = None
        self._fetchmany_cursor = 0

        # 1. Table-existence probe.
        if sql_norm.startswith("SELECT to_regclass"):
            assert params is not None and len(params) == 1
            table_name = params[0].split(".", 1)[-1]
            if table_name in self._conn.existing_tables:
                self._last_singleton = (f"public.{table_name}",)
            else:
                self._last_singleton = (None,)
            return

        # 2. coach_discoveries scan.
        if sql_norm.startswith("SELECT id, name, email FROM coach_discoveries"):
            windowed = "WHERE first_seen_at" in sql_norm
            if windowed:
                assert params is not None and len(params) == 1
                window_days = int(params[0])
                cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
                matched = [
                    r for r in self._conn.discoveries
                    if _first_seen_at(r) >= cutoff
                ]
            else:
                matched = list(self._conn.discoveries)

            # Simulate ORDER BY id.
            matched.sort(key=lambda r: r["id"])

            # Simulate inline LIMIT N.
            if "LIMIT" in sql_norm:
                # Extract the integer literal after LIMIT.
                tail = sql_norm.split("LIMIT", 1)[1].strip()
                limit_n = int(tail.split()[0])
                matched = matched[:limit_n]

            self._last_result = [
                (r["id"], r["name"], r["email"])
                for r in matched
            ]
            self._fetchmany_cursor = 0
            return

        # 3. Insert + ON CONFLICT DO NOTHING.
        if sql_norm.startswith("INSERT INTO coach_quality_flags"):
            assert params is not None
            discovery_id, metadata_wrapped = params
            metadata = (
                metadata_wrapped.adapted
                if hasattr(metadata_wrapped, "adapted")
                else metadata_wrapped
            )
            existing = self._conn.flags_by_discovery.get(discovery_id)
            if existing is None:
                # Fresh insert — assign an id like Postgres would.
                self._conn.next_flag_id += 1
                flag_id = self._conn.next_flag_id
                self._conn.flags_by_discovery[discovery_id] = {
                    "id": flag_id,
                    "metadata": metadata,
                }
                self._last_singleton = (flag_id,)  # RETURNING id
            else:
                # ON CONFLICT DO NOTHING — no row returned.
                self._last_singleton = None
            return

        raise AssertionError(f"unexpected SQL: {sql_norm[:200]}")

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._last_result)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._last_singleton

    def fetchmany(self, size: int) -> List[Tuple[Any, ...]]:
        """Page through `_last_result` in `size`-row chunks."""
        start = self._fetchmany_cursor
        end = start + size
        chunk = self._last_result[start:end]
        self._fetchmany_cursor = end
        return chunk


class FakeConn:
    def __init__(
        self,
        discoveries: List[Dict[str, Any]],
        existing_tables: Optional[List[str]] = None,
    ) -> None:
        self.discoveries = discoveries
        self.existing_tables = set(
            existing_tables
            if existing_tables is not None
            else ["coach_quality_flags", "coach_discoveries"]
        )
        self.flags_by_discovery: Dict[int, Dict[str, Any]] = {}
        self.next_flag_id = 0
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def _disc(
    disc_id: int,
    name: Any,
    email: Optional[str] = None,
    first_seen_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    return {
        "id": disc_id,
        "name": name,
        "email": email,
        # Default = NOW() so `--window-days`-filtered tests pass when
        # they don't explicitly set a timestamp.
        "first_seen_at": first_seen_at
        if first_seen_at is not None
        else datetime.now(timezone.utc),
    }


def _first_seen_at(row: Dict[str, Any]) -> datetime:
    return row["first_seen_at"]


# ---------------------------------------------------------------------------
# _classify_reject — pure-function coverage
# ---------------------------------------------------------------------------

def test_classify_reject_on_canonical_pollution_strings() -> None:
    """Exercise the seven representative pollution strings called out in
    the PR brief. Each MUST reject; the reason is whatever the guard's
    first-failing-check returns. We pin the EXACT reason so a future
    guard refactor that reorders checks is loud rather than silent."""
    cases = [
        # Too long (> 50 chars) — "Newsletter Sign-Up" is short enough,
        # but contains the digit-less blocklist token "sign up" after
        # normalization, AND an em-dash-ish hyphen. With the current
        # guard this reduces to: first-char upper ✓, not all-caps ✓,
        # alpha-start ✓, no digits ✓, normalized -> "newsletter signup"
        # which is NOT in the phrase blocklist; "sign" is also not a
        # single-token blocklist entry. Tokens: "Newsletter", "Sign-Up"
        # — len == 2, OK. Actually this passes every check except...
        # let me trace: "Newsletter Sign-Up" -> passes len, tokens=2,
        # upper first, not all caps, all alpha-start (N, S), no digits,
        # normalized = "newsletter signup" NOT in blocklist, lower_parts
        # = ["newsletter", "signup"]. "signup" is in _BLOCKLIST_TOKENS.
        # So: token_in_blocklist.
        ("Newsletter Sign-Up", "token_in_blocklist"),
        # All-caps short-circuits before token check runs.
        ("OPEN TRAINING & TRYOUTS", "all_caps"),
        # All-caps short-circuits.
        ("RELATED ARTICLES", "all_caps"),
        # "Saturday, April 11" splits into ["Saturday,", "April", "11"].
        # Third token's first char is "1" — not alphabetic. The guard
        # checks `not all(p[0].isalpha() ...)` BEFORE the digit-anywhere
        # check, so not_alpha_start fires first.
        ("Saturday, April 11", "not_alpha_start"),
        # Leading emoji → first token's first char is not uppercase
        # alphabetic. Guard order: len OK, token-count OK (emoji+Great
        # +for+all = 5? check), but first check that fails is
        # wrong_token_count (5 tokens) OR first_char_not_upper. Split
        # on whitespace: ["⭐️", "Great", "for", "all", "levels"] = 5
        # tokens → wrong_token_count fires first.
        ("\u2b50\ufe0f Great for all levels", "wrong_token_count"),
        # Role-label phrase matches "head coach" blocklist after
        # normalization. Token-count check: 2 tokens ✓. First char
        # upper ✓. Not all caps ✓. Alpha-start ✓. No digits ✓. Then
        # in_blocklist fires on "head coach".
        ("Head Coach", "in_blocklist"),
        # "Follow Us" is in _NAME_BLOCKLIST.
        ("Follow Us", "in_blocklist"),
    ]
    for raw, expected_reason in cases:
        got = detector._classify_reject(raw)
        assert got == expected_reason, f"{raw!r}: expected {expected_reason}, got {got}"


def test_classify_reject_passes_a_real_name() -> None:
    """A real coach name must not be flagged."""
    assert detector._classify_reject("Jane Smith") is None
    assert detector._classify_reject("Lionel Messi") is None
    assert detector._classify_reject("Maria Garcia-Lopez") is None


def test_classify_reject_handles_null_and_empty() -> None:
    """None / "" / whitespace all reject as too_short."""
    assert detector._classify_reject(None) == "too_short"
    assert detector._classify_reject("") == "too_short"
    assert detector._classify_reject("   ") == "too_short"


# ---------------------------------------------------------------------------
# detect_all — dry-run
# ---------------------------------------------------------------------------

def test_dry_run_produces_stats_without_writing() -> None:
    discoveries = [
        _disc(1, "Jane Smith", "jane@club.com"),          # clean
        _disc(2, "Head Coach", "coach@club.com"),         # in_blocklist
        _disc(3, "RELATED ARTICLES", None),               # all_caps
        _disc(4, "Pete D3igit", "x@y.com"),               # contains_digit
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=False)

    assert stats.discoveries_scanned == 4
    assert stats.discoveries_flagged == 3
    assert stats.flags_inserted == 0
    assert stats.flags_skipped_existing == 0
    assert stats.reject_reason_counts == {
        "in_blocklist": 1,
        "all_caps": 1,
        "contains_digit": 1,
    }
    # No writes — rolled back.
    assert conn.flags_by_discovery == {}
    assert conn.rollbacks == 1
    assert conn.commits == 0


# ---------------------------------------------------------------------------
# detect_all — commit mode
# ---------------------------------------------------------------------------

def test_commit_mode_inserts_one_flag_per_rejected_row() -> None:
    discoveries = [
        _disc(10, "Jane Smith", "jane@club.com"),         # clean
        _disc(11, "Head Coach", "hc@club.com"),           # in_blocklist
        _disc(12, "RELATED ARTICLES", None),              # all_caps
        _disc(13, "Pete D3igit", "z@y.com"),              # contains_digit
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True)

    assert stats.discoveries_scanned == 4
    assert stats.discoveries_flagged == 3
    assert stats.flags_inserted == 3
    assert stats.flags_skipped_existing == 0
    assert conn.commits == 1
    assert conn.rollbacks == 0

    # One flag row per rejected discovery_id.
    assert set(conn.flags_by_discovery.keys()) == {11, 12, 13}

    # Metadata shape contract.
    md_11 = conn.flags_by_discovery[11]["metadata"]
    assert md_11 == {
        "reject_reason": "in_blocklist",
        "raw_name": "Head Coach",
        "raw_email": "hc@club.com",
    }

    md_12 = conn.flags_by_discovery[12]["metadata"]
    assert md_12 == {
        "reject_reason": "all_caps",
        "raw_name": "RELATED ARTICLES",
        "raw_email": None,  # email was None on the source row
    }

    md_13 = conn.flags_by_discovery[13]["metadata"]
    assert md_13 == {
        "reject_reason": "contains_digit",
        "raw_name": "Pete D3igit",
        "raw_email": "z@y.com",
    }


def test_commit_mode_is_idempotent() -> None:
    """Second run against identical data writes 0 additional rows via
    the (discovery_id, flag_type) ON CONFLICT DO NOTHING."""
    discoveries = [
        _disc(20, "Head Coach", "hc@club.com"),
        _disc(21, "RELATED ARTICLES", None),
    ]
    conn = FakeConn(discoveries)

    stats1 = detector.detect_all(conn, commit=True)
    assert stats1.flags_inserted == 2
    assert stats1.flags_skipped_existing == 0

    stats2 = detector.detect_all(conn, commit=True)
    assert stats2.flags_inserted == 0
    assert stats2.flags_skipped_existing == 2

    # Underlying store unchanged between runs.
    assert set(conn.flags_by_discovery.keys()) == {20, 21}


# ---------------------------------------------------------------------------
# detect_all — missing-table no-op
# ---------------------------------------------------------------------------

def test_missing_coach_quality_flags_table_is_noop() -> None:
    """When the table isn't there (schema push hasn't happened yet on
    Replit), detect_all returns cleanly with zero counters and writes
    nothing. No exception."""
    discoveries = [
        _disc(30, "Head Coach", "hc@club.com"),
    ]
    conn = FakeConn(discoveries, existing_tables=["coach_discoveries"])

    stats = detector.detect_all(conn, commit=True)

    assert stats.discoveries_scanned == 0
    assert stats.discoveries_flagged == 0
    assert stats.flags_inserted == 0
    assert conn.flags_by_discovery == {}
    # Neither commit nor rollback — we bailed before the scan loop.
    assert conn.commits == 0
    assert conn.rollbacks == 0


# ---------------------------------------------------------------------------
# detect_all — window filter
# ---------------------------------------------------------------------------

def test_window_days_filter_restricts_scan() -> None:
    now = datetime.now(timezone.utc)
    recent = now - timedelta(days=2)
    ancient = now - timedelta(days=90)

    discoveries = [
        _disc(100, "Head Coach", "hc@a.com", first_seen_at=recent),
        _disc(101, "Jane Smith", "js@b.com", first_seen_at=recent),
        # Outside the window — should not be scanned.
        _disc(200, "RELATED ARTICLES", None, first_seen_at=ancient),
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True, window_days=7)

    # Only the two recent rows got scanned; the ancient one was filtered.
    assert stats.discoveries_scanned == 2
    assert stats.discoveries_flagged == 1
    assert 100 in conn.flags_by_discovery
    assert 200 not in conn.flags_by_discovery


def test_limit_caps_rows_scanned() -> None:
    discoveries = [_disc(i, "Head Coach", f"c{i}@x.com") for i in range(1, 11)]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True, limit=3)

    assert stats.discoveries_scanned == 3
    assert stats.discoveries_flagged == 3
    # First three ids (ORDER BY id) only.
    assert set(conn.flags_by_discovery.keys()) == {1, 2, 3}


# ---------------------------------------------------------------------------
# Sample-flags slicing
# ---------------------------------------------------------------------------

def test_sample_flags_caps_at_10() -> None:
    """stats.sample_flags is capped at 10 entries regardless of how
    many rejects the run produces."""
    discoveries = [_disc(i, "Head Coach", f"c{i}@x.com") for i in range(1, 21)]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=False)

    assert stats.discoveries_flagged == 20
    assert len(stats.sample_flags) == 10
    # Each sample is (discovery_id, raw_name, reject_reason).
    for disc_id, raw_name, reason in stats.sample_flags:
        assert isinstance(disc_id, int)
        assert raw_name == "Head Coach"
        assert reason == "in_blocklist"


# ---------------------------------------------------------------------------
# Multi-page commit — regression guard for the cursor-clobber bug.
# ---------------------------------------------------------------------------

def test_commit_mode_scans_all_rows_across_multiple_pages(monkeypatch) -> None:
    """Regression guard. Previously the detector used a single cursor
    for both the SELECT iteration and the `_upsert_flag` INSERT. Each
    INSERT's `execute()` on that shared cursor wiped the SELECT's
    buffered rows, so the fetchmany(PAGE_SIZE) loop terminated after
    the first page — production saw 1,000 of 2,647 rows scanned.

    The fix opens a dedicated write cursor so the read cursor's buffer
    survives each INSERT. This test lowers `PAGE_SIZE` to 3 and feeds
    10 polluted rows; the whole-scan contract must hold across
    multiple pages AND with writes happening between pages.
    """
    monkeypatch.setattr(detector, "PAGE_SIZE", 3)

    # 10 rows → 4 pages at PAGE_SIZE=3. Every row is a reject so each
    # iteration triggers _upsert_flag.
    discoveries = [_disc(i, "Head Coach", f"c{i}@x.com") for i in range(1, 11)]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True)

    assert stats.discoveries_scanned == 10, (
        "Expected all 10 rows scanned across 4 pages; got "
        f"{stats.discoveries_scanned}. Likely regression: write path is "
        "clobbering the read cursor's buffered result set."
    )
    assert stats.discoveries_flagged == 10
    assert stats.flags_inserted == 10
    assert stats.flags_skipped_existing == 0
    assert set(conn.flags_by_discovery.keys()) == set(range(1, 11))
    assert conn.commits == 1
