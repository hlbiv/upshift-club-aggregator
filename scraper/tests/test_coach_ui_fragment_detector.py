"""
Tests for `coach_ui_fragment_detector.detect_all` + the pure gazetteer
matcher `classify_ui_fragment`.

Structural clone of `test_coach_pollution_detector.py`. Uses a
hand-rolled in-memory `FakeConn` / `FakeCursor` rather than spinning up
a real Postgres. Covers:

* Pure-function `classify_ui_fragment` over representative strings from
  each gazetteer category, plus legitimate coach names (must NOT match),
  null / non-string inputs, and case / whitespace insensitivity.
* Gazetteer-build sanity — frozenset, case-folded, dedup across
  categories.
* Dry-run (commit=False) produces stats but NO writes and rolls back.
* Commit mode inserts one flag per gazetteer hit with the documented
  metadata shape.
* Idempotency: a second run against the same data writes 0 additional
  rows (ON CONFLICT DO NOTHING).
* Missing `coach_quality_flags` table → no-op + no exception.
* `--window-days` filter restricts the scan to recent rows.
* `--limit` caps the scan.
* Regression guard: the dedicated write-cursor design survives multi-
  page scans (same bug as `coach_pollution_detector`; same guard shape).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

# Make the scraper/ package root importable.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

import coach_ui_fragment_detector as detector  # noqa: E402


# ---------------------------------------------------------------------------
# Fake DB — same shape as test_coach_pollution_detector. Kept local
# rather than shared because each detector owns its own flag_type and
# the test harness shouldn't share state across them.
# ---------------------------------------------------------------------------

class FakeCursor:
    """Minimal psycopg2-cursor stand-in for the UI-fragment detector.

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

        # Faithfully reproduce psycopg2's client-side cursor behavior:
        # every execute() wipes the previously-buffered result set.
        # Regression guard downstream depends on this.
        self._last_result = []
        self._last_singleton = None
        self._fetchmany_cursor = 0

        if sql_norm.startswith("SELECT to_regclass"):
            assert params is not None and len(params) == 1
            table_name = params[0].split(".", 1)[-1]
            if table_name in self._conn.existing_tables:
                self._last_singleton = (f"public.{table_name}",)
            else:
                self._last_singleton = (None,)
            return

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

            matched.sort(key=lambda r: r["id"])

            if "LIMIT" in sql_norm:
                tail = sql_norm.split("LIMIT", 1)[1].strip()
                limit_n = int(tail.split()[0])
                matched = matched[:limit_n]

            self._last_result = [
                (r["id"], r["name"], r["email"])
                for r in matched
            ]
            self._fetchmany_cursor = 0
            return

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
                self._conn.next_flag_id += 1
                flag_id = self._conn.next_flag_id
                self._conn.flags_by_discovery[discovery_id] = {
                    "id": flag_id,
                    "metadata": metadata,
                }
                self._last_singleton = (flag_id,)
            else:
                self._last_singleton = None
            return

        raise AssertionError(f"unexpected SQL: {sql_norm[:200]}")

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._last_result)

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._last_singleton

    def fetchmany(self, size: int) -> List[Tuple[Any, ...]]:
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
        "first_seen_at": first_seen_at
        if first_seen_at is not None
        else datetime.now(timezone.utc),
    }


def _first_seen_at(row: Dict[str, Any]) -> datetime:
    return row["first_seen_at"]


# ---------------------------------------------------------------------------
# Gazetteer sanity
# ---------------------------------------------------------------------------

def test_gazetteer_is_nonempty_frozenset_of_lower_case() -> None:
    """The module-level GAZETTEER should be a frozenset of case-folded,
    whitespace-stripped strings. Catches drift if someone forgets the
    build step for new entries."""
    assert isinstance(detector.GAZETTEER, frozenset)
    assert len(detector.GAZETTEER) > 20, \
        f"gazetteer suspiciously small: {len(detector.GAZETTEER)}"
    for s in detector.GAZETTEER:
        assert s == s.casefold(), f"not case-folded: {s!r}"
        assert s == s.strip(), f"not whitespace-trimmed: {s!r}"


def test_gazetteer_has_no_duplicates_across_categories() -> None:
    """Same string appearing in two categories would mean the per-hit
    `_category_of` result depends on check order. Catch that at test
    time, not in production."""
    cats = (
        detector._NAV_LABELS,
        detector._MARKETING_TILES,
        detector._PRICING_AND_DATES,
        detector._SECTION_HEADINGS,
    )
    seen: Dict[str, str] = {}
    for cat_idx, members in enumerate(cats):
        for raw in members:
            folded = raw.strip().casefold()
            if folded in seen:
                raise AssertionError(
                    f"{raw!r} appears in two categories "
                    f"(already in {seen[folded]})"
                )
            seen[folded] = detector.GAZETTEER_CATEGORIES[cat_idx]


# ---------------------------------------------------------------------------
# classify_ui_fragment — pure-function coverage
# ---------------------------------------------------------------------------

def test_classify_hits_one_from_each_category() -> None:
    """Exercise a representative string from each gazetteer category.
    Pins the exact category code so a gazetteer reshuffle surfaces as a
    test failure, not a silent data change."""
    cases = [
        ("Where We Are", "nav_label"),
        ("Get In Touch", "nav_label"),
        ("Fashion Magazine", "marketing_tile"),
        ("Technical Ball Mastery", "marketing_tile"),
        ("One Week", "pricing_or_date"),
        ("Camp Dates", "pricing_or_date"),
        ("Premier Tryouts", "section_heading"),
        ("DME Sarasota", "section_heading"),
    ]
    for raw, expected in cases:
        got = detector.classify_ui_fragment(raw)
        assert got == expected, f"{raw!r}: expected {expected}, got {got}"


def test_classify_is_case_insensitive() -> None:
    assert detector.classify_ui_fragment("WHERE WE ARE") == "nav_label"
    assert detector.classify_ui_fragment("where we are") == "nav_label"
    assert detector.classify_ui_fragment("Where we Are") == "nav_label"


def test_classify_trims_whitespace() -> None:
    assert detector.classify_ui_fragment("  Where We Are  ") == "nav_label"
    assert detector.classify_ui_fragment("\tGet In Touch\n") == "nav_label"


def test_classify_passes_real_names() -> None:
    """Legitimate coach names must NOT match. Guards against
    gazetteer entries that accidentally overlap a plausible name."""
    for name in [
        "Jane Smith",
        "Lionel Messi",
        "Maria Garcia-Lopez",
        "Tomas Fox",
        "Rocky Harmon",
        "Adel Alchamat",
    ]:
        assert detector.classify_ui_fragment(name) is None, \
            f"false positive on real name: {name!r}"


def test_classify_handles_null_and_non_string() -> None:
    """None / non-string inputs never match — can't be in the gazetteer."""
    assert detector.classify_ui_fragment(None) is None
    assert detector.classify_ui_fragment("") is None
    assert detector.classify_ui_fragment("   ") is None
    assert detector.classify_ui_fragment(42) is None
    assert detector.classify_ui_fragment([]) is None


# ---------------------------------------------------------------------------
# detect_all — dry-run
# ---------------------------------------------------------------------------

def test_dry_run_produces_stats_without_writing() -> None:
    discoveries = [
        _disc(1, "Jane Smith", "jane@club.com"),           # clean
        _disc(2, "Where We Are", "hc@club.com"),           # nav_label
        _disc(3, "Fashion Magazine", None),                # marketing_tile
        _disc(4, "One Week", "x@y.com"),                   # pricing_or_date
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=False)

    assert stats.discoveries_scanned == 4
    assert stats.discoveries_flagged == 3
    assert stats.flags_inserted == 0
    assert stats.flags_skipped_existing == 0
    assert stats.category_counts == {
        "nav_label": 1,
        "marketing_tile": 1,
        "pricing_or_date": 1,
    }
    # No writes — rolled back.
    assert conn.flags_by_discovery == {}
    assert conn.rollbacks == 1
    assert conn.commits == 0


# ---------------------------------------------------------------------------
# detect_all — commit mode
# ---------------------------------------------------------------------------

def test_commit_mode_inserts_one_flag_per_hit() -> None:
    discoveries = [
        _disc(10, "Jane Smith", "jane@club.com"),           # clean
        _disc(11, "Get In Touch", "gt@club.com"),           # nav_label
        _disc(12, "Fashion Magazine", None),                # marketing_tile
        _disc(13, "One Week", "z@y.com"),                   # pricing_or_date
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True)

    assert stats.discoveries_scanned == 4
    assert stats.discoveries_flagged == 3
    assert stats.flags_inserted == 3
    assert stats.flags_skipped_existing == 0
    assert conn.commits == 1
    assert conn.rollbacks == 0

    # One flag per hit.
    assert set(conn.flags_by_discovery.keys()) == {11, 12, 13}

    # Metadata shape contract (matches docstring in coach-quality-flags.ts).
    md_11 = conn.flags_by_discovery[11]["metadata"]
    assert md_11 == {
        "matched_raw": "Get In Touch",
        "matched_category": "nav_label",
        "raw_email": "gt@club.com",
    }

    md_12 = conn.flags_by_discovery[12]["metadata"]
    assert md_12 == {
        "matched_raw": "Fashion Magazine",
        "matched_category": "marketing_tile",
        "raw_email": None,  # source email was None
    }

    md_13 = conn.flags_by_discovery[13]["metadata"]
    assert md_13 == {
        "matched_raw": "One Week",
        "matched_category": "pricing_or_date",
        "raw_email": "z@y.com",
    }


def test_commit_mode_is_idempotent() -> None:
    """Second run against identical data writes 0 additional rows via
    the (discovery_id, flag_type) ON CONFLICT DO NOTHING."""
    discoveries = [
        _disc(20, "Where We Are", "a@club.com"),
        _disc(21, "One Week", None),
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
    """When the CHECK-constraint-extended coach_quality_flags table
    isn't there yet (db push hasn't run on Replit), detect_all returns
    cleanly with zero counters and writes nothing. No exception —
    critical so the PR can merge before Replit applies the schema."""
    discoveries = [
        _disc(30, "Where We Are", "a@club.com"),
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
        _disc(100, "Where We Are", "a@a.com", first_seen_at=recent),
        _disc(101, "Jane Smith", "b@b.com", first_seen_at=recent),
        # Outside the window — should not be scanned.
        _disc(200, "Fashion Magazine", None, first_seen_at=ancient),
    ]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True, window_days=7)

    assert stats.discoveries_scanned == 2
    assert stats.discoveries_flagged == 1
    assert 100 in conn.flags_by_discovery
    assert 200 not in conn.flags_by_discovery


def test_limit_caps_rows_scanned() -> None:
    discoveries = [_disc(i, "Where We Are", f"c{i}@x.com") for i in range(1, 11)]
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
    many hits the run produces. Prevents the details-JSON from blowing
    past scrape_run_logs.details_json's 3900-char limit."""
    discoveries = [_disc(i, "Where We Are", f"c{i}@x.com") for i in range(1, 21)]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=False)

    assert stats.discoveries_flagged == 20
    assert len(stats.sample_flags) == 10
    for disc_id, raw_name, category in stats.sample_flags:
        assert isinstance(disc_id, int)
        assert raw_name == "Where We Are"
        assert category == "nav_label"


# ---------------------------------------------------------------------------
# Multi-page commit — regression guard for the cursor-clobber bug.
# Inherited from coach_pollution_detector's scan shape; guarding the
# same pattern here so a future refactor that collapses the two
# detectors can't regress the multi-page contract.
# ---------------------------------------------------------------------------

def test_commit_mode_scans_all_rows_across_multiple_pages(monkeypatch) -> None:
    """Regression guard. This detector uses the same read/write cursor
    pair pattern as `coach_pollution_detector`. Lower PAGE_SIZE to 3,
    feed 10 gazetteer-hit rows; the whole-scan contract must hold."""
    monkeypatch.setattr(detector, "PAGE_SIZE", 3)
    discoveries = [_disc(i, "Where We Are", f"c{i}@x.com") for i in range(1, 11)]
    conn = FakeConn(discoveries)

    stats = detector.detect_all(conn, commit=True)

    assert stats.discoveries_scanned == 10, \
        f"expected all 10 rows scanned; got {stats.discoveries_scanned}"
    assert stats.discoveries_flagged == 10
    assert stats.flags_inserted == 10
    assert set(conn.flags_by_discovery.keys()) == set(range(1, 11))
