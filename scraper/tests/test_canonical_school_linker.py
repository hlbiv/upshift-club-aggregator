"""
Unit tests for canonical_school_linker — state-scoped 4-pass resolver +
DB interaction.

Patterned on scraper/tests/test_canonical_club_linker.py. DB layer is
stubbed with an in-memory cursor double so tests don't need a live
Postgres.
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest  # noqa: F401

# Stub psycopg2 before importing so tests don't need a live Postgres.
sys.modules.setdefault("psycopg2", mock.MagicMock())
sys.modules.setdefault("psycopg2.extras", mock.MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from canonical_school_linker import (  # noqa: E402
    SchoolIndex,
    LinkerStats,
    resolve_raw_school_name,
    normalize_school_name,
    link_all,
)


# ---------------------------------------------------------------------------
# normalize_school_name
# ---------------------------------------------------------------------------

def test_normalize_strips_punctuation():
    assert normalize_school_name("St. Mary's H.S.") == "St Mary s H S"


def test_normalize_collapses_whitespace():
    assert normalize_school_name("  Lincoln   High   School  ") == "Lincoln High School"


def test_normalize_empty_inputs():
    assert normalize_school_name("") == ""
    assert normalize_school_name(None) == ""  # type: ignore


def test_normalize_keeps_high_school_tokens():
    # Unlike the club linker, we do NOT strip "High" / "Academy" tokens —
    # they're load-bearing parts of school names.
    assert normalize_school_name("Lincoln High School") == "Lincoln High School"


# ---------------------------------------------------------------------------
# resolver — state-scoped passes
# ---------------------------------------------------------------------------

def _make_index() -> SchoolIndex:
    """Two states, two canonical schools in NE + one in CA, one alias in NE."""
    idx = SchoolIndex()
    idx.canonical_exact["NE"] = {
        "lincoln high school": 201,
        "millard north high school": 202,
    }
    idx.canonical_exact["CA"] = {
        "lincoln high school": 301,
        "mater dei high school": 302,
    }
    idx.alias_exact["NE"] = {"lincoln hs": 201}
    idx.fuzzy_by_state["NE"] = (
        ["lincoln high school", "millard north high school", "lincoln hs"],
        [201, 202, 201],
    )
    idx.fuzzy_by_state["CA"] = (
        ["lincoln high school", "mater dei high school"],
        [301, 302],
    )
    return idx


def test_resolve_pass1_exact_alias_state_scoped():
    idx = _make_index()
    res = resolve_raw_school_name("Lincoln HS", "NE", idx)
    assert res.school_id == 201
    assert res.pass_number == 1


def test_resolve_pass2_exact_canonical_state_scoped():
    idx = _make_index()
    res = resolve_raw_school_name("Lincoln High School", "CA", idx)
    assert res.school_id == 301
    assert res.pass_number == 2


def test_resolve_same_name_different_states_do_not_cross_match():
    """CRITICAL: "Lincoln High School" in NE (id 201) and CA (id 301) are
    distinct canonical rows. A raw row tagged "NE" must never hit the CA
    row, and vice versa."""
    idx = _make_index()

    ne_res = resolve_raw_school_name("Lincoln High School", "NE", idx)
    assert ne_res.school_id == 201

    ca_res = resolve_raw_school_name("Lincoln High School", "CA", idx)
    assert ca_res.school_id == 301


def test_resolve_pass3_fuzzy_hit_state_scoped():
    idx = _make_index()
    # Misspelling close enough to hit pass 3 within NE.
    res = resolve_raw_school_name("Lincoln Highschool", "NE", idx)
    assert res.school_id == 201
    assert res.pass_number == 3
    assert res.score is not None and res.score >= 88


def test_resolve_fuzzy_does_not_leak_across_states():
    """The CA index has no "Millard North" school — a NE-only name tagged
    CA must NOT match the NE row via fuzzy."""
    idx = _make_index()
    res = resolve_raw_school_name("Millard North High School", "CA", idx)
    # No CA canonical close enough, no cross-state leak.
    assert res.school_id is None
    assert res.pass_number == 4


def test_resolve_no_match():
    idx = _make_index()
    res = resolve_raw_school_name("Totally Unknown School", "NE", idx)
    assert res.school_id is None
    assert res.pass_number == 4


def test_resolve_empty_state_returns_no_match():
    idx = _make_index()
    res = resolve_raw_school_name("Lincoln High School", "", idx)
    assert res.school_id is None
    assert res.pass_number == 4


def test_resolve_empty_name_returns_no_match():
    idx = _make_index()
    res = resolve_raw_school_name("", "NE", idx)
    assert res.school_id is None
    assert res.pass_number == 4


def test_resolve_state_casing_normalized():
    idx = _make_index()
    # Lower-case state code should still scope correctly.
    res = resolve_raw_school_name("Lincoln HS", "ne", idx)
    assert res.school_id == 201
    assert res.pass_number == 1


def test_resolve_pass3_difflib_fallback_when_rapidfuzz_missing():
    """Fallback path must still resolve pass-3 via stdlib difflib."""
    import canonical_school_linker as linker

    idx = _make_index()
    with mock.patch.object(linker, "_RAPIDFUZZ_AVAILABLE", False):
        res = resolve_raw_school_name("Lincoln Highschool", "NE", idx)
    assert res.school_id == 201
    assert res.pass_number == 3


# ---------------------------------------------------------------------------
# link_all — stubbed DB
# ---------------------------------------------------------------------------

class FakeCursor:
    """Minimal in-memory cursor. Supports exactly the queries the linker issues."""

    def __init__(self, state):
        self.state = state
        self._last_result: list = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql: str, params=None):
        sql_stripped = sql.strip()
        if sql_stripped.startswith(
            "SELECT id, school_name_canonical, school_state FROM canonical_schools"
        ):
            self._last_result = list(self.state["canonical_schools"])
        elif sql_stripped.startswith(
            "SELECT school_id, alias_name, school_state FROM school_aliases"
        ):
            self._last_result = list(self.state["school_aliases"])
        elif sql_stripped.startswith(
            "SELECT id, school_name_raw, school_state FROM hs_rosters"
        ):
            self._last_result = [
                (r["id"], r["school_name_raw"], r["school_state"])
                for r in self.state["hs_rosters"]
                if r["school_id"] is None
            ]
        elif sql_stripped.startswith("UPDATE hs_rosters"):
            school_id, row_id = params
            for r in self.state["hs_rosters"]:
                if r["id"] == row_id and r["school_id"] is None:
                    r["school_id"] = school_id
                    self.state["writes"].append(("hs_rosters", row_id, school_id))
        elif sql_stripped.startswith("INSERT INTO school_aliases"):
            school_id, alias_name, school_state = params
            key = (alias_name, school_state)
            if key not in self.state["alias_keys"]:
                self.state["alias_keys"].add(key)
                self.state["school_aliases"].append(
                    (school_id, alias_name, school_state)
                )
                self.state["writes"].append(
                    ("alias_insert", school_id, alias_name, school_state)
                )
        else:
            raise AssertionError(f"unexpected SQL: {sql_stripped!r}")

    def fetchall(self):
        return self._last_result


class FakeConn:
    def __init__(self, state):
        self.state = state
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return FakeCursor(self.state)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _base_state():
    """Seed two canonical schools in different states with the same name to
    exercise the state-scoping invariant end-to-end."""
    return {
        "canonical_schools": [
            (201, "Lincoln High School", "NE"),
            (301, "Lincoln High School", "CA"),
            (202, "Millard North High School", "NE"),
        ],
        "school_aliases": [
            (201, "Lincoln HS", "NE"),
        ],
        "alias_keys": {("Lincoln HS", "NE")},
        "hs_rosters": [],
        "writes": [],
    }


# (a) exact alias hit
def test_link_all_exact_alias_hit():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        "school_name_raw": "Lincoln HS",
        "school_state": "NE",
        "school_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.hs_rosters_linked == 1
    assert stats.pass_hits[1] == 1
    assert state["hs_rosters"][0]["school_id"] == 201
    assert conn.committed


# (b) state-scoped matching — same name in two states must not cross-match
def test_link_all_state_scope_prevents_cross_match():
    state = _base_state()
    state["hs_rosters"].extend([
        {
            "id": 1,
            "school_name_raw": "Lincoln High School",
            "school_state": "NE",
            "school_id": None,
        },
        {
            "id": 2,
            "school_name_raw": "Lincoln High School",
            "school_state": "CA",
            "school_id": None,
        },
    ])
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.hs_rosters_linked == 2
    # Each row resolved to the correct in-state canonical id.
    ne_row = next(r for r in state["hs_rosters"] if r["id"] == 1)
    ca_row = next(r for r in state["hs_rosters"] if r["id"] == 2)
    assert ne_row["school_id"] == 201
    assert ca_row["school_id"] == 301


# (c) idempotency — a second run is a no-op
def test_link_all_idempotent_second_run_is_noop():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        "school_name_raw": "Lincoln HS",
        "school_state": "NE",
        "school_id": None,
    })
    conn = FakeConn(state)

    stats1 = link_all(conn, dry_run=False)
    assert stats1.hs_rosters_linked == 1
    assert state["hs_rosters"][0]["school_id"] == 201

    # Second run: the same row now has school_id set, so the NULL-only
    # SELECT returns zero rows.
    writes_before = len(state["writes"])
    stats2 = link_all(conn, dry_run=False)
    assert stats2.hs_rosters_linked == 0
    assert stats2.pass_hits.get(1, 0) == 0
    assert stats2.pass_hits.get(2, 0) == 0
    assert stats2.pass_hits.get(3, 0) == 0
    # And no further writes happened.
    assert len(state["writes"]) == writes_before


# (d) fuzzy match writes an alias row
def test_link_all_fuzzy_hit_writes_alias_row():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        # Misspelling close enough to hit pass 3 against "lincoln high school".
        "school_name_raw": "Lincoln Highschool",
        "school_state": "NE",
        "school_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.hs_rosters_linked == 1
    assert stats.pass_hits[3] == 1
    assert stats.aliases_written == 1
    alias_inserts = [w for w in state["writes"] if w[0] == "alias_insert"]
    assert len(alias_inserts) == 1
    # (kind, school_id, alias_name, school_state)
    assert alias_inserts[0][1] == 201
    assert alias_inserts[0][3] == "NE"


def test_link_all_no_match_leaves_null():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        "school_name_raw": "Totally Unrelated Academy",
        "school_state": "NE",
        "school_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.hs_rosters_linked == 0
    assert stats.pass_hits[4] == 1
    assert state["hs_rosters"][0]["school_id"] is None
    assert len(stats.unmatched_names) == 1


def test_link_all_dry_run_does_not_write():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        "school_name_raw": "Lincoln HS",
        "school_state": "NE",
        "school_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=True)
    assert stats.hs_rosters_linked == 1
    # But no row was mutated.
    assert state["hs_rosters"][0]["school_id"] is None
    assert conn.rolled_back and not conn.committed


def test_link_all_skips_rows_missing_state():
    state = _base_state()
    state["hs_rosters"].append({
        "id": 1,
        "school_name_raw": "Lincoln HS",
        # DB column is NOT NULL so this can't happen in practice, but the
        # fetch filter in the linker uses "AND school_state <> ''" — make
        # sure that branch is covered.
        "school_state": "",
        "school_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.hs_rosters_linked == 0


def test_linker_stats_to_details():
    stats = LinkerStats()
    stats.hs_rosters_linked = 10
    stats.pass_hits.update({1: 5, 2: 3, 3: 2, 4: 1})
    stats.unmatched_names["Unknown HS (NE)"] = 1
    details = stats.to_details()
    assert details["hs_rosters_linked"] == 10
    assert details["pass_1_alias_hits"] == 5
    assert details["no_match_count"] == 1
    assert details["unmatched_unique_count"] == 1
    assert "Unknown HS (NE)" in details["unmatched_sample"]
    assert stats.total_linked() == 10
