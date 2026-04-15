"""
Unit tests for canonical_club_linker — 4-pass resolver + DB interaction.

DB layer is stubbed with an in-memory cursor double, following the same
pattern as the other scraper tests (see test_scrape_staff.py).
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

# Stub psycopg2 before importing so tests don't need a live Postgres.
sys.modules.setdefault("psycopg2", mock.MagicMock())
sys.modules.setdefault("psycopg2.extras", mock.MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from canonical_club_linker import (  # noqa: E402
    ClubIndex,
    LinkerStats,
    resolve_raw_team_name,
    strip_team_descriptors,
    link_all,
)


# ---------------------------------------------------------------------------
# strip_team_descriptors
# ---------------------------------------------------------------------------

def test_strip_age_and_gender_tokens():
    assert strip_team_descriptors("Concorde Fire Premier 2011 Boys") == "Concorde Fire"


def test_strip_u15_age_pattern():
    assert strip_team_descriptors("NTH Tophat U15 Boys Gold") == "NTH Tophat"


def test_strip_keeps_fc_sc_ac():
    # These are club-name tokens, not descriptors.
    assert strip_team_descriptors("Atlanta United FC 2010 Boys") == "Atlanta United FC"


def test_strip_empty_input():
    assert strip_team_descriptors("") == ""
    assert strip_team_descriptors(None) == ""  # type: ignore


def test_strip_collapses_whitespace():
    assert strip_team_descriptors("  Boston   Bolts    2012  Boys  ") == "Boston Bolts"


# ---------------------------------------------------------------------------
# Fuzzy index + resolver
# ---------------------------------------------------------------------------

def _make_index() -> ClubIndex:
    idx = ClubIndex()
    idx.canonical_exact = {"concorde fire": 101, "nth tophat": 102, "atlanta united": 103}
    idx.alias_exact = {"concorde fire soccer club": 101}
    idx.fuzzy_choices = list(idx.canonical_exact.keys()) + list(idx.alias_exact.keys())
    idx.fuzzy_club_ids = [101, 102, 103, 101]
    return idx


def test_resolve_pass1_exact_alias():
    idx = _make_index()
    res = resolve_raw_team_name("Concorde Fire Soccer Club", idx)
    assert res.club_id == 101
    assert res.pass_number == 1


def test_resolve_pass2_exact_canonical_after_strip():
    idx = _make_index()
    # Raw name with descriptors — stripped key should hit canonical_exact.
    res = resolve_raw_team_name("Concorde Fire 2011 Boys", idx)
    assert res.club_id == 101
    assert res.pass_number == 2


def test_resolve_pass3_fuzzy_hit():
    idx = _make_index()
    # Extra descriptive token that's NOT in the stopword list, so the
    # stripped key won't hit canonical_exact — forces pass 3.
    res = resolve_raw_team_name("Concorde Fire Phoenix 2011 Boys", idx)
    assert res.club_id == 101
    assert res.pass_number == 3
    assert res.score is not None and res.score >= 88


def test_resolve_pass3_difflib_fallback_when_rapidfuzz_missing():
    """Regression guard: when rapidfuzz is unavailable we must still
    resolve pass-3 via stdlib difflib rather than silently returning
    pass 4 (the 0/224 linker regression on Replit).
    """
    import canonical_club_linker as linker

    idx = _make_index()
    with mock.patch.object(linker, "_RAPIDFUZZ_AVAILABLE", False):
        res = resolve_raw_team_name("Concorde Fire Phoenix 2011 Boys", idx)
    assert res.club_id == 101
    assert res.pass_number == 3
    assert res.score is not None and res.score >= 88


def test_resolve_pass3_difflib_fallback_no_match_returns_pass4():
    """Fallback path still returns pass 4 when nothing is close enough."""
    import canonical_club_linker as linker

    idx = _make_index()
    with mock.patch.object(linker, "_RAPIDFUZZ_AVAILABLE", False):
        res = resolve_raw_team_name("Totally Unrelated Team XYZ", idx)
    assert res.club_id is None
    assert res.pass_number == 4


def test_resolve_no_match():
    idx = _make_index()
    res = resolve_raw_team_name("Completely Unrelated Club XYZ", idx)
    assert res.club_id is None
    assert res.pass_number == 4


def test_resolve_empty_string():
    idx = _make_index()
    res = resolve_raw_team_name("", idx)
    assert res.club_id is None
    assert res.pass_number == 4


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
        sql = sql.strip()
        if sql.startswith("SELECT id, club_name_canonical FROM canonical_clubs"):
            self._last_result = list(self.state["canonical_clubs"])
        elif sql.startswith("SELECT club_id, alias_name FROM club_aliases"):
            self._last_result = list(self.state["club_aliases"])
        elif sql.startswith("SELECT id, team_name_raw FROM event_teams"):
            self._last_result = [
                (r["id"], r["team_name_raw"])
                for r in self.state["event_teams"]
                if r["canonical_club_id"] is None
            ]
        elif "FROM matches" in sql and "home_team_name" in sql and sql.startswith("SELECT"):
            self._last_result = [
                (r["id"], r["home_team_name"])
                for r in self.state["matches"]
                if r["home_club_id"] is None
            ]
        elif "FROM matches" in sql and "away_team_name" in sql and sql.startswith("SELECT"):
            self._last_result = [
                (r["id"], r["away_team_name"])
                for r in self.state["matches"]
                if r["away_club_id"] is None
            ]
        elif sql.startswith("SELECT id, club_name_raw FROM club_roster_snapshots"):
            self._last_result = [
                (r["id"], r["club_name_raw"])
                for r in self.state["club_roster_snapshots"]
                if r["club_id"] is None
            ]
        elif sql.startswith("SELECT id, club_name_raw FROM roster_diffs"):
            self._last_result = [
                (r["id"], r["club_name_raw"])
                for r in self.state["roster_diffs"]
                if r["club_id"] is None
            ]
        elif sql.startswith("SELECT id, club_name_raw FROM tryouts"):
            self._last_result = [
                (r["id"], r["club_name_raw"])
                for r in self.state["tryouts"]
                if r["club_id"] is None
            ]
        elif sql.startswith("UPDATE club_roster_snapshots"):
            club_id, row_id = params
            for r in self.state["club_roster_snapshots"]:
                if r["id"] == row_id and r["club_id"] is None:
                    r["club_id"] = club_id
                    self.state["writes"].append(("club_roster_snapshots", row_id, club_id))
        elif sql.startswith("UPDATE roster_diffs"):
            club_id, row_id = params
            for r in self.state["roster_diffs"]:
                if r["id"] == row_id and r["club_id"] is None:
                    r["club_id"] = club_id
                    self.state["writes"].append(("roster_diffs", row_id, club_id))
        elif sql.startswith("UPDATE tryouts"):
            club_id, row_id = params
            for r in self.state["tryouts"]:
                if r["id"] == row_id and r["club_id"] is None:
                    r["club_id"] = club_id
                    self.state["writes"].append(("tryouts", row_id, club_id))
        elif sql.startswith("UPDATE event_teams"):
            club_id, row_id = params
            for r in self.state["event_teams"]:
                if r["id"] == row_id and r["canonical_club_id"] is None:
                    r["canonical_club_id"] = club_id
                    self.state["writes"].append(("event_teams", row_id, club_id))
        elif sql.startswith("UPDATE matches"):
            club_id, row_id = params
            # Determine side from the SET column.
            side = "home" if "home_club_id" in sql else "away"
            col = f"{side}_club_id"
            for r in self.state["matches"]:
                if r["id"] == row_id and r[col] is None:
                    r[col] = club_id
                    self.state["writes"].append(("matches", side, row_id, club_id))
        elif sql.startswith("INSERT INTO club_aliases"):
            club_id, alias_name = params
            key = (club_id, alias_name)
            if key not in self.state["alias_keys"]:
                self.state["alias_keys"].add(key)
                self.state["club_aliases"].append((club_id, alias_name))
                self.state["writes"].append(("alias_insert", club_id, alias_name))
        else:
            raise AssertionError(f"unexpected SQL: {sql!r}")

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
    return {
        "canonical_clubs": [
            (101, "Concorde Fire"),
            (102, "NTH Tophat"),
            (103, "Atlanta United"),
        ],
        "club_aliases": [
            (101, "Concorde Fire Soccer Club"),
        ],
        "alias_keys": {(101, "Concorde Fire Soccer Club")},
        "event_teams": [],
        "matches": [],
        "club_roster_snapshots": [],
        "roster_diffs": [],
        "tryouts": [],
        "writes": [],
    }


def test_link_all_exact_alias_hit():
    state = _base_state()
    state["event_teams"].append({
        "id": 1,
        "team_name_raw": "Concorde Fire Soccer Club",
        "canonical_club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.event_teams_linked == 1
    assert stats.pass_hits[1] == 1
    assert state["event_teams"][0]["canonical_club_id"] == 101
    assert conn.committed


def test_link_all_exact_canonical_hit():
    state = _base_state()
    state["event_teams"].append({
        "id": 2,
        "team_name_raw": "Concorde Fire 2011 Boys",
        "canonical_club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.event_teams_linked == 1
    assert stats.pass_hits[2] == 1
    assert state["event_teams"][0]["canonical_club_id"] == 101


def test_link_all_fuzzy_hit_writes_new_alias():
    state = _base_state()
    state["event_teams"].append({
        "id": 3,
        "team_name_raw": "Concorde Fire Phoenix 2011 Boys",
        "canonical_club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.event_teams_linked == 1
    assert stats.pass_hits[3] == 1
    assert stats.aliases_written == 1
    alias_inserts = [w for w in state["writes"] if w[0] == "alias_insert"]
    assert len(alias_inserts) == 1
    # Alias is written on the STRIPPED key.
    assert alias_inserts[0][1] == 101
    assert "Concorde Fire" in alias_inserts[0][2]


def test_link_all_no_match_leaves_null():
    state = _base_state()
    state["event_teams"].append({
        "id": 4,
        "team_name_raw": "Totally Unrelated Team XYZ",
        "canonical_club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.event_teams_linked == 0
    assert stats.pass_hits[4] == 1
    assert state["event_teams"][0]["canonical_club_id"] is None
    # Unmatched bucket captured the stripped name.
    assert len(stats.unmatched_names) == 1


def test_link_all_dry_run_does_not_write():
    state = _base_state()
    state["event_teams"].append({
        "id": 5,
        "team_name_raw": "Concorde Fire Soccer Club",
        "canonical_club_id": None,
    })
    state["matches"].append({
        "id": 10,
        "home_team_name": "Concorde Fire Soccer Club",
        "away_team_name": "NTH Tophat",
        "home_club_id": None,
        "away_club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=True)
    # Stats still reflect what WOULD have been linked.
    assert stats.event_teams_linked == 1
    assert stats.matches_home_linked == 1
    assert stats.matches_away_linked == 1
    # But no row was mutated.
    assert state["event_teams"][0]["canonical_club_id"] is None
    assert state["matches"][0]["home_club_id"] is None
    assert state["matches"][0]["away_club_id"] is None
    assert conn.rolled_back and not conn.committed
    # And no alias was written even though there would be a fuzzy hit.
    assert not any(w[0] == "alias_insert" for w in state["writes"])


def test_link_all_idempotent_skips_non_null_rows():
    state = _base_state()
    state["event_teams"].append({
        "id": 6,
        "team_name_raw": "Concorde Fire",
        "canonical_club_id": 999,  # already linked
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    # Already-linked row was not picked up by the SELECT WHERE NULL.
    assert stats.event_teams_linked == 0
    assert state["event_teams"][0]["canonical_club_id"] == 999


def test_linker_stats_details():
    stats = LinkerStats()
    stats.event_teams_linked = 5
    stats.matches_home_linked = 3
    stats.matches_away_linked = 2
    stats.roster_snapshots_linked = 7
    stats.roster_diffs_linked = 4
    stats.tryouts_linked = 1
    stats.pass_hits.update({1: 6, 2: 2, 3: 2, 4: 1})
    stats.unmatched_names["Weird Team"] = 1
    details = stats.to_details()
    assert details["event_teams_linked"] == 5
    assert details["roster_snapshots_linked"] == 7
    assert details["roster_diffs_linked"] == 4
    assert details["tryouts_linked"] == 1
    assert details["pass_1_alias_hits"] == 6
    assert details["no_match_count"] == 1
    assert details["unmatched_unique_count"] == 1
    assert "Weird Team" in details["unmatched_sample"]
    # total_linked sums all six sources.
    assert stats.total_linked() == 5 + 3 + 2 + 7 + 4 + 1


# ---------------------------------------------------------------------------
# link_all — new Path A tables (rosters + diffs + tryouts)
# ---------------------------------------------------------------------------

def test_link_all_roster_snapshot_exact_alias_hit():
    state = _base_state()
    state["club_roster_snapshots"].append({
        "id": 1,
        "club_name_raw": "Concorde Fire Soccer Club",
        "club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.roster_snapshots_linked == 1
    assert state["club_roster_snapshots"][0]["club_id"] == 101
    assert conn.committed


def test_link_all_roster_diff_exact_canonical_hit():
    state = _base_state()
    state["roster_diffs"].append({
        "id": 1,
        "club_name_raw": "Concorde Fire 2011 Boys",
        "club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.roster_diffs_linked == 1
    assert state["roster_diffs"][0]["club_id"] == 101


def test_link_all_tryout_fuzzy_hit():
    state = _base_state()
    state["tryouts"].append({
        "id": 1,
        "club_name_raw": "Concorde Fire Phoenix 2011 Boys",
        "club_id": None,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.tryouts_linked == 1
    assert state["tryouts"][0]["club_id"] == 101


def test_link_all_new_tables_skip_non_null_rows():
    state = _base_state()
    state["club_roster_snapshots"].append({
        "id": 1,
        "club_name_raw": "Concorde Fire",
        "club_id": 999,
    })
    state["roster_diffs"].append({
        "id": 2,
        "club_name_raw": "Concorde Fire",
        "club_id": 999,
    })
    state["tryouts"].append({
        "id": 3,
        "club_name_raw": "Concorde Fire",
        "club_id": 999,
    })
    conn = FakeConn(state)
    stats = link_all(conn, dry_run=False)
    assert stats.roster_snapshots_linked == 0
    assert stats.roster_diffs_linked == 0
    assert stats.tryouts_linked == 0
    assert state["club_roster_snapshots"][0]["club_id"] == 999
    assert state["roster_diffs"][0]["club_id"] == 999
    assert state["tryouts"][0]["club_id"] == 999
