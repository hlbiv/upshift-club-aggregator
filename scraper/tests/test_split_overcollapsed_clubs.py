"""
Unit tests for `dedup/split_overcollapsed_clubs.py` — REPORT-ONLY audit
of canonical_clubs rows whose attached aliases describe distinct
underlying clubs (task #85).
"""

from __future__ import annotations

import os
import sys
from unittest import mock

# Stub psycopg2 only when missing — same pattern as the linker tests.
try:
    import psycopg2  # noqa: F401
    import psycopg2.extras  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = mock.MagicMock()
    sys.modules["psycopg2.extras"] = mock.MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dedup.split_overcollapsed_clubs import (  # noqa: E402
    _is_meaningful_root,
    _stripped_root,
    find_overcollapsed_rows,
    split_canonical_row,
)


# ---------------------------------------------------------------------------
# _is_meaningful_root
# ---------------------------------------------------------------------------

def test_meaningful_root_excludes_empty():
    assert _is_meaningful_root("", "dallas") is False


def test_meaningful_root_excludes_canonical_match():
    assert _is_meaningful_root("dallas", "dallas") is False


def test_meaningful_root_excludes_single_token():
    """A single-token root like "dallas" doesn't prove a distinct
    underlying club exists — could just be an alias of the canonical."""
    assert _is_meaningful_root("dallas", "houston") is False


def test_meaningful_root_accepts_multi_token():
    assert _is_meaningful_root("dallas texans", "dallas") is True


def test_stripped_root_uses_post_fix_stripper():
    """Sanity: the stripper drops 16G/Pre-ECNL noise."""
    assert _stripped_root("FC Dallas 16G Pre-ECNL McAnally") == "fc dallas mcanally"


# ---------------------------------------------------------------------------
# find_overcollapsed_rows — fake DB
# ---------------------------------------------------------------------------

class FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self._last_result = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        # Optional canonical_filter narrowing.
        if params and "LOWER" in sql:
            target = params[0].lower()
            self._last_result = [
                r for r in self._rows
                if (r[1] or "").lower() == target
            ]
        else:
            self._last_result = list(self._rows)

    def fetchall(self):
        return self._last_result


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)


def test_find_flags_dallas_row_with_distinct_roots():
    """The Dallas regression: canonical "Dallas" with aliases pointing
    at FC Dallas, Dallas Texans, and Dallas Sting — should flag."""
    rows = [
        # (canonical_id, canonical_name, state, alias_name)
        (1, "Dallas", "TX", "FC Dallas 16G Pre-ECNL McAnally"),
        (1, "Dallas", "TX", "FC Dallas 17B"),
        (1, "Dallas", "TX", "Dallas Texans 15G"),
        (1, "Dallas", "TX", "Dallas Texans Coach Adams"),
        (1, "Dallas", "TX", "Dallas Sting 16G"),
    ]
    findings = find_overcollapsed_rows(FakeConn(rows))
    assert len(findings) == 1
    f = findings[0]
    assert f.canonical_id == 1
    assert f.canonical_name == "Dallas"
    # 3 distinct meaningful roots: fc dallas mcanally, dallas texans,
    # dallas sting (the bare "fc dallas" / "fc dallas" both reduce to
    # the same multi-token root after stripping).
    assert "fc dallas mcanally" in f.distinct_roots
    assert "dallas texans" in f.distinct_roots
    assert "dallas sting" in f.distinct_roots


def test_find_does_not_flag_clean_row():
    """A canonical row with all aliases reducing to the same root must
    not be flagged."""
    rows = [
        (10, "Concorde Fire", "GA", "Concorde Fire SC"),
        (10, "Concorde Fire", "GA", "Concorde Fire 2011 Boys"),
        (10, "Concorde Fire", "GA", "Concorde Fire U15"),
        (10, "Concorde Fire", "GA", "Concorde Fire Premier"),
    ]
    findings = find_overcollapsed_rows(FakeConn(rows))
    assert findings == []


def test_find_skips_rows_below_min_alias_count():
    rows = [
        (5, "Dallas", "TX", "FC Dallas 16G"),
        (5, "Dallas", "TX", "Dallas Texans 17B"),
    ]
    findings = find_overcollapsed_rows(FakeConn(rows), min_alias_count=3)
    assert findings == []


def test_find_canonical_filter_narrows():
    rows = [
        (1, "Dallas", "TX", "FC Dallas 16G McAnally"),
        (1, "Dallas", "TX", "Dallas Texans 17B Adams"),
        (1, "Dallas", "TX", "Dallas Sting 15G"),
        (2, "Houston", "TX", "FC Houston 16G McAnally"),
        (2, "Houston", "TX", "Houston Dynamo 17B Adams"),
        (2, "Houston", "TX", "Houston Dash 15G"),
    ]
    findings = find_overcollapsed_rows(FakeConn(rows), canonical_filter="Dallas")
    assert len(findings) == 1
    assert findings[0].canonical_name == "Dallas"


def test_find_ignores_single_token_alias_noise():
    """A bare "dallas" alias on the Dallas canonical row is not by
    itself evidence of over-collapse — only multi-token roots count.
    """
    rows = [
        (1, "Dallas", "TX", "Dallas"),
        (1, "Dallas", "TX", "DALLAS"),
        (1, "Dallas", "TX", "Dallas U15"),
        (1, "Dallas", "TX", "Dallas Boys"),
    ]
    findings = find_overcollapsed_rows(FakeConn(rows))
    # Every alias collapses to "dallas" — single-token, matches
    # canonical, NOT flagged.
    assert findings == []


# ---------------------------------------------------------------------------
# split_canonical_row — fake DB integration test
# ---------------------------------------------------------------------------

class _FakeDB:
    """In-memory simulation of just the columns the splitter touches."""

    def __init__(self):
        # canonical_clubs: {id: {name, state}}
        self.canonical_clubs = {}
        self._next_id = 1
        # club_aliases: list of {id, club_id, alias_name}
        self.club_aliases = []
        # generic raw-name dependent tables: name -> list of {id, fk_col_value, raw}
        # store as dict-of-list keyed by (table, fk_col, raw_col)
        self.tables = {
            ("event_teams", "canonical_club_id", "team_name_raw"): [],
            ("club_roster_snapshots", "club_id", "club_name_raw"): [],
            ("roster_diffs", "club_id", "club_name_raw"): [],
            ("tryouts", "club_id", "club_name_raw"): [],
            ("commitments", "club_id", "club_name_raw"): [],
            ("ynt_call_ups", "club_id", "club_name_raw"): [],
            ("odp_roster_entries", "club_id", "club_name_raw"): [],
            ("player_id_selections", "club_id", "club_name_raw"): [],
        }
        # matches: list of {id, home_club_id, away_club_id, home_team_name, away_team_name}
        self.matches = []
        self._committed = False
        self._snapshots = []  # for rollback

    def add_canonical(self, name, state):
        cid = self._next_id
        self._next_id += 1
        self.canonical_clubs[cid] = {"name": name, "state": state}
        return cid

    def add_alias(self, club_id, alias_name):
        aid = len(self.club_aliases) + 1
        self.club_aliases.append({"id": aid, "club_id": club_id, "alias_name": alias_name})

    def add_dep(self, table, fk_col, raw_col, fk_val, raw):
        rows = self.tables[(table, fk_col, raw_col)]
        rid = len(rows) + 1
        rows.append({"id": rid, fk_col: fk_val, raw_col: raw})

    def add_match(self, home_id, away_id, home_name, away_name):
        mid = len(self.matches) + 1
        self.matches.append({
            "id": mid,
            "home_club_id": home_id,
            "away_club_id": away_id,
            "home_team_name": home_name,
            "away_team_name": away_name,
        })

    def _snapshot(self):
        import copy
        return {
            "canonical_clubs": copy.deepcopy(self.canonical_clubs),
            "_next_id": self._next_id,
            "club_aliases": copy.deepcopy(self.club_aliases),
            "tables": copy.deepcopy(self.tables),
            "matches": copy.deepcopy(self.matches),
        }

    def _restore(self, snap):
        self.canonical_clubs = snap["canonical_clubs"]
        self._next_id = snap["_next_id"]
        self.club_aliases = snap["club_aliases"]
        self.tables = snap["tables"]
        self.matches = snap["matches"]


class _FakeSplitCursor:
    """Small SQL dispatcher that handles only the queries split_canonical_row issues."""

    def __init__(self, db: _FakeDB):
        self.db = db
        self._result = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        params = params or ()
        s = " ".join(sql.split())  # collapse whitespace
        self._result = []

        # SELECT id, club_name_canonical, state FROM canonical_clubs WHERE id = %s FOR UPDATE
        if s.startswith("SELECT id, club_name_canonical, state FROM canonical_clubs WHERE id"):
            cid = params[0]
            row = self.db.canonical_clubs.get(cid)
            if row:
                self._result = [(cid, row["name"], row["state"])]
            return

        # INSERT INTO canonical_clubs
        if s.startswith("INSERT INTO canonical_clubs"):
            name, state = params
            cid = self.db.add_canonical(name, state)
            self._result = [(cid,)]
            return

        # SELECT id, alias_name FROM club_aliases WHERE club_id = %s
        if s.startswith("SELECT id, alias_name FROM club_aliases WHERE club_id"):
            cid = params[0]
            self._result = [
                (a["id"], a["alias_name"])
                for a in self.db.club_aliases if a["club_id"] == cid
            ]
            return

        # UPDATE club_aliases SET club_id = %s WHERE id = %s
        if s.startswith("UPDATE club_aliases SET club_id"):
            new_cid, aid = params
            for a in self.db.club_aliases:
                if a["id"] == aid:
                    a["club_id"] = new_cid
            return

        # SELECT id, <raw> FROM <table> WHERE <fk> = %s AND <raw> IS NOT NULL AND <raw> <> ''
        import re
        m = re.match(
            r"SELECT id, (\w+) FROM (\w+) WHERE (\w+) = %s "
            r"AND \w+ IS NOT NULL AND \w+ <> ''",
            s,
        )
        if m:
            raw_col, table, fk_col = m.group(1), m.group(2), m.group(3)
            fk_val = params[0]
            if table == "matches":
                self._result = [
                    (r["id"], r[raw_col]) for r in self.db.matches
                    if r.get(fk_col) == fk_val and r.get(raw_col)
                ]
            else:
                rows = self.db.tables.get((table, fk_col, raw_col), [])
                self._result = [(r["id"], r[raw_col]) for r in rows if r[raw_col]]
            return

        # UPDATE <table> SET <fk> = %s WHERE id = %s
        m = re.match(r"UPDATE (\w+) SET (\w+) = %s WHERE id = %s", s)
        if m:
            table, fk_col = m.group(1), m.group(2)
            new_val, row_id = params
            if table == "matches":
                for r in self.db.matches:
                    if r["id"] == row_id:
                        r[fk_col] = new_val
            else:
                for key, rows in self.db.tables.items():
                    if key[0] == table and key[1] == fk_col:
                        for r in rows:
                            if r["id"] == row_id:
                                r[fk_col] = new_val
            return

        raise AssertionError(f"Unhandled SQL in fake cursor: {sql!r}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result


class _FakeSplitConn:
    def __init__(self, db: _FakeDB):
        self.db = db
        self._snapshot = db._snapshot()

    def cursor(self):
        return _FakeSplitCursor(self.db)

    def commit(self):
        self.db._committed = True
        self._snapshot = self.db._snapshot()

    def rollback(self):
        self.db._restore(self._snapshot)


def _seed_dallas_db() -> _FakeDB:
    db = _FakeDB()
    cid = db.add_canonical("Dallas", "TX")
    # 5 aliases, 3 distinct multi-token roots
    db.add_alias(cid, "FC Dallas 16G Pre-ECNL McAnally")
    db.add_alias(cid, "FC Dallas 17B")
    db.add_alias(cid, "Dallas Texans 15G")
    db.add_alias(cid, "Dallas Texans 17B")
    db.add_alias(cid, "Dallas Sting 16G")
    # event_teams rows pointing at the over-collapsed canonical
    db.add_dep("event_teams", "canonical_club_id", "team_name_raw", cid,
               "FC Dallas 16G Pre-ECNL McAnally")
    db.add_dep("event_teams", "canonical_club_id", "team_name_raw", cid,
               "Dallas Texans 15G")
    db.add_dep("event_teams", "canonical_club_id", "team_name_raw", cid,
               "Dallas Sting 16G")
    # tryouts pointing at it
    db.add_dep("tryouts", "club_id", "club_name_raw", cid, "Dallas Texans")
    # match where home is Dallas Texans, away is FC Dallas
    db.add_match(cid, cid, "Dallas Texans 16G", "FC Dallas 16G")
    return db


def test_split_dallas_redirects_aliases_and_dependents():
    db = _seed_dallas_db()
    conn = _FakeSplitConn(db)
    result = split_canonical_row(
        conn,
        source_canonical_id=1,
        keep_root="fc dallas",
        new_canonicals={
            "dallas texans": "Dallas Texans",
            "dallas sting": "Dallas Sting",
        },
    )
    assert result.error is None
    assert result.committed is True
    assert result.dry_run is False
    # Two new canonical rows created.
    assert set(result.new_rows.keys()) == {"dallas texans", "dallas sting"}
    texans_id = result.new_rows["dallas texans"]
    sting_id = result.new_rows["dallas sting"]
    assert db.canonical_clubs[texans_id]["name"] == "Dallas Texans"
    assert db.canonical_clubs[sting_id]["name"] == "Dallas Sting"
    # Aliases re-pointed correctly.
    by_alias = {a["alias_name"]: a["club_id"] for a in db.club_aliases}
    assert by_alias["FC Dallas 16G Pre-ECNL McAnally"] == 1  # kept
    assert by_alias["FC Dallas 17B"] == 1  # kept
    assert by_alias["Dallas Texans 15G"] == texans_id
    assert by_alias["Dallas Texans 17B"] == texans_id
    assert by_alias["Dallas Sting 16G"] == sting_id
    # event_teams dependents.
    et = db.tables[("event_teams", "canonical_club_id", "team_name_raw")]
    assert et[0]["canonical_club_id"] == 1  # FC Dallas (kept)
    assert et[1]["canonical_club_id"] == texans_id
    assert et[2]["canonical_club_id"] == sting_id
    # tryouts.
    tr = db.tables[("tryouts", "club_id", "club_name_raw")]
    assert tr[0]["club_id"] == texans_id
    # Match redirected on both sides.
    m = db.matches[0]
    assert m["home_club_id"] == texans_id  # Dallas Texans 16G
    assert m["away_club_id"] == 1  # FC Dallas 16G (kept)


def test_split_dry_run_rolls_back_all_changes():
    db = _seed_dallas_db()
    conn = _FakeSplitConn(db)
    result = split_canonical_row(
        conn,
        source_canonical_id=1,
        keep_root="fc dallas",
        new_canonicals={"dallas texans": "Dallas Texans"},
        dry_run=True,
    )
    assert result.error is None
    assert result.committed is False
    assert result.dry_run is True
    # No persisted change: only the original canonical row remains.
    assert list(db.canonical_clubs.keys()) == [1]
    by_alias = {a["alias_name"]: a["club_id"] for a in db.club_aliases}
    # All aliases stay on cid=1.
    assert all(v == 1 for v in by_alias.values())


def test_split_missing_source_returns_skipped():
    db = _FakeDB()
    db.add_canonical("Dallas", "TX")
    conn = _FakeSplitConn(db)
    result = split_canonical_row(
        conn,
        source_canonical_id=999,
        keep_root="fc dallas",
        new_canonicals={"dallas texans": "Dallas Texans"},
    )
    assert result.skipped is True
    assert result.committed is False
    assert "not found" in (result.skip_reason or "")


def test_split_unknown_root_aliases_stay_on_source():
    """Aliases whose root is neither keep_root nor in new_canonicals must
    remain on the source canonical (operator did not opt to split them)."""
    db = _seed_dallas_db()
    conn = _FakeSplitConn(db)
    result = split_canonical_row(
        conn,
        source_canonical_id=1,
        keep_root="fc dallas",
        new_canonicals={"dallas texans": "Dallas Texans"},
        # Note: dallas sting is NOT in new_canonicals
    )
    assert result.committed is True
    by_alias = {a["alias_name"]: a["club_id"] for a in db.club_aliases}
    # Sting alias stays on the original (id=1) — operator chose not to split it.
    assert by_alias["Dallas Sting 16G"] == 1
