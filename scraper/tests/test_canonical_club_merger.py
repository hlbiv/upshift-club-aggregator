"""
Unit tests for canonical_club_merger — tiering + FK redirect behaviour.

DB layer is stubbed with an in-memory cursor double that mirrors a small
slice of the canonical_clubs schema (just the tables the merger touches).
Same pattern as test_canonical_club_linker.py.
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

# Stub psycopg2 ONLY if not installed, so tests don't need a live DB.
# Unconditional stubs leak MagicMocks into later-collected test modules
# (pytest imports all test files before any tests run) and break imports
# like `from psycopg2.extras import Json`.
try:
    import psycopg2  # noqa: F401
    import psycopg2.extras  # noqa: F401
except ImportError:
    sys.modules["psycopg2"] = mock.MagicMock()
    sys.modules["psycopg2.extras"] = mock.MagicMock()

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dedup.club_dedup import DedupPair  # noqa: E402
from dedup.canonical_club_merger import (  # noqa: E402
    AUTO_MERGE_SIMILARITY,
    ClubMeta,
    fetch_club_meta,
    merge_canonical_clubs,
    pick_winner,
    tier_pairs,
)


# ---------------------------------------------------------------------------
# Tiering
# ---------------------------------------------------------------------------

def _meta(
    cid, name="Club", state="GA", website=None, manually_merged=False, completeness=3
):
    return ClubMeta(
        id=cid, name=name, state=state, website=website,
        manually_merged=manually_merged, completeness=completeness,
    )


def _pair(a_id, b_id, similarity=0.97, state="GA"):
    return DedupPair(
        club_a_id=a_id, club_a_name=f"Club {a_id}",
        club_b_id=b_id, club_b_name=f"Club {b_id}",
        state=state, similarity=similarity,
        match_reason="fuzzy_name",
    )


def test_tier_auto_merge_same_state_high_similarity():
    pairs = [_pair(1, 2, similarity=0.97)]
    meta = {1: _meta(1, completeness=4), 2: _meta(2, completeness=2)}
    out = tier_pairs(pairs, meta)
    assert len(out) == 1
    assert out[0].tier == "auto_merge"
    # Higher completeness wins.
    assert out[0].recommended_winner_id == 1


def test_tier_review_when_below_auto_threshold():
    pairs = [_pair(1, 2, similarity=0.90)]
    meta = {1: _meta(1), 2: _meta(2)}
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "review"
    assert "0.90" in out[0].reasoning or "0.95" in out[0].reasoning


def test_tier_review_when_states_differ():
    pairs = [_pair(1, 2, similarity=0.99, state="GA")]
    meta = {1: _meta(1, state="GA"), 2: _meta(2, state="OH")}
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "review"
    assert "state mismatch" in out[0].reasoning


def test_tier_review_when_websites_conflict():
    pairs = [_pair(1, 2, similarity=0.99)]
    meta = {
        1: _meta(1, website="https://clubA.com"),
        2: _meta(2, website="https://clubB.com"),
    }
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "review"
    assert "websites differ" in out[0].reasoning


def test_tier_auto_merge_when_websites_match_loosely():
    """https/www differences should NOT block auto-merge."""
    pairs = [_pair(1, 2, similarity=0.99)]
    meta = {
        1: _meta(1, website="https://www.club.com/", completeness=3),
        2: _meta(2, website="http://club.com",      completeness=2),
    }
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "auto_merge"


def test_tier_skip_when_manually_merged_set():
    pairs = [_pair(1, 2, similarity=0.99)]
    meta = {1: _meta(1, manually_merged=True), 2: _meta(2)}
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "skip"
    assert "manually_merged" in out[0].reasoning


def test_tier_skip_when_meta_missing():
    pairs = [_pair(1, 2, similarity=0.99)]
    meta = {1: _meta(1)}  # 2 missing
    out = tier_pairs(pairs, meta)
    assert out[0].tier == "skip"


def test_pick_winner_completeness_then_oldest_id():
    a = _meta(10, completeness=5)
    b = _meta(20, completeness=3)
    winner_id, loser_id = pick_winner(a, b)
    assert winner_id == 10 and loser_id == 20

    # Tie on completeness — older id wins.
    a2 = _meta(50, completeness=4)
    b2 = _meta(7, completeness=4)
    winner_id, loser_id = pick_winner(a2, b2)
    assert winner_id == 7 and loser_id == 50


# ---------------------------------------------------------------------------
# Merger — stubbed DB
# ---------------------------------------------------------------------------

class FakeCursor:
    """In-memory psycopg2 cursor stand-in that supports the merger's SQL."""

    def __init__(self, state):
        self.state = state
        self._last_result = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    # ----- helpers -------------------------------------------------------

    def _select_lock_clubs(self, ids):
        rows = []
        for cid in ids:
            r = self.state["canonical_clubs"].get(cid)
            if r is not None:
                rows.append((cid, r["club_name_canonical"], r["manually_merged"]))
        self._last_result = rows
        self.rowcount = len(rows)

    def _redirect_with_uq(self, table, column, unique_cols, loser_id, winner_id):
        # DELETE pass: drop loser rows whose composite key already exists
        # under the winner.
        rows = self.state[table]
        winner_keys = set()
        for r in rows:
            if r.get(column) == winner_id:
                key = tuple(r.get(c) for c in unique_cols)
                winner_keys.add(key)
        deleted = 0
        kept = []
        for r in rows:
            if r.get(column) == loser_id:
                key = tuple(r.get(c) for c in unique_cols)
                if key in winner_keys:
                    deleted += 1
                    continue
            kept.append(r)
        self.state[table] = kept
        return deleted

    def _redirect_simple(self, table, column, loser_id, winner_id):
        n = 0
        for r in self.state[table]:
            if r.get(column) == loser_id:
                r[column] = winner_id
                n += 1
        return n

    # ----- execute -------------------------------------------------------

    def execute(self, sql: str, params=None):
        sql_clean = sql.strip()

        # FOR UPDATE on canonical_clubs (lookup before merge).
        if (
            sql_clean.startswith("SELECT id, club_name_canonical, manually_merged")
            and "FOR UPDATE" in sql_clean
        ):
            self._select_lock_clubs(list(params))
            return

        # Existence ping.
        if sql_clean.startswith("SELECT 1 FROM"):
            self.rowcount = 0
            self._last_result = []
            return

        # Bulk fetch_club_meta select.
        if sql_clean.startswith("SELECT id, club_name_canonical, state, website"):
            ids = list(params[0])
            rows = []
            cols = [
                "id", "club_name_canonical", "state", "website", "manually_merged",
                "city", "logo_url", "founded_year", "twitter", "instagram",
                "facebook", "staff_page_url",
            ]
            for cid in ids:
                meta = self.state.get("canonical_clubs", {}).get(cid)
                if meta is None:
                    continue
                rows.append(tuple(meta.get(c) for c in cols))
            self._last_result = rows
            self.rowcount = len(rows)
            return

        # DELETE … USING … WHERE … (composite-key dedup).
        if sql_clean.startswith("DELETE FROM ") and " USING " in sql_clean:
            # Polymorphic career-history: use special handler.
            if "coach_career_history" in sql_clean:
                loser_id, winner_id = params
                deleted = 0
                rows = self.state["coach_career_history"]
                winner_keys = set()
                for r in rows:
                    if (
                        r.get("entity_type") == "club"
                        and r.get("entity_id") == winner_id
                    ):
                        winner_keys.add(
                            (r.get("coach_id"), r.get("role"),
                             r.get("start_year") if r.get("start_year") is not None else -1)
                        )
                kept = []
                for r in rows:
                    if (
                        r.get("entity_type") == "club"
                        and r.get("entity_id") == loser_id
                    ):
                        key = (
                            r.get("coach_id"), r.get("role"),
                            r.get("start_year") if r.get("start_year") is not None else -1,
                        )
                        if key in winner_keys:
                            deleted += 1
                            continue
                    kept.append(r)
                self.state["coach_career_history"] = kept
                self.rowcount = deleted
                return

            # Generic: parse "DELETE FROM <table> AS l USING …"
            after_delete = sql_clean.split("DELETE FROM ", 1)[1]
            table = after_delete.split(" AS ", 1)[0].strip()
            # Determine column from the WHERE predicate text. The merger
            # always writes "l.<column> = %s".
            col_after = sql_clean.split("WHERE l.", 1)[1]
            column = col_after.split(" =", 1)[0].strip()
            # Identify unique cols from the trailing AND … predicates.
            unique_cols: list = []
            for chunk in sql_clean.split(" AND "):
                # Match "l.<col> = w.<col>"
                if chunk.strip().startswith("l.") and " = w." in chunk:
                    col = chunk.strip()[2:].split(" =", 1)[0].strip()
                    if col != column:
                        unique_cols.append(col)
            loser_id, winner_id = params
            deleted = self._redirect_with_uq(
                table, column, tuple(unique_cols), loser_id, winner_id,
            )
            self.rowcount = deleted
            return

        # UPDATE coach_career_history SET entity_id = %s WHERE entity_type='club' AND entity_id=%s
        if sql_clean.startswith("UPDATE coach_career_history"):
            winner_id, loser_id = params
            n = 0
            for r in self.state["coach_career_history"]:
                if r.get("entity_type") == "club" and r.get("entity_id") == loser_id:
                    r["entity_id"] = winner_id
                    n += 1
            self.rowcount = n
            return

        # UPDATE <table> SET <col> = %s WHERE <col> = %s
        if sql_clean.startswith("UPDATE "):
            after_update = sql_clean.split("UPDATE ", 1)[1]
            table = after_update.split(" SET ", 1)[0].strip()
            after_set = sql_clean.split(" SET ", 1)[1]
            column = after_set.split(" =", 1)[0].strip()
            winner_id, loser_id = params
            n = self._redirect_simple(table, column, loser_id, winner_id)
            self.rowcount = n
            return

        # INSERT alias.
        if sql_clean.startswith("INSERT INTO club_aliases"):
            club_id, alias_name, merged_from_id = params
            existing = {
                (r["club_id"], r["alias_name"])
                for r in self.state["club_aliases"]
            }
            if (club_id, alias_name) in existing:
                self.rowcount = 0
                return
            self.state["club_aliases"].append({
                "club_id": club_id,
                "alias_name": alias_name,
                "merged_from_canonical_id": merged_from_id,
            })
            self.rowcount = 1
            return

        # DELETE FROM canonical_clubs WHERE id = %s
        if sql_clean.startswith("DELETE FROM canonical_clubs"):
            (loser_id,) = params
            if loser_id in self.state["canonical_clubs"]:
                del self.state["canonical_clubs"][loser_id]
                self.rowcount = 1
            else:
                self.rowcount = 0
            return

        raise AssertionError(f"unexpected SQL: {sql!r}")

    def fetchall(self):
        return self._last_result

    def fetchone(self):
        return self._last_result[0] if self._last_result else None


class FakeConn:
    def __init__(self, state):
        self.state = state
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self.state)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def _empty_state(loser_id=10, winner_id=20):
    state = {
        "canonical_clubs": {
            loser_id: {
                "id": loser_id, "club_name_canonical": "Club Loser",
                "state": "GA", "website": None, "manually_merged": False,
                "city": None, "logo_url": None, "founded_year": None,
                "twitter": None, "instagram": None, "facebook": None,
                "staff_page_url": None,
            },
            winner_id: {
                "id": winner_id, "club_name_canonical": "Club Winner",
                "state": "GA", "website": None, "manually_merged": False,
                "city": None, "logo_url": None, "founded_year": None,
                "twitter": None, "instagram": None, "facebook": None,
                "staff_page_url": None,
            },
        },
        "club_aliases": [],
        "club_affiliations": [],
        "coach_discoveries": [],
        "event_teams": [],
        "matches": [],
        "player_id_selections": [],
        "commitments": [],
        "ynt_call_ups": [],
        "odp_roster_entries": [],
        "club_roster_snapshots": [],
        "roster_diffs": [],
        "tryouts": [],
        "club_results": [],
        "coach_scrape_snapshots": [],
        "club_site_changes": [],
        "coach_career_history": [],
    }
    return state


# ---------------------------------------------------------------------------
# Merger — happy paths
# ---------------------------------------------------------------------------

def test_merge_redirects_simple_fk_tables():
    state = _empty_state()
    state["coach_discoveries"].append({
        "club_id": 10, "name": "Coach A", "title": "Head Coach",
    })
    state["matches"].append({
        "home_club_id": 10, "away_club_id": 20,
    })
    state["matches"].append({
        "home_club_id": 20, "away_club_id": 10,
    })
    state["tryouts"].append({"club_id": 10})
    state["roster_diffs"].append({"club_id": 10})
    state["player_id_selections"].append({"club_id": 10})
    state["event_teams"].append({"canonical_club_id": 10})

    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)

    assert res.committed
    assert res.alias_inserted
    assert state["canonical_clubs"].get(10) is None
    # Every loser FK redirected to winner.
    assert state["coach_discoveries"][0]["club_id"] == 20
    assert state["tryouts"][0]["club_id"] == 20
    assert state["roster_diffs"][0]["club_id"] == 20
    assert state["player_id_selections"][0]["club_id"] == 20
    assert state["event_teams"][0]["canonical_club_id"] == 20
    # Both home + away sides flipped.
    assert all(m["home_club_id"] == 20 for m in state["matches"])
    assert all(m["away_club_id"] == 20 for m in state["matches"])
    # Alias points at the winner with merged_from pointer.
    aliases_for_winner = [a for a in state["club_aliases"] if a["club_id"] == 20]
    assert len(aliases_for_winner) == 1
    assert aliases_for_winner[0]["alias_name"] == "Club Loser"
    assert aliases_for_winner[0]["merged_from_canonical_id"] == 10


def test_merge_redirects_commitments_ynt_odp():
    """commitments, ynt_call_ups, odp_roster_entries — newer domains added
    after the original merger; ensure their club_id FKs get redirected so
    the final DELETE FROM canonical_clubs doesn't FK-violate (ynt/odp have
    NO ACTION onDelete) and commitments don't get their club link nulled."""
    state = _empty_state()
    state["commitments"].append({"club_id": 10, "player_name": "Jane Doe"})
    state["ynt_call_ups"].append({"club_id": 10, "player_name": "Jane Doe"})
    state["odp_roster_entries"].append({
        "club_id": 10, "player_name": "Jane Doe",
    })

    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)

    assert res.committed
    assert state["canonical_clubs"].get(10) is None
    assert state["commitments"][0]["club_id"] == 20
    assert state["ynt_call_ups"][0]["club_id"] == 20
    assert state["odp_roster_entries"][0]["club_id"] == 20
    # Redirect counts reported in the result.
    assert res.rows_redirected.get("commitments.club_id") == 1
    assert res.rows_redirected.get("ynt_call_ups.club_id") == 1
    assert res.rows_redirected.get("odp_roster_entries.club_id") == 1


def test_merge_dedupes_composite_uq_collisions():
    """coach_discoveries (club_id, name, title) — duplicate row on loser side
    must be deleted rather than UPDATE-collide."""
    state = _empty_state()
    # Both loser and winner have the same (name, title) — UPDATE would collide.
    state["coach_discoveries"].append({
        "club_id": 10, "name": "Coach A", "title": "Head Coach",
    })
    state["coach_discoveries"].append({
        "club_id": 20, "name": "Coach A", "title": "Head Coach",
    })
    # Loser also has a unique row that must survive.
    state["coach_discoveries"].append({
        "club_id": 10, "name": "Coach B", "title": "Assistant",
    })

    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.committed
    # The colliding row was deleted from the loser side …
    assert any(
        v >= 1
        for k, v in res.rows_deleted_from_loser.items()
        if "coach_discoveries" in k
    )
    # … and the unique loser row survived as a redirect.
    surviving_winner_rows = [
        r for r in state["coach_discoveries"] if r["club_id"] == 20
    ]
    assert len(surviving_winner_rows) == 2
    # All winner rows now contain "Coach A" + "Coach B".
    names = {r["name"] for r in surviving_winner_rows}
    assert names == {"Coach A", "Coach B"}


def test_merge_redirects_polymorphic_career_history():
    state = _empty_state()
    state["coach_career_history"].append({
        "coach_id": 5, "entity_type": "club", "entity_id": 10,
        "role": "head_coach", "start_year": 2020,
    })
    state["coach_career_history"].append({
        "coach_id": 6, "entity_type": "college", "entity_id": 10,  # NOT redirected
        "role": "head_coach", "start_year": 2020,
    })
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.committed
    # Club row redirected.
    club_row = next(r for r in state["coach_career_history"] if r["coach_id"] == 5)
    assert club_row["entity_id"] == 20
    # College row untouched (entity_type='college', NOT 'club').
    college_row = next(r for r in state["coach_career_history"] if r["coach_id"] == 6)
    assert college_row["entity_id"] == 10


# ---------------------------------------------------------------------------
# Merger — guards
# ---------------------------------------------------------------------------

def test_merge_skipped_when_loser_already_gone():
    """Idempotency: re-running on a previously merged loser is a no-op."""
    state = _empty_state()
    del state["canonical_clubs"][10]  # loser already merged
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.skipped
    assert "already absent" in (res.skip_reason or "")
    assert not conn.committed
    assert conn.rolled_back


def test_merge_skipped_when_winner_missing():
    state = _empty_state()
    del state["canonical_clubs"][20]
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.skipped
    assert "winner_id 20 not found" in (res.skip_reason or "")
    assert not conn.committed


def test_merge_skipped_when_loser_pinned():
    state = _empty_state()
    state["canonical_clubs"][10]["manually_merged"] = True
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.skipped
    assert "manually_merged" in (res.skip_reason or "")
    # Loser row preserved.
    assert state["canonical_clubs"].get(10) is not None


def test_merge_skipped_when_winner_pinned():
    state = _empty_state()
    state["canonical_clubs"][20]["manually_merged"] = True
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn)
    assert res.skipped
    assert "manually_merged" in (res.skip_reason or "")


def test_merge_skipped_when_loser_equals_winner():
    res = merge_canonical_clubs(loser_id=42, winner_id=42, conn=mock.MagicMock())
    assert res.skipped
    assert "loser_id == winner_id" in (res.skip_reason or "")


def test_merge_dry_run_does_not_commit():
    state = _empty_state()
    state["coach_discoveries"].append({
        "club_id": 10, "name": "Coach A", "title": "Head Coach",
    })
    conn = FakeConn(state)
    res = merge_canonical_clubs(loser_id=10, winner_id=20, conn=conn, dry_run=True)
    # Stats reflect what would happen.
    assert "coach_discoveries.club_id" in res.rows_redirected
    assert not res.committed
    assert conn.rolled_back


# ---------------------------------------------------------------------------
# fetch_club_meta — wired through the FakeCursor protocol
# ---------------------------------------------------------------------------

def test_fetch_club_meta_returns_completeness_count():
    state = _empty_state()
    state["canonical_clubs"][10].update({
        "city": "Atlanta",
        "website": "https://x.com",
        "twitter": "@x",
    })
    conn = FakeConn(state)
    out = fetch_club_meta(conn, [10, 20])
    assert 10 in out and 20 in out
    # state + city + website + twitter = 4 (state was already set to "GA").
    assert out[10].completeness == 4
    # Winner has only state set.
    assert out[20].completeness == 1
