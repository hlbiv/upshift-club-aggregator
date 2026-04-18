"""
Tests for the club_results rollup — scoped vs unscoped recompute.

The scope-isolation test seeds matches in two seasons, runs an
unscoped rollup, mutates one 2025-26 match score, then runs a
`season=2025-26` scoped rollup and asserts the 2024-25 partition
(both row count AND ``last_calculated_at``) is untouched.

Like ``test_coach_effectiveness.py``, the scope-isolation test
requires a live Postgres because the rollup SQL is non-trivial
(``::int`` casts, ``IS NOT DISTINCT FROM``, partial unique indexes
on ``matches``). It skips cleanly when ``DATABASE_URL`` is unset.

There are also pure unit tests for the scope-clause builder that run
without a database.

Run (with DB):
    DATABASE_URL=postgres://... python -m pytest \\
        scraper/tests/test_club_results_rollup.py -v

Run (unit-only, no DB):
    python -m pytest scraper/tests/test_club_results_rollup.py -v
    # → DB-dependent tests skip; unit tests still execute.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore


# ---------------------------------------------------------------------------
# Unit tests — scope-clause builder. No DB required.
# ---------------------------------------------------------------------------

class TestScopeClause:
    def test_unscoped_returns_empty_fragment(self):
        from rollups.club_results import _scope_clause

        sql, params = _scope_clause(None, None)
        assert sql == ""
        assert params == []

    def test_season_only(self):
        from rollups.club_results import _scope_clause

        sql, params = _scope_clause("2025-26", None)
        assert "season = %s" in sql
        assert "league" not in sql
        assert sql.startswith(" AND ")
        assert params == ["2025-26"]

    def test_season_and_league(self):
        from rollups.club_results import _scope_clause

        sql, params = _scope_clause("2025-26", "ECNL")
        assert "season = %s" in sql
        assert "league IS NOT DISTINCT FROM %s" in sql
        assert " AND " in sql.strip()
        assert params == ["2025-26", "ECNL"]

    def test_where_prefix_for_delete(self):
        from rollups.club_results import _scope_clause

        sql, _ = _scope_clause("2025-26", None, prefix="WHERE")
        assert sql.startswith(" WHERE ")

    def test_format_scope_label_unscoped(self):
        from rollups.club_results import _format_scope

        assert _format_scope(None, None) == "all"

    def test_format_scope_label_season_only(self):
        from rollups.club_results import _format_scope

        assert _format_scope("2025-26", None) == "season=2025-26"

    def test_format_scope_label_full(self):
        from rollups.club_results import _format_scope

        assert _format_scope("2025-26", "ECNL") == "season=2025-26 league=ECNL"

    def test_unscoped_delete_sql_is_blanket(self):
        """Backwards-compat: with no flags, DELETE has no WHERE clause."""
        from rollups.club_results import _delete_sql

        sql, params = _delete_sql(None, None)
        assert sql.strip().upper() == "DELETE FROM CLUB_RESULTS"
        assert params == []

    def test_scoped_delete_sql_has_where(self):
        from rollups.club_results import _delete_sql

        sql, params = _delete_sql("2025-26", None)
        assert "WHERE" in sql
        assert "season = %s" in sql
        assert params == ["2025-26"]


# ---------------------------------------------------------------------------
# DB integration tests — scope isolation.
# ---------------------------------------------------------------------------

needs_db = pytest.mark.skipif(
    psycopg2 is None or not os.environ.get("DATABASE_URL"),
    reason="DATABASE_URL + psycopg2 required for rollup SQL tests",
)


@pytest.fixture
def conn():
    c = psycopg2.connect(os.environ["DATABASE_URL"])
    c.autocommit = False
    try:
        yield c
    finally:
        try:
            c.rollback()
        except Exception:
            pass
        c.close()


def _seed_club(cur, name: str, state: str = "GA") -> int:
    cur.execute(
        """
        INSERT INTO canonical_clubs (club_name_canonical, state, country, status)
        VALUES (%s, %s, 'USA', 'active')
        RETURNING id
        """,
        (name, state),
    )
    return cur.fetchone()[0]


def _seed_match(
    cur,
    *,
    home_club_id: int,
    away_club_id: int,
    home_team_name: str,
    away_team_name: str,
    home_score: int,
    away_score: int,
    season: str,
    league: str,
    age_group: str = "U15",
    gender: str = "boys",
    division: str = "Premier",
    match_date: datetime,
    platform_match_id: str,
) -> int:
    cur.execute(
        """
        INSERT INTO matches (
            home_club_id, away_club_id,
            home_team_name, away_team_name,
            home_score, away_score,
            match_date, age_group, gender, division,
            season, league, status, source, platform_match_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'final', 'test', %s
        )
        RETURNING id
        """,
        (
            home_club_id, away_club_id,
            home_team_name, away_team_name,
            home_score, away_score,
            match_date, age_group, gender, division,
            season, league, platform_match_id,
        ),
    )
    return cur.fetchone()[0]


@needs_db
class TestScopeIsolation:
    def test_scoped_recompute_does_not_touch_other_seasons(self, conn):
        """Mutate one 2025-26 match, scoped recompute, assert 2024-25 unchanged.

        Verifies both row count AND ``last_calculated_at`` for the
        2024-25 partition stay frozen across the scoped rerun.
        """
        from rollups.club_results import recompute_club_results

        # ---------------- seed ----------------
        with conn.cursor() as cur:
            club_a = _seed_club(cur, f"Scope Test FC A {os.getpid()}")
            club_b = _seed_club(cur, f"Scope Test FC B {os.getpid()}")

            ts = int(time.time())
            _seed_match(
                cur,
                home_club_id=club_a, away_club_id=club_b,
                home_team_name="Scope A 2024", away_team_name="Scope B 2024",
                home_score=3, away_score=1,
                season="2024-25", league="ScopeTestLeague",
                match_date=datetime(2024, 11, 1),
                platform_match_id=f"scope-test-2024-{ts}",
            )
            _seed_match(
                cur,
                home_club_id=club_a, away_club_id=club_b,
                home_team_name="Scope A 2025", away_team_name="Scope B 2025",
                home_score=2, away_score=2,
                season="2025-26", league="ScopeTestLeague",
                match_date=datetime(2025, 11, 1),
                platform_match_id=f"scope-test-2025-{ts}",
            )

        # ---------------- unscoped rollup ----------------
        result = recompute_club_results(conn=conn, dry_run=False)
        assert result["rows_written"] > 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT club_id, season, wins, losses, draws,
                       goals_for, goals_against, last_calculated_at
                FROM club_results
                WHERE season IN ('2024-25', '2025-26')
                  AND league = 'ScopeTestLeague'
                  AND club_id IN (%s, %s)
                ORDER BY season, club_id
                """,
                (club_a, club_b),
            )
            initial = {(r[0], r[1]): r for r in cur.fetchall()}

        # 2024-25: A wins (3-1), B loses → 4 rows total (one per club per season).
        assert (club_a, "2024-25") in initial
        assert (club_b, "2024-25") in initial
        assert (club_a, "2025-26") in initial
        assert (club_b, "2025-26") in initial

        # Capture the 2024-25 baseline so we can assert it's frozen.
        baseline_2024 = {
            k: v for k, v in initial.items() if k[1] == "2024-25"
        }

        # ---------------- mutate one 2025-26 match ----------------
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE matches
                SET home_score = 5, away_score = 0
                WHERE season = '2025-26'
                  AND home_team_name = 'Scope A 2025'
                """
            )

            # Stamp the 2024-25 partition with a known-old sentinel
            # last_calculated_at. If the scoped rollup incorrectly
            # touched 2024-25 rows, the column would jump to the
            # transaction's NOW(); the assertion below would catch it.
            # We can't rely on NOW() advancing between recompute calls
            # because the fixture keeps everything in one transaction,
            # so the sentinel approach is the only reliable proof of
            # non-touch.
            sentinel = datetime(2000, 1, 1, 0, 0, 0)
            cur.execute(
                """
                UPDATE club_results
                SET last_calculated_at = %s
                WHERE season = '2024-25'
                  AND league = 'ScopeTestLeague'
                  AND club_id IN (%s, %s)
                """,
                (sentinel, club_a, club_b),
            )

        # ---------------- scoped rollup (season=2025-26) ----------------
        result = recompute_club_results(
            conn=conn, dry_run=False, season="2025-26",
        )
        assert result["rows_written"] > 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT club_id, season, wins, losses, draws,
                       goals_for, goals_against, last_calculated_at
                FROM club_results
                WHERE season IN ('2024-25', '2025-26')
                  AND league = 'ScopeTestLeague'
                  AND club_id IN (%s, %s)
                ORDER BY season, club_id
                """,
                (club_a, club_b),
            )
            after = {(r[0], r[1]): r for r in cur.fetchall()}

        # 2024-25 partition: row count unchanged.
        after_2024 = {k: v for k, v in after.items() if k[1] == "2024-25"}
        assert len(after_2024) == len(baseline_2024)

        # 2024-25 rows: aggregate columns frozen vs baseline; sentinel
        # last_calculated_at proves the row was never re-INSERTed by
        # the scoped rerun.
        for key, baseline_row in baseline_2024.items():
            assert key in after_2024, f"missing 2024-25 row for {key}"
            after_row = after_2024[key]
            # Cols 0..6 = (club_id, season, wins, losses, draws, gf, ga)
            # — all aggregate columns must match the pre-rerun snapshot.
            assert after_row[:7] == baseline_row[:7], (
                f"2024-25 aggregate columns for {key} changed: "
                f"before={baseline_row[:7]} after={after_row[:7]}"
            )
            # Col 7 = last_calculated_at — must equal the sentinel we
            # stamped, not the rerun's NOW().
            assert after_row[7] == sentinel, (
                f"2024-25 last_calculated_at for {key} was overwritten "
                f"by scoped rerun (expected sentinel {sentinel}, "
                f"got {after_row[7]})"
            )

        # 2025-26 partition: club_a's score change was picked up.
        new_a_2025 = after[(club_a, "2025-26")]
        # wins=1 (5-0), goals_for=5, goals_against=0
        assert new_a_2025[2] == 1, f"expected wins=1, got {new_a_2025[2]}"
        assert new_a_2025[5] == 5, f"expected gf=5, got {new_a_2025[5]}"
        assert new_a_2025[6] == 0, f"expected ga=0, got {new_a_2025[6]}"

    def test_unscoped_dry_run_returns_zero(self, conn):
        from rollups.club_results import recompute_club_results

        result = recompute_club_results(conn=conn, dry_run=True)
        assert result == {"rows_written": 0, "skipped_linker_pending": 0}

    def test_scoped_dry_run_returns_zero(self, conn):
        from rollups.club_results import recompute_club_results

        result = recompute_club_results(
            conn=conn, dry_run=True, season="2025-26", league="ECNL",
        )
        assert result == {"rows_written": 0, "skipped_linker_pending": 0}
