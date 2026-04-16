"""
Tests for coach career builder and effectiveness rollup.

These tests require a live Postgres (DATABASE_URL) because both rollups
are SQL-heavy. Stubbing psycopg2 would only test the Python shape.

Run:
    DATABASE_URL=postgres://... python -m pytest \
        scraper/tests/test_coach_effectiveness.py -v

The tests skip cleanly when DATABASE_URL is unset.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg2  # type: ignore
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

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


def _seed_coach(cur, person_hash: str, display_name: str) -> int:
    cur.execute(
        """
        INSERT INTO coaches (person_hash, display_name, first_seen_at, last_seen_at, created_at, updated_at)
        VALUES (%s, %s, NOW(), NOW(), NOW(), NOW())
        RETURNING id
        """,
        (person_hash, display_name),
    )
    return cur.fetchone()[0]


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


def _seed_college(cur, name: str, division: str = "D1") -> int:
    slug = name.lower().replace(" ", "-") + f"-{os.getpid()}"
    cur.execute(
        """
        INSERT INTO colleges (name, slug, division, gender_program)
        VALUES (%s, %s, %s, 'mens')
        RETURNING id
        """,
        (name, slug, division),
    )
    return cur.fetchone()[0]


def _seed_discovery(cur, club_id: int, coach_id: int, name: str, title: str = "Head Coach") -> int:
    cur.execute(
        """
        INSERT INTO coach_discoveries (club_id, coach_id, name, title, confidence, platform_family, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, 1.0, 'unknown', NOW(), NOW())
        RETURNING id
        """,
        (club_id, coach_id, name, title),
    )
    return cur.fetchone()[0]


def _seed_college_coach(cur, college_id: int, coach_id: int, name: str, is_head: bool = True) -> int:
    cur.execute(
        """
        INSERT INTO college_coaches (college_id, coach_id, name, is_head_coach, first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        RETURNING id
        """,
        (college_id, coach_id, name, is_head),
    )
    return cur.fetchone()[0]


def _seed_roster_entry(cur, college_id: int, player_name: str, prev_club: str, academic_year: str = "2024-25") -> int:
    cur.execute(
        """
        INSERT INTO college_roster_history (college_id, player_name, prev_club, academic_year)
        VALUES (%s, %s, %s, %s)
        RETURNING id
        """,
        (college_id, player_name, prev_club, academic_year),
    )
    return cur.fetchone()[0]


# ---------------------------------------------------------------------------
# Career builder tests
# ---------------------------------------------------------------------------

@needs_db
class TestCareerBuilder:
    def test_builds_club_career_from_discovery(self, conn):
        from rollups.coach_career_builder import build_coach_careers

        with conn.cursor() as cur:
            coach_id = _seed_coach(cur, "test-hash-cb-1", "Test Coach 1")
            club_id = _seed_club(cur, "Test FC Career Builder")
            _seed_discovery(cur, club_id, coach_id, "Test Coach 1", "Head Coach")

        result = build_coach_careers(conn=conn, dry_run=False)
        assert result["career_rows"] > 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT entity_type, entity_id, role, is_current FROM coach_career_history WHERE coach_id = %s",
                (coach_id,),
            )
            rows = cur.fetchall()
            assert len(rows) >= 1
            club_row = [r for r in rows if r[0] == "club"][0]
            assert club_row[1] == club_id
            assert club_row[2] == "head_coach"
            assert club_row[3] is True  # is_current

    def test_builds_college_career_from_college_coaches(self, conn):
        from rollups.coach_career_builder import build_coach_careers

        with conn.cursor() as cur:
            coach_id = _seed_coach(cur, "test-hash-cb-2", "College Coach 1")
            college_id = _seed_college(cur, "Test University CB")
            _seed_college_coach(cur, college_id, coach_id, "College Coach 1")

        result = build_coach_careers(conn=conn, dry_run=False)
        assert result["career_rows"] > 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT entity_type, entity_id, role FROM coach_career_history WHERE coach_id = %s AND entity_type = 'college'",
                (coach_id,),
            )
            rows = cur.fetchall()
            assert len(rows) >= 1
            assert rows[0][1] == college_id
            assert rows[0][2] == "head_coach"

    def test_dry_run_writes_nothing(self, conn):
        from rollups.coach_career_builder import build_coach_careers

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int FROM coach_career_history")
            before = cur.fetchone()[0]

        result = build_coach_careers(conn=conn, dry_run=True)
        assert result["career_rows"] == 0

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int FROM coach_career_history")
            after = cur.fetchone()[0]

        assert before == after

    def test_detects_joined_movement(self, conn):
        from rollups.coach_career_builder import build_coach_careers

        with conn.cursor() as cur:
            coach_id = _seed_coach(cur, "test-hash-cb-3", "Movement Coach")
            club_id = _seed_club(cur, "Movement FC")
            _seed_discovery(cur, club_id, coach_id, "Movement Coach")

        build_coach_careers(conn=conn, dry_run=False)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT event_type, to_entity_type, to_entity_id FROM coach_movement_events WHERE coach_id = %s",
                (coach_id,),
            )
            rows = cur.fetchall()
            joined = [r for r in rows if r[0] == "joined"]
            assert len(joined) >= 1
            assert joined[0][1] == "club"
            assert joined[0][2] == club_id


# ---------------------------------------------------------------------------
# Effectiveness rollup tests
# ---------------------------------------------------------------------------

@needs_db
class TestEffectivenessRollup:
    def test_computes_placements(self, conn):
        from rollups.coach_career_builder import build_coach_careers
        from rollups.coach_effectiveness import recompute_coach_effectiveness

        with conn.cursor() as cur:
            # Setup: coach at a club, players from that club in college rosters
            coach_id = _seed_coach(cur, "test-hash-eff-1", "Effectiveness Coach")
            club_id = _seed_club(cur, "Effectiveness FC")
            _seed_discovery(cur, club_id, coach_id, "Effectiveness Coach")

            college_d1 = _seed_college(cur, "D1 University Eff")
            college_d3 = _seed_college(cur, "D3 College Eff", "D3")

            # Players whose prev_club matches the club name
            _seed_roster_entry(cur, college_d1, "Player A", "Effectiveness FC")
            _seed_roster_entry(cur, college_d1, "Player B", "Effectiveness FC")
            _seed_roster_entry(cur, college_d3, "Player C", "Effectiveness FC")

        # Build careers first (required for effectiveness)
        build_coach_careers(conn=conn, dry_run=False)

        # Now compute effectiveness
        result = recompute_coach_effectiveness(conn=conn, dry_run=False)
        assert result["rows_written"] > 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT players_placed_d1, players_placed_d3, players_placed_total,
                       clubs_coached
                FROM coach_effectiveness
                WHERE coach_id = %s
                """,
                (coach_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] >= 2  # D1 placements
            assert row[1] >= 1  # D3 placements
            assert row[2] >= 3  # total
            assert row[3] >= 1  # clubs coached

    def test_dry_run_writes_nothing(self, conn):
        from rollups.coach_effectiveness import recompute_coach_effectiveness

        result = recompute_coach_effectiveness(conn=conn, dry_run=True)
        assert result["rows_written"] == 0

    def test_aborts_without_career_history(self, conn):
        from rollups.coach_effectiveness import recompute_coach_effectiveness

        # In a transaction with no career history rows, should raise
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int FROM coach_career_history WHERE entity_type = 'club'")
            count = cur.fetchone()[0]

        if count == 0:
            with pytest.raises(RuntimeError, match="no coach_career_history"):
                recompute_coach_effectiveness(conn=conn, dry_run=False)

    def test_matches_via_alias(self, conn):
        from rollups.coach_career_builder import build_coach_careers
        from rollups.coach_effectiveness import recompute_coach_effectiveness

        with conn.cursor() as cur:
            coach_id = _seed_coach(cur, "test-hash-eff-alias", "Alias Coach")
            club_id = _seed_club(cur, "Full Name Soccer Club")
            _seed_discovery(cur, club_id, coach_id, "Alias Coach")

            # Add an alias
            cur.execute(
                "INSERT INTO club_aliases (club_id, alias_name, source) VALUES (%s, %s, 'test')",
                (club_id, "FNSC"),
            )

            college_id = _seed_college(cur, "Alias Test University")
            # Player's prev_club uses the alias
            _seed_roster_entry(cur, college_id, "Alias Player", "FNSC")

        build_coach_careers(conn=conn, dry_run=False)
        result = recompute_coach_effectiveness(conn=conn, dry_run=False)
        assert result["rows_written"] > 0

        with conn.cursor() as cur:
            cur.execute(
                "SELECT players_placed_total FROM coach_effectiveness WHERE coach_id = %s",
                (coach_id,),
            )
            row = cur.fetchone()
            assert row is not None
            assert row[0] >= 1


# ---------------------------------------------------------------------------
# Unit tests for role normalization (no DB required)
# ---------------------------------------------------------------------------

class TestRoleNormalization:
    def test_head_coach(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Head Coach") == "head_coach"
        assert _normalize_role("head_coach") == "head_coach"

    def test_assistant(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Assistant Coach") == "assistant"
        assert _normalize_role("Asst Coach") == "assistant"

    def test_director(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Director of Coaching") == "club_director"
        assert _normalize_role("DOC") == "doc"

    def test_gk_coach(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Goalkeeper Coach") == "gk_coach"

    def test_fitness(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Fitness Coach") == "fitness"
        assert _normalize_role("Strength and Conditioning") == "fitness"

    def test_none_returns_other(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role(None) == "other"
        assert _normalize_role("") == "other"

    def test_unknown_returns_other(self):
        from rollups.coach_career_builder import _normalize_role
        assert _normalize_role("Some Random Title") == "other"
