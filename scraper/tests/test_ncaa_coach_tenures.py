"""
Tests for PR-7 — historical coaching tenures.

Covers:
- ``_normalize_coach_tenure`` — shape validation + academic_year format
- ``upsert_coach_tenures`` — named-constraint SQL, batching, dry-run,
  bad-row skip, idempotent re-upsert (no-drift guarantee)
- Writer docstring invariant: existing ``upsert_coaches`` still targets
  the college_coaches named constraint (regression guard; PR-7 must not
  silently break the current-directory path)
- Natural-key invariant: writer SQL pins the
  ``college_coach_tenures_college_name_title_year_uq`` constraint by
  name (matches Shape B design from the design conversation)

Not covered here (intentional):
- End-to-end bulk-loop integration — covered by manual Replit smoke
  after merge (documented in PR body)
- Season-gating in the caller — tested in test_ncaa_historical_rosters
  via the current-vs-historical URL selection logic; PR-7 just hooks
  the extractor into the same branches

Run::

    python -m pytest scraper/tests/test_ncaa_coach_tenures.py -v
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ingest import ncaa_roster_writer  # noqa: E402


# ---------------------------------------------------------------------------
# _normalize_coach_tenure
# ---------------------------------------------------------------------------


class TestNormalizeCoachTenure:
    def test_happy_path(self):
        row = ncaa_roster_writer._normalize_coach_tenure(
            {
                "name": "Brian Wiese",
                "title": "Head Coach",
                "is_head_coach": True,
                "source_url": "https://guhoyas.com/sports/mens-soccer/roster/2023",
            },
            college_id=14,
            academic_year="2023-24",
        )
        assert row["college_id"] == 14
        assert row["name"] == "Brian Wiese"
        assert row["title"] == "Head Coach"
        assert row["academic_year"] == "2023-24"
        assert row["is_head_coach"] is True
        assert row["source_url"] == "https://guhoyas.com/sports/mens-soccer/roster/2023"

    def test_missing_name_rejected(self):
        with pytest.raises(ValueError, match="missing name"):
            ncaa_roster_writer._normalize_coach_tenure(
                {"title": "Head Coach"},
                college_id=14,
                academic_year="2023-24",
            )

    @pytest.mark.parametrize(
        "bad",
        ["", "2023", "2023-2024", "2023/24", None],
    )
    def test_bad_academic_year_rejected(self, bad):
        with pytest.raises(ValueError, match="academic_year"):
            ncaa_roster_writer._normalize_coach_tenure(
                {"name": "X"}, college_id=14, academic_year=bad,
            )

    def test_defaults(self):
        """Minimal input — optional fields default to None/False."""
        row = ncaa_roster_writer._normalize_coach_tenure(
            {"name": "Y"}, college_id=1, academic_year="2020-21",
        )
        assert row["title"] is None
        assert row["is_head_coach"] is False
        assert row["source_url"] is None


# ---------------------------------------------------------------------------
# SQL invariants — regression guards
# ---------------------------------------------------------------------------


class TestWriterSqlInvariants:
    def test_tenure_sql_targets_named_constraint(self):
        """Shape B design pins identity via named unique. Future refactor
        to implicit column-list would still work but silently change
        behavior — make that a loud test failure."""
        assert (
            "ON CONFLICT ON CONSTRAINT college_coach_tenures_college_name_title_year_uq"
            in ncaa_roster_writer._UPSERT_COACH_TENURE_SQL
        )

    def test_tenure_sql_updates_expected_fields_only(self):
        """Idempotent on conflict — bumps scraped_at + source_url, leaves
        identity fields untouched. If a future edit starts mutating
        name/title/academic_year in the UPDATE, that's a bug."""
        sql = ncaa_roster_writer._UPSERT_COACH_TENURE_SQL
        assert "is_head_coach = EXCLUDED.is_head_coach" in sql
        assert "source_url    = EXCLUDED.source_url" in sql
        assert "scraped_at    = now()" in sql
        # Identity columns should NOT appear in the UPDATE clause
        update_block = sql.split("DO UPDATE SET", 1)[1].split("RETURNING", 1)[0]
        for forbidden in ("college_id", "name  ", "title ", "academic_year"):
            assert forbidden not in update_block, (
                f"UPDATE must not mutate identity column {forbidden!r}"
            )

    def test_college_coaches_sql_regression_guard(self):
        """PR-7 must not silently break the existing college_coaches
        upsert (used by single-school path + PR-7 current-season hook)."""
        assert (
            "ON CONFLICT ON CONSTRAINT college_coaches_college_name_title_uq"
            in ncaa_roster_writer._UPSERT_COACH_SQL
        )


# ---------------------------------------------------------------------------
# upsert_coach_tenures — batch, dry-run, idempotent
# ---------------------------------------------------------------------------


class TestUpsertCoachTenures:
    def test_dry_run_does_not_touch_conn(self):
        with mock.patch.object(ncaa_roster_writer, "_get_connection") as mock_conn:
            counts = ncaa_roster_writer.upsert_coach_tenures(
                [{"name": "Coach A", "title": "Head Coach", "is_head_coach": True}],
                college_id=14,
                academic_year="2023-24",
                dry_run=True,
            )
        assert counts == {"inserted": 0, "updated": 0, "skipped": 0}
        mock_conn.assert_not_called()

    def test_empty_input_is_noop(self):
        counts = ncaa_roster_writer.upsert_coach_tenures(
            [], college_id=14, academic_year="2023-24",
        )
        assert counts == {"inserted": 0, "updated": 0, "skipped": 0}

    def test_successful_insert(self):
        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None
        # xmax = 0 → inserted=True
        fake_cursor.fetchone.return_value = (True,)

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        counts = ncaa_roster_writer.upsert_coach_tenures(
            [
                {
                    "name": "Brian Wiese",
                    "title": "Head Coach",
                    "is_head_coach": True,
                    "source_url": "https://guhoyas.com/sports/mens-soccer/roster/2023",
                }
            ],
            college_id=14,
            academic_year="2023-24",
            conn=fake_conn,
        )

        assert counts == {"inserted": 1, "updated": 0, "skipped": 0}
        sql, params = fake_cursor.execute.call_args[0]
        assert "college_coach_tenures_college_name_title_year_uq" in sql
        assert params["college_id"] == 14
        assert params["name"] == "Brian Wiese"
        assert params["academic_year"] == "2023-24"
        assert params["is_head_coach"] is True

    def test_second_call_same_key_is_update(self):
        """Silent idempotent re-upsert — second call returns updated=1."""
        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None
        fake_cursor.fetchone.return_value = (False,)  # xmax != 0

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        counts = ncaa_roster_writer.upsert_coach_tenures(
            [{"name": "Brian Wiese", "title": "Head Coach"}],
            college_id=14,
            academic_year="2023-24",
            conn=fake_conn,
        )
        assert counts == {"inserted": 0, "updated": 1, "skipped": 0}

    def test_bad_row_is_skipped_not_raised(self):
        """One missing-name row in a batch shouldn't abort the batch.
        The writer skips + logs + continues (same pattern as upsert_coaches)."""
        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None
        fake_cursor.fetchone.return_value = (True,)

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        counts = ncaa_roster_writer.upsert_coach_tenures(
            [
                {"title": "Head Coach"},  # missing name — will be skipped
                {"name": "Coach B", "title": "Asst"},
            ],
            college_id=1,
            academic_year="2023-24",
            conn=fake_conn,
        )
        assert counts["skipped"] == 1
        assert counts["inserted"] == 1

    def test_conflict_insert_then_update_simulation(self):
        """Two sequential calls with different names → one insert, one insert
        (different natural keys); same name twice → one insert, one update.
        Tests the caller loop + per-row dispatch."""
        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None
        # First two calls insert, third (same key as first) updates
        fake_cursor.fetchone.side_effect = [(True,), (True,), (False,)]

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        counts = ncaa_roster_writer.upsert_coach_tenures(
            [
                {"name": "Coach A", "title": "Head Coach"},
                {"name": "Coach B", "title": "Head Coach"},
                {"name": "Coach A", "title": "Head Coach"},  # re-upsert
            ],
            college_id=1,
            academic_year="2023-24",
            conn=fake_conn,
        )
        assert counts == {"inserted": 2, "updated": 1, "skipped": 0}
