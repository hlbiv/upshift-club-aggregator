"""
Tests for ingest.college_flag_writer.write_college_flag.

Covers:
- validate flag_type and url_needs_review reason values
- dry_run path: validate + return None without DB access
- happy-path DB write via mocked cursor (inserted=True case)
- idempotent re-run (already exists, xmax != 0 → inserted=False)
- pre-PR-24 table-missing graceful skip (UndefinedTable catch)
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2.errors  # noqa: E402

from ingest.college_flag_writer import write_college_flag  # noqa: E402


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_invalid_flag_type_raises(self):
        with pytest.raises(ValueError, match="invalid flag_type"):
            write_college_flag(
                college_id=1,
                academic_year="2025-26",
                flag_type="bad_type",
                dry_run=True,
            )

    def test_url_needs_review_missing_reason_raises(self):
        with pytest.raises(ValueError, match="requires metadata"):
            write_college_flag(
                college_id=1,
                academic_year="2025-26",
                flag_type="url_needs_review",
                metadata={},
                dry_run=True,
            )

    def test_url_needs_review_bad_reason_raises(self):
        with pytest.raises(ValueError, match="requires metadata"):
            write_college_flag(
                college_id=1,
                academic_year="2025-26",
                flag_type="url_needs_review",
                metadata={"reason": "not_a_real_reason"},
                dry_run=True,
            )

    def test_url_needs_review_valid_reason_passes(self):
        result = write_college_flag(
            college_id=1,
            academic_year="2025-26",
            flag_type="url_needs_review",
            metadata={"reason": "no_url_at_all"},
            dry_run=True,
        )
        assert result is None

    def test_historical_no_data_no_metadata_required(self):
        result = write_college_flag(
            college_id=1,
            academic_year="2024-25",
            flag_type="historical_no_data",
            dry_run=True,
        )
        assert result is None

    def test_partial_parse_valid(self):
        result = write_college_flag(
            college_id=2,
            academic_year="2025-26",
            flag_type="partial_parse",
            metadata={"player_count": 3, "threshold": 5},
            dry_run=True,
        )
        assert result is None


# ---------------------------------------------------------------------------
# DB write path
# ---------------------------------------------------------------------------


class TestDbWrite:
    def _make_conn(self, fetchone_return):
        cursor = mock.MagicMock()
        cursor.__enter__ = mock.Mock(return_value=cursor)
        cursor.__exit__ = mock.Mock(return_value=False)
        cursor.fetchone.return_value = fetchone_return
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor
        return conn, cursor

    def test_inserted_new_row(self):
        conn, cursor = self._make_conn((42, True))
        result = write_college_flag(
            college_id=5,
            academic_year="2025-26",
            flag_type="url_needs_review",
            metadata={"reason": "static_404"},
            conn=conn,
        )
        assert result == {"id": 42, "inserted": True}
        conn.commit.assert_called_once()

    def test_idempotent_refresh(self):
        conn, cursor = self._make_conn((7, False))
        result = write_college_flag(
            college_id=5,
            academic_year="2025-26",
            flag_type="url_needs_review",
            metadata={"reason": "no_url_at_all"},
            conn=conn,
        )
        assert result == {"id": 7, "inserted": False}

    def test_already_resolved_flag_returns_none(self):
        """ON CONFLICT WHERE resolved_at IS NULL skips resolved rows."""
        conn, cursor = self._make_conn(None)
        result = write_college_flag(
            college_id=5,
            academic_year="2025-26",
            flag_type="partial_parse",
            conn=conn,
        )
        assert result is None

    def test_undefined_table_gracefully_skipped(self):
        """Pre-PR-24: table not yet pushed to DB. Should warn + return None."""
        cursor = mock.MagicMock()
        cursor.__enter__ = mock.Mock(return_value=cursor)
        cursor.__exit__ = mock.Mock(return_value=False)
        cursor.execute.side_effect = psycopg2.errors.UndefinedTable(
            "relation does not exist"
        )
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor

        result = write_college_flag(
            college_id=1,
            academic_year="2025-26",
            flag_type="historical_no_data",
            conn=conn,
        )
        assert result is None
        conn.rollback.assert_called_once()
