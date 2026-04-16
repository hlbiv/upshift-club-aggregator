"""
Tests for the GotSport batch matches runner.

Run:
    python -m pytest scraper/tests/test_gotsport_matches_runner.py -v
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from gotsport_matches_runner import (  # noqa: E402
    _fetch_gotsport_events,
    run_gotsport_matches_batch,
    MatchRunOutcome,
    _rate_limit,
)


# ---------------------------------------------------------------------------
# _fetch_gotsport_events — query logic
# ---------------------------------------------------------------------------

class TestFetchGotsportEvents:
    """Test the SQL query builder for fetching GotSport events."""

    def _make_mock_conn(self, rows, columns=None):
        """Build a mock psycopg2 connection that returns the given rows."""
        if columns is None:
            columns = ["id", "name", "platform_event_id", "season", "league_name"]
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = rows
        mock_cur.description = [(col,) for col in columns]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cur
        return mock_conn, mock_cur

    def test_returns_all_events(self):
        rows = [
            (1, "Event A", "45000", "2025-26", "ECNL Boys"),
            (2, "Event B", "45001", "2025-26", "GA Premier"),
        ]
        conn, cur = self._make_mock_conn(rows)
        result = _fetch_gotsport_events(conn)

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[0]["platform_event_id"] == "45000"
        assert result[1]["name"] == "Event B"

    def test_filters_by_event_id(self):
        rows = [(1, "Event A", "45000", "2025-26", "ECNL Boys")]
        conn, cur = self._make_mock_conn(rows)
        result = _fetch_gotsport_events(conn, event_id="45000")

        assert len(result) == 1
        # Verify the SQL included the platform_event_id filter.
        executed_sql = cur.execute.call_args[0][0]
        assert "platform_event_id = %s" in executed_sql
        params = cur.execute.call_args[0][1]
        assert "45000" in params

    def test_respects_limit(self):
        rows = [(1, "Event A", "45000", "2025-26", None)]
        conn, cur = self._make_mock_conn(rows)
        result = _fetch_gotsport_events(conn, limit=5)

        executed_sql = cur.execute.call_args[0][0]
        assert "LIMIT" in executed_sql

    def test_empty_result(self):
        conn, _ = self._make_mock_conn([])
        result = _fetch_gotsport_events(conn)
        assert result == []


# ---------------------------------------------------------------------------
# event_id FK resolution
# ---------------------------------------------------------------------------

class TestEventFkResolution:
    """Verify the runner stamps event_fk_id on match rows before writing."""

    @patch("gotsport_matches_runner._get_connection")
    @patch("gotsport_matches_runner.scrape_gotsport_matches")
    @patch("gotsport_matches_runner.insert_matches")
    def test_stamps_event_fk_id(self, mock_insert, mock_scrape, mock_conn):
        """Each match row should get event_fk_id = events.id (DB FK)."""
        # Mock DB returns one event.
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            (42, "Test Event", "99999", "2025-26", "Test League")
        ]
        mock_cur.description = [
            ("id",), ("name",), ("platform_event_id",), ("season",), ("league_name",),
        ]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        conn_obj = MagicMock()
        conn_obj.cursor.return_value = mock_cur
        mock_conn.return_value = conn_obj

        # Scraper returns 2 match rows.
        mock_scrape.return_value = [
            {"home_team_name": "A", "away_team_name": "B"},
            {"home_team_name": "C", "away_team_name": "D"},
        ]
        mock_insert.return_value = {"inserted": 2, "updated": 0, "skipped": 0}

        outcomes = run_gotsport_matches_batch(dry_run=False)

        # insert_matches was called with rows that have event_fk_id=42.
        call_args = mock_insert.call_args
        rows_passed = call_args[0][0]
        assert all(r["event_fk_id"] == 42 for r in rows_passed)

    @patch("gotsport_matches_runner._get_connection")
    def test_empty_events_returns_empty(self, mock_conn):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        mock_cur.description = [
            ("id",), ("name",), ("platform_event_id",), ("season",), ("league_name",),
        ]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        conn_obj = MagicMock()
        conn_obj.cursor.return_value = mock_cur
        mock_conn.return_value = conn_obj

        outcomes = run_gotsport_matches_batch()
        assert outcomes == []


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------

class TestDryRun:

    @patch("gotsport_matches_runner._get_connection")
    @patch("gotsport_matches_runner.scrape_gotsport_matches")
    @patch("gotsport_matches_runner.insert_matches")
    def test_dry_run_passes_flag_to_writer(self, mock_insert, mock_scrape, mock_conn):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = [
            (1, "Ev", "55555", "2025-26", None)
        ]
        mock_cur.description = [
            ("id",), ("name",), ("platform_event_id",), ("season",), ("league_name",),
        ]
        mock_cur.__enter__ = lambda s: s
        mock_cur.__exit__ = MagicMock(return_value=False)
        conn_obj = MagicMock()
        conn_obj.cursor.return_value = mock_cur
        mock_conn.return_value = conn_obj

        mock_scrape.return_value = [
            {"home_team_name": "X", "away_team_name": "Y"},
        ]
        mock_insert.return_value = {"inserted": 0, "updated": 0, "skipped": 0}

        run_gotsport_matches_batch(dry_run=True)

        mock_insert.assert_called_once()
        assert mock_insert.call_args[1]["dry_run"] is True


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

class TestRateLimit:

    @patch("gotsport_matches_runner.time.sleep")
    def test_rate_limit_sleeps_between_events(self, mock_sleep):
        _rate_limit(0, 3)
        mock_sleep.assert_called_once_with(2.0)

    @patch("gotsport_matches_runner.time.sleep")
    def test_rate_limit_skips_after_last(self, mock_sleep):
        _rate_limit(2, 3)
        mock_sleep.assert_not_called()
