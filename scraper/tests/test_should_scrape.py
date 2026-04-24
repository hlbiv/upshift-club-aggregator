"""
Tests for extractors.ncaa_soccer_rosters.should_scrape.

Covers all decision tree branches:
1. force_rescrape=True → always go
2. force_historical matches season → go
3. Current season freshness gate (skip if < skip_fresh_days)
4. Historical with ≥10 existing players → skip
5. Historical with unresolved url_needs_review flag → skip
6. Historical at max attempts → skip
7. Happy path → go
8. Pre-PR-24 UndefinedTable → degraded gracefully, proceed
"""

from __future__ import annotations

import datetime
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from psycopg2.errors import UndefinedTable as _UndefinedTable  # noqa: E402
except (ImportError, AttributeError):
    class _UndefinedTable(Exception):  # type: ignore[no-redef]
        pass

from extractors.ncaa_soccer_rosters import should_scrape, _MAX_HISTORICAL_ATTEMPTS, LIKELY_COVID_SEASONS  # noqa: E402

CURRENT = "2025-26"
HISTORICAL = "2024-25"

COLLEGE = {"id": 42, "name": "Test University"}


def _conn(fetchone_returns):
    """Build a mock connection whose cursor().fetchone() returns values in order."""
    cursor = mock.MagicMock()
    cursor.__enter__ = mock.Mock(return_value=cursor)
    cursor.__exit__ = mock.Mock(return_value=False)
    cursor.fetchone.side_effect = list(fetchone_returns)
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestForceFlags:
    def test_force_rescrape_always_go(self):
        conn = _conn([])
        go, reason = should_scrape(COLLEGE, CURRENT, CURRENT, conn=conn, force_rescrape=True)
        assert go is True
        assert "force_rescrape" in reason

    def test_force_historical_matching_season_go(self):
        conn = _conn([])
        go, reason = should_scrape(
            COLLEGE, HISTORICAL, CURRENT, conn=conn, force_historical=HISTORICAL
        )
        assert go is True
        assert "force_historical" in reason

    def test_force_historical_non_matching_season_proceeds_normally(self):
        # Non-matching force_historical should not override; DB returns 0 players → go
        conn = _conn([(0,), None, None])
        go, _ = should_scrape(
            COLLEGE, HISTORICAL, CURRENT, conn=conn, force_historical="2022-23"
        )
        assert go is True


class TestCovidShortCircuit:
    COVID_SEASON = "2020-21"

    def test_covid_season_is_skipped(self):
        """2020-21 season should be skipped without force_rescrape."""
        conn = _conn([])
        go, reason = should_scrape(
            COLLEGE, self.COVID_SEASON, CURRENT, conn=conn
        )
        assert go is False
        assert reason == "likely_covid_cancelled"

    def test_covid_season_force_rescrape_overrides(self):
        """force_rescrape=True should bypass the COVID skip entirely."""
        # With force_rescrape the function returns before reaching the COVID guard;
        # just confirm the early-return fires and reason is force_rescrape.
        conn = _conn([])
        go, reason = should_scrape(
            COLLEGE, self.COVID_SEASON, CURRENT, conn=conn, force_rescrape=True
        )
        assert go is True
        assert "force_rescrape" in reason

    def test_covid_season_force_historical_matching_overrides(self):
        """force_historical matching the COVID season should bypass the COVID skip."""
        conn = _conn([])
        go, reason = should_scrape(
            COLLEGE, self.COVID_SEASON, CURRENT, conn=conn, force_historical=self.COVID_SEASON
        )
        assert go is True
        assert "force_historical" in reason

    def test_covid_season_force_historical_non_matching_still_skips(self):
        """force_historical for a different season does not override the COVID skip."""
        conn = _conn([])
        go, reason = should_scrape(
            COLLEGE, self.COVID_SEASON, CURRENT, conn=conn, force_historical="2022-23"
        )
        assert go is False
        assert reason == "likely_covid_cancelled"

    def test_likely_covid_seasons_constant_contains_2020_21(self):
        """Sanity-check the constant itself."""
        assert "2020-21" in LIKELY_COVID_SEASONS


class TestCurrentSeasonFreshness:
    def _make_ts(self, days_ago: float):
        dt = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_ago)
        return dt

    def test_skip_if_scraped_within_window(self):
        conn = _conn([(self._make_ts(5),)])
        go, reason = should_scrape(
            COLLEGE, CURRENT, CURRENT, conn=conn, skip_fresh_days=30
        )
        assert go is False
        assert "fresh" in reason

    def test_go_if_scraped_outside_window(self):
        conn = _conn([(self._make_ts(35),)])
        go, _ = should_scrape(
            COLLEGE, CURRENT, CURRENT, conn=conn, skip_fresh_days=30
        )
        assert go is True

    def test_go_if_never_scraped(self):
        conn = _conn([(None,)])
        go, _ = should_scrape(COLLEGE, CURRENT, CURRENT, conn=conn)
        assert go is True

    def test_no_college_row_go(self):
        conn = _conn([None])
        go, _ = should_scrape(COLLEGE, CURRENT, CURRENT, conn=conn)
        assert go is True


class TestHistoricalPlayerCount:
    def test_skip_if_ten_or_more_players(self):
        conn = _conn([(10,), None, None])
        go, reason = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is False
        assert "historical_has_data" in reason

    def test_go_if_fewer_than_ten_players(self):
        # 9 players, no flags
        conn = _conn([(9,), None, None])
        go, _ = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is True


class TestHistoricalFlagChecks:
    def test_skip_if_unresolved_url_needs_review(self):
        # 0 players, url_needs_review flag present
        flag_meta = {"reason": "static_404"}
        conn = _conn([(0,), (flag_meta,), None])
        go, reason = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is False
        assert "url_needs_review" in reason

    def test_skip_if_max_attempts_reached(self):
        # 0 players, no url_needs_review, historical_no_data at max attempts
        conn = _conn([
            (0,),
            None,  # url_needs_review flag → None
            ({"attempts": _MAX_HISTORICAL_ATTEMPTS},),
        ])
        go, reason = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is False
        assert "max_attempts" in reason

    def test_go_if_attempts_below_max(self):
        conn = _conn([
            (0,),
            None,
            ({"attempts": _MAX_HISTORICAL_ATTEMPTS - 1},),
        ])
        go, _ = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is True

    def test_go_if_no_flags_at_all(self):
        conn = _conn([(0,), None, None])
        go, _ = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is True


class TestDegradedFlagTable:
    def test_undefined_table_proceeds(self):
        cursor = mock.MagicMock()
        cursor.__enter__ = mock.Mock(return_value=cursor)
        cursor.__exit__ = mock.Mock(return_value=False)
        # First call: player count; second call: flag table raises UndefinedTable
        cursor.fetchone.side_effect = [
            (0,),
            _UndefinedTable("relation does not exist"),
        ]
        cursor.execute.side_effect = [None, _UndefinedTable("x")]
        conn = mock.MagicMock()
        conn.cursor.return_value = cursor

        go, _ = should_scrape(COLLEGE, HISTORICAL, CURRENT, conn=conn)
        assert go is True
        conn.rollback.assert_called()
