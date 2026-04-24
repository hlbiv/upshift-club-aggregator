"""Tests for the season-aware staleness skip guard."""
import pytest
from unittest.mock import MagicMock, patch


def _make_college(cid=1, name="Test U"):
    return {
        "id": cid, "name": name, "division": "D1",
        "gender_program": "womens",
        "soccer_program_url": "https://example.com",
        "website": None, "last_scraped_at": None,
    }


class TestFilterFreshColleges:
    def test_college_with_rows_and_recent_scrape_is_skipped(self):
        from extractors.ncaa_soccer_rosters import _filter_fresh_colleges

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchall.return_value = [(1,)]  # college_id=1 is fresh

        colleges = [_make_college(1), _make_college(2)]
        remaining, skipped = _filter_fresh_colleges(conn, colleges, "2025-26", 30)

        assert skipped == 1
        assert len(remaining) == 1
        assert remaining[0]["id"] == 2

    def test_college_with_rows_but_stale_scrape_is_not_skipped(self):
        from extractors.ncaa_soccer_rosters import _filter_fresh_colleges

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchall.return_value = []  # nothing fresh

        colleges = [_make_college(1)]
        remaining, skipped = _filter_fresh_colleges(conn, colleges, "2025-26", 30)

        assert skipped == 0
        assert len(remaining) == 1

    def test_empty_colleges_list_returns_empty(self):
        from extractors.ncaa_soccer_rosters import _filter_fresh_colleges

        conn = MagicMock()
        remaining, skipped = _filter_fresh_colleges(conn, [], "2025-26", 30)
        assert remaining == []
        assert skipped == 0
        conn.cursor.assert_not_called()

    def test_force_rescrape_bypasses_filter(self, monkeypatch):
        from extractors.ncaa_soccer_rosters import scrape_college_rosters

        filter_calls = []

        def fake_filter(conn, colleges, season, max_age_days):
            filter_calls.append(True)
            return (colleges, 0)

        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._filter_fresh_colleges", fake_filter
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._fetch_colleges", lambda conn, **kw: []
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._get_connection", lambda: MagicMock()
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters.ScrapeRunLogger", MagicMock()
        )

        scrape_college_rosters(dry_run=True, force_rescrape=True)

        assert filter_calls == []  # filter must NOT be called when force_rescrape=True

    def test_max_age_days_zero_bypasses_filter(self, monkeypatch):
        from extractors.ncaa_soccer_rosters import scrape_college_rosters

        filter_calls = []

        def fake_filter(conn, colleges, season, max_age_days):
            filter_calls.append(True)
            return (colleges, 0)

        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._filter_fresh_colleges", fake_filter
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._fetch_colleges", lambda conn, **kw: []
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._get_connection", lambda: MagicMock()
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters.ScrapeRunLogger", MagicMock()
        )

        scrape_college_rosters(dry_run=True, max_age_days=0)

        assert filter_calls == []  # max_age_days=0 treated same as force_rescrape=True

    def test_all_fresh_returns_empty_remaining(self):
        from extractors.ncaa_soccer_rosters import _filter_fresh_colleges

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchall.return_value = [(1,), (2,)]  # both fresh

        colleges = [_make_college(1), _make_college(2)]
        remaining, skipped = _filter_fresh_colleges(conn, colleges, "2025-26", 30)

        assert skipped == 2
        assert remaining == []

    def test_none_conn_returns_all_colleges(self):
        from extractors.ncaa_soccer_rosters import _filter_fresh_colleges

        colleges = [_make_college(1), _make_college(2)]
        remaining, skipped = _filter_fresh_colleges(None, colleges, "2025-26", 30)

        assert skipped == 0
        assert remaining == colleges

    def test_result_includes_skipped_fresh_key(self, monkeypatch):
        """scrape_college_rosters result dict includes skipped_fresh."""
        from extractors.ncaa_soccer_rosters import scrape_college_rosters

        def fake_filter(conn, colleges, season, max_age_days):
            # Simulate 3 colleges skipped
            return ([], 3)

        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._filter_fresh_colleges", fake_filter
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._fetch_colleges", lambda conn, **kw: [_make_college(i) for i in range(5)]
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters._get_connection", lambda: MagicMock()
        )
        monkeypatch.setattr(
            "extractors.ncaa_soccer_rosters.ScrapeRunLogger", MagicMock()
        )

        result = scrape_college_rosters(dry_run=True)
        assert "skipped_fresh" in result
        assert result["skipped_fresh"] == 3
