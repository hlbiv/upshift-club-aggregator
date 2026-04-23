"""Tests for the 2020-21 COVID season short-circuit."""
import pytest
from unittest.mock import patch, MagicMock


def test_covid_season_skipped_no_http(monkeypatch):
    """2020-21 season must not trigger any HTTP or Playwright calls."""
    from extractors.ncaa_soccer_rosters import scrape_college_rosters

    http_calls = []

    def fake_discover(session, college, gender):
        return "https://example.com/roster"

    def fake_fetch(session, url):
        http_calls.append(url)
        return "<html>fake</html>"

    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters.discover_roster_url", fake_discover
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters.fetch_with_retry", fake_fetch
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._fetch_and_parse_with_fallback",
        lambda sess, url: ("<html>fake</html>", []),
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._find_historical_roster",
        lambda sess, url, season: (None, None, []),
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._fetch_colleges",
        lambda conn, **kw: [{"id": 1, "name": "Test U", "division": "D1",
                              "gender_program": "womens",
                              "soccer_program_url": "https://example.com",
                              "website": None, "last_scraped_at": None}],
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._get_connection", lambda: MagicMock()
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters.ScrapeRunLogger", MagicMock()
    )

    result = scrape_college_rosters(
        division="D1", gender="womens",
        dry_run=True, backfill_seasons=5,
        force_covid=False,
    )

    # 2020-21 should be in the backfill range and should be skipped
    assert result.get("covid_skipped", 0) >= 1
    # No HTTP calls for the 2020-21 season
    assert not any("2020" in u for u in http_calls)


def test_force_covid_bypasses_skip(monkeypatch):
    """--force-covid=True should attempt to scrape 2020-21."""
    from extractors.ncaa_soccer_rosters import scrape_college_rosters

    historical_calls = []

    def fake_find_historical(sess, url, season):
        historical_calls.append(season)
        return (None, None, [])

    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters.discover_roster_url",
        lambda sess, college, gender: "https://example.com/roster",
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._fetch_and_parse_with_fallback",
        lambda sess, url: ("<html>x</html>", []),
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._find_historical_roster",
        fake_find_historical,
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._fetch_colleges",
        lambda conn, **kw: [{"id": 1, "name": "Test U", "division": "D1",
                              "gender_program": "womens",
                              "soccer_program_url": "https://example.com",
                              "website": None, "last_scraped_at": None}],
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters._get_connection", lambda: MagicMock()
    )
    monkeypatch.setattr(
        "extractors.ncaa_soccer_rosters.ScrapeRunLogger", MagicMock()
    )

    scrape_college_rosters(
        division="D1", gender="womens",
        dry_run=True, backfill_seasons=5,
        force_covid=True,
    )

    assert "2020-21" in historical_calls
