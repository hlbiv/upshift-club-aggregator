"""
Tests for the EDP extractor's pure-function parser.

EDP orchestrates many GotSport events; ``parse_html`` delegates to
``parse_gotsport_event_html`` for single-page replay coverage.

Uses the shared :func:`_fixture_helpers.parse_fixture` helper. See
``scraper/tests/README.md`` for the pattern.
"""

from __future__ import annotations

from _fixture_helpers import parse_fixture

FIXTURE = "edp_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/47702/clubs"
LEAGUE = "EDP"


def _rows() -> list[dict]:
    return parse_fixture("edp", FIXTURE, source_url=SOURCE_URL, league_name=LEAGUE)


def test_parse_html_returns_at_least_three_clubs():
    assert len(_rows()) >= 3


def test_parse_html_extracts_real_club_names():
    names = {r["club_name"] for r in _rows()}
    assert "PDA Soccer" in names
    assert "Cedar Stars Academy" in names
    assert "Match Fit Academy" in names


def test_parse_html_filters_zz_placeholder_rows():
    assert not any(r["club_name"].startswith("ZZ-") for r in _rows())


def test_parse_html_stamps_source_url_and_league():
    rows = _rows()
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == LEAGUE


def test_parse_html_empty_returns_empty_list():
    from extractors.edp import parse_html
    assert parse_html("", source_url=SOURCE_URL, league_name=LEAGUE) == []
