"""
Tests for the SOCAL extractor's pure-function parser.

SOCAL has a single GotSport event; ``parse_html`` delegates to
``parse_gotsport_event_html`` and stamps CA state. Used by replay flow.

This test is one of the reference implementations for the shared
:func:`_fixture_helpers.parse_fixture` helper. See ``scraper/tests/README.md``
for the recommended pattern when adding a new extractor fixture test.
"""

from __future__ import annotations

from _fixture_helpers import parse_fixture

FIXTURE = "socal_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/43086/clubs"
LEAGUE = "SOCAL"


def _rows() -> list[dict]:
    return parse_fixture("socal", FIXTURE, source_url=SOURCE_URL, league_name=LEAGUE)


def test_parse_html_returns_at_least_three_clubs():
    assert len(_rows()) >= 3


def test_parse_html_extracts_real_club_names():
    names = {r["club_name"] for r in _rows()}
    assert "Beach FC" in names
    assert "Pateadores Soccer Club" in names
    assert "LA Galaxy Academy" in names


def test_parse_html_filters_zz_placeholder_rows():
    assert not any(r["club_name"].startswith("ZZ-") for r in _rows())


def test_parse_html_stamps_ca_state():
    rows = _rows()
    assert rows
    for r in rows:
        assert r["state"] == "CA"


def test_parse_html_stamps_source_url_and_league():
    rows = _rows()
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == LEAGUE


def test_parse_html_empty_returns_empty_list():
    # Empty HTML still goes through the module-level parse_html directly so
    # we don't need the fixture loader. Import is kept local to avoid leaking
    # the symbol to consumers of _fixture_helpers.
    from extractors.socal import parse_html
    assert parse_html("", source_url=SOURCE_URL, league_name=LEAGUE) == []
