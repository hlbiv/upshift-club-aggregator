"""
Tests for the NE Impact NPL extractor's pure-function parser.

The live scrape_ne_impact falls back to a curated seed list when the
GotSport event 404s; that fallback is orchestration-level. ``parse_html``
covers the pure parse path over a GotSport event-clubs page for replay.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ne_impact import parse_html  # noqa: E402


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ne_impact_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/21393/clubs"


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_parse_html_returns_at_least_three_clubs():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="NE Impact NPL")
    assert len(rows) >= 3


def test_parse_html_extracts_real_club_names():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="NE Impact NPL")
    names = {r["club_name"] for r in rows}
    assert "FC Stars of Massachusetts" in names
    assert "Seacoast United SC" in names
    assert "Boston Bolts" in names


def test_parse_html_filters_zz_placeholder_rows():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="NE Impact NPL")
    assert not any(r["club_name"].startswith("ZZ-") for r in rows)


def test_parse_html_stamps_source_url_and_league():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="NE Impact NPL")
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == "NE Impact NPL"


def test_parse_html_empty_returns_empty_list():
    assert parse_html("", source_url=SOURCE_URL, league_name="NE Impact NPL") == []
