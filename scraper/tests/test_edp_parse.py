"""
Tests for the EDP extractor's pure-function parser.

EDP orchestrates many GotSport events; ``parse_html`` delegates to
``parse_gotsport_event_html`` for single-page replay coverage.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.edp import parse_html  # noqa: E402


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "edp_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/47702/clubs"


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_parse_html_returns_at_least_three_clubs():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="EDP")
    assert len(rows) >= 3


def test_parse_html_extracts_real_club_names():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="EDP")
    names = {r["club_name"] for r in rows}
    assert "PDA Soccer" in names
    assert "Cedar Stars Academy" in names
    assert "Match Fit Academy" in names


def test_parse_html_filters_zz_placeholder_rows():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="EDP")
    assert not any(r["club_name"].startswith("ZZ-") for r in rows)


def test_parse_html_stamps_source_url_and_league():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="EDP")
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == "EDP"


def test_parse_html_empty_returns_empty_list():
    assert parse_html("", source_url=SOURCE_URL, league_name="EDP") == []
