"""
Tests for the TCSL extractor's pure-function parser.

TCSL orchestrates three GotSport events; ``parse_html`` delegates to
``parse_gotsport_event_html`` and stamps TX state. Used by replay flow.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.tcsl import parse_html  # noqa: E402


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "tcsl_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/50731/clubs"


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_parse_html_returns_at_least_three_clubs():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="TCSL")
    assert len(rows) >= 3


def test_parse_html_extracts_real_club_names():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="TCSL")
    names = {r["club_name"] for r in rows}
    assert "Dallas Texans" in names
    assert "FC Dallas Youth" in names
    assert "Solar Soccer Club" in names


def test_parse_html_filters_zz_placeholder_rows():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="TCSL")
    assert not any(r["club_name"].startswith("ZZ-") for r in rows)


def test_parse_html_stamps_tx_state():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="TCSL")
    assert rows
    for r in rows:
        assert r["state"] == "TX"


def test_parse_html_stamps_source_url_and_league():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="TCSL")
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == "TCSL"


def test_parse_html_empty_returns_empty_list():
    assert parse_html("", source_url=SOURCE_URL, league_name="TCSL") == []
