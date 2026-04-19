"""
Tests for the SSSL extractor's pure-function parser.

SSSL orchestrates two GotSport events; ``parse_html`` delegates to
``parse_gotsport_event_html`` and stamps FL state. Used by replay flow.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.sssl import parse_html  # noqa: E402


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sssl_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/32708/clubs"


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_parse_html_returns_at_least_three_clubs():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="SSSL")
    assert len(rows) >= 3


def test_parse_html_extracts_real_club_names():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="SSSL")
    names = {r["club_name"] for r in rows}
    assert "Florida Rush SC" in names
    assert "Weston FC" in names
    assert "IMG Academy SC" in names


def test_parse_html_filters_zz_placeholder_rows():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="SSSL")
    assert not any(r["club_name"].startswith("ZZ-") for r in rows)


def test_parse_html_stamps_fl_state():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="SSSL")
    assert rows
    for r in rows:
        assert r["state"] == "FL"


def test_parse_html_stamps_source_url_and_league():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="SSSL")
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == "SSSL"


def test_parse_html_empty_returns_empty_list():
    assert parse_html("", source_url=SOURCE_URL, league_name="SSSL") == []
