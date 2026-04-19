"""
Tests for the NPL sub-leagues extractor module's shared pure-function parser.

All extractors in ``extractors/npl_extra.py`` (GLA, NISL, FCL, Red River,
Minnesota, SAPL, PNW, Ohio Valley, Desert, NYCSL, Southeast, Great Lakes,
Keystone, Gulf Coast, WA Premier, Florida Premier, Empire, MASA) orchestrate
one or more GotSport events via ``_multi_event_scrape``. ``parse_html`` is
a single shared pure-function entry used by the replay handler.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.npl_extra import parse_html  # noqa: E402


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "npl_extra_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/43157/clubs"


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


def test_parse_html_returns_at_least_three_clubs():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="GLA NPL")
    assert len(rows) >= 3


def test_parse_html_extracts_real_club_names():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="GLA NPL")
    names = {r["club_name"] for r in rows}
    assert "Cleveland Force SC" in names
    assert "Ohio Premier Soccer Club" in names
    assert "Internationals SC" in names


def test_parse_html_filters_zz_placeholder_rows():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="GLA NPL")
    assert not any(r["club_name"].startswith("ZZ-") for r in rows)


def test_parse_html_stamps_source_url_and_league():
    rows = parse_html(_read_fixture(), source_url=SOURCE_URL, league_name="GLA NPL")
    assert rows
    for r in rows:
        assert r["source_url"] == SOURCE_URL
        assert r["league_name"] == "GLA NPL"


def test_parse_html_empty_returns_empty_list():
    assert parse_html("", source_url=SOURCE_URL, league_name="GLA NPL") == []
