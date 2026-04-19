"""
Tests for the Heartland Soccer Association extractor's pure-function parser.

Heartland's seedings CGI endpoint returns an HTML table per level/gender/age
combination; extraction is a pure function over the fixture HTML so replay
(PR #80) can re-run it without touching the network.

Fixture: scraper/tests/fixtures/heartland_seedings_sample.html is a real
seedings.cgi snapshot for U-14 Boys Premier. All 7 Heartland member club
abbreviations appear on that page (Kansas Rush, KPSL, NEU, OPSC, SBV,
KC Fusion, SPLS), plus "Non Member" guest rows which must be filtered.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.heartland import _ABBR_MAP, parse_html  # noqa: E402


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "heartland_seedings_sample.html"
)

_EXPECTED_CANONICAL_NAMES = {full for full, _city, _state in _ABBR_MAP.values()}


def _read_fixture() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Core acceptance test — all 7 Heartland member clubs come through
# ---------------------------------------------------------------------------


def test_parse_fixture_returns_all_seven_member_clubs():
    html = _read_fixture()
    rows = parse_html(
        html,
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    )

    # All 7 Heartland member clubs, mapped to their canonical full names.
    got_names = {r["club_name"] for r in rows}
    assert got_names == _EXPECTED_CANONICAL_NAMES, (
        f"expected the 7 Heartland members {_EXPECTED_CANONICAL_NAMES}, "
        f"got {got_names}"
    )
    assert len(rows) == 7, f"expected 7 rows (one per abbr), got {len(rows)}"


def test_parse_fixture_filters_non_member_guest_rows():
    """The 'Non Member' column value is guest teams and must not surface."""
    html = _read_fixture()
    rows = parse_html(
        html,
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    )
    names = {r["club_name"] for r in rows}
    assert "Non Member" not in names


def test_parse_fixture_record_shape():
    """Each emitted record has the canonical club-record fields."""
    html = _read_fixture()
    rows = parse_html(
        html,
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    )
    for r in rows:
        assert set(r.keys()) == {
            "club_name", "league_name", "city", "state", "source_url"
        }, f"unexpected record shape: {r}"
        assert r["league_name"] == "Heartland Soccer Association"
        assert r["source_url"] == (
            "https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi"
        )
        assert r["state"] == "KS"
        assert r["club_name"], "club_name must not be empty"


def test_parse_fixture_spot_check_abbr_mapping():
    """Known abbreviation must land on its full canonical name + city."""
    html = _read_fixture()
    rows = parse_html(
        html,
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    )
    by_name = {r["club_name"]: r for r in rows}

    kpsl = by_name.get("Kansas Premier Soccer League")
    assert kpsl is not None, f"KPSL missing from parsed rows: {list(by_name)}"
    assert kpsl["city"] == "Overland Park"
    assert kpsl["state"] == "KS"

    opsc = by_name.get("Overland Park Soccer Club")
    assert opsc is not None
    assert opsc["city"] == "Overland Park"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_parse_empty_html_returns_empty_list():
    assert parse_html(
        "",
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    ) == []


def test_parse_whitespace_html_returns_empty_list():
    assert parse_html(
        "   \n\t  ",
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    ) == []


def test_parse_html_with_no_textsm_rows_returns_empty_list():
    assert parse_html(
        "<html><body><p>nothing here</p></body></html>",
        url="https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi",
        league_name="Heartland Soccer Association",
    ) == []


def test_parse_html_honours_source_url_argument():
    """The `url` arg is surfaced verbatim on every record (not the const)."""
    html = _read_fixture()
    custom_url = (
        "https://heartlandsoccer.net/reports/cgi-jrb/seedings.cgi"
        "?level1=Premier&b_g1=Boys&age1=U-14"
    )
    rows = parse_html(
        html,
        url=custom_url,
        league_name="Heartland Soccer Association",
    )
    assert rows, "fixture should yield at least one row"
    for r in rows:
        assert r["source_url"] == custom_url
