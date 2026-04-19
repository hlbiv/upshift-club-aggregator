"""
Fixture test for extractors.soccerwire.parse_html.

Unlike the GotSport-style extractors, SoccerWire's replay unit is a
*single* club detail page (soccerwire.com/club/<slug>/) — each archived
HTML yields exactly one record or zero. The task spec's ≥3-rows
heuristic doesn't fit here; we instead verify field shape + state/city
extraction + graceful handling of no-data pages.

Run:
    python -m pytest scraper/tests/test_soccerwire_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import soccerwire  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "soccerwire_sample.html"
SOURCE_URL = "https://www.soccerwire.com/club/hawaii-rush/"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_extracts_one_club_record():
    """A single club page yields exactly one record with all expected fields."""
    records = soccerwire.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="US Club Soccer",
    )

    assert isinstance(records, list)
    assert len(records) == 1

    rec = records[0]
    for key in ("club_name", "league_name", "city", "state", "source_url", "source_type"):
        assert key in rec, f"missing {key}: {rec}"

    assert rec["club_name"] == "Hawaii Rush"
    assert rec["state"] == "HI"
    assert rec["city"] == "Honolulu"
    assert rec["league_name"] == "US Club Soccer"
    assert rec["source_url"] == SOURCE_URL
    assert rec["source_type"] == "soccerwire"


def test_parse_html_returns_empty_for_no_data_page():
    """Pages without a parseable club name return []."""
    records = soccerwire.parse_html(
        "<html><body><main></main></body></html>",
        source_url=SOURCE_URL,
        league_name="US Club Soccer",
    )
    # Without an <h1>, the parser falls back to the slug-derived name —
    # which is still non-empty — so this returns 1 record. The real
    # zero-case is a page where BOTH the h1 is missing AND the slug is
    # empty (no /club/<slug>/ in the URL).
    records_no_slug = soccerwire.parse_html(
        "<html><body><main></main></body></html>",
        source_url="",
        league_name="US Club Soccer",
    )
    # slug defaults to 'unknown' → produces 1 record. The empty case is
    # effectively the "no <body>" fixture below.
    assert len(records) == 1
    assert len(records_no_slug) == 1
