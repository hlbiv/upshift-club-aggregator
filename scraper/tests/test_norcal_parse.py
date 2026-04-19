"""
Tests for the NorCal Premier extractor's pure-function parser.

Covers the ``--source replay-html`` contract: the extractor module must
expose ``parse_html(html, source_url=..., league_name=...)`` so the
replay handler can parse archived raw HTML without re-fetching.

Run:
    python -m pytest scraper/tests/test_norcal_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.norcal import parse_html, CLUBS_URL  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "norcal_clubs_sample.html"


def _load() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    """Fixture parses into a healthy set of club records."""
    html = _load()

    records = parse_html(
        html,
        source_url="https://norcalpremier.com/clubs/",
        league_name="NorCal Premier",
    )

    # NorCal has ~50+ member clubs; fixture is a live snapshot.
    assert len(records) >= 10, (
        f"expected ≥10 clubs from NorCal fixture, got {len(records)}"
    )

    first = records[0]
    assert set(first.keys()) >= {
        "club_name",
        "league_name",
        "city",
        "state",
        "source_url",
        "region",
    }, f"record shape missing required keys: {first.keys()}"

    # Every record should carry the stamped metadata.
    for rec in records:
        assert rec["club_name"], "club_name must be non-empty"
        assert rec["state"] == "CA", "NorCal is California-only"
        assert rec["league_name"] == "NorCal Premier"
        assert rec["source_url"] == "https://norcalpremier.com/clubs/"


def test_parse_html_defaults_source_url_to_clubs_url():
    """When source_url is None the parser falls back to the canonical URL."""
    html = _load()

    records = parse_html(html, source_url=None, league_name=None)

    assert records, "expected at least one record"
    assert all(r["source_url"] == CLUBS_URL for r in records), (
        "parse_html should default source_url to CLUBS_URL when not supplied"
    )
    assert all(r["league_name"] is None for r in records), (
        "league_name=None should flow through to records"
    )


def test_parse_html_empty_document():
    """Empty / non-matching HTML returns [] rather than raising."""
    assert parse_html("", source_url=None, league_name=None) == []
    assert parse_html(
        "<html><body><p>no tables</p></body></html>",
        source_url="https://norcalpremier.com/clubs/",
        league_name="NorCal Premier",
    ) == []
