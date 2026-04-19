"""
Fixture test for extractors.az_soccer.parse_html.

Loads a saved GotSport event clubs-list HTML snapshot and asserts the
pure parser returns AZ-stamped club records. Exercised by
--source replay-html in run.py.

Run:
    python -m pytest scraper/tests/test_az_soccer_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import az_soccer  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "az_soccer_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/44446/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    """parse_html returns ≥3 rows with the canonical club record shape."""
    records = az_soccer.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="AZ Advanced Leagues",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    # Field-shape check on the first record.
    rec = records[0]
    for key in ("club_name", "league_name", "city", "state", "source_url"):
        assert key in rec, f"missing {key}: {rec}"

    # AZ-specific invariants.
    assert all(r["state"] == "AZ" for r in records)
    assert all(r["league_name"] == "AZ Advanced Leagues" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)

    # ZZ-placeholder and empty rows must be filtered out.
    names = [r["club_name"] for r in records]
    assert not any(n.startswith("ZZ-") for n in names), names
    assert "" not in names


def test_parse_html_handles_empty_html():
    """Empty/minimal HTML returns an empty list — no crash."""
    records = az_soccer.parse_html(
        "<html><body></body></html>",
        source_url=SOURCE_URL,
        league_name="AZ Advanced Leagues",
    )
    assert records == []
