"""
Fixture test for extractors.mountain_west.parse_html.

Run:
    python -m pytest scraper/tests/test_mountain_west_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import mountain_west  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "mountain_west_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/44839/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    records = mountain_west.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="Mountain West NPL",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    # Multi-state event — state intentionally empty.
    assert all(r["state"] == "" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)

    names = {r["club_name"] for r in records}
    assert "La Roca FC" in names
