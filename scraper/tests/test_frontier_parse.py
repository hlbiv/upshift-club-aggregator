"""
Fixture test for extractors.frontier.parse_html.

Run:
    python -m pytest scraper/tests/test_frontier_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import frontier  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "frontier_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/50988/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    records = frontier.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="Frontier Premier League",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    # Frontier spans MO/AR/OK/TX — state left empty.
    assert all(r["state"] == "" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)
    assert all(r["league_name"] == "Frontier Premier League" for r in records)

    for r in records:
        for key in ("club_name", "league_name", "city", "state", "source_url"):
            assert key in r
        assert r["club_name"], "club_name must be non-empty"
