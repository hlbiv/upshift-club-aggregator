"""
Fixture test for extractors.central_states.parse_html.

Run:
    python -m pytest scraper/tests/test_central_states_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import central_states  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "central_states_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/46428/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    records = central_states.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="Central States NPL",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    rec = records[0]
    for key in ("club_name", "league_name", "city", "state", "source_url"):
        assert key in rec

    # Central States is multi-state — no hard-coded state.
    assert all(r["state"] == "" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)
    assert all(r["league_name"] == "Central States NPL" for r in records)

    names = {r["club_name"] for r in records}
    assert "Gateway Rush" in names
    assert not any(n.startswith("ZZ-") for n in names)
