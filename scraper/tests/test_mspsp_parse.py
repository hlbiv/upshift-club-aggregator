"""
Fixture test for extractors.mspsp.parse_html.

Run:
    python -m pytest scraper/tests/test_mspsp_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import mspsp  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "mspsp_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/50611/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    records = mspsp.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="MSPSP",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    # MSPSP is Michigan-only.
    assert all(r["state"] == "MI" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)

    names = {r["club_name"] for r in records}
    assert "Michigan Rush" in names
    assert not any(n.startswith("ZZ-") for n in names)
