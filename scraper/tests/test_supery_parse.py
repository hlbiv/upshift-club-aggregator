"""
Fixture test for extractors.supery.parse_html.

The Super Y extractor parses the sylsoccer.com historical club list —
a nav-list of anchors whose href contains /page/show/ or /clubs/.

Run:
    python -m pytest scraper/tests/test_supery_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import supery  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "supery_sample.html"
SOURCE_URL = "https://www.sylsoccer.com/clubs-home"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_clubs():
    records = supery.parse_html(
        _load_fixture(),
        source_url=SOURCE_URL,
        league_name="Super Y League",
    )

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected ≥3 clubs, got {len(records)}"

    # Every record should have the normalizer-shaped keys.
    for r in records:
        for key in ("club_name", "league_name", "city", "state", "source_url"):
            assert key in r
        assert r["club_name"], "club_name must be non-empty"
        assert r["league_name"] == "Super Y League"
        assert r["source_url"] == SOURCE_URL

    names = {r["club_name"] for r in records}
    # Anchors from /page/show/ and /clubs/ are both picked up.
    assert "Arsenal FC" in names
    assert "FC Phoenix" in names
    # The non-matching /about and external links are filtered.
    assert "About" not in names
    assert "Ignored External Link" not in names
