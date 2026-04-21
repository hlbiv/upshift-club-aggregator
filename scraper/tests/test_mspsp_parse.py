"""
Fixture test for extractors.mspsp.parse_html.

Uses the shared :func:`_fixture_helpers.parse_fixture` helper. See
``scraper/tests/README.md`` for the recommended pattern.

Run:
    python -m pytest scraper/tests/test_mspsp_parse.py -v
"""

from __future__ import annotations

from _fixture_helpers import parse_fixture

FIXTURE = "mspsp_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/50611/clubs"
LEAGUE = "MSPSP"


def test_parse_html_returns_clubs():
    records = parse_fixture("mspsp", FIXTURE, source_url=SOURCE_URL, league_name=LEAGUE)

    assert isinstance(records, list)
    assert len(records) >= 3, f"expected >=3 clubs, got {len(records)}"

    # MSPSP is Michigan-only.
    assert all(r["state"] == "MI" for r in records)
    assert all(r["source_url"] == SOURCE_URL for r in records)

    names = {r["club_name"] for r in records}
    assert "Michigan Rush" in names
    assert not any(n.startswith("ZZ-") for n in names)
