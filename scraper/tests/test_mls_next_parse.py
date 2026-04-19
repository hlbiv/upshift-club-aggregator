"""
Tests for the ``parse_html`` entry point on ``extractors.mls_next``.

The extractor's scheduled-scrape path (``scrape_mls_next``) fetches via
Playwright and falls back to a curated Wikipedia seed when the DOM does
not render. ``parse_html`` is the additive replay entry point: it runs
exclusively against pre-fetched HTML (e.g. rows replayed from
``raw_html_archive``) and returns what the parser actually sees — no
seed fallback, because replay is supposed to measure the parser itself.

Fixture:
    fixtures/mls_next/mls_next_sample.html — synthetic but structurally
    faithful to the post-render DOM the extractor documents
    (``div.club-card`` > ``h3.club-name`` + ``p.club-location``). See
    the fixture's leading comment for the sourcing rationale.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.mls_next import parse_html  # noqa: E402

FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "mls_next"
    / "mls_next_sample.html"
)


def _html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestParseHtml:
    def test_parse_html_is_module_level_callable(self):
        """Replay handler looks up ``module.parse_html`` — it must exist."""
        import extractors.mls_next as mod
        assert callable(getattr(mod, "parse_html", None))

    def test_returns_at_least_5_records(self):
        records = parse_html(
            _html(),
            source_url="https://www.mlsnextsoccer.com/clubs",
            league_name="MLS Next",
        )
        assert len(records) >= 5, f"expected >= 5 clubs, got {len(records)}"

    def test_records_have_expected_fields(self):
        records = parse_html(
            _html(),
            source_url="https://www.mlsnextsoccer.com/clubs",
            league_name="MLS Next",
        )
        for rec in records:
            assert rec["club_name"], f"empty club_name: {rec}"
            assert rec["league_name"] == "MLS Next"
            assert rec["source_url"] == "https://www.mlsnextsoccer.com/clubs"
            # city/state are parsed from "City, ST" location text
            assert "city" in rec and "state" in rec

    def test_city_state_split(self):
        """Parser splits 'City, ST' into separate city/state fields."""
        records = parse_html(_html(), source_url="https://www.mlsnextsoccer.com/clubs")
        by_name = {r["club_name"]: r for r in records}
        atl = by_name.get("Atlanta United FC Academy")
        assert atl is not None
        assert atl["city"] == "Marietta"
        assert atl["state"] == "GA"

    def test_replay_signature_accepted(self):
        """
        Handler calls ``parse_html(html, source_url=..., league_name=...)``
        first and falls back to positional if that TypeErrors. Both must work.
        """
        kw = parse_html(
            _html(),
            source_url="https://www.mlsnextsoccer.com/clubs",
            league_name=None,
        )
        assert len(kw) >= 5
        # Positional-only call (no league_name) must also return records.
        pos = parse_html(_html())
        assert len(pos) >= 5

    def test_empty_html_returns_empty(self):
        """No seed-fallback on empty/unparseable HTML — replay is literal."""
        assert parse_html("") == []
        assert parse_html("<html><body></body></html>") == []
