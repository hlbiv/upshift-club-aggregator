"""
Tests for the ``parse_html`` entry point on ``extractors.usl_academy``.

USL Academy's directory is served by Modular11's standings API (see
module docstring at ``scraper/extractors/usl_academy.py``). Each HTTP
response contains one gender division's standings; the scheduled
scraper fetches both genders and dedupes across them. ``parse_html``
operates on exactly one archived response at a time — replay runs it
per-row and the aggregation layer (if any) handles cross-response
dedup.

Fixture:
    fixtures/usl/usl_academy_men_sample.html — archived Modular11
    standings HTML for the men's division (UID_gender=1). Contains
    real club rows + division-heading rows; the parser must skip the
    headings.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.usl_academy import parse_html  # noqa: E402

FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "usl"
    / "usl_academy_men_sample.html"
)


def _html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestParseHtml:
    def test_parse_html_is_module_level_callable(self):
        """Replay handler looks up ``module.parse_html`` — it must exist."""
        import extractors.usl_academy as mod
        assert callable(getattr(mod, "parse_html", None))

    def test_returns_at_least_30_clubs(self):
        """Men's division fixture has the full league — should be well over 30."""
        records = parse_html(
            _html(),
            source_url="https://www.usl-academy.com/league-standings",
            league_name="USL Academy League",
        )
        assert len(records) >= 30, f"expected >= 30 clubs, got {len(records)}"

    def test_records_have_expected_fields(self):
        records = parse_html(
            _html(),
            source_url="https://www.usl-academy.com/league-standings",
            league_name="USL Academy League",
        )
        for rec in records:
            assert rec["club_name"], f"empty club_name: {rec}"
            assert rec["league_name"] == "USL Academy League"
            assert rec["source_url"] == "https://www.usl-academy.com/league-standings"
            assert "city" in rec and "state" in rec

    def test_skips_division_headings(self):
        """
        Division-heading `data-title` values contain 'Male'/'Female'/
        'Men'/'Women' and must be filtered out — they are not club names.
        """
        records = parse_html(_html())
        names = {r["club_name"] for r in records}
        for bad_word in ("Male", "Female", "Men", "Women"):
            for name in names:
                assert bad_word not in name.split(), (
                    f"division heading leaked through: {name}"
                )

    def test_deduplicates_within_single_response(self):
        """Club names appearing in multiple divisions collapse to one record."""
        records = parse_html(_html())
        names = [r["club_name"] for r in records]
        assert len(names) == len(set(names)), (
            f"duplicate club names in single-response output: {names}"
        )

    def test_replay_signature_accepted(self):
        """
        Replay handler calls ``parse_html(html, source_url=..., league_name=...)``
        first and falls back to positional on TypeError. Both paths must work.
        """
        kw = parse_html(
            _html(),
            source_url="https://www.usl-academy.com/league-standings",
            league_name=None,
        )
        assert len(kw) >= 30
        pos = parse_html(_html())
        assert len(pos) >= 30

    def test_empty_html_returns_empty(self):
        """No seed-fallback on empty/unparseable HTML — replay is literal."""
        assert parse_html("") == []
        # Very short HTML (< 500 bytes) is treated as a truncated response.
        assert parse_html("<html></html>") == []
