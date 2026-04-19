"""
Tests for the ``parse_html`` entry point on ``extractors.nwsl_academy``.

NWSL Academy has no machine-readable public directory — the extractor
emits a curated seed list (``NWSL_ACADEMY_CLUBS``) regardless of the
input HTML. ``parse_html`` preserves that behaviour so the replay
handler reproduces the scheduled scrape's output when fed an archived
HTML body. See the module docstring at
``scraper/extractors/nwsl_academy.py`` for the rationale.

Fixture:
    fixtures/nwsl/nwsl_academy_sample.html — placeholder Next.js shell
    (the real URL 404s for server-side fetches). The file's content is
    not actually parsed; it exists only so tests assert the seed output
    is stable under an archived HTML body.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.nwsl_academy import NWSL_ACADEMY_CLUBS, parse_html  # noqa: E402

FIXTURE = (
    Path(__file__).parent
    / "fixtures"
    / "nwsl"
    / "nwsl_academy_sample.html"
)


def _html() -> str:
    return FIXTURE.read_text(encoding="utf-8")


class TestParseHtml:
    def test_parse_html_is_module_level_callable(self):
        """Replay handler looks up ``module.parse_html`` — it must exist."""
        import extractors.nwsl_academy as mod
        assert callable(getattr(mod, "parse_html", None))

    def test_returns_all_seed_records(self):
        """Should return exactly one record per curated seed entry."""
        records = parse_html(
            _html(),
            source_url="https://www.nwslsoccer.com/nwsl-academy",
            league_name="NWSL Academy",
        )
        assert len(records) == len(NWSL_ACADEMY_CLUBS)
        assert len(records) >= 3

    def test_records_have_expected_fields(self):
        records = parse_html(
            _html(),
            source_url="https://www.nwslsoccer.com/nwsl-academy",
            league_name="NWSL Academy",
        )
        for rec in records:
            assert rec["club_name"], f"empty club_name: {rec}"
            assert rec["league_name"] == "NWSL Academy"
            assert rec["source_url"] == "https://www.nwslsoccer.com/nwsl-academy"
            assert "city" in rec and "state" in rec

    def test_seed_contents_preserved(self):
        """Every curated seed entry appears in the parsed output."""
        records = parse_html(_html())
        by_name = {r["club_name"]: r for r in records}
        for club_name, city, state in NWSL_ACADEMY_CLUBS:
            rec = by_name.get(club_name)
            assert rec is not None, f"missing curated club: {club_name}"
            assert rec["city"] == city
            assert rec["state"] == state

    def test_replay_signature_accepted(self):
        """
        Replay handler calls ``parse_html(html, source_url=..., league_name=...)``
        first and falls back to positional on TypeError. Both paths must work.
        """
        kw = parse_html(
            _html(),
            source_url="https://www.nwslsoccer.com/nwsl-academy",
            league_name=None,
        )
        assert len(kw) == len(NWSL_ACADEMY_CLUBS)
        pos = parse_html(_html())
        assert len(pos) == len(NWSL_ACADEMY_CLUBS)

    def test_empty_html_still_returns_seed(self):
        """
        NWSL Academy's parser intentionally ignores HTML (the live page has
        no directory), so even an empty HTML body returns the curated seed.
        This is the documented behaviour — see module docstring.
        """
        assert len(parse_html("")) == len(NWSL_ACADEMY_CLUBS)
