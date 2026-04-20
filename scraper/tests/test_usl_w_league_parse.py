"""
Tests for the ``parse_html`` entry point and internal parsers on
``extractors.usl_w_league``.

The USL W League directory lives behind two complementary sources:

  1. ``https://uslwleague.com/league-teams`` — a ~132 KB server-rendered
     HTML page where every team is a ``<span class="teamname">`` inside a
     ``<a href="https://www.uslwleague.com/<slug>">`` anchor, grouped
     under ``<h2 class="division">…</h2>`` headings.
  2. The Modular11 iframe at
     ``https://www.modular11.com/league-schedule/w-league`` which ships a
     ``<select name="team">`` containing every team as an ``<option>``.

Fixtures:
    fixtures/usl/usl_w_league_teams_sample.html — trimmed copy of the
        SportNgin ``/league-teams`` HTML covering 2 divisions x 4-5 teams,
        including division headings to verify the parser ignores them.
    fixtures/usl/usl_w_league_modular11_sample.html — trimmed copy of
        the Modular11 iframe HTML with the team dropdown + inline
        scheduleConfig JS block (for the UID_event / tournament-id
        drift check).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.usl_w_league import (  # noqa: E402
    _extract_modular11_tournament_id,
    _parse_league_teams_html,
    _parse_modular11_html,
    parse_html,
)

FIXTURES = Path(__file__).parent / "fixtures" / "usl"
LEAGUE_TEAMS_FIXTURE = FIXTURES / "usl_w_league_teams_sample.html"
MODULAR11_FIXTURE = FIXTURES / "usl_w_league_modular11_sample.html"


def _league_teams_html() -> str:
    return LEAGUE_TEAMS_FIXTURE.read_text(encoding="utf-8")


def _modular11_html() -> str:
    return MODULAR11_FIXTURE.read_text(encoding="utf-8")


class TestParseHtml:
    def test_parse_html_is_module_level_callable(self):
        """Replay handler looks up ``module.parse_html`` — it must exist."""
        import extractors.usl_w_league as mod
        assert callable(getattr(mod, "parse_html", None))

    def test_league_teams_fixture_has_expected_team_count(self):
        """
        Fixture covers 2 divisions x 4-5 teams = 9 team rows. The parser
        must return all of them as canonical club records.
        """
        records = parse_html(
            _league_teams_html(),
            source_url="https://www.uslwleague.com/league-teams",
            league_name="USL W League",
        )
        assert len(records) == 9, (
            f"expected 9 clubs from league-teams fixture, got {len(records)}: "
            f"{[r['club_name'] for r in records]}"
        )

    def test_league_teams_records_have_expected_fields(self):
        records = parse_html(
            _league_teams_html(),
            source_url="https://www.uslwleague.com/league-teams",
            league_name="USL W League",
        )
        for rec in records:
            assert rec["club_name"], f"empty club_name: {rec}"
            assert rec["league_name"] == "USL W League"
            assert rec["source_url"] == "https://www.uslwleague.com/league-teams"
            assert "city" in rec and "state" in rec

    def test_skips_division_headings(self):
        """
        ``<h2 class="division">…DIVISION</h2>`` headings must be excluded.
        They live in <h2> not <span class="teamname">, but the defensive
        _DIVISION_RE filter should still guarantee "DIVISION" never leaks
        through.
        """
        records = parse_html(_league_teams_html())
        names = {r["club_name"] for r in records}
        for n in names:
            assert "DIVISION" not in n.upper(), (
                f"division heading leaked through: {n}"
            )

    def test_deduplicates_across_sources(self):
        """
        The fixture's 9 teams should produce 9 unique output records even
        after round-tripping through the Modular11 cross-check shape
        (which shares some team names with the primary source).
        """
        records = parse_html(_league_teams_html())
        names = [r["club_name"] for r in records]
        assert len(names) == len(set(names)), (
            f"duplicate club names: {names}"
        )

    def test_modular11_fallback_shape(self):
        """
        When fed Modular11 iframe HTML, ``parse_html`` falls back to the
        team-dropdown parser and returns the ``<option>`` values.
        """
        records = parse_html(
            _modular11_html(),
            source_url="https://www.modular11.com/league-schedule/w-league",
            league_name="USL W League",
        )
        # Modular11 fixture has 10 team options (excluding "Nothing Selected").
        assert len(records) == 10, (
            f"expected 10 clubs from modular11 fixture, got {len(records)}"
        )
        names = {r["club_name"] for r in records}
        assert "AC Connecticut" in names
        assert "Detroit City FC" in names
        assert "Nothing Selected" not in names

    def test_replay_signature_accepted(self):
        """
        Replay handler calls ``parse_html(html, source_url=..., league_name=...)``
        first and falls back to positional on TypeError. Both paths must work.
        """
        kw = parse_html(
            _league_teams_html(),
            source_url="https://www.uslwleague.com/league-teams",
            league_name=None,
        )
        assert len(kw) == 9
        pos = parse_html(_league_teams_html())
        assert len(pos) == 9

    def test_empty_html_returns_empty(self):
        """No seed-fallback on empty / short HTML — replay is literal."""
        assert parse_html("") == []
        assert parse_html("<html></html>") == []

    def test_403_like_body_returns_empty(self):
        """
        A Cloudflare 403-shaped placeholder HTML body (short, no team spans,
        no select) must produce an empty list rather than raising.
        """
        body = "<html><head></head><body><h1>403 Forbidden</h1></body></html>"
        assert parse_html(body) == []


class TestInternalParsers:
    def test_league_teams_parser_extracts_all_spans(self):
        names = _parse_league_teams_html(_league_teams_html())
        assert len(names) == 9
        assert "AFC Ann Arbor" in names
        assert "Racing Louisville FC" in names

    def test_league_teams_parser_ignores_empty_html(self):
        assert _parse_league_teams_html("") == []
        assert _parse_league_teams_html("<html></html>") == []

    def test_modular11_parser_reads_team_select(self):
        names = _parse_modular11_html(_modular11_html())
        assert len(names) == 10
        assert names[0] == "AC Connecticut"

    def test_modular11_parser_skips_nothing_selected(self):
        names = _parse_modular11_html(_modular11_html())
        assert "Nothing Selected" not in names

    def test_modular11_parser_ignores_when_no_select(self):
        """HTML without a ``<select name="team">`` yields no names."""
        body = "<html><body>" + ("x" * 1000) + "</body></html>"
        assert _parse_modular11_html(body) == []

    def test_extract_tournament_id_from_scheduleconfig(self):
        tid = _extract_modular11_tournament_id(_modular11_html())
        assert tid == 25

    def test_extract_tournament_id_returns_none_when_absent(self):
        assert _extract_modular11_tournament_id("") is None
        assert _extract_modular11_tournament_id("<html></html>") is None
