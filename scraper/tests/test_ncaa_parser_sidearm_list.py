"""
Tests for Strategy 7 in ``parse_roster_html``: the SIDEARM list-template
DOM fallback. Sibling of Strategy 5 (Vue-embedded JSON, merged via #167)
and Strategy 6 (WMT/WordPress cards, merged via #172) that targets the
same operator-toggled SIDEARM list display but parses the post-hydration
DOM rather than the embedded JSON blob. Provides defense-in-depth for
sites where the Vue JSON is absent or malformed.

Programs observed using this variant (from live PR-5 run logs +
George Mason DOM capture 2026-04-21):
  George Mason, Georgia Tech, Pepperdine (mens), Richmond,
  USC (mens), Virginia Tech, Minnesota (mens), San Diego State,
  Tulane, Penn State (womens)

Fixture is modeled on George Mason's gomason.com/sports/mens-soccer/roster
rendered DOM. Player personal details are fictional; class names,
nesting depth, comment sentinels, and the .sidearm-roster-list-item-*
field inventory match the live site.

Run::

    python -m pytest scraper/tests/test_ncaa_parser_sidearm_list.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_soccer_rosters import parse_roster_html  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
LIST_FIXTURE = FIXTURE_DIR / "sidearm_list_roster.html"
UCLA_FIXTURE = FIXTURE_DIR / "ucla_mens_soccer_roster.html"
NUXT_FIXTURE = FIXTURE_DIR / "nuxt_roster_template.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestSidearmListParser:
    def test_returns_three_players_skipping_broken_row(self):
        """Fixture has 3 real players + 1 name-less row; parser skips broken."""
        players = parse_roster_html(_read(LIST_FIXTURE))
        assert len(players) == 3
        names = [p.player_name for p in players]
        assert names == ["Fernando Carreon Garcia", "Juan Alvarez", "Taylor Kim"]

    def test_player_one_all_fields_extracted(self):
        players = parse_roster_html(_read(LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Fernando Carreon Garcia")
        assert p.jersey_number == "1"
        assert p.position == "GK"
        assert p.year == "senior"  # "Sr." via normalize_year
        assert p.hometown == "Saltillo, Mexico"
        assert p.prev_club == "CF Monterrey"

    def test_high_school_field_variant_mapped_to_prev_club(self):
        """Some sites populate .sidearm-roster-list-item-highschool instead
        of -previous-school. Parser must accept either."""
        players = parse_roster_html(_read(LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Juan Alvarez")
        assert p.jersey_number == "12"
        assert p.position == "M"
        assert p.year == "freshman"
        assert p.hometown == "Madrid, Spain"
        assert p.prev_club == "Colegio San Jose"

    def test_redshirt_year_normalized(self):
        """'R-So.' is a normalize_year redshirt-prefix unwrap; should → sophomore."""
        players = parse_roster_html(_read(LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Taylor Kim")
        assert p.year == "sophomore"
        assert p.jersey_number == "22"
        assert p.position == "F"

    def test_missing_prev_school_is_none_not_error(self):
        """Taylor Kim's row has no prev-school/highschool field — stays None."""
        players = parse_roster_html(_read(LIST_FIXTURE))
        p = next(p for p in players if p.player_name == "Taylor Kim")
        assert p.prev_club is None


class TestStrategyOrderingPreserved:
    def test_ucla_card_template_still_hits_strategy_one(self):
        """Regression guard: UCLA's card-template SIDEARM fixture (PR-0)
        must still return via Strategy 1 after PR-8 adds Strategy 7."""
        players = parse_roster_html(_read(UCLA_FIXTURE))
        assert len(players) >= 4
        # Known UCLA players from the MVP fixture
        names = [p.player_name for p in players]
        assert any("UCLA" not in n for n in names)  # not the fixture label

    def test_nuxt_template_still_hits_strategy_four(self):
        """Regression guard: the Nuxt fixture from PR-5 still parses via
        Strategy 4. Strategy 7 selector (`.sidearm-roster-list-item`)
        doesn't collide with Strategy 4's (`.roster-card-item`)."""
        players = parse_roster_html(_read(NUXT_FIXTURE))
        assert len(players) == 3
        names = [p.player_name for p in players]
        assert "Rowan Schnebly" in names  # Nuxt-specific fictional name

    def test_empty_html_returns_empty(self):
        assert parse_roster_html("<html></html>") == []
