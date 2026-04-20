"""
Tests for Strategy 4 in ``parse_roster_html``: the Nuxt-based non-SIDEARM
roster template used by ~20 D1 programs (Stanford, Penn State, USC,
Oregon, Virginia, Virginia Tech, Notre Dame women's, Georgia Tech,
Oklahoma, New Mexico, Iowa women's, Minnesota, Pepperdine, Auburn,
George Mason, SDSU, San Jose State, Tulane, Wyoming, Utah State).

Template signature:
  <div class="roster-card-item"> ... </div>  with
  ``.roster-card-item__title`` (name), ``.roster-card-item__jersey-number``,
  ``.roster-card-item__position``, and labeled/unlabeled
  ``.roster-player-card-profile-field__value`` children.

Fixture shape is modeled on a live capture of
gostanford.com/sports/mens-soccer/roster. Staff cards (.roster-card-item
with .roster-staff-members-card-item) must not be returned.

Run::

    python -m pytest scraper/tests/test_ncaa_parser_nuxt.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_rosters import parse_roster_html  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
NUXT_FIXTURE = FIXTURE_DIR / "nuxt_roster_template.html"
UCLA_FIXTURE = FIXTURE_DIR / "ucla_mens_soccer_roster.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class TestNuxtRosterParser:
    def test_returns_three_players_excluding_staff(self):
        """Fixture has 3 player cards + 1 staff card; only 3 returned."""
        players = parse_roster_html(_read(NUXT_FIXTURE))
        assert len(players) == 3
        names = [p.player_name for p in players]
        assert names == ["Rowan Schnebly", "Juan Alvarez", "Taylor Kim"]
        assert "Jamie Coach" not in names

    def test_player_one_all_fields_extracted(self):
        players = parse_roster_html(_read(NUXT_FIXTURE))
        p = next(p for p in players if p.player_name == "Rowan Schnebly")
        assert p.jersey_number == "1"
        assert p.position == "GK"
        # "Redshirt Junior" (full phrase) isn't in YEAR_MAP and doesn't
        # start with the rs-/r- abbreviated prefixes that normalize_year
        # unwraps, so it stays None. This is existing normalize_year
        # behavior; extending it to full phrases is a separate concern.
        assert p.year is None
        assert p.hometown == "Portland, Ore."
        assert p.prev_club == "Lincoln High"

    def test_player_two_minimal_fields(self):
        """No major, no previous school — parser shouldn't crash on missing labels."""
        players = parse_roster_html(_read(NUXT_FIXTURE))
        p = next(p for p in players if p.player_name == "Juan Alvarez")
        assert p.jersey_number == "7"
        assert p.position == "M"
        assert p.year == "freshman"
        assert p.hometown == "Madrid, Spain"
        assert p.prev_club is None

    def test_high_school_label_variant_mapped_to_prev_club(self):
        """Some programs label as 'High School' instead of 'Previous School'."""
        players = parse_roster_html(_read(NUXT_FIXTURE))
        p = next(p for p in players if p.player_name == "Taylor Kim")
        assert p.jersey_number == "22"
        assert p.position == "F"
        assert p.year == "senior"
        assert p.hometown == "Seattle, Wash."
        assert p.prev_club == "West Seattle HS"

    def test_staff_cards_skipped_via_class_guard(self):
        """A .roster-card-item with .roster-staff-members-card-item must not
        be returned as a player — the class-list filter is the guard.
        """
        players = parse_roster_html(_read(NUXT_FIXTURE))
        for p in players:
            assert "Coach" not in p.player_name


class TestNuxtStrategyOrdering:
    def test_ucla_sidearm_still_hits_strategy_one(self):
        """Regression guard: existing SIDEARM fixture (UCLA) still parses via
        Strategy 1. Strategy 4 doesn't accidentally short-circuit SIDEARM."""
        players = parse_roster_html(_read(UCLA_FIXTURE))
        # UCLA fixture is intentionally small (4 players) — just confirm
        # it still extracts via Strategy 1 after PR-5 adds Strategy 4.
        assert len(players) >= 4
        # UCLA players don't have the Nuxt fixture's fictional names
        names = [p.player_name for p in players]
        assert "Rowan Schnebly" not in names
        assert "Juan Alvarez" not in names

    def test_empty_html_returns_empty(self):
        assert parse_roster_html("<html></html>") == []

    def test_only_staff_cards_returns_empty(self):
        html = """
        <html><body>
          <div class="roster-card-item roster-staff-members-card-item">
            <a class="roster-card-item__title">Head Coach Person</a>
          </div>
        </body></html>
        """
        assert parse_roster_html(html) == []
