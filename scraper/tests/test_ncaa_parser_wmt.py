"""
Tests for Strategy 6 in ``parse_roster_html``: the WMT Digital / WordPress
roster template observed on ramblinwreck.com (Georgia Tech).

(Strategies 1-5 cover SIDEARM live HTML, header-aware tables, SIDEARM card
variants, the Nuxt non-SIDEARM template, and the Sidearm Vue-embedded JSON
variant respectively. Strategy 6 runs last, only if all of those return 0
players, because the WMT card markup is sparse by comparison — cards don't
ship hometown/prev_club, so earlier strategies always give richer rows when
they fire.)

Platform signature:
  - ``server: WMT`` / ``x-powered-by: WMT`` response headers
  - WordPress ``wp-content/themes/...`` stylesheet links
  - inline ``wmtMobileAppFrontendConfig`` script on every page
  - roster pages wrapped in ``<section class="wrapper roster">`` with two
    sibling containers: ``.roster__list`` (figure/figcaption cards) and
    ``.roster__table`` (a real ``<table>`` with ``<th>`` headers).

Strategy 2 (header-aware table) already handles the ``.roster__table`` form,
so when both containers are present Strategy 2 wins. Strategy 6 is the
belt-and-suspenders fallback for WMT themes that ship only the card list —
it keeps this scraper resilient to DOM churn where the table gets dropped.

Fixture ``wmt_wordpress_roster.html`` is modeled on a live capture of
``ramblinwreck.com/sports/m-basebl/roster/``. Georgia Tech does not sponsor
NCAA soccer (confirmed on the school's sports list and Wikipedia — GT is the
only Power Four school without women's soccer, and no men's soccer either),
so the baseball roster is used as the representative live WMT capture. The
DOM template is shared across sports within the same WMT theme.

Run::

    python -m pytest scraper/tests/test_ncaa_parser_wmt.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bs4 import BeautifulSoup  # noqa: E402

from extractors.ncaa_soccer_rosters import parse_roster_html  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
WMT_FIXTURE = FIXTURE_DIR / "wmt_wordpress_roster.html"
UCLA_FIXTURE = FIXTURE_DIR / "ucla_mens_soccer_roster.html"
NUXT_FIXTURE = FIXTURE_DIR / "nuxt_roster_template.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _strip_table(html: str) -> str:
    """Remove the ``.roster__table`` sibling so only the card list remains.

    Forces Strategy 6 instead of Strategy 2 — the fallback-only path.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one(".roster__table")
    if table:
        table.decompose()
    return str(soup)


class TestWmtFixtureFull:
    """With both ``.roster__table`` and ``.roster__list`` present, Strategy 2
    (header-aware table) wins — it returns a richer row (hometown + prev_club).
    """

    def test_fixture_parses_twelve_players(self):
        players = parse_roster_html(_read(WMT_FIXTURE))
        assert len(players) == 12

    def test_fixture_strategy2_extracts_hometown_and_prev_club(self):
        """The table form has Hometown + Last School columns Strategy 6 can't see."""
        players = parse_roster_html(_read(WMT_FIXTURE))
        mike = next(p for p in players if p.player_name == "Michael Dee")
        assert mike.jersey_number == "0"
        assert mike.position == "INF"
        assert mike.year == "freshman"
        assert mike.hometown == "Stone Mountain, Ga."
        assert mike.prev_club == "St. Pius X"

    def test_fixture_senior_year_normalized(self):
        players = parse_roster_html(_read(WMT_FIXTURE))
        parker = next(p for p in players if p.player_name == "Parker Brosius")
        assert parker.year == "senior"
        assert parker.position == "OF"


class TestWmtCardFallback:
    """With the table stripped, Strategy 6 must still recover a player list.

    Cards don't carry hometown / prev_club, so those fields come back None.
    Jersey number, name, position, and year — the fields encoded into the
    figure/figcaption markup — are all preserved.
    """

    def test_card_only_parses_twelve_players(self):
        html = _strip_table(_read(WMT_FIXTURE))
        players = parse_roster_html(html)
        assert len(players) == 12

    def test_card_only_covers_required_fields(self):
        html = _strip_table(_read(WMT_FIXTURE))
        players = parse_roster_html(html)
        mike = next(p for p in players if p.player_name == "Michael Dee")
        assert mike.jersey_number == "0"
        assert mike.position == "INF"
        assert mike.year == "freshman"

    def test_card_only_hometown_and_prev_club_are_none(self):
        """Fields not present in the card layout — must degrade gracefully."""
        html = _strip_table(_read(WMT_FIXTURE))
        players = parse_roster_html(html)
        mike = next(p for p in players if p.player_name == "Michael Dee")
        assert mike.hometown is None
        assert mike.prev_club is None

    def test_card_only_jersey_strip_hash(self):
        """``<div class="icon"><span>#12</span></div>`` — hash must be stripped."""
        html = _strip_table(_read(WMT_FIXTURE))
        players = parse_roster_html(html)
        # First item in fixture has jersey "#0"; the stripped value is "0"
        assert any(p.jersey_number == "0" for p in players)

    def test_card_only_returns_at_least_ten_players(self):
        """The acceptance bar from the PR brief — ≥10 players from the live shape."""
        html = _strip_table(_read(WMT_FIXTURE))
        players = parse_roster_html(html)
        assert len(players) >= 10


class TestWmtStrategyOrdering:
    """Regression guards: Strategy 6 must not accidentally absorb other platforms."""

    def test_ucla_sidearm_still_hits_strategy_one(self):
        players = parse_roster_html(_read(UCLA_FIXTURE))
        assert len(players) >= 4

    def test_nuxt_fixture_still_hits_strategy_four(self):
        players = parse_roster_html(_read(NUXT_FIXTURE))
        assert len(players) == 3

    def test_empty_html_returns_empty(self):
        assert parse_roster_html("<html></html>") == []

    def test_ramblinwreck_home_page_redirect_returns_empty(self):
        """A WMT page with no roster markup (e.g. the 302-to-home that
        ``ramblinwreck.com/sports/mens-soccer/roster`` produces — Georgia Tech
        doesn't sponsor soccer, so the path redirects to the home page
        template) must parse to zero players, not raise, and not mistake
        WordPress theme markup for roster rows.
        """
        home_like = """
        <!DOCTYPE html><html><head>
          <script>var wmtMobileAppFrontendConfig = {"user_agents":["WMT"]};</script>
          <link rel='stylesheet' href='/wp-content/themes/gt/build/style.css' />
        </head><body class="home page-template page-template-home">
          <section class="hero"><h1>Georgia Tech Athletics</h1></section>
          <div class="latest-news"></div>
        </body></html>
        """
        assert parse_roster_html(home_like) == []

    def test_card_without_figcaption_is_skipped(self):
        """A malformed WMT card (figure only, no figcaption) must not produce
        a row — required name field comes from the figcaption anchor."""
        html = """
        <html><body>
          <div class="roster__list_item"><figure><img alt="" /></figure></div>
        </body></html>
        """
        assert parse_roster_html(html) == []

    def test_card_with_only_name_extracts_minimum(self):
        """A card with just a name anchor in the figcaption still produces a
        row — jersey, position, year degrade to None."""
        html = """
        <html><body>
          <div class="roster__list_item">
            <figure>
              <figcaption>
                <a href="/player/john-doe/">John Doe</a>
              </figcaption>
            </figure>
          </div>
        </body></html>
        """
        players = parse_roster_html(html)
        assert len(players) == 1
        assert players[0].player_name == "John Doe"
        assert players[0].jersey_number is None
        assert players[0].position is None
        assert players[0].year is None
