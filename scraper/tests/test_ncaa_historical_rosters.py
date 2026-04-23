"""
Tests for the historical-season roster backfill (PR-6).

Covers:
- ``_start_year_from_academic_year`` — parses "YYYY-YY" → "YYYY"
- ``_prior_academic_years`` — emits [current, current-1, ...] strings
- ``compose_historical_roster_urls`` — returns SIDEARM + Nuxt candidates
  in a stable order
- ``_find_historical_roster`` — probes candidates, first-hit-wins with
  players-nonzero tiebreak (false-200 with empty parse moves to next)

``scrape_college_rosters`` end-to-end is covered by regression —
existing default behavior (backfill_seasons=0) must be preserved.

Run::

    python -m pytest scraper/tests/test_ncaa_historical_rosters.py -v
"""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_soccer_rosters import (  # noqa: E402
    RosterPlayer,
    _find_historical_roster,
    _prior_academic_years,
    _start_year_from_academic_year,
    compose_historical_roster_urls,
)
from extractors import ncaa_rosters  # noqa: E402


# ---------------------------------------------------------------------------
# _start_year_from_academic_year
# ---------------------------------------------------------------------------


class TestStartYearFromAcademicYear:
    @pytest.mark.parametrize(
        "season,expected",
        [
            ("2025-26", "2025"),
            ("2023-24", "2023"),
            ("1999-00", "1999"),
        ],
    )
    def test_canonical_shapes(self, season, expected):
        assert _start_year_from_academic_year(season) == expected

    @pytest.mark.parametrize(
        "bad",
        ["", "2025", "2025-2026", "2025/26", "twenty-five", None],
    )
    def test_rejects_bad_shape(self, bad):
        with pytest.raises(ValueError):
            _start_year_from_academic_year(bad)


# ---------------------------------------------------------------------------
# _prior_academic_years
# ---------------------------------------------------------------------------


class TestPriorAcademicYears:
    def test_current_only(self):
        assert _prior_academic_years("2025-26", 0) == ["2025-26"]

    def test_includes_n_prior_seasons(self):
        assert _prior_academic_years("2025-26", 2) == [
            "2025-26",
            "2024-25",
            "2023-24",
        ]

    def test_handles_decade_wrap(self):
        """2010-11 → 2009-10 must wrap the two-digit suffix correctly."""
        assert _prior_academic_years("2010-11", 1) == ["2010-11", "2009-10"]

    def test_handles_century_wrap(self):
        """2000-01 → 1999-00 — two-digit suffix is (start+1) % 100."""
        assert _prior_academic_years("2000-01", 1) == ["2000-01", "1999-00"]

    def test_negative_n_raises(self):
        with pytest.raises(ValueError):
            _prior_academic_years("2025-26", -1)


# ---------------------------------------------------------------------------
# compose_historical_roster_urls
# ---------------------------------------------------------------------------


class TestComposeHistoricalRosterUrls:
    def test_sidearm_first_nuxt_second(self):
        urls = compose_historical_roster_urls(
            "https://guhoyas.com/sports/mens-soccer/roster", "2023-24"
        )
        assert urls == [
            "https://guhoyas.com/sports/mens-soccer/roster/2023",
            "https://guhoyas.com/sports/mens-soccer/roster/season/2023",
        ]

    def test_trailing_slash_on_input_is_normalized(self):
        urls = compose_historical_roster_urls(
            "https://example.edu/sports/womens-soccer/roster/", "2022-23"
        )
        assert urls[0].endswith("/roster/2022")
        assert urls[1].endswith("/roster/season/2022")

    def test_preserves_nontrivial_path_segments(self):
        """The scraper sometimes has a prefix path (e.g. subdomain-style
        athletics sites). The /roster trim must be scoped to the trailing
        segment, not any occurrence of the word.
        """
        urls = compose_historical_roster_urls(
            "https://sports.example.edu/roster-main/sports/m-soccer/roster",
            "2022-23",
        )
        # The /roster-main/ segment earlier in the URL must survive.
        assert all("roster-main" in u for u in urls)

    def test_empty_url_raises(self):
        with pytest.raises(ValueError):
            compose_historical_roster_urls("", "2023-24")

    def test_bad_season_raises(self):
        with pytest.raises(ValueError):
            compose_historical_roster_urls(
                "https://example.edu/sports/mens-soccer/roster", "2023"
            )


# ---------------------------------------------------------------------------
# _find_historical_roster — probe ordering + players-nonzero tiebreak
# ---------------------------------------------------------------------------


def _fake_player(name: str) -> RosterPlayer:
    return RosterPlayer(player_name=name)


class TestFindHistoricalRoster:
    def test_sidearm_hit_short_circuits(self):
        """First candidate returns players → don't probe Nuxt variant."""
        players = [_fake_player("Luca Ulrich"), _fake_player("Ryan Schewe")]

        with mock.patch.object(
            ncaa_rosters,
            "_fetch_and_parse_with_fallback",
            return_value=("<html/>", players),
        ) as mock_fetch:
            url, html, got = _find_historical_roster(
                mock.Mock(),
                "https://guhoyas.com/sports/mens-soccer/roster",
                "2023-24",
            )

        assert url == "https://guhoyas.com/sports/mens-soccer/roster/2023"
        assert got == players
        assert mock_fetch.call_count == 1  # short-circuited

    def test_sidearm_false_200_falls_through_to_nuxt(self):
        """SIDEARM URL returns HTML with 0 players (false 200 serving
        current-season shell on a Nuxt site) → try Nuxt candidate next."""
        nuxt_players = [_fake_player("Rowan Schnebly")]
        side_effects = [
            ("<html/>", []),            # SIDEARM candidate: 0 players
            ("<html/>", nuxt_players),  # Nuxt candidate: hit
        ]

        with mock.patch.object(
            ncaa_rosters, "_fetch_and_parse_with_fallback",
            side_effect=side_effects,
        ) as mock_fetch:
            url, html, got = _find_historical_roster(
                mock.Mock(),
                "https://gostanford.com/sports/mens-soccer/roster",
                "2023-24",
            )

        assert url == "https://gostanford.com/sports/mens-soccer/roster/season/2023"
        assert got == nuxt_players
        assert mock_fetch.call_count == 2

    def test_both_miss_returns_none(self):
        """Both candidates fail → caller SKIPs that season for that college."""
        with mock.patch.object(
            ncaa_rosters, "_fetch_and_parse_with_fallback",
            side_effect=[(None, []), (None, [])],
        ):
            url, html, got = _find_historical_roster(
                mock.Mock(),
                "https://example.edu/sports/mens-soccer/roster",
                "2023-24",
            )

        assert url is None
        assert html == ""
        assert got == []

    def test_both_return_shell_returns_none(self):
        """Both candidates return HTML with 0 players → caller SKIPs."""
        with mock.patch.object(
            ncaa_rosters, "_fetch_and_parse_with_fallback",
            side_effect=[("<html/>", []), ("<html/>", [])],
        ):
            url, html, got = _find_historical_roster(
                mock.Mock(),
                "https://example.edu/sports/mens-soccer/roster",
                "2023-24",
            )

        assert url is None
        assert got == []
