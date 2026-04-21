"""
Tests for the naia.org-backed seeder (NAIA).

Fixture models naia.org's 2021-22 Men's Soccer teams index — a list
of ``<a href="/sports/msoc/2021-22/teams/<slug>">NAME (STATE)</a>``
anchors mixed in with unrelated navigation / non-team hrefs. Tests
cover:

- Parser pulls team-href anchors out of the page and skips everything
  else (nav, footer, release-archive links, empty anchors)
- Anchor text parsed into (name, state) — handles "Calif.", 2-letter
  "KS", full-name "Iowa", no-parens "Aquinas"
- Dedup by (name.lower(), gender) when the same program shows up
  multiple times (alphabetical + conference subindexes coexist)
- Division/gender assigned to all seeds
- ``fetch_naia_programs`` composes the right URL per gender
- Defensive: a future-season anchor (``/2025-26/teams/...``) still
  matches if naia.org ever starts rendering current-season lists

Run::

    python -m pytest scraper/tests/test_naia_directory.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.naia_directory import (  # noqa: E402
    _name_and_state_from_anchor_text,
    _parse_state_parenthetical,
    directory_url,
    fetch_naia_programs,
    parse_naia_index,
    supported_genders,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
MENS_FIXTURE = FIXTURE_DIR / "naia_2021_22_mens_index.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# directory_url / supported_genders
# ---------------------------------------------------------------------------


class TestDirectoryUrl:
    def test_mens_url_shape(self):
        assert directory_url("mens") == "https://www.naia.org/sports/msoc/2021-22/teams"

    def test_womens_url_shape(self):
        assert directory_url("womens") == "https://www.naia.org/sports/wsoc/2021-22/teams"

    def test_unsupported_gender_raises(self):
        with pytest.raises(ValueError):
            directory_url("boys")

    def test_supported_genders_list(self):
        assert set(supported_genders()) == {"mens", "womens"}


# ---------------------------------------------------------------------------
# _parse_state_parenthetical
# ---------------------------------------------------------------------------


class TestParseStateParenthetical:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            # naia.org's canonical abbreviated-with-period form
            ("Calif.", "CA"),
            ("Mo.", "MO"),
            ("Neb.", "NE"),
            ("Ariz.", "AZ"),
            ("Ky.", "KY"),
            ("Miss.", "MS"),
            ("Texas", "TX"),  # full name
            ("N.Y.", "NY"),
            ("N.M.", "NM"),
            ("S.C.", "SC"),
            # 2-letter passthrough
            ("KS", "KS"),
            ("FL", "FL"),
            # naia.org uses "Iowa" / "Ohio" / "Utah" as full name (no abbreviation)
            ("Iowa", "IA"),
            ("Ohio", "OH"),
            ("Utah", "UT"),
            # Full state names
            ("California", "CA"),
            ("Missouri", "MO"),
        ],
    )
    def test_known_states(self, raw, expected):
        assert _parse_state_parenthetical(raw) == expected

    @pytest.mark.parametrize(
        "raw",
        ["", "   ", "Atlantis", "Moon", "Ontario"],
    )
    def test_unknown_returns_none(self, raw):
        assert _parse_state_parenthetical(raw) is None

    def test_strips_surrounding_whitespace(self):
        assert _parse_state_parenthetical("  Calif.  ") == "CA"


# ---------------------------------------------------------------------------
# _name_and_state_from_anchor_text
# ---------------------------------------------------------------------------


class TestNameAndStateFromAnchorText:
    def test_with_state_parenthetical(self):
        name, state = _name_and_state_from_anchor_text("Antelope Valley (Calif.)")
        assert name == "Antelope Valley"
        assert state == "CA"

    def test_with_two_letter_state(self):
        name, state = _name_and_state_from_anchor_text("Benedictine (KS)")
        assert name == "Benedictine"
        assert state == "KS"

    def test_no_parens_means_no_state(self):
        name, state = _name_and_state_from_anchor_text("Aquinas")
        assert name == "Aquinas"
        assert state is None

    def test_multiword_school_with_state(self):
        name, state = _name_and_state_from_anchor_text("Wayland Baptist (Texas)")
        assert name == "Wayland Baptist"
        assert state == "TX"

    def test_unknown_state_parens_still_keeps_name(self):
        name, state = _name_and_state_from_anchor_text("Mystery U (Atlantis)")
        assert name == "Mystery U"
        assert state is None

    def test_whitespace_normalized(self):
        name, state = _name_and_state_from_anchor_text("  Antelope  Valley  (Calif.) ")
        assert name == "Antelope Valley"
        assert state == "CA"

    def test_empty_input(self):
        name, state = _name_and_state_from_anchor_text("")
        assert name == ""
        assert state is None


# ---------------------------------------------------------------------------
# parse_naia_index
# ---------------------------------------------------------------------------


class TestParseNaiaIndex:
    def test_parses_core_programs(self):
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        names = [s.name for s in seeds]
        assert "Antelope Valley" in names
        assert "Aquinas" in names
        assert "Benedictine" in names
        assert "Wayland Baptist" in names
        assert "William Woods" in names

    def test_dedupes_by_name_and_gender(self):
        """Fixture repeats 'Antelope Valley' in a conference subindex.
        Parser must collapse into one seed."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        av = [s for s in seeds if s.name == "Antelope Valley"]
        assert len(av) == 1

    def test_skips_empty_anchor(self):
        """Fixture has an empty-text <a> pointing at /teams/phantom."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert all(s.name != "" for s in seeds)

    def test_skips_non_team_anchors(self):
        """Fixture has /releases/25_Polls and /Fan_Central nav links.
        Parser must not emit 'Polls Archive' or 'FAN CENTRAL' rows."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        names = {s.name for s in seeds}
        assert "Polls Archive" not in names
        assert "FAN CENTRAL" not in names
        assert "Men's Soccer Home" not in names

    def test_matches_future_season_hrefs_defensively(self):
        """If naia.org ever starts rendering current-season listings,
        a /2025-26/teams/... anchor should still parse correctly."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert any(s.name == "Future Program" and s.state == "NM" for s in seeds)

    def test_states_extracted(self):
        """Confirm the state extraction pipeline works end-to-end."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        state_by_name = {s.name: s.state for s in seeds}
        assert state_by_name["Antelope Valley"] == "CA"
        assert state_by_name["Avila"] == "MO"
        assert state_by_name["Bellevue"] == "NE"
        assert state_by_name["Benedictine"] == "KS"
        assert state_by_name["Briar Cliff"] == "IA"
        assert state_by_name["Aquinas"] is None  # no parens

    def test_division_and_gender_in_all_seeds(self):
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert seeds  # non-empty
        for s in seeds:
            assert s.division == "NAIA"
            assert s.gender_program == "mens"

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            parse_naia_index("<html></html>", "boys")

    def test_empty_html_returns_empty(self):
        assert parse_naia_index("<html><body></body></html>", "mens") == []

    def test_expected_count_from_fixture(self):
        """Fixture has 13 unique programs + 1 future-season + 2 dupes/empties
        that should be filtered. Expected: 14 unique seeds."""
        seeds = parse_naia_index(_read(MENS_FIXTURE), "mens")
        assert len(seeds) == 14


# ---------------------------------------------------------------------------
# fetch_naia_programs — happy path with mocked HTTP
# ---------------------------------------------------------------------------


class TestFetchNaiaPrograms:
    def test_fetch_parses_response_body(self):
        html = _read(MENS_FIXTURE)
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.naia_directory.requests.Session",
            return_value=fake_session,
        ):
            seeds = fetch_naia_programs("mens")

        assert len(seeds) == 14
        assert "Antelope Valley" in {s.name for s in seeds}
        fake_session.get.assert_called_once()
        called_url = fake_session.get.call_args[0][0]
        assert called_url == "https://www.naia.org/sports/msoc/2021-22/teams"

    def test_fetch_womens_uses_wsoc_url(self):
        html = _read(MENS_FIXTURE)  # content irrelevant for URL check
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.naia_directory.requests.Session",
            return_value=fake_session,
        ):
            fetch_naia_programs("womens")

        called_url = fake_session.get.call_args[0][0]
        assert called_url == "https://www.naia.org/sports/wsoc/2021-22/teams"

    def test_fetch_unsupported_gender_raises(self):
        with pytest.raises(ValueError):
            fetch_naia_programs("boys")
