"""
Tests for the Wikipedia-backed seeder (D2/D3/NAIA).

Fixture models Wikipedia's "List of NCAA Division II men's soccer
programs" table. Tests cover:

- Parser reads ``<table class="wikitable sortable">`` header row to
  infer column positions (header-aware, not hardcoded offsets)
- Footnote ``<sup>[1]</sup>`` markers stripped from name cell
- Non-program tables (no "Institution" header) skipped
- Footer rows ("Total") skipped
- Dedup by (name.lower(), gender) — same-program-twice rows collapse
- State parser: "City, State Name" → "ST" abbrev; "City, ST" passthrough
- Happy fields: name, conference, state populated
- ``fetch_division_programs`` composes the right URL per (division, gender)

Run::

    python -m pytest scraper/tests/test_ncaa_wikipedia_directory.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_wikipedia_directory import (  # noqa: E402
    _state_from_location,
    directory_url,
    fetch_division_programs,
    parse_wikipedia_table,
    supported_divisions,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
D2_MENS_FIXTURE = FIXTURE_DIR / "wikipedia_d2_mens.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# directory_url
# ---------------------------------------------------------------------------


class TestDirectoryUrl:
    def test_d2_mens_url_shape(self):
        url = directory_url("D2", "mens")
        assert url == "https://en.wikipedia.org/wiki/List_of_NCAA_Division_II_men%27s_soccer_programs"

    def test_d2_womens_url_shape(self):
        url = directory_url("D2", "womens")
        assert "Division_II_women" in url

    def test_d3_womens_url_shape(self):
        url = directory_url("D3", "womens")
        assert "Division_III_women" in url

    def test_naia_mens_url_shape(self):
        url = directory_url("NAIA", "mens")
        assert "NAIA_men" in url

    def test_unsupported_division_raises(self):
        with pytest.raises(ValueError, match="No Wikipedia source"):
            directory_url("NJCAA", "mens")

    def test_unknown_gender_raises(self):
        with pytest.raises(ValueError):
            directory_url("D2", "boys")

    def test_supported_divisions_list(self):
        assert set(supported_divisions()) == {"D2", "D3", "NAIA"}


# ---------------------------------------------------------------------------
# parse_wikipedia_table
# ---------------------------------------------------------------------------


class TestParseWikipediaTable:
    def test_returns_expected_d2_programs(self):
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        names = [s.name for s in seeds]
        assert "Adelphi University" in names
        assert "Barry University" in names
        assert "Cal State LA" in names
        assert "Saint Leo University" in names
        # "Total" footer + duplicate Adelphi both skipped
        assert len(seeds) == 4

    def test_dedupes_by_name_and_gender(self):
        """Fixture has Adelphi listed twice; parser collapses."""
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        adelphi = [s for s in seeds if s.name == "Adelphi University"]
        assert len(adelphi) == 1

    def test_strips_sup_footnote_markers_from_name(self):
        """Row 1 has ``<sup>[1]</sup>`` — parser must strip so the
        name is 'Adelphi University' not 'Adelphi University[1]'."""
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        adelphi = next(s for s in seeds if s.name.startswith("Adelphi"))
        assert adelphi.name == "Adelphi University"

    def test_conference_populated_from_header(self):
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        adelphi = next(s for s in seeds if s.name == "Adelphi University")
        assert "Northeast-10" in (adelphi.conference or "")

    def test_state_from_full_name(self):
        """Location 'Garden City, New York' → 'NY'."""
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        adelphi = next(s for s in seeds if s.name == "Adelphi University")
        assert adelphi.state == "NY"

    def test_state_from_two_letter_code(self):
        """Location 'St. Leo, FL' → 'FL' (two-letter passthrough)."""
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        saint_leo = next(s for s in seeds if s.name == "Saint Leo University")
        assert saint_leo.state == "FL"

    def test_division_and_gender_in_all_seeds(self):
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        for s in seeds:
            assert s.division == "D2"
            assert s.gender_program == "mens"

    def test_sidebar_non_program_table_skipped(self):
        """Fixture has a non-program <table class='wikitable'> (Championship/Year).
        Parser must skip — no 'Institution' header."""
        seeds = parse_wikipedia_table(_read(D2_MENS_FIXTURE), "D2", "mens")
        # Programs list has 4 real entries; sidebar table would leak
        # extra rows if not skipped.
        assert all(s.name not in ("NCAA tournament", "2023") for s in seeds)

    def test_invalid_division_raises(self):
        with pytest.raises(ValueError):
            parse_wikipedia_table("<html></html>", "D1", "mens")

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            parse_wikipedia_table("<html></html>", "D2", "boys")

    def test_empty_page_returns_empty(self):
        assert parse_wikipedia_table("<html><body></body></html>", "D2", "mens") == []


# ---------------------------------------------------------------------------
# _state_from_location — direct unit tests
# ---------------------------------------------------------------------------


class TestStateFromLocation:
    @pytest.mark.parametrize(
        "location,expected",
        [
            ("Garden City, New York", "NY"),
            ("Miami Shores, Florida", "FL"),
            ("Los Angeles, California", "CA"),
            ("St. Leo, FL", "FL"),
            ("Houston, Texas", "TX"),
            ("Washington, District of Columbia", "DC"),
            ("Collegeville, Pennsylvania", "PA"),
        ],
    )
    def test_known_states(self, location, expected):
        assert _state_from_location(location) == expected

    @pytest.mark.parametrize(
        "location",
        ["", "Unknown", "Somewhere, Ontario", "No comma here",
         "Springfield, Springfieldtonia"],
    )
    def test_unknown_or_unparseable_returns_none(self, location):
        assert _state_from_location(location) is None

    def test_footnote_tail_stripped(self):
        """Location like 'Nashville, Tennessee[1]' — strip the [1]."""
        assert _state_from_location("Nashville, Tennessee[1]") == "TN"


# ---------------------------------------------------------------------------
# fetch_division_programs — happy path with mocked HTTP
# ---------------------------------------------------------------------------


class TestFetchDivisionPrograms:
    def test_fetch_parses_response_body(self):
        html = _read(D2_MENS_FIXTURE)
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_wikipedia_directory.requests.Session",
            return_value=fake_session,
        ):
            seeds = fetch_division_programs("D2", "mens")

        assert len(seeds) == 4
        assert {s.name for s in seeds} == {
            "Adelphi University",
            "Barry University",
            "Cal State LA",
            "Saint Leo University",
        }
        fake_session.get.assert_called_once()
        call_url = fake_session.get.call_args[0][0]
        assert "Division_II_men" in call_url

    def test_fetch_unsupported_division_raises(self):
        with pytest.raises(ValueError):
            fetch_division_programs("NJCAA", "mens")
