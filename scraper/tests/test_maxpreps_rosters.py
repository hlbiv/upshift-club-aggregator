"""
Tests for the MaxPreps HS roster parser.

The parser is a pure function over HTML — no network, no DB. Fixture
``fixtures/maxpreps/maxpreps_roster_sample.html`` mirrors the shape of
a real MaxPreps public roster page (header row ``#|Name|Grade|Position
|Height|Weight``, tbody of ~12 players).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.maxpreps_rosters import (  # noqa: E402
    _current_spring_year,
    _grade_to_grad_year,
    parse_maxpreps_roster,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "maxpreps"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


class TestParseMaxPrepsRoster:
    """Core parser assertions against the fixture."""

    def test_extracts_at_least_10_players(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        assert len(players) >= 10, f"expected >= 10 players, got {len(players)}"

    def test_all_rows_have_non_empty_name(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        for p in players:
            assert p["player_name"], f"empty name in row: {p}"
            assert len(p["player_name"]) >= 2

    def test_names_are_clean_no_jersey_prefix(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        names = {p["player_name"] for p in players}
        # The fixture uses a separate '#' column, so names never have
        # jersey-number prefixes.
        for name in names:
            assert not name.startswith("#"), name
        # Spot-check a few known names.
        assert "Luis Mendoza" in names
        assert "Carlos Reyes" in names
        assert "Marco Alvarez" in names

    def test_captain_suffix_stripped(self):
        """Ryan Nguyen (C) → Ryan Nguyen."""
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        by_name = {p["player_name"]: p for p in players}
        assert "Ryan Nguyen" in by_name, list(by_name.keys())

    def test_jersey_numbers_populated(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        by_name = {p["player_name"]: p for p in players}
        assert by_name["Luis Mendoza"]["jersey_number"] == "1"
        assert by_name["Marco Alvarez"]["jersey_number"] == "10"
        assert by_name["Ryan Nguyen"]["jersey_number"] == "8"

    def test_positions_populated(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        by_name = {p["player_name"]: p for p in players}
        assert by_name["Luis Mendoza"]["position"] == "GK"
        assert by_name["Marco Alvarez"]["position"] == "FW"
        assert by_name["Antonio Lopez"]["position"] == "MF"

    def test_heights_stored_as_is(self):
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        by_name = {p["player_name"]: p for p in players}
        # Height is stored verbatim — includes the quote character.
        assert by_name["Luis Mendoza"]["height"] == "6'2\""
        assert by_name["Ryan Nguyen"]["height"] == "5'11\""

    def test_graduation_year_computed_from_grade(self):
        """Grade → grad-year math uses the current spring-end year as
        the anchor. Verify the relative offsets are consistent."""
        html = _read("maxpreps_roster_sample.html")
        players = parse_maxpreps_roster(html)
        by_name = {p["player_name"]: p for p in players}

        spring_end = _current_spring_year()
        # Sr → graduates this spring; Jr → +1; So → +2; Fr → +3.
        assert by_name["Luis Mendoza"]["graduation_year"] == spring_end       # Sr
        assert by_name["Carlos Reyes"]["graduation_year"] == spring_end + 1   # Jr
        assert by_name["Diego Martinez"]["graduation_year"] == spring_end + 2  # So
        assert by_name["Jordan Kim"]["graduation_year"] == spring_end + 3     # Fr


class TestGradeNormalization:
    """Unit tests for the grade → grad-year mapping, independent of fixture."""

    def test_none_and_empty(self):
        assert _grade_to_grad_year(None) is None
        assert _grade_to_grad_year("") is None
        assert _grade_to_grad_year("   ") is None

    def test_abbreviations(self):
        spring = _current_spring_year()
        assert _grade_to_grad_year("Sr") == spring
        assert _grade_to_grad_year("Jr") == spring + 1
        assert _grade_to_grad_year("So") == spring + 2
        assert _grade_to_grad_year("Fr") == spring + 3

    def test_numeric_grades(self):
        spring = _current_spring_year()
        assert _grade_to_grad_year("12") == spring
        assert _grade_to_grad_year("11") == spring + 1
        assert _grade_to_grad_year("9th") == spring + 3

    def test_class_of_string(self):
        assert _grade_to_grad_year("Class of 2027") == 2027

    def test_direct_year_preferred(self):
        # If the cell already contains a 4-digit year, use it verbatim.
        assert _grade_to_grad_year("2028") == 2028

    def test_unrecognized_returns_none(self):
        assert _grade_to_grad_year("PG") is None
        assert _grade_to_grad_year("wat") is None


class TestEmptyInputs:
    """Parser must degrade gracefully on garbage HTML."""

    def test_empty_html_returns_empty_list(self):
        assert parse_maxpreps_roster("") == []

    def test_no_roster_table_returns_empty_list(self):
        html = "<html><body><h1>Welcome</h1><p>Nothing here.</p></body></html>"
        assert parse_maxpreps_roster(html) == []

    def test_table_without_name_column_is_skipped(self):
        """A table whose headers don't include a 'Name' column is not a
        roster table. We must not guess columns positionally."""
        html = """
        <table>
          <thead><tr><th>Foo</th><th>Bar</th></tr></thead>
          <tbody><tr><td>a</td><td>b</td></tr></tbody>
        </table>
        """
        assert parse_maxpreps_roster(html) == []
