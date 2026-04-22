"""
Tests for the Wikipedia-category-backed seeder (D3 fallback).

Fixture models the MediaWiki API response for
``action=query&list=categorymembers&cmtitle=Category:NCAA_Division_III_men's_soccer_teams``.
Covers:

- Category-title composition (3 tests): mens/womens titles, unsupported
  division raises, supported set is {D3}.
- _school_name_from_article_title (8 parametrized): strips straight-
  apostrophe suffix, curly-apostrophe suffix, case-insensitive; wrong-
  gender returns None; no-suffix noise returns None; empty returns None.
- parse_article_titles_to_seeds (4): happy path, dedup, wrong-gender
  skipped, empty → empty.
- fetch_category_members (3, mocked HTTP): single-page, paginated with
  cmcontinue token, API error surfaces.

Run::

    python -m pytest scraper/tests/test_ncaa_wikipedia_category_directory.py -v
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_wikipedia_category_directory import (  # noqa: E402
    _school_name_from_article_title,
    category_title,
    fetch_category_members,
    fetch_division_programs,
    parse_article_titles_to_seeds,
    supported_divisions_categories,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
D3_MENS_FIXTURE = FIXTURE_DIR / "wikipedia_d3_category_mens.json"


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# category_title / supported_divisions_categories
# ---------------------------------------------------------------------------


class TestCategoryTitle:
    def test_d3_mens_title(self):
        assert category_title("D3", "mens") == "Category:NCAA Division III men's soccer teams"

    def test_d3_womens_title(self):
        assert category_title("D3", "womens") == "Category:NCAA Division III women's soccer teams"

    def test_unsupported_division_raises(self):
        with pytest.raises(ValueError, match="No Wikipedia category"):
            category_title("D1", "mens")

    def test_supported_divisions_is_d3(self):
        assert set(supported_divisions_categories()) == {"D3"}


# ---------------------------------------------------------------------------
# _school_name_from_article_title
# ---------------------------------------------------------------------------


class TestSchoolNameFromArticleTitle:
    @pytest.mark.parametrize(
        "title,gender,expected",
        [
            # Straight apostrophe
            ("Stanford Cardinal men's soccer", "mens", "Stanford Cardinal"),
            ("Messiah Falcons men's soccer", "mens", "Messiah Falcons"),
            ("Amherst Mammoths women's soccer", "womens", "Amherst Mammoths"),
            # Curly apostrophe (Wikipedia canonical form)
            ("Kenyon Lords men’s soccer", "mens", "Kenyon Lords"),
            ("Williams Ephs women’s soccer", "womens", "Williams Ephs"),
            # Case insensitive
            ("Stanford Cardinal Men's Soccer", "mens", "Stanford Cardinal"),
            ("Stanford Cardinal MEN'S SOCCER", "mens", "Stanford Cardinal"),
            # Multiword school + multiword nickname
            ("Notre Dame Fighting Irish men's soccer", "mens", "Notre Dame Fighting Irish"),
        ],
    )
    def test_happy_paths(self, title, gender, expected):
        assert _school_name_from_article_title(title, gender) == expected

    def test_wrong_gender_returns_none(self):
        """A women's-soccer title with gender='mens' must NOT match —
        prevents cross-gender pollution."""
        assert _school_name_from_article_title(
            "Stanford Cardinal women's soccer", "mens"
        ) is None
        assert _school_name_from_article_title(
            "Stanford Cardinal men's soccer", "womens"
        ) is None

    def test_no_suffix_returns_none(self):
        """A disambiguation article accidentally in the category."""
        assert _school_name_from_article_title("NCAA Division III", "mens") is None

    def test_empty_input_returns_none(self):
        assert _school_name_from_article_title("", "mens") is None

    def test_suffix_only_returns_none(self):
        """Edge case: title is JUST the suffix, no school name before it."""
        assert _school_name_from_article_title("men's soccer", "mens") is None

    def test_invalid_gender_returns_none(self):
        assert _school_name_from_article_title(
            "Stanford Cardinal men's soccer", "boys"
        ) is None


# ---------------------------------------------------------------------------
# parse_article_titles_to_seeds
# ---------------------------------------------------------------------------


class TestParseArticleTitlesToSeeds:
    def test_happy_path(self):
        titles = [
            "Amherst Mammoths men's soccer",
            "Messiah Falcons men's soccer",
            "Tufts Jumbos men's soccer",
        ]
        seeds = parse_article_titles_to_seeds(titles, "D3", "mens")
        assert len(seeds) == 3
        names = [s.name for s in seeds]
        assert names == ["Amherst Mammoths", "Messiah Falcons", "Tufts Jumbos"]
        for s in seeds:
            assert s.division == "D3"
            assert s.gender_program == "mens"
            assert s.state is None
            assert s.conference is None

    def test_dedup_same_name_twice(self):
        titles = [
            "Amherst Mammoths men's soccer",
            "Amherst Mammoths men's soccer",  # duplicate (name.lower() dedup)
        ]
        seeds = parse_article_titles_to_seeds(titles, "D3", "mens")
        assert len(seeds) == 1

    def test_wrong_gender_titles_skipped(self):
        titles = [
            "Amherst Mammoths men's soccer",
            "Amherst Mammoths women's soccer",  # skip — gender doesn't match
        ]
        seeds = parse_article_titles_to_seeds(titles, "D3", "mens")
        assert len(seeds) == 1
        assert seeds[0].name == "Amherst Mammoths"

    def test_empty_input(self):
        assert parse_article_titles_to_seeds([], "D3", "mens") == []

    def test_unsupported_division_raises(self):
        with pytest.raises(ValueError):
            parse_article_titles_to_seeds(["X"], "D1", "mens")

    def test_unsupported_gender_raises(self):
        with pytest.raises(ValueError):
            parse_article_titles_to_seeds(["X"], "D3", "boys")


# ---------------------------------------------------------------------------
# fetch_category_members — mocked HTTP
# ---------------------------------------------------------------------------


def _mock_response(payload: dict) -> mock.Mock:
    """Helper that returns a requests.Response-like mock whose .json()
    returns the given payload and raise_for_status is a no-op."""
    resp = mock.Mock()
    resp.json.return_value = payload
    resp.raise_for_status = mock.Mock()
    return resp


class TestFetchCategoryMembers:
    def test_single_page_response(self):
        payload = _read_json(D3_MENS_FIXTURE)
        # Drop the continue token to simulate a non-paginated response.
        payload.pop("continue", None)

        fake_session = mock.Mock()
        fake_session.get.return_value = _mock_response(payload)
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_wikipedia_category_directory.requests.Session",
            return_value=fake_session,
        ):
            titles = fetch_category_members(
                "Category:NCAA Division III men's soccer teams"
            )

        assert len(titles) == 5
        assert "Amherst Mammoths men's soccer" in titles
        # API called exactly once (no pagination)
        assert fake_session.get.call_count == 1
        called_params = fake_session.get.call_args[1]["params"]
        assert called_params["cmtitle"] == "Category:NCAA Division III men's soccer teams"
        assert called_params["list"] == "categorymembers"
        assert "cmcontinue" not in called_params

    def test_paginated_response_walks_continue(self):
        """Two-page walk: first response has cmcontinue → second fetch,
        second response has no continue → loop exits."""
        page1 = _read_json(D3_MENS_FIXTURE)  # has continue
        page2 = {
            "batchcomplete": "",
            "query": {
                "categorymembers": [
                    {"pageid": 2001, "ns": 0, "title": "Rutgers-Camden Scarlet Raptors men's soccer"},
                    {"pageid": 2002, "ns": 0, "title": "Wesleyan Cardinals men's soccer"},
                ]
            },
        }

        fake_session = mock.Mock()
        fake_session.get.side_effect = [_mock_response(page1), _mock_response(page2)]
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_wikipedia_category_directory.requests.Session",
            return_value=fake_session,
        ):
            titles = fetch_category_members(
                "Category:NCAA Division III men's soccer teams"
            )

        # 5 from page1 + 2 from page2
        assert len(titles) == 7
        assert fake_session.get.call_count == 2
        # Second call carries the cmcontinue token from page1.
        second_call_params = fake_session.get.call_args_list[1][1]["params"]
        assert second_call_params["cmcontinue"] == "page|messiah-falcons|200"

    def test_api_error_surfaces_after_retries(self):
        fake_session = mock.Mock()
        fake_session.get.side_effect = requests.RequestException("boom")
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_wikipedia_category_directory.requests.Session",
            return_value=fake_session,
        ):
            with pytest.raises(requests.RequestException):
                fetch_category_members(
                    "Category:NCAA Division III men's soccer teams"
                )


# ---------------------------------------------------------------------------
# fetch_division_programs — top-level orchestrator
# ---------------------------------------------------------------------------


class TestFetchDivisionPrograms:
    def test_happy_end_to_end(self):
        """API returns 5 titles → parser outputs 5 seeds with correct
        division/gender + school names stripped of the sport suffix."""
        payload = _read_json(D3_MENS_FIXTURE)
        payload.pop("continue", None)

        fake_session = mock.Mock()
        fake_session.get.return_value = _mock_response(payload)
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_wikipedia_category_directory.requests.Session",
            return_value=fake_session,
        ):
            seeds = fetch_division_programs("D3", "mens")

        assert len(seeds) == 5
        names = {s.name for s in seeds}
        assert "Amherst Mammoths" in names
        assert "Kenyon Lords" in names  # curly-apostrophe title
        for s in seeds:
            assert s.division == "D3"
            assert s.gender_program == "mens"

    def test_unsupported_division_raises(self):
        with pytest.raises(ValueError):
            fetch_division_programs("D1", "mens")
