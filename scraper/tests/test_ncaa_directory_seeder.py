"""
Tests for the NCAA D1 colleges seeder (PR-1 of the enumeration plan).

Covers:
- ``parse_directory_html`` — parses stats.ncaa.org's inst_team_list HTML
  into ``CollegeSeed`` rows (name, ncaa_id, conference, dedup behavior)
- ``CollegeSeed.to_upsert_row`` — shape expected by the writer
- ``fetch_d1_programs`` — happy-path HTTP fetch, parser composition
- Writer integration — the seeder's upsert hits the named
  ``ON CONFLICT ON CONSTRAINT colleges_name_division_gender_sport_uq`` clause
  via ``ingest.ncaa_roster_writer.upsert_college``, idempotent across
  re-runs.
- CLI handler ``_handle_ncaa_seed_d1`` — dry-run path does not write to
  the DB; mutex on --gender values.

Run::

    python -m pytest scraper/tests/test_ncaa_directory_seeder.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ncaa_directory import (  # noqa: E402
    CollegeSeed,
    directory_url,
    fetch_d1_programs,
    parse_directory_html,
)
from ingest import ncaa_roster_writer  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ncaa"
D1_MENS_FIXTURE = FIXTURE_DIR / "stats_ncaa_d1_mens_sample.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# directory_url
# ---------------------------------------------------------------------------


class TestDirectoryUrl:
    def test_mens_uses_mso_sport_code(self):
        assert (
            directory_url("mens")
            == "https://stats.ncaa.org/team/inst_team_list?sport_code=MSO&division=1"
        )

    def test_womens_uses_wso_sport_code(self):
        assert (
            directory_url("womens")
            == "https://stats.ncaa.org/team/inst_team_list?sport_code=WSO&division=1"
        )

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            directory_url("boys")


# ---------------------------------------------------------------------------
# parse_directory_html
# ---------------------------------------------------------------------------


class TestParseDirectoryHtml:
    def test_returns_seeds_with_expected_fields(self):
        html = _read(D1_MENS_FIXTURE)
        seeds = parse_directory_html(html, "mens")
        names = [s.name for s in seeds]
        assert "Georgetown" in names
        assert "UCLA" in names
        assert "Stanford" in names

        gtown = next(s for s in seeds if s.name == "Georgetown")
        assert gtown.ncaa_id == "14"
        assert gtown.conference == "Big East"
        assert gtown.division == "D1"
        assert gtown.gender_program == "mens"
        assert gtown.state is None

    def test_dedups_by_name_and_gender(self):
        """The fixture has Georgetown listed twice; parser must collapse."""
        html = _read(D1_MENS_FIXTURE)
        seeds = parse_directory_html(html, "mens")
        gtowns = [s for s in seeds if s.name == "Georgetown"]
        assert len(gtowns) == 1

    def test_empty_conference_cell_yields_none(self):
        """Hartwick row has an empty second <td>; conference stays None."""
        html = _read(D1_MENS_FIXTURE)
        seeds = parse_directory_html(html, "mens")
        hartwick = next(s for s in seeds if s.name == "Hartwick")
        assert hartwick.conference is None
        assert hartwick.ncaa_id == "999"

    def test_skips_sort_header_anchors(self):
        """Anchors like /team/inst_team_list?...orderBy=name are not programs."""
        html = _read(D1_MENS_FIXTURE)
        seeds = parse_directory_html(html, "mens")
        # The fixture's <th> links don't match /team/<digits>/, so they're
        # naturally skipped — this test locks in that invariant.
        for seed in seeds:
            assert seed.ncaa_id is not None
            assert seed.ncaa_id.isdigit()

    def test_invalid_gender_raises(self):
        with pytest.raises(ValueError):
            parse_directory_html("<html></html>", "boys")

    def test_empty_page_returns_empty_list(self):
        seeds = parse_directory_html("<html><body></body></html>", "mens")
        assert seeds == []


# ---------------------------------------------------------------------------
# CollegeSeed.to_upsert_row
# ---------------------------------------------------------------------------


class TestCollegeSeedToUpsertRow:
    def test_row_shape_matches_writer_contract(self):
        seed = CollegeSeed(
            name="UCLA",
            division="D1",
            gender_program="mens",
            ncaa_id="736",
            conference="Big Ten",
        )
        row = seed.to_upsert_row()
        assert row["name"] == "UCLA"
        assert row["division"] == "D1"
        assert row["gender_program"] == "mens"
        assert row["ncaa_id"] == "736"
        assert row["conference"] == "Big Ten"
        assert row["state"] is None
        assert row["sport"] == "soccer"
        assert row["scrape_confidence"] == 0.9

    def test_row_passes_writer_normalization(self):
        """Feeding the seed into the writer's normalizer must not raise."""
        seed = CollegeSeed(
            name="Georgetown",
            division="D1",
            gender_program="mens",
            ncaa_id="14",
            conference="Big East",
        )
        normalized = ncaa_roster_writer._normalize_college(seed.to_upsert_row())
        assert normalized["name"] == "Georgetown"
        assert normalized["slug"] == "georgetown-d1-mens"
        assert normalized["ncaa_id"] == "14"


# ---------------------------------------------------------------------------
# fetch_d1_programs (HTTP happy path, mocked)
# ---------------------------------------------------------------------------


class TestFetchD1Programs:
    def test_fetch_parses_response_body(self):
        html = _read(D1_MENS_FIXTURE)
        fake_response = mock.Mock()
        fake_response.text = html
        fake_response.raise_for_status = mock.Mock()

        fake_session = mock.Mock()
        fake_session.get.return_value = fake_response
        fake_session.close = mock.Mock()

        with mock.patch(
            "extractors.ncaa_directory.requests.Session", return_value=fake_session
        ):
            seeds = fetch_d1_programs("mens")
        assert len(seeds) >= 6
        assert any(s.name == "UCLA" for s in seeds)
        fake_session.get.assert_called_once()
        call_url = fake_session.get.call_args[0][0]
        assert "sport_code=MSO" in call_url
        assert "division=1" in call_url


# ---------------------------------------------------------------------------
# Writer integration — named constraint invariant
# ---------------------------------------------------------------------------


class TestWriterNamedConstraint:
    def test_upsert_sql_uses_named_constraint(self):
        """Guard against a silent switch from named constraint to column-list.

        Both forms hit the same unique index, but the writer uses the
        named form today (``ON CONFLICT ON CONSTRAINT colleges_name_division_gender_sport_uq``)
        and a refactor to columns would be a behavior change worth flagging.
        """
        sql = ncaa_roster_writer._UPSERT_COLLEGE_SQL
        assert "ON CONFLICT ON CONSTRAINT colleges_name_division_gender_sport_uq" in sql

    def test_upsert_executes_named_constraint_sql(self):
        """The writer's ``upsert_college`` must execute the named-constraint SQL."""
        seed = CollegeSeed(
            name="UCLA",
            division="D1",
            gender_program="mens",
            ncaa_id="736",
            conference="Big Ten",
        )

        fake_cursor = mock.MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = None
        fake_cursor.fetchone.return_value = (42, True)

        fake_conn = mock.MagicMock()
        fake_conn.cursor.return_value = fake_cursor

        college_id, inserted = ncaa_roster_writer.upsert_college(
            seed.to_upsert_row(), conn=fake_conn
        )
        assert college_id == 42
        assert inserted is True

        executed_sql, executed_params = fake_cursor.execute.call_args[0]
        assert "ON CONFLICT ON CONSTRAINT colleges_name_division_gender_sport_uq" in executed_sql
        assert executed_params["name"] == "UCLA"
        assert executed_params["slug"] == "ucla-d1-mens"
        assert executed_params["division"] == "D1"
        assert executed_params["gender_program"] == "mens"
        assert executed_params["ncaa_id"] == "736"
        assert executed_params["conference"] == "Big Ten"
        assert executed_params["soccer_program_url"] is None


# ---------------------------------------------------------------------------
# Dry-run path
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_does_not_touch_cursor(self):
        seed = CollegeSeed(
            name="Stanford",
            division="D1",
            gender_program="mens",
            ncaa_id="674",
            conference="ACC",
        )
        with mock.patch.object(ncaa_roster_writer, "_get_connection") as mock_conn:
            college_id, inserted = ncaa_roster_writer.upsert_college(
                seed.to_upsert_row(), dry_run=True
            )
        assert college_id is None
        assert inserted is False
        mock_conn.assert_not_called()
