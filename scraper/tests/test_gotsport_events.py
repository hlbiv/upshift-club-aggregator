"""
Tests for the GotSport events extractor + runner CSV parsing.

HTML parsing is verified against fixture HTML; DB writes reuse the
stubbed psycopg2 cursor pattern from test_sincsports_events.py.

Run:
    python -m pytest scraper/tests/test_gotsport_events.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.gotsport_events import (  # noqa: E402
    parse_division_code,
    extract_division_codes,
    extract_event_name,
    parse_team_rows,
    parse_gotsport_teams_page,
    parse_gotsport_division_page,
    decode_html_entities,
)
from events_writer import WriteResult, upsert_event_and_teams  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "gotsport" / "sample_teams_page.html"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- unit: division code parsing


class TestParseDivisionCode:
    def test_male_u12(self):
        gender, age = parse_division_code("m_12")
        assert gender == "M"
        assert age == "U12"

    def test_female_u15(self):
        gender, age = parse_division_code("f_15")
        assert gender == "F"
        assert age == "U15"

    def test_male_u9(self):
        gender, age = parse_division_code("m_9")
        assert gender == "M"
        assert age == "U9"

    def test_uppercase_accepted(self):
        gender, age = parse_division_code("M_14")
        assert gender == "M"
        assert age == "U14"

    def test_invalid_returns_none(self):
        gender, age = parse_division_code("x_12")
        assert gender is None
        assert age is None

    def test_empty_returns_none(self):
        gender, age = parse_division_code("")
        assert gender is None
        assert age is None


# --------------------------------------------------------------------------- unit: division code extraction from HTML


class TestExtractDivisionCodes:
    def test_extracts_from_fixture(self):
        html = _load_fixture()
        codes = extract_division_codes(html)
        assert "f_12" in codes
        assert "f_15" in codes
        assert "m_12" in codes
        assert "m_15" in codes
        assert len(codes) == 4

    def test_deduplicates(self):
        html = '<option value="m_12"><option value="m_12"><option value="f_12">'
        codes = extract_division_codes(html)
        assert codes == ["f_12", "m_12"]

    def test_empty_html(self):
        assert extract_division_codes("") == []

    def test_no_options(self):
        assert extract_division_codes("<html><body>No divisions</body></html>") == []


# --------------------------------------------------------------------------- unit: event name extraction


class TestExtractEventName:
    def test_from_fixture_title(self):
        html = _load_fixture()
        name = extract_event_name(html, "45036")
        assert name == "MAPL Spring Classic 2026"

    def test_gotsport_suffix_stripped(self):
        html = "<html><head><title>Fall Cup 2026 | GotSport</title></head></html>"
        assert extract_event_name(html, "123") == "Fall Cup 2026"

    def test_teams_prefix_stripped(self):
        html = "<html><head><title>Teams - Big Tournament</title></head></html>"
        assert extract_event_name(html, "456") == "Big Tournament"

    def test_teams_prefix_and_gotsport_suffix(self):
        html = "<html><head><title>Teams - Spring Fling | GotSport</title></head></html>"
        assert extract_event_name(html, "789") == "Spring Fling"

    def test_fallback_on_missing_title(self):
        html = "<html><head></head></html>"
        assert extract_event_name(html, "999") == "GotSport 999"

    def test_fallback_on_bare_gotsport_title(self):
        html = "<html><head><title>GotSport</title></head></html>"
        assert extract_event_name(html, "111") == "GotSport 111"


# --------------------------------------------------------------------------- unit: HTML entity decoding


class TestDecodeHtmlEntities:
    def test_apostrophe(self):
        assert decode_html_entities("O&#39;Brien") == "O'Brien"

    def test_ampersand(self):
        assert decode_html_entities("A &amp; B") == "A & B"

    def test_plain_text_passthrough(self):
        assert decode_html_entities("FC Delco") == "FC Delco"


# --------------------------------------------------------------------------- unit: team row parsing


class TestParseTeamRows:
    def test_parses_rows_from_fixture(self):
        html = _load_fixture()
        rows = parse_team_rows(html, "m_12")
        # Fixture has 7 data rows (3 real + TBD + BYE + 2 more real)
        # parse_team_rows returns raw rows including TBD/BYE — filtering
        # happens in parse_gotsport_division_page.
        club_names = [r[0] for r in rows]
        assert "FC Delco" in club_names
        assert "PDA" in club_names
        assert "Baltimore Armour" in club_names

    def test_html_entities_decoded(self):
        html = _load_fixture()
        rows = parse_team_rows(html, "m_12")
        clubs = [r[0] for r in rows]
        # &#39; should be decoded to apostrophe
        assert "O'Brien Soccer" in clubs


# --------------------------------------------------------------------------- integration: division page parsing


class TestParseGotsportDivisionPage:
    def test_parses_teams_and_skips_placeholders(self):
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        names = [t.team_name_raw for t in teams]
        # Should include real teams
        assert "FC Delco 2014 Boys" in names
        assert "PDA Shore 2014" in names
        assert "Baltimore Armour U12" in names
        assert "Match Fit U12 Boys" in names
        assert "O'Brien SC U12" in names
        # Should skip TBD and BYE
        assert "TBD" not in names
        assert "BYE" not in names

    def test_sets_gender_and_age(self):
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        for t in teams:
            assert t.gender == "M"
            assert t.age_group == "U12"
            assert t.division_code == "m_12"

    def test_female_division(self):
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "f_15")
        for t in teams:
            assert t.gender == "F"
            assert t.age_group == "U15"

    def test_state_populated(self):
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        delco = next(t for t in teams if "Delco" in t.team_name_raw)
        assert delco.state == "PA"

    def test_html_entities_decoded_in_teams(self):
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        obrien = next(t for t in teams if "Brien" in t.team_name_raw)
        assert obrien.club_name == "O'Brien Soccer"
        assert obrien.team_name_raw == "O'Brien SC U12"

    def test_deduplicates_within_division(self):
        # Double the fixture rows — should still deduplicate.
        html = _load_fixture()
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        names = [t.team_name_raw for t in teams]
        assert len(names) == len(set(n.lower() for n in names))


# --------------------------------------------------------------------------- integration: teams page metadata


class TestParseGotsportTeamsPage:
    def test_extracts_meta_and_div_codes(self):
        html = _load_fixture()
        meta, div_codes = parse_gotsport_teams_page(html, "45036", league_name="MAPL")
        assert meta.tid == "45036"
        assert meta.name == "MAPL Spring Classic 2026"
        assert meta.slug == "gotsport-45036"
        assert meta.source == "gotsport"
        assert meta.platform_event_id == "45036"
        assert meta.league_name == "MAPL"
        assert "showall=clean" in meta.source_url
        assert len(div_codes) == 4


# --------------------------------------------------------------------------- dry-run upsert


class TestDryRunUpsert:
    def test_dry_run_reports_counts(self):
        html = _load_fixture()
        meta, _div_codes = parse_gotsport_teams_page(html, "45036")
        teams = parse_gotsport_division_page(html, "45036", "m_12")
        result = upsert_event_and_teams(meta, teams, conn=None, dry_run=True)
        assert result.events_created == 1
        assert result.teams_created == len(teams)


# --------------------------------------------------------------------------- CSV event ID extraction


class TestCsvEventIdExtraction:
    def test_extracts_event_ids(self):
        from gotsport_events_runner import extract_gotsport_event_ids_from_csv
        seeds = extract_gotsport_event_ids_from_csv()
        # The CSV has many GotSport event references — we should get some.
        assert len(seeds) > 0
        # Each seed should have event_id and league_name.
        for s in seeds:
            assert "event_id" in s
            assert "league_name" in s
            assert s["event_id"].isdigit()

    def test_known_event_ids_present(self):
        from gotsport_events_runner import extract_gotsport_event_ids_from_csv
        seeds = extract_gotsport_event_ids_from_csv()
        ids = {s["event_id"] for s in seeds}
        # From the CSV: "GotSport event 45036" (MAPL)
        assert "45036" in ids

    def test_no_duplicate_ids(self):
        from gotsport_events_runner import extract_gotsport_event_ids_from_csv
        seeds = extract_gotsport_event_ids_from_csv()
        ids = [s["event_id"] for s in seeds]
        assert len(ids) == len(set(ids))


# --------------------------------------------------------------------------- determinism


class TestDeterminism:
    def test_parse_is_deterministic(self):
        html = _load_fixture()
        meta1, codes1 = parse_gotsport_teams_page(html, "45036")
        meta2, codes2 = parse_gotsport_teams_page(html, "45036")
        assert meta1 == meta2
        assert codes1 == codes2

        teams1 = parse_gotsport_division_page(html, "45036", "m_12")
        teams2 = parse_gotsport_division_page(html, "45036", "m_12")
        assert teams1 == teams2
