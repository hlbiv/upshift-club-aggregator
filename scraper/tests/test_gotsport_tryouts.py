"""
Tests for the GotSport tryout extractor.

Covers: division code parsing, division code extraction, event name
extraction, date parsing, HTML entity decoding, tryout keyword detection,
full page parse integration, and determinism.
"""

from __future__ import annotations

import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.gotsport_tryouts import (  # noqa: E402
    parse_division_code,
    extract_division_codes,
    extract_event_name,
    parse_date_from_text,
    decode_html_entities,
    TRYOUT_KEYWORDS,
    parse_gotsport_tryout_page,
    parse_gotsport_tryout_division,
)

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "gotsport")


def _load_fixture(name: str) -> str:
    with open(os.path.join(FIXTURES_DIR, name), "r") as f:
        return f.read()


# ── Division code parsing ─────────────────────────────────────────────

class TestParseDivisionCode:
    def test_male_u12(self):
        gender, age = parse_division_code("m_12")
        assert gender == "M"
        assert age == "U12"

    def test_female_u15(self):
        gender, age = parse_division_code("f_15")
        assert gender == "F"
        assert age == "U15"

    def test_uppercase(self):
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


# ── Division code extraction from HTML ────────────────────────────────

class TestExtractDivisionCodes:
    def test_extracts_from_fixture(self):
        html = _load_fixture("sample_tryout_page.html")
        codes = extract_division_codes(html)
        assert len(codes) == 3
        assert "m_12" in [c.lower() for c in codes]
        assert "m_14" in [c.lower() for c in codes]
        assert "f_12" in [c.lower() for c in codes]

    def test_deduplication(self):
        html = '<option value="m_12"><option value="m_12"><option value="f_14">'
        codes = extract_division_codes(html)
        assert len(codes) == 2

    def test_empty_html(self):
        codes = extract_division_codes("")
        assert codes == []


# ── Event name extraction ─────────────────────────────────────────────

class TestExtractEventName:
    def test_from_h1(self):
        html = "<h1>Spring Tryout Showcase 2026</h1>"
        assert extract_event_name(html) == "Spring Tryout Showcase 2026"

    def test_from_title(self):
        html = "<title>GotSport - Fall Tryout Event</title>"
        name = extract_event_name(html)
        assert name is not None
        assert "Tryout" in name

    def test_no_name(self):
        html = "<html><body>No heading here</body></html>"
        assert extract_event_name(html) is None


# ── Date parsing ──────────────────────────────────────────────────────

class TestParseDateFromText:
    def test_iso_format(self):
        assert parse_date_from_text("2026-08-10") == "2026-08-10"

    def test_us_format(self):
        assert parse_date_from_text("08/10/2026") == "2026-08-10"

    def test_month_name_format(self):
        result = parse_date_from_text("August 10, 2026")
        assert result == "2026-08-10"

    def test_abbreviated_month(self):
        result = parse_date_from_text("Aug 10, 2026")
        assert result == "2026-08-10"

    def test_no_date(self):
        assert parse_date_from_text("no date here") is None

    def test_empty(self):
        assert parse_date_from_text("") is None


# ── HTML entity decoding ──────────────────────────────────────────────

class TestDecodeHtmlEntities:
    def test_apostrophe(self):
        assert decode_html_entities("O&#39;Brien") == "O'Brien"

    def test_ampersand(self):
        assert decode_html_entities("A &amp; B") == "A & B"

    def test_plain_text(self):
        assert decode_html_entities("plain text") == "plain text"


# ── Tryout keyword detection ─────────────────────────────────────────

class TestTryoutKeywords:
    def test_tryout_matches(self):
        assert TRYOUT_KEYWORDS.search("ECNL Tryout Showcase 2026")

    def test_combine_matches(self):
        assert TRYOUT_KEYWORDS.search("Fall Combine Event")

    def test_id_camp_matches(self):
        assert TRYOUT_KEYWORDS.search("Summer ID Camp")

    def test_open_practice_matches(self):
        assert TRYOUT_KEYWORDS.search("Open Practice Session")

    def test_no_match(self):
        assert not TRYOUT_KEYWORDS.search("Regular League Match Day 5")


# ── Full page parse integration ───────────────────────────────────────

class TestParseGotsportTryoutPage:
    def test_extracts_divisions(self):
        html = _load_fixture("sample_tryout_page.html")
        result = parse_gotsport_tryout_page(html, "99999")
        assert result["event_name"] == "ECNL Tryout Showcase 2026"
        assert len(result["division_codes"]) == 3

    def test_extracts_event_date(self):
        html = _load_fixture("sample_tryout_page.html")
        result = parse_gotsport_tryout_page(html, "99999")
        assert result.get("event_date") == "2026-08-10"


# ── Division page parse ──────────────────────────────────────────────

class TestParseGotsportTryoutDivision:
    def test_extracts_teams(self):
        html = _load_fixture("sample_tryout_page.html")
        rows = parse_gotsport_tryout_division(html, "99999", "m_12")
        # Should get 4 real clubs (Springfield FC, Capital United, Metro SC, O'Brien Soccer)
        # TBD and BYE should be filtered out
        assert len(rows) == 4
        club_names = [r["club_name_raw"] for r in rows]
        assert "Springfield FC" in club_names
        assert "Capital United" in club_names
        assert "Metro SC" in club_names

    def test_obrien_decoded(self):
        html = _load_fixture("sample_tryout_page.html")
        rows = parse_gotsport_tryout_division(html, "99999", "m_12")
        club_names = [r["club_name_raw"] for r in rows]
        # HTML entity &#39; should be decoded
        assert any("O'Brien" in c for c in club_names)

    def test_row_shape(self):
        html = _load_fixture("sample_tryout_page.html")
        rows = parse_gotsport_tryout_division(html, "99999", "m_12")
        assert len(rows) > 0
        row = rows[0]
        assert "club_name_raw" in row
        assert "source" in row
        assert row["source"] == "gotsport"
        assert row["gender"] == "M"
        assert row["age_group"] == "U12"

    def test_tbd_bye_filtered(self):
        html = _load_fixture("sample_tryout_page.html")
        rows = parse_gotsport_tryout_division(html, "99999", "m_12")
        club_names = [r["club_name_raw"].lower() for r in rows]
        assert "tbd" not in club_names
        assert "bye" not in club_names


# ── Determinism ───────────────────────────────────────────────────────

class TestDeterminism:
    def test_same_input_same_output(self):
        html = _load_fixture("sample_tryout_page.html")
        r1 = parse_gotsport_tryout_division(html, "99999", "m_12")
        r2 = parse_gotsport_tryout_division(html, "99999", "m_12")
        assert r1 == r2
