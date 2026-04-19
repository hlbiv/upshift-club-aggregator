"""
Tests for the GotSport event-clubs HTML parser.

Covers the pure-parse half of the fetch/parse split in
``scraper/extractors/gotsport.py`` — feeds a fixture HTML page into
``parse_gotsport_event_html`` and asserts on the returned club records.
No HTTP is exercised here; live-scrape coverage lives alongside the
scraper integration tests.

Run:
    python -m pytest scraper/tests/test_gotsport_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.gotsport import (  # noqa: E402
    _parse_clubs_with_ids_from_html,
    parse_gotsport_event_html,
)

FIXTURE = Path(__file__).parent / "fixtures" / "gotsport" / "gotsport_event_sample.html"
SOURCE_URL = "https://system.gotsport.com/org_event/events/12345/clubs"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- module surface


class TestPublicSurface:
    def test_parse_is_importable(self):
        # Explicit check so the 13 downstream extractors can rely on the
        # symbol being exported once they migrate to the split helpers.
        from extractors import gotsport

        assert hasattr(gotsport, "parse_gotsport_event_html")
        assert callable(gotsport.parse_gotsport_event_html)

    def test_fetch_helper_exists(self):
        from extractors import gotsport

        assert hasattr(gotsport, "_fetch_gotsport_event")
        assert callable(gotsport._fetch_gotsport_event)

    def test_public_wrapper_still_exported(self):
        # scrape_gotsport_event is the compat wrapper the 13 callers use;
        # refactor must not rename or remove it.
        from extractors import gotsport

        assert hasattr(gotsport, "scrape_gotsport_event")
        assert callable(gotsport.scrape_gotsport_event)


# --------------------------------------------------------------------------- fixture sanity


class TestFixtureSanity:
    def test_fixture_exists(self):
        assert FIXTURE.exists(), f"Missing fixture: {FIXTURE}"

    def test_fixture_has_content(self):
        html = _load_fixture()
        assert "<table" in html
        assert "Beach FC" in html
        assert "ZZ-" in html  # confirm the filter has something to drop


# --------------------------------------------------------------------------- parse_gotsport_event_html


class TestParseGotsportEventHtml:
    def test_returns_list_of_dicts(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        assert isinstance(records, list)
        assert all(isinstance(r, dict) for r in records)

    def test_extracts_valid_clubs(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        names = [r["club_name"] for r in records]
        assert "Beach FC" in names
        assert "Pateadores Soccer Club" in names
        assert "Slammers FC HB Koge" in names
        assert "LA Galaxy Academy" in names

    def test_filters_zz_placeholder_rows(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        names = [r["club_name"] for r in records]
        assert not any(n.startswith("ZZ-") for n in names)
        assert "ZZ-Admin Placeholder" not in names

    def test_strips_schedule_suffix(self):
        # The first column text can be "Beach FCSchedule" when the row
        # contains a "Schedule" link; parser must strip it.
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        names = [r["club_name"] for r in records]
        assert "Beach FCSchedule" not in names
        assert "Beach FC" in names

    def test_drops_short_and_empty_names(self):
        # The fixture contains a row with name "A" (len < 2) and an
        # empty row; both must be dropped.
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        names = [r["club_name"] for r in records]
        assert "A" not in names
        assert "" not in names

    def test_stamps_source_url(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="Test League")

        assert records, "fixture should yield at least one record"
        for rec in records:
            assert rec["source_url"] == SOURCE_URL

    def test_stamps_league_name(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="NPL Boys")

        for rec in records:
            assert rec["league_name"] == "NPL Boys"

    def test_single_state_propagates(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(
            html, SOURCE_URL, league_name="L", state="CA"
        )

        for rec in records:
            assert rec["state"] == "CA"
            assert "_state_derived" not in rec

    def test_multi_state_blanks_state_and_marks_derived(self):
        html = _load_fixture()
        records = parse_gotsport_event_html(
            html, SOURCE_URL, league_name="L", state="CA", multi_state=True
        )

        for rec in records:
            assert rec["state"] == ""
            assert rec.get("_state_derived") is True

    def test_empty_html_returns_empty_list(self):
        records = parse_gotsport_event_html("", SOURCE_URL, league_name="L")
        assert records == []

    def test_html_with_no_rows_returns_empty_list(self):
        html = "<html><body><table></table></body></html>"
        records = parse_gotsport_event_html(html, SOURCE_URL, league_name="L")
        assert records == []

    def test_record_shape_matches_legacy(self):
        # Guards against accidental key churn — the 13 caller extractors
        # pipe these dicts straight into the normalizer.
        html = _load_fixture()
        records = parse_gotsport_event_html(
            html, SOURCE_URL, league_name="L", state="CA"
        )

        assert records
        rec = records[0]
        assert set(rec.keys()) == {
            "club_name", "league_name", "city", "state", "source_url",
        }
        assert rec["city"] == ""

    def test_parse_is_pure(self):
        # Invoking twice on the same HTML must yield identical output.
        html = _load_fixture()
        a = parse_gotsport_event_html(html, SOURCE_URL, league_name="L", state="CA")
        b = parse_gotsport_event_html(html, SOURCE_URL, league_name="L", state="CA")
        assert a == b


# --------------------------------------------------------------------------- _parse_clubs_with_ids_from_html


class TestParseClubsWithIds:
    def test_extracts_club_id_from_link(self):
        html = _load_fixture()
        pairs = _parse_clubs_with_ids_from_html(html)

        by_name = dict(pairs)
        assert by_name.get("Beach FC") == "1001"
        assert by_name.get("Pateadores Soccer Club") == "1002"
        assert by_name.get("Slammers FC HB Koge") == "1003"
        assert by_name.get("LA Galaxy Academy") == "1004"

    def test_filters_zz_rows(self):
        html = _load_fixture()
        pairs = _parse_clubs_with_ids_from_html(html)

        assert not any(name.startswith("ZZ-") for name, _ in pairs)
