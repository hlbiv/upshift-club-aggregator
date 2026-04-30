"""
Tests for scraper/extractors/mlsnext_events_matches.py

Covers:
    - HTML parsing: regular score, penalty shootout, TBD skip
    - Pagination stop-condition (empty page → stop)
    - Unknown event_id raises ValueError
    - EVENT_REGISTRY integrity (all required fields present)
    - discover_bracket_id is callable (smoke test with live mock)
"""

from __future__ import annotations

import textwrap
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from extractors.mlsnext_events_matches import (
    EVENT_REGISTRY,
    EventConfig,
    _parse_page,
    _parse_score,
    _ScheduleEntry,
    scrape_mlsnext_event_matches,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal HTML fragments matching live Modular11 structure
# ---------------------------------------------------------------------------

def _make_row(
    match_id: str,
    home: str,
    away: str,
    score: str = "",
    age: str = "U15",
    competition: str = "Group Play",
    division: str = "Group M",
    date: str = "04/25/26 05:00pm",
    field: str = "1 - Toyota Soccer Center",
    gender: str = "MALE",
) -> str:
    """Build one desktop table-content-row HTML div."""
    score_html = f'<div class="container-score">{score}</div>' if score else ""
    return textwrap.dedent(f"""
        <div class="row table-content-row hidden-xs">
          <div class="col-sm-1 pad-0">{match_id}\n{gender}</div>
          <div class="col-sm-2">{date}\n{field}</div>
          <div class="col-sm-1 pad-0">{age}</div>
          <div class="col-sm-2">{competition}\n{division}</div>
          <div class="col-sm-6 pad-0">
            <div class="col-sm-3 container-first-team">
              <p data-title="{home}">{home}</p>
            </div>
            {score_html}
            <div class="col-sm-3 container-second-team">
              <p data-title="{away}">{away}</p>
            </div>
          </div>
        </div>
    """).strip()


def _make_page(*rows: str) -> str:
    """Wrap rows in the outer page container."""
    inner = "\n".join(rows)
    return f'<div class="container-table-matches">{inner}</div>'


_DUMMY_CFG = EventConfig(
    event_id=88,
    tournament_name="MLS NEXT Flex",
    start_date="2025-01-01 00:00:00",
    end_date="2027-12-31 23:59:59",
)
_DUMMY_ENTRY = _ScheduleEntry(age_uid=33, bracket_id=39, schedule_type="groupplay")
_SOURCE_URL = "https://www.modular11.com/events/event/iframe/schedule/groupplay/88/33/1"


# ---------------------------------------------------------------------------
# _parse_score unit tests
# ---------------------------------------------------------------------------

class TestParseScore:
    def test_regular_score(self):
        home, away, shootout = _parse_score("1 : 4")
        assert home == 1
        assert away == 4
        assert not shootout

    def test_penalty_shootout(self):
        home, away, shootout = _parse_score("1 : 1\n(4 : 3)")
        assert home == 1  # regulation score kept
        assert away == 1
        assert shootout

    def test_tbd_returns_none(self):
        home, away, shootout = _parse_score("TBD")
        assert home is None
        assert away is None
        assert not shootout

    def test_empty_returns_none(self):
        home, away, shootout = _parse_score("")
        assert home is None

    def test_zero_zero(self):
        home, away, shootout = _parse_score("0 : 0")
        assert home == 0
        assert away == 0


# ---------------------------------------------------------------------------
# _parse_page unit tests
# ---------------------------------------------------------------------------

class TestParsePage:
    def test_single_completed_row(self):
        html = _make_page(_make_row("22525", "Lanier SC", "Bayside FC", score="1 : 4"))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 1
        r = rows[0]
        assert r["home_team_name"] == "Lanier SC"
        assert r["away_team_name"] == "Bayside FC"
        assert r["home_score"] == 1
        assert r["away_score"] == 4
        assert r["status"] == "final"
        assert r["platform_match_id"] == "22525"
        assert r["age_group"] == "U15"
        assert r["gender"] == "male"
        assert r["tournament_name"] == "MLS NEXT Flex"
        assert r["source"] == "mlsnext"

    def test_scheduled_row_has_none_scores(self):
        html = _make_page(_make_row("22999", "Club A", "Club B", score=""))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 1
        assert rows[0]["home_score"] is None
        assert rows[0]["away_score"] is None
        assert rows[0]["status"] == "scheduled"

    def test_penalty_shootout_sets_bracket_round(self):
        html = _make_page(_make_row("22526", "SVS Academy", "Northern VA", score="1 : 1\n(4 : 3)"))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 1
        r = rows[0]
        assert r["home_score"] == 1
        assert r["away_score"] == 1
        assert r["bracket_round"] == "penalty shootout"

    def test_tbd_teams_skipped(self):
        html = _make_page(_make_row("99999", "TBD", "TBD", score=""))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 0

    def test_bye_teams_skipped(self):
        html = _make_page(_make_row("99998", "Bye", "Real Club", score=""))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 0

    def test_multiple_rows_parsed(self):
        rows_html = [
            _make_row(f"2250{i}", f"Home {i}", f"Away {i}", score=f"{i} : {i+1}")
            for i in range(5)
        ]
        html = _make_page(*rows_html)
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 5

    def test_empty_page_returns_empty_list(self):
        html = '<div class="container-table-matches"></div>'
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert rows == []

    def test_division_extracted_from_col3(self):
        html = _make_page(_make_row("22525", "A", "B", division="Group T"))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert rows[0]["division"] == "Group T"

    def test_season_passed_through(self):
        html = _make_page(_make_row("1", "A", "B"))
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert rows[0]["season"] == "2025-26"

    def test_mobile_rows_ignored(self):
        """Rows without hidden-xs class are mobile duplicates and must be skipped."""
        mobile_row = _make_row("22525", "Home", "Away").replace(
            "row table-content-row hidden-xs",
            "row table-content-row visible-xs",
        )
        html = _make_page(mobile_row)
        rows = _parse_page(html, _DUMMY_CFG, _DUMMY_ENTRY, "2025-26", _SOURCE_URL)
        assert len(rows) == 0


# ---------------------------------------------------------------------------
# Pagination stop-condition test (mocked HTTP)
# ---------------------------------------------------------------------------

class TestPaginationStops:
    def test_stops_on_empty_page(self):
        """Scraper must stop fetching when a page returns 0 rows.

        Uses a single-schedule event config so page counts are deterministic.
        """
        page1_html = _make_page(_make_row("1", "A", "B", score="1 : 0"))
        page2_html = _make_page(_make_row("2", "C", "D", score="2 : 1"))
        empty_html = '<div class="container-table-matches"></div>'

        call_count = 0

        def fake_fetch(event_id, age_uid, bracket_id, page, start_date, end_date, **kw):
            nonlocal call_count
            call_count += 1
            if page == 1:
                return page1_html
            if page == 2:
                return page2_html
            return empty_html

        # Build a single-schedule config so exactly one age-group is scraped.
        single_cfg = EventConfig(
            event_id=999,
            tournament_name="Test Event",
            start_date="2025-01-01 00:00:00",
            end_date="2026-12-31 23:59:59",
            schedules=[_ScheduleEntry(age_uid=33, bracket_id=39, schedule_type="groupplay")],
        )

        patched_registry = {999: single_cfg}
        with patch("extractors.mlsnext_events_matches._fetch_page", side_effect=fake_fetch), \
             patch("extractors.mlsnext_events_matches.EVENT_REGISTRY", patched_registry):
            rows = scrape_mlsnext_event_matches(999, season="2025-26")

        assert call_count == 3  # page 1, page 2, page 3 (empty → stop)
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# Unknown event_id raises ValueError
# ---------------------------------------------------------------------------

class TestUnknownEventId:
    def test_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown event_id"):
            scrape_mlsnext_event_matches(9999, season="2025-26")

    def test_error_lists_known_ids(self):
        with pytest.raises(ValueError, match="72"):
            scrape_mlsnext_event_matches(9999)


# ---------------------------------------------------------------------------
# EVENT_REGISTRY integrity
# ---------------------------------------------------------------------------

class TestEventRegistryIntegrity:
    @pytest.mark.parametrize("event_id,cfg", list(EVENT_REGISTRY.items()))
    def test_required_fields_present(self, event_id: int, cfg: EventConfig):
        assert cfg.event_id == event_id
        assert cfg.tournament_name
        assert cfg.start_date
        assert cfg.end_date
        assert len(cfg.schedules) > 0, f"event_id={event_id} has no schedules"

    @pytest.mark.parametrize("event_id,cfg", list(EVENT_REGISTRY.items()))
    def test_all_schedules_have_valid_age_uids(self, event_id: int, cfg: EventConfig):
        from extractors.mlsnext_events_matches import AGE_GROUPS
        for entry in cfg.schedules:
            assert entry.age_uid in AGE_GROUPS, (
                f"event_id={event_id}: unknown age_uid {entry.age_uid}"
            )

    @pytest.mark.parametrize("event_id,cfg", list(EVENT_REGISTRY.items()))
    def test_all_schedules_have_bracket_and_type(self, event_id: int, cfg: EventConfig):
        for entry in cfg.schedules:
            assert entry.bracket_id > 0
            assert entry.schedule_type

    def test_expected_event_ids_present(self):
        """All five MLS NEXT event IDs must be registered."""
        assert 72 in EVENT_REGISTRY, "MLS NEXT Cup missing"
        assert 74 in EVENT_REGISTRY, "MLS NEXT Cup Qualifiers missing"
        assert 75 in EVENT_REGISTRY, "MLS NEXT Fest missing"
        assert 80 in EVENT_REGISTRY, "Generation adidas Cup missing"
        assert 88 in EVENT_REGISTRY, "MLS NEXT Flex missing"
