"""
Fixture tests for the state-association extractor pure-function parsers.

Covers the three sub-parsers migrated for ``--source replay-html`` plus
the top-level :func:`extractors.state_assoc.parse_html` dispatcher:

* ``gotsport``      → ``parse_gotsport_html`` (via an ``*.gotsport.com`` URL)
* ``html_club_list``→ ``parse_html_club_list_html`` (OYSA / PA West pages)
* ``soccerwire``    → ``parse_soccerwire_html`` (individual club page)

Skipped sub-scrapers (documented in the module docstring): ``google_maps``
(KML/XML feed), ``js_club_list``, ``html_club_page``, ``curated_seed``.

Run:
    python -m pytest scraper/tests/test_state_assoc_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import state_assoc  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"
GOTSPORT_FIXTURE = FIXTURES / "state_assoc_gotsport_sample.html"
HTML_LIST_FIXTURE = FIXTURES / "state_assoc_html_club_list_sample.html"
SOCCERWIRE_FIXTURE = FIXTURES / "state_assoc_soccerwire_sample.html"

# Canonical URLs that the state-assoc dispatcher should route correctly.
GOTSPORT_URL = "https://system.gotsport.com/org_event/events/49334/clubs"
OYSA_URL = "https://www.oregonyouthsoccer.org/find-a-club/"
SOCCERWIRE_URL = "https://www.soccerwire.com/club/honolulu-bulls-soccer-club/"


def _load(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# PARSERS registry
# ---------------------------------------------------------------------------

def test_parsers_registry_exposes_three_sub_parsers():
    assert set(state_assoc.PARSERS.keys()) == {
        "gotsport",
        "html_club_list",
        "soccerwire",
    }
    for key, fn in state_assoc.PARSERS.items():
        assert callable(fn), f"PARSERS[{key!r}] is not callable"


# ---------------------------------------------------------------------------
# GotSport sub-parser
# ---------------------------------------------------------------------------

def test_parse_gotsport_html_returns_clubs_and_filters_zz():
    records = state_assoc.parse_gotsport_html(
        _load(GOTSPORT_FIXTURE),
        source_url=GOTSPORT_URL,
        league_name="Alabama State Soccer Association",
        state="Alabama",
    )

    assert isinstance(records, list)
    assert len(records) == 5, f"expected 5 clubs, got {len(records)}"

    names = {r["club_name"] for r in records}
    assert "Birmingham United" in names
    assert "Huntsville FC" in names
    assert not any(n.startswith("ZZ-") for n in names)

    for rec in records:
        assert rec["state"] == "Alabama"
        assert rec["source_url"] == GOTSPORT_URL
        assert rec["league_name"] == "Alabama State Soccer Association"
        assert rec["city"] == ""


def test_parse_gotsport_html_multi_state_leaves_state_empty():
    records = state_assoc.parse_gotsport_html(
        _load(GOTSPORT_FIXTURE),
        source_url=GOTSPORT_URL,
        league_name="Multi-State Showcase",
        state="Alabama",
        multi_state=True,
    )
    assert len(records) == 5
    assert all(r["state"] == "" for r in records)
    assert all(r.get("_state_derived") is True for r in records)


def test_parse_gotsport_html_empty_inputs():
    assert state_assoc.parse_gotsport_html("") == []
    assert state_assoc.parse_gotsport_html(
        "<html><body>nothing here</body></html>",
        source_url=GOTSPORT_URL,
    ) == []


# ---------------------------------------------------------------------------
# HTML club-list sub-parser
# ---------------------------------------------------------------------------

def test_parse_html_club_list_html_extracts_club_names():
    records = state_assoc.parse_html_club_list_html(
        _load(HTML_LIST_FIXTURE),
        source_url=OYSA_URL,
        league_name="Oregon Youth Soccer Association",
        state="Oregon",
    )

    assert isinstance(records, list)
    # At least the real club lines (skip chrome + dash-prefixed sections).
    assert len(records) >= 6, f"expected ≥6 clubs, got {len(records)}"

    names = {r["club_name"] for r in records}
    assert "Portland Timbers Academy" in names
    assert "Eugene Metro FC" in names
    assert "Westside Metros" in names
    assert "Salem United Soccer" in names
    assert "Bend FC Timbers" in names

    # Chrome/nav/skip-phrase lines must not leak in.
    assert "Skip to main content" not in names
    assert "Privacy Policy" not in names
    assert "SportsEngine, Inc." not in names
    # "MEMBER CLUBS" is an all-uppercase section header without any known
    # club keyword, so it should also be filtered out.
    assert "MEMBER CLUBS" not in names
    # Lines starting with a dash are suppressed.
    assert not any(n.startswith("-") or n.startswith("–") for n in names)

    for rec in records:
        assert rec["state"] == "Oregon"
        assert rec["source_url"] == OYSA_URL
        assert rec["league_name"] == "Oregon Youth Soccer Association"
        assert rec["city"] == ""


def test_parse_html_club_list_html_respects_custom_skip_phrases():
    # Custom skip_phrases should filter specific names even if they'd
    # otherwise survive the default heuristic.
    records = state_assoc.parse_html_club_list_html(
        _load(HTML_LIST_FIXTURE),
        source_url=OYSA_URL,
        league_name="Oregon Youth Soccer Association",
        state="Oregon",
        skip_phrases=["bend fc", "westside"],
    )
    names = {r["club_name"] for r in records}
    assert "Bend FC Timbers" not in names
    assert "Westside Metros" not in names
    assert "Portland Timbers Academy" in names


def test_parse_html_club_list_html_empty_input():
    assert state_assoc.parse_html_club_list_html("") == []


# ---------------------------------------------------------------------------
# SoccerWire sub-parser
# ---------------------------------------------------------------------------

def test_parse_soccerwire_html_returns_one_club():
    records = state_assoc.parse_soccerwire_html(
        _load(SOCCERWIRE_FIXTURE),
        source_url=SOCCERWIRE_URL,
        league_name="Hawaii State Youth Soccer Association",
        state="Hawaii",
    )

    assert isinstance(records, list)
    assert len(records) == 1
    rec = records[0]
    assert rec["club_name"] == "Honolulu Bulls Soccer Club"
    # SoccerWire's parser returns the 2-letter code for state.
    assert rec["state"] == "HI"
    assert rec["city"] == "Honolulu"
    assert rec["source_url"] == SOCCERWIRE_URL
    assert rec["league_name"] == "Hawaii State Youth Soccer Association"


def test_parse_soccerwire_html_state_mismatch_drops_record():
    # Target state doesn't match the parsed club's state → empty.
    records = state_assoc.parse_soccerwire_html(
        _load(SOCCERWIRE_FIXTURE),
        source_url=SOCCERWIRE_URL,
        league_name="Alabama State Soccer Association",
        state="Alabama",
    )
    assert records == []


def test_parse_soccerwire_html_no_state_returns_record():
    # When no state filter is supplied, the parsed record passes through.
    records = state_assoc.parse_soccerwire_html(
        _load(SOCCERWIRE_FIXTURE),
        source_url=SOCCERWIRE_URL,
        league_name="",
    )
    assert len(records) == 1
    assert records[0]["state"] == "HI"


# ---------------------------------------------------------------------------
# Top-level parse_html dispatcher
# ---------------------------------------------------------------------------

def test_parse_html_dispatches_gotsport_url_to_gotsport_parser():
    records = state_assoc.parse_html(
        _load(GOTSPORT_FIXTURE),
        source_url=GOTSPORT_URL,
        league_name="Alabama State Soccer Association",
    )
    assert isinstance(records, list)
    assert len(records) == 5
    names = {r["club_name"] for r in records}
    assert "Birmingham United" in names
    assert not any(n.startswith("ZZ-") for n in names)
    # State comes from no-op default (GotSport URL has no state hint), so
    # records get state="" on replay. The live path fills this in from
    # state_assoc_config.json.
    assert all(r["source_url"] == GOTSPORT_URL for r in records)


def test_parse_html_dispatches_oysa_url_to_html_club_list_parser():
    records = state_assoc.parse_html(
        _load(HTML_LIST_FIXTURE),
        source_url=OYSA_URL,
        league_name="Oregon Youth Soccer Association",
    )
    assert isinstance(records, list)
    names = {r["club_name"] for r in records}
    assert "Portland Timbers Academy" in names
    assert "Eugene Metro FC" in names
    # State is looked up from the state_assoc_config domain map.
    assert all(r["state"] == "Oregon" for r in records)


def test_parse_html_dispatches_soccerwire_url_to_soccerwire_parser():
    records = state_assoc.parse_html(
        _load(SOCCERWIRE_FIXTURE),
        source_url=SOCCERWIRE_URL,
        league_name="Hawaii State Youth Soccer Association",
    )
    # The dispatcher only has the club URL, not a state-assoc URL, so the
    # URL-based state lookup returns "" and no state filter is applied —
    # we get the one parsed record.
    assert len(records) == 1
    assert records[0]["club_name"] == "Honolulu Bulls Soccer Club"
    assert records[0]["state"] == "HI"


def test_parse_html_unknown_url_returns_empty_list():
    # URL hostname is not mapped to any parser → skip.
    records = state_assoc.parse_html(
        _load(GOTSPORT_FIXTURE),
        source_url="https://example.com/unrelated/page",
        league_name="Anything",
    )
    assert records == []


def test_parse_html_empty_html_returns_empty_list():
    assert state_assoc.parse_html("", source_url=GOTSPORT_URL) == []
    assert state_assoc.parse_html("", source_url="") == []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def test_detect_source_type_hostname_mapping():
    assert state_assoc._detect_source_type(GOTSPORT_URL) == "gotsport"
    assert state_assoc._detect_source_type(
        "https://system.gotsport.com/org_event/events/99/clubs"
    ) == "gotsport"
    assert state_assoc._detect_source_type(SOCCERWIRE_URL) == "soccerwire"
    assert state_assoc._detect_source_type(OYSA_URL) == "html_club_list"
    assert state_assoc._detect_source_type(
        "https://www.pawest-soccer.org/club-list/"
    ) == "html_club_list"
    assert state_assoc._detect_source_type("https://example.com") == ""
    assert state_assoc._detect_source_type("") == ""


def test_state_for_url_resolves_configured_domains():
    # OYSA and PA West are declared as html_club_list in the config.
    assert state_assoc._state_for_url(OYSA_URL) == "Oregon"
    assert state_assoc._state_for_url(
        "https://www.pawest-soccer.org/club-list/"
    ) == "PA West"
    # Unknown domains resolve to empty string, never raise.
    assert state_assoc._state_for_url("https://example.com") == ""
    assert state_assoc._state_for_url("") == ""
