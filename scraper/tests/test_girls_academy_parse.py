"""
Tests for the Girls Academy / GA Aspire pure-function parser.

Covers the module-level ``parse_html`` that the ``--source replay-html``
handler (see ``run.py::_handle_replay_html``) dispatches to. Pure over
fixture HTML — no network, no DB.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.girls_academy import (  # noqa: E402
    _parse_location,
    parse_html,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures"
GA_MEMBERS_FIXTURE = FIXTURE_DIR / "girls_academy_members_sample.html"
GA_ASPIRE_FIXTURE = FIXTURE_DIR / "girls_academy_aspire_sample.html"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# _parse_location helper
# ---------------------------------------------------------------------------


def test_parse_location_short_state_code():
    assert _parse_location("Dallas Surf (Dallas, TX)") == (
        "Dallas Surf", "Dallas", "TX",
    )


def test_parse_location_long_state_name():
    assert _parse_location("Lou Fusz Athletic (St. Louis, Missouri)") == (
        "Lou Fusz Athletic", "St. Louis", "Missouri",
    )


def test_parse_location_no_parenthetical_falls_back_to_full_name():
    assert _parse_location("Some Club With No Location") == (
        "Some Club With No Location", "", "",
    )


# ---------------------------------------------------------------------------
# Fixture-driven parse_html
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture_path,source_url,league_name",
    [
        (
            GA_MEMBERS_FIXTURE,
            "https://girlsacademyleague.com/members/",
            "Girls Academy",
        ),
        (
            GA_ASPIRE_FIXTURE,
            "https://girlsacademyleague.com/aspire-membership/",
            "GA Aspire",
        ),
    ],
)
def test_parse_html_extracts_required_fields(fixture_path, source_url, league_name):
    html = _read(fixture_path)
    records = parse_html(html, source_url=source_url, league_name=league_name)

    assert len(records) >= 5, (
        f"expected >=5 clubs from {fixture_path.name}, got {len(records)}"
    )

    for rec in records:
        assert rec["club_name"], f"missing club_name: {rec}"
        # City and state are populated from the trailing "(City, ST)" on
        # every row in both fixtures — assert they are present.
        assert rec["city"], f"missing city: {rec}"
        assert rec["state"], f"missing state: {rec}"
        # Metadata passed through from the caller.
        assert rec["source_url"] == source_url
        assert rec["league_name"] == league_name


def test_parse_html_members_fixture_known_club_present():
    html = _read(GA_MEMBERS_FIXTURE)
    records = parse_html(
        html,
        source_url="https://girlsacademyleague.com/members/",
        league_name="Girls Academy",
    )
    dallas_surf = next(
        (r for r in records if r["club_name"] == "Dallas Surf"),
        None,
    )
    assert dallas_surf is not None, "Dallas Surf should appear in members fixture"
    assert dallas_surf["city"] == "Dallas"
    assert dallas_surf["state"] == "TX"
    # Conference was carried forward from the preceding <h3>.
    assert dallas_surf["conference"], "conference should be non-empty"


def test_parse_html_aspire_fixture_known_club_present():
    html = _read(GA_ASPIRE_FIXTURE)
    records = parse_html(
        html,
        source_url="https://girlsacademyleague.com/aspire-membership/",
        league_name="GA Aspire",
    )
    # ALBION SC San Diego appears at the top of the Aspire members list.
    albion = next(
        (r for r in records if r["club_name"].upper().startswith("ALBION SC SAN DIEGO")),
        None,
    )
    assert albion is not None, "ALBION SC San Diego should appear in aspire fixture"
    assert albion["state"] == "CA"


# ---------------------------------------------------------------------------
# Degenerate inputs
# ---------------------------------------------------------------------------


def test_parse_html_empty_string_returns_empty_list():
    assert parse_html("", source_url="https://x", league_name="GA") == []


def test_parse_html_no_article_returns_empty_list():
    html = "<html><body><p>No article element here.</p></body></html>"
    assert parse_html(html, source_url="https://x", league_name="GA") == []


def test_parse_html_defaults_source_url_and_league_name_to_empty():
    """parse_html must tolerate being called with only `html` — the replay
    handler falls back to positional when the kwarg signature doesn't match."""
    html = _read(GA_MEMBERS_FIXTURE)
    records = parse_html(html)
    assert len(records) >= 5
    # Defaults propagate into the records.
    assert records[0]["source_url"] == ""
    assert records[0]["league_name"] == ""
