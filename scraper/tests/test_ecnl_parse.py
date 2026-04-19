"""
Fixture tests for extractors.ecnl pure-function parsers.

Covers both dispatch paths exposed to ``--source replay-html``:

* Dropdown discovery page (event_id=0) → ``parse_event_select_dropdown_html``
  returns ``(event_id, conference_name)`` tuples and ``parse_html`` returns
  ``[]`` (discovery metadata doesn't produce club rows).
* Per-conference standings (event_id>0) → ``parse_conference_standings_html``
  returns one record per team row, and ``parse_html`` collapses that down
  to the ``{club_name, league_name, city, state, source_url}`` shape the
  clubs pipeline consumes.

Run:
    python -m pytest scraper/tests/test_ecnl_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import ecnl  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"
DROPDOWN_FIXTURE = FIXTURES / "ecnl_dropdown_sample.html"
STANDINGS_FIXTURE = FIXTURES / "ecnl_standings_sample.html"

# Canonical URL shapes — documented in the ecnl.py module docstring.
DROPDOWN_URL = (
    "https://api.athleteone.com/api/Script/"
    "get-conference-standings/0/12/70/0/0"
)
STANDINGS_URL = (
    "https://api.athleteone.com/api/Script/"
    "get-conference-standings/12345/12/70/0/0"
)


def _load(fixture: Path) -> str:
    return fixture.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Dropdown parser
# ---------------------------------------------------------------------------

def test_parse_event_select_dropdown_html_returns_tuples():
    events = ecnl.parse_event_select_dropdown_html(_load(DROPDOWN_FIXTURE))

    assert isinstance(events, list)
    # 5 valid options (value="0" + value="" are filtered out)
    assert len(events) == 5

    for event_id, conf_name in events:
        assert event_id and event_id != "0"
        assert conf_name

    event_ids = {e[0] for e in events}
    assert event_ids == {"12345", "12346", "12347", "12348", "12349"}

    names = {e[1] for e in events}
    assert "ECNL Boys Far West 2025-26" in names


def test_parse_event_select_dropdown_html_empty_input():
    assert ecnl.parse_event_select_dropdown_html("") == []
    assert ecnl.parse_event_select_dropdown_html("<html><body>no select</body></html>") == []


# ---------------------------------------------------------------------------
# Per-conference standings parser
# ---------------------------------------------------------------------------

def test_parse_conference_standings_html_returns_team_records():
    records = ecnl.parse_conference_standings_html(
        _load(STANDINGS_FIXTURE),
        league_name="ECNL Boys",
        source_url=STANDINGS_URL,
        org_season_id="70",
        event_id="12345",
        conf_name="ECNL Boys Far West 2025-26",
    )

    assert isinstance(records, list)
    # 4 matching club rows; "Random Non-Matching Row" and the colspan footer skip.
    assert len(records) == 4

    first = records[0]
    # Every record must have the full documented shape.
    expected_keys = {
        "club_name", "team_name_raw", "age_group", "gender", "conference",
        "org_season_id", "event_id", "club_id", "team_id", "qualification",
        "rank", "gp", "w", "l", "d", "gf", "ga", "gd", "ppg", "pts",
        "source_url", "league_name",
    }
    assert expected_keys.issubset(first.keys())

    # Standings data is carried through verbatim.
    assert first["rank"] == "1"
    assert first["gp"] == "10"
    assert first["pts"] == "25"
    assert first["league_name"] == "ECNL Boys"
    assert first["org_season_id"] == "70"
    assert first["event_id"] == "12345"
    assert first["conference"] == "ECNL Boys Far West 2025-26"
    assert first["source_url"] == STANDINGS_URL

    # Club name extraction strips the " ECNL B13..." suffix.
    names = {r["club_name"] for r in records}
    assert "Oregon Premier" in names
    assert "Florida Kraze" in names
    assert "Seattle United" in names
    assert "San Diego Surf" in names  # ECNL RL stripped too

    # Age + gender decoded from the raw team name.
    oregon = next(r for r in records if r["club_name"] == "Oregon Premier")
    assert oregon["age_group"] == "13"
    assert oregon["gender"] == "Male"
    assert oregon["club_id"] == "101"
    assert oregon["team_id"] == "1001"

    # Qualification is split from the raw name.
    florida = next(r for r in records if r["club_name"] == "Florida Kraze")
    assert florida["qualification"].startswith("Champions League")

    surf = next(r for r in records if r["club_name"] == "San Diego Surf")
    assert surf["age_group"] == "12"
    assert surf["gender"] == "Female"


def test_parse_conference_standings_html_empty_input():
    assert ecnl.parse_conference_standings_html("") == []
    assert ecnl.parse_conference_standings_html("<html><body></body></html>") == []


# ---------------------------------------------------------------------------
# Top-level parse_html dispatcher
# ---------------------------------------------------------------------------

def test_parse_html_dispatches_dropdown_url_to_empty_list():
    """
    The dropdown snapshot is discovery metadata — it has no club rows to
    feed the clubs pipeline, so parse_html returns []. The parser is
    still exercised internally so a malformed snapshot would surface.
    """
    records = ecnl.parse_html(
        _load(DROPDOWN_FIXTURE),
        source_url=DROPDOWN_URL,
        league_name="ECNL Boys",
    )
    assert records == []


def test_parse_html_dispatches_standings_url_to_club_records():
    records = ecnl.parse_html(
        _load(STANDINGS_FIXTURE),
        source_url=STANDINGS_URL,
        league_name="ECNL Boys",
    )

    # Per-conference standings with 4 unique club rows (all 4 teams are
    # from distinct clubs in the fixture).
    assert isinstance(records, list)
    assert len(records) == 4

    for r in records:
        # Match the shape documented for the clubs pipeline.
        assert set(r.keys()) == {
            "club_name", "league_name", "city", "state", "source_url"
        }
        assert r["club_name"]
        assert r["league_name"] == "ECNL Boys"
        assert r["source_url"] == STANDINGS_URL
        assert r["city"] == ""
        assert r["state"] == ""

    names = {r["club_name"] for r in records}
    assert names == {
        "Oregon Premier",
        "Florida Kraze",
        "Seattle United",
        "San Diego Surf",
    }


def test_parse_html_empty_inputs_return_empty_list():
    """Both empty HTML and empty URL should return [] without raising."""
    assert ecnl.parse_html("") == []
    assert ecnl.parse_html("", source_url=STANDINGS_URL) == []
    assert ecnl.parse_html(
        "<html><body>nothing interesting</body></html>",
        source_url=STANDINGS_URL,
    ) == []


def test_parse_html_unknown_url_falls_back_to_standings_parser():
    """
    A URL that matches neither dispatch regex shouldn't raise — we
    best-effort re-parse it as a standings page. With no matching
    standings rows, the result is [].
    """
    records = ecnl.parse_html(
        _load(STANDINGS_FIXTURE),
        source_url="https://example.com/unrelated/page",
        league_name="ECNL Boys",
    )
    # Standings HTML is still valid — should still yield the clubs.
    assert len(records) == 4
