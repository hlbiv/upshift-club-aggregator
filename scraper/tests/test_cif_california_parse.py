"""
Tests for the CIF California state-tournament extractor.

Pure-function parsing over three fixture shapes:

  * bracket — fixtures/cif_california_bracket_sample.html
  * results — fixtures/cif_california_results_sample.html
  * rankings — fixtures/cif_california_rankings_sample.html

No network, no DB. The parser returns a dict
``{"matches": [...], "rankings": [...]}`` so a single page with both
shapes can emit both — every test here uses a single-shape fixture
for clarity.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.cif_california import parse_cif_california_html  # noqa: E402


FIXTURES = Path(__file__).parent / "fixtures"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Bracket pages — fixtures with no scores yet
# ---------------------------------------------------------------------------


def test_bracket_extracts_all_valid_matches():
    html = _read("cif_california_bracket_sample.html")
    source_url = "https://www.cifstate.org/sports/boys_soccer/state/bracket.html"
    out = parse_cif_california_html(html, source_url=source_url)

    # Fixture has 5 body rows; 2 are malformed (missing opponent / both
    # schools) and must drop. 3 remain.
    assert len(out["matches"]) == 3, f"got {len(out['matches'])}: {out['matches']}"
    assert out["rankings"] == []

    names = [(r["school_name_raw"], r["opponent_raw"]) for r in out["matches"]]
    assert ("Bellarmine College Prep", "De La Salle") in names
    assert ("Mountain View", "Los Gatos") in names
    assert ("Bellarmine College Prep", "Mountain View") in names


def test_bracket_scores_are_null_before_games_played():
    html = _read("cif_california_bracket_sample.html")
    out = parse_cif_california_html(
        html,
        source_url="https://www.cifstate.org/x",
    )
    for r in out["matches"]:
        assert r["score_for"] is None, r
        assert r["score_against"] is None, r
        assert r["result"] is None, r


def test_bracket_infers_gender_from_page_title():
    """Page title contains "Boys" — parser must infer gender=boys."""
    html = _read("cif_california_bracket_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    assert out["matches"], "expected non-empty matches"
    for r in out["matches"]:
        assert r["gender"] == "boys", r


def test_bracket_parses_match_date_to_iso_string():
    html = _read("cif_california_bracket_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    # 03/04/2026 and 03/07/2026 should become ISO.
    dates = sorted({r["match_date"] for r in out["matches"]})
    assert dates == ["2026-03-04", "2026-03-07"], dates


# ---------------------------------------------------------------------------
# Results pages — same shape, scores filled in
# ---------------------------------------------------------------------------


def test_results_extracts_scores_and_derives_result():
    html = _read("cif_california_results_sample.html")
    out = parse_cif_california_html(
        html,
        source_url="https://www.cifstate.org/sports/girls_soccer/state/results.html",
    )
    assert len(out["matches"]) == 4, out["matches"]
    assert out["rankings"] == []

    by_pair = {
        (r["school_name_raw"], r["opponent_raw"]): r for r in out["matches"]
    }

    mater_st_margarets = by_pair[("Mater Dei", "St. Margaret's")]
    assert mater_st_margarets["score_for"] == 3
    assert mater_st_margarets["score_against"] == 1
    assert mater_st_margarets["result"] == "W"

    redondo_pv = by_pair[("Redondo Union", "Palos Verdes")]
    assert redondo_pv["score_for"] == 0
    assert redondo_pv["score_against"] == 2
    assert redondo_pv["result"] == "L"

    mater_pv_final = by_pair[("Mater Dei", "Palos Verdes")]
    assert mater_pv_final["score_for"] == 2
    assert mater_pv_final["score_against"] == 2
    assert mater_pv_final["result"] == "T"

    mater_bellarmine = by_pair[("Mater Dei", "Bellarmine Prep")]
    assert mater_bellarmine["score_for"] == 4
    assert mater_bellarmine["score_against"] == 0
    assert mater_bellarmine["result"] == "W"


def test_results_gender_from_page_title_is_girls():
    html = _read("cif_california_results_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    for r in out["matches"]:
        assert r["gender"] == "girls", r


def test_results_natural_key_fields_present_on_every_row():
    """Writer contract: every match row must carry the five
    natural-key fields non-empty (match_date may be None on bracket
    pages but the results fixture fills it in)."""
    html = _read("cif_california_results_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    assert out["matches"], "expected matches"
    for r in out["matches"]:
        assert r["school_name_raw"], r
        assert r["school_state"] == "CA", r
        assert r["opponent_raw"], r
        assert r["match_date"] is not None, r
        assert r["gender"] in ("boys", "girls"), r
        assert r["source_url"] == "https://x", r


# ---------------------------------------------------------------------------
# Rankings pages — ordered list of schools grouped by section
# ---------------------------------------------------------------------------


def test_rankings_extracts_ordered_list_grouped_by_section():
    html = _read("cif_california_rankings_sample.html")
    out = parse_cif_california_html(
        html,
        source_url="https://www.cifstate.org/sports/boys_soccer/rankings.html",
    )
    assert out["matches"] == []
    # 5 valid ranking rows (3 NorCal + 2 SoCal D-I); 2 dropped for
    # missing school / missing rank.
    assert len(out["rankings"]) == 5, out["rankings"]

    norcal = [r for r in out["rankings"] if r["section"] == "CIF Northern California"]
    socal = [
        r for r in out["rankings"]
        if r["section"] == "CIF Southern Section — Division I"
    ]
    assert len(norcal) == 3
    assert len(socal) == 2

    # NorCal ordering
    norcal_sorted = sorted(norcal, key=lambda r: r["rank"])
    assert [r["school_name_raw"] for r in norcal_sorted] == [
        "Bellarmine College Prep",
        "Mountain View",
        "De La Salle",
    ]


def test_rankings_record_and_points_parsed():
    html = _read("cif_california_rankings_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    by_name = {r["school_name_raw"]: r for r in out["rankings"]}

    assert by_name["Bellarmine College Prep"]["record"] == "18-2-1"
    assert by_name["Bellarmine College Prep"]["points"] == 250
    assert by_name["Mater Dei"]["record"] == "19-1-0"
    assert by_name["Mater Dei"]["points"] == 260


def test_rankings_natural_key_fields_present():
    html = _read("cif_california_rankings_sample.html")
    out = parse_cif_california_html(html, source_url="https://x")
    for r in out["rankings"]:
        assert r["state"] == "CA", r
        assert r["gender"] in ("boys", "girls"), r
        assert r["season"], r
        assert isinstance(r["rank"], int), r
        assert r["school_name_raw"], r


# ---------------------------------------------------------------------------
# Malformed input / edge cases
# ---------------------------------------------------------------------------


def test_empty_html_returns_empty_lists():
    out = parse_cif_california_html("", source_url="https://x")
    assert out == {"matches": [], "rankings": []}


def test_page_with_no_tables_returns_empty_lists():
    html = "<html><body><h1>Nothing</h1><p>No data yet</p></body></html>"
    out = parse_cif_california_html(html, source_url="https://x")
    assert out == {"matches": [], "rankings": []}


def test_unrelated_tables_are_ignored():
    """A page whose only table is unrelated (venues, sponsors) must
    yield zero matches + zero rankings. The bracket fixture includes
    such a table alongside the real one — proven separately; here we
    test a pure-unrelated page."""
    html = """
    <html><body>
      <table>
        <thead><tr><th>Venue</th><th>City</th></tr></thead>
        <tbody><tr><td>Oracle Park</td><td>San Francisco</td></tr></tbody>
      </table>
    </body></html>
    """
    out = parse_cif_california_html(html, source_url="https://x")
    assert out == {"matches": [], "rankings": []}


def test_default_gender_and_season_overrides_are_respected():
    """If the caller knows the URL metadata (runner drives it via
    ``default_gender`` / ``default_season``), those values must win
    over page-title inference."""
    html = _read("cif_california_bracket_sample.html")
    out = parse_cif_california_html(
        html,
        source_url="https://x",
        default_gender="girls",    # overrides page-title "Boys"
        default_season="2027-28",  # overrides page-title "2026"
    )
    assert out["matches"], "expected matches"
    for r in out["matches"]:
        assert r["gender"] == "girls", r
        assert r["season"] == "2027-28", r
