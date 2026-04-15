"""
Tests for the GotSport matches extractor + club_results rollup SQL.

Run:
    python -m pytest scraper/tests/test_gotsport_matches.py -v
"""

from __future__ import annotations

import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.gotsport_matches import (  # noqa: E402
    _extract_matches_from_html,
    _parse_age_gender,
    _parse_date,
    _parse_score,
    _dedup_matches,
    _normalize_status,
)


FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "gotsport")


def _read_fixture(name: str) -> str:
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------

def test_parse_score_basic():
    assert _parse_score("3-1") == (3, 1)
    assert _parse_score("0 - 4") == (0, 4)
    assert _parse_score("5:2") == (5, 2)


def test_parse_score_invalid_returns_none():
    assert _parse_score("") == (None, None)
    assert _parse_score("TBD") == (None, None)


def test_parse_age_gender_boys():
    assert _parse_age_gender("B15 Premier") == ("U15", "M")


def test_parse_age_gender_girls():
    assert _parse_age_gender("G13 Elite") == ("U13", "F")


def test_parse_age_gender_u_prefix():
    assert _parse_age_gender("U12 Boys") == ("U12", "M")
    assert _parse_age_gender("U16 Girls") == ("U16", "F")


def test_parse_date_iso():
    assert _parse_date("2026-03-14 15:30") == datetime(2026, 3, 14, 15, 30)


def test_parse_date_us_format():
    assert _parse_date("3/14/2026 3:30 PM") == datetime(2026, 3, 14, 15, 30)


def test_parse_date_garbage_returns_none():
    assert _parse_date("TBD") is None
    assert _parse_date("") is None


def test_normalize_status_final_from_score():
    assert _normalize_status(None, 3, 1) == "final"


def test_normalize_status_scheduled_when_no_score():
    assert _normalize_status("Scheduled", None, None) == "scheduled"


def test_normalize_status_explicit_final():
    assert _normalize_status("Final", 0, 0) == "final"


def test_normalize_status_cancelled():
    assert _normalize_status("Cancelled", None, None) == "cancelled"


# ---------------------------------------------------------------------------
# End-to-end HTML extraction
# ---------------------------------------------------------------------------

def test_extract_matches_from_fixture_row_count():
    html = _read_fixture("schedules_sample.html")
    rows = _extract_matches_from_html(
        html,
        event_id=99999,
        source_url="https://system.gotsport.com/org_event/events/99999/schedules",
        default_season="2025-26",
        default_league="Sample League",
    )
    assert len(rows) == 4


def test_extract_matches_from_fixture_shape():
    html = _read_fixture("schedules_sample.html")
    rows = _extract_matches_from_html(
        html,
        event_id=99999,
        source_url="https://system.gotsport.com/org_event/events/99999/schedules",
        default_season="2025-26",
        default_league="Sample League",
    )
    first = rows[0]
    assert first["home_team_name"] == "Concorde Fire SC"
    assert first["away_team_name"] == "NTH Tophat"
    assert first["home_score"] == 3
    assert first["away_score"] == 1
    assert first["match_date"] == datetime(2026, 3, 14, 15, 30)
    assert first["age_group"] == "U15"
    assert first["gender"] == "M"
    assert first["platform_match_id"] == "GS-1001"
    assert first["status"] == "final"
    assert first["source"] == "gotsport"
    assert first["season"] == "2025-26"
    assert first["league"] == "Sample League"
    # Canonicals get stripped + title-cased by normalizer._canonical
    assert first["home_club_canonical"]  # non-empty
    assert first["away_club_canonical"]


def test_extract_matches_scheduled_row_has_none_scores():
    html = _read_fixture("schedules_sample.html")
    rows = _extract_matches_from_html(
        html,
        event_id=99999,
        source_url="https://example.test",
    )
    scheduled = [r for r in rows if r["platform_match_id"] == "GS-1003"]
    assert len(scheduled) == 1
    s = scheduled[0]
    assert s["home_score"] is None
    assert s["away_score"] is None
    assert s["status"] == "scheduled"


def test_extract_matches_idempotent_within_single_scrape():
    """Parsing the same HTML twice produces the same row count (extractor is pure)."""
    html = _read_fixture("schedules_sample.html")
    rows_a = _extract_matches_from_html(html, event_id=99999, source_url="x")
    rows_b = _extract_matches_from_html(html, event_id=99999, source_url="x")
    assert len(rows_a) == len(rows_b)


def test_dedup_matches_collapses_platform_ids():
    rows = [
        {
            "source": "gotsport",
            "platform_match_id": "A",
            "home_team_name": "X",
            "away_team_name": "Y",
        },
        {
            "source": "gotsport",
            "platform_match_id": "A",
            "home_team_name": "X",
            "away_team_name": "Y",
        },
        {
            "source": "gotsport",
            "platform_match_id": "B",
            "home_team_name": "X",
            "away_team_name": "Z",
        },
    ]
    out = _dedup_matches(rows)
    assert len(out) == 2


def test_dedup_matches_collapses_natural_key():
    rows = [
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": None,
            "age_group": "U15",
            "gender": "M",
        },
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": None,
            "age_group": "U15",
            "gender": "M",
        },
    ]
    out = _dedup_matches(rows)
    assert len(out) == 1


# ---------------------------------------------------------------------------
# Rollup correctness — emulated in pure Python to mirror the SQL logic
# ---------------------------------------------------------------------------

def _python_rollup(matches):
    """Mirror of the SQL rollup in scraper/rollups/club_results.py.

    We don't execute the SQL here (no DB fixture), but we verify the
    algorithm produces the expected W/L/D + GF/GA shape for a synthetic
    match set. This is the algorithmic contract that the SQL must honor.
    """
    from collections import defaultdict
    key = lambda m, side: (
        m[f"{side}_club_id"], m["season"], m.get("league"), m.get("division"),
        m.get("age_group"), m.get("gender"),
    )
    agg = defaultdict(lambda: {"wins": 0, "losses": 0, "draws": 0,
                                "gf": 0, "ga": 0, "mp": 0})
    for m in matches:
        if m["status"] != "final":
            continue
        if m["home_club_id"] is None or m["away_club_id"] is None:
            continue
        if m["home_score"] is None or m["away_score"] is None:
            continue
        if m["season"] is None:
            continue
        hs, as_ = m["home_score"], m["away_score"]
        for side, gf, ga in (("home", hs, as_), ("away", as_, hs)):
            k = key(m, side)
            cell = agg[k]
            if gf > ga:
                cell["wins"] += 1
            elif gf < ga:
                cell["losses"] += 1
            else:
                cell["draws"] += 1
            cell["gf"] += gf
            cell["ga"] += ga
            cell["mp"] += 1
    return agg


def test_rollup_algorithm_wld():
    matches = [
        # Concorde beats NTH 3-1
        dict(home_club_id=1, away_club_id=2, home_score=3, away_score=1,
             status="final", season="2025-26", league="L", division="D",
             age_group="U15", gender="M"),
        # Concorde loses to Atlanta 0-2
        dict(home_club_id=1, away_club_id=3, home_score=0, away_score=2,
             status="final", season="2025-26", league="L", division="D",
             age_group="U15", gender="M"),
        # NTH draws with Atlanta 1-1
        dict(home_club_id=2, away_club_id=3, home_score=1, away_score=1,
             status="final", season="2025-26", league="L", division="D",
             age_group="U15", gender="M"),
        # Scheduled row — ignored
        dict(home_club_id=1, away_club_id=2, home_score=None, away_score=None,
             status="scheduled", season="2025-26", league="L", division="D",
             age_group="U15", gender="M"),
        # Linker-unresolved row — ignored
        dict(home_club_id=None, away_club_id=2, home_score=5, away_score=0,
             status="final", season="2025-26", league="L", division="D",
             age_group="U15", gender="M"),
    ]
    agg = _python_rollup(matches)

    # Concorde (club_id=1): 1W 1L 0D, GF=3, GA=3
    concorde = agg[(1, "2025-26", "L", "D", "U15", "M")]
    assert concorde["wins"] == 1
    assert concorde["losses"] == 1
    assert concorde["draws"] == 0
    assert concorde["gf"] == 3
    assert concorde["ga"] == 3
    assert concorde["mp"] == 2

    # NTH (club_id=2): 0W 1L 1D, GF=2, GA=4
    nth = agg[(2, "2025-26", "L", "D", "U15", "M")]
    assert nth["wins"] == 0
    assert nth["losses"] == 1
    assert nth["draws"] == 1
    assert nth["gf"] == 2
    assert nth["ga"] == 4
    assert nth["mp"] == 2

    # Atlanta (club_id=3): 1W 0L 1D, GF=3, GA=1
    atl = agg[(3, "2025-26", "L", "D", "U15", "M")]
    assert atl["wins"] == 1
    assert atl["losses"] == 0
    assert atl["draws"] == 1
    assert atl["gf"] == 3
    assert atl["ga"] == 1
    assert atl["mp"] == 2


def test_rollup_idempotent():
    """Running the rollup twice on the same matches yields identical agg."""
    matches = [
        dict(home_club_id=1, away_club_id=2, home_score=2, away_score=0,
             status="final", season="2025-26", league="L", division=None,
             age_group="U15", gender="M"),
    ]
    a = _python_rollup(matches)
    b = _python_rollup(matches)
    assert dict(a) == dict(b)
