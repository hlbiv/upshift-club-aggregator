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

import logging  # noqa: E402

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


def test_parse_score_with_result_prefix():
    # "W 3-2", "L 1-3", "D 2-2", "T 1-1"
    assert _parse_score("W 3-2") == (3, 2)
    assert _parse_score("L 1-3") == (1, 3)
    assert _parse_score("D 2-2") == (2, 2)
    assert _parse_score("T 1-1") == (1, 1)


def test_parse_score_with_forfeit_marker():
    # Forfeit markers: "F 1-0" (leading), "0-0 FF" (trailing).
    assert _parse_score("F 1-0") == (1, 0)
    assert _parse_score("0-0 FF") == (0, 0)


def test_parse_score_with_shootout():
    # "2-2 (4-3)" — main score goes to DB, PK score is discarded.
    assert _parse_score("2-2 (4-3)") == (2, 2)
    assert _parse_score("1-1 (5-4)") == (1, 1)


def test_parse_score_with_prefix_and_shootout():
    assert _parse_score("W 2-2 (4-3)") == (2, 2)


def test_normalize_status_forfeit_from_score_cell():
    # "0-0 FF" should classify as forfeit even with no explicit status col.
    from extractors.gotsport_matches import _normalize_status as ns
    assert ns(None, 0, 0, "0-0 FF") == "forfeit"
    assert ns(None, 1, 0, "F 1-0") == "forfeit"


def test_bye_cell_detection():
    from extractors.gotsport_matches import _is_bye_cell
    assert _is_bye_cell("BYE") is True
    assert _is_bye_cell("bye") is True
    assert _is_bye_cell("3-2") is False
    assert _is_bye_cell("") is False


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


def test_extract_matches_score_variants_fixture():
    """BYE is skipped; W-prefix, F/FF forfeit, shootout, and empty-score
    (future match) rows all parse correctly.
    """
    html = _read_fixture("schedules_score_variants.html")
    rows = _extract_matches_from_html(
        html,
        event_id=88888,
        source_url="https://example.test",
        default_season="2025-26",
    )
    by_id = {r["platform_match_id"]: r for r in rows}

    # SV-5 is a BYE and must NOT appear.
    assert "SV-5" not in by_id
    # The other four all should.
    assert set(by_id.keys()) == {"SV-1", "SV-2", "SV-3", "SV-4"}

    # SV-1: "W 3-2" → scores 3/2, final
    assert by_id["SV-1"]["home_score"] == 3
    assert by_id["SV-1"]["away_score"] == 2
    assert by_id["SV-1"]["status"] == "final"

    # SV-2: "F 1-0" → scores 1/0, status = forfeit
    assert by_id["SV-2"]["home_score"] == 1
    assert by_id["SV-2"]["away_score"] == 0
    assert by_id["SV-2"]["status"] == "forfeit"

    # SV-3: "2-2 (4-3)" → main score 2/2, status = final, PK score discarded
    assert by_id["SV-3"]["home_score"] == 2
    assert by_id["SV-3"]["away_score"] == 2
    assert by_id["SV-3"]["status"] == "final"

    # SV-4: empty score cell, future match → scheduled with null scores
    assert by_id["SV-4"]["home_score"] is None
    assert by_id["SV-4"]["away_score"] is None
    assert by_id["SV-4"]["status"] == "scheduled"


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


# ---------------------------------------------------------------------------
# Non-canonicalizable team-name guard (PR 8)
# ---------------------------------------------------------------------------

# Single-row inline fixtures isolate the guard logic without depending on
# the larger schedules fixtures (which would need to be re-shaped to add a
# garbage-name row). The structure mirrors `schedules_score_variants.html`.

_GOOD_ROW_HTML = """
<!doctype html>
<html><body>
<table>
  <thead><tr><th>Date</th><th>Home</th><th>Score</th><th>Away</th></tr></thead>
  <tbody>
    <tr class="match" data-match-id="OK-1">
      <td class="match-date">2026-03-14 10:00</td>
      <td class="home">Concorde Fire SC</td>
      <td>3-1</td>
      <td class="away">NTH Tophat</td>
    </tr>
  </tbody>
</table>
</body></html>
"""

# An away cell containing only a parenthetical suffix. _canonical strips
# parens first, leaving "" — exactly the failure mode the guard targets.
_GARBAGE_AWAY_ROW_HTML = """
<!doctype html>
<html><body>
<table>
  <thead><tr><th>Date</th><th>Home</th><th>Score</th><th>Away</th></tr></thead>
  <tbody>
    <tr class="match" data-match-id="OK-1">
      <td class="match-date">2026-03-14 10:00</td>
      <td class="home">Concorde Fire SC</td>
      <td>3-1</td>
      <td class="away">NTH Tophat</td>
    </tr>
    <tr class="match" data-match-id="BAD-1">
      <td class="match-date">2026-03-14 12:00</td>
      <td class="home">Real Team FC</td>
      <td>2-0</td>
      <td class="away">(U-12)</td>
    </tr>
  </tbody>
</table>
</body></html>
"""


def test_canonicalizable_row_is_emitted_normally():
    """Both team names canonicalize → row emitted, stats counter stays at 0."""
    stats = {"dropped_non_canonicalizable": 0}
    rows = _extract_matches_from_html(
        _GOOD_ROW_HTML,
        event_id=12345,
        source_url="https://example.test",
        stats=stats,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["home_team_name"] == "Concorde Fire SC"
    assert row["away_team_name"] == "NTH Tophat"
    assert row["home_club_canonical"]  # non-empty
    assert row["away_club_canonical"]
    assert stats["dropped_non_canonicalizable"] == 0


def test_non_canonicalizable_row_is_dropped_counted_and_logged(caplog):
    """One team name returns "" from _canonical → row dropped, counter
    incremented, warning logged. The other (good) row in the fixture
    is still emitted.
    """
    stats = {"dropped_non_canonicalizable": 0}
    with caplog.at_level(logging.WARNING, logger="extractors.gotsport_matches"):
        rows = _extract_matches_from_html(
            _GARBAGE_AWAY_ROW_HTML,
            event_id=12345,
            source_url="https://example.test",
            stats=stats,
        )

    # Only the well-formed row survived.
    assert len(rows) == 1
    assert rows[0]["home_team_name"] == "Concorde Fire SC"
    assert rows[0]["away_team_name"] == "NTH Tophat"

    # The garbage-away row was counted as dropped.
    assert stats["dropped_non_canonicalizable"] == 1

    # And a warning was logged with both raw names + canonicals.
    drop_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING
        and "non-canonicalizable" in r.getMessage()
    ]
    assert len(drop_warnings) == 1
    msg = drop_warnings[0].getMessage()
    assert "Real Team FC" in msg
    assert "(U-12)" in msg


# ---------------------------------------------------------------------------
# PR 15 — multi-match-per-row split, word-boundary score regex,
# microsecond-truncated dedup key
# ---------------------------------------------------------------------------

def test_extract_matches_multi_match_per_row():
    """A single <tr> packing TWO matches (alternating home/away columns)
    should yield BOTH matches, not just the first.
    """
    html = _read_fixture("schedules_multi_match_per_row.html")
    rows = _extract_matches_from_html(
        html,
        event_id=12345,
        source_url="https://example.test",
        default_season="2025-26",
    )
    by_id = {r["platform_match_id"]: r for r in rows}
    assert "MM-1" in by_id, f"expected first match in row; got {list(by_id)}"
    assert "MM-2" in by_id, f"expected second match in row; got {list(by_id)}"

    # Per-pair name + score resolution.
    assert by_id["MM-1"]["home_team_name"] == "Concorde Fire SC"
    assert by_id["MM-1"]["away_team_name"] == "NTH Tophat"
    assert by_id["MM-1"]["home_score"] == 3
    assert by_id["MM-1"]["away_score"] == 1

    assert by_id["MM-2"]["home_team_name"] == "Atlanta United Academy"
    assert by_id["MM-2"]["away_team_name"] == "Bethesda SC"
    assert by_id["MM-2"]["home_score"] == 2
    assert by_id["MM-2"]["away_score"] == 2

    # Shared row cells (date, division) are broadcast to both sub-matches.
    assert by_id["MM-1"]["match_date"] == datetime(2026, 4, 11, 9, 0)
    assert by_id["MM-2"]["match_date"] == datetime(2026, 4, 11, 9, 0)
    assert by_id["MM-1"]["age_group"] == "U15"
    assert by_id["MM-2"]["age_group"] == "U15"


def test_word_boundary_score_regex_does_not_eat_year_in_team_name():
    """A team name like "FC 2010-2012" must NOT have its year eaten by the
    inline score regex. With the old eager pattern, "10-20" would be
    pulled out as a 10-20 score and the residual name corrupted.
    """
    html = """
    <html><body>
    <table>
      <thead><tr><th>Match</th></tr></thead>
      <tbody>
        <tr class="match">
          <td>FC 2010-2012 vs Tigers SC 2010-2012</td>
        </tr>
      </tbody>
    </table>
    </body></html>
    """
    rows = _extract_matches_from_html(
        html, event_id=1, source_url="https://example.test"
    )
    assert len(rows) == 1
    r = rows[0]
    # The year-numbers must remain in the team names; no spurious score.
    assert "2010-2012" in r["home_team_name"], (
        f"home_team_name lost its year; got {r['home_team_name']!r}"
    )
    assert "2010-2012" in r["away_team_name"], (
        f"away_team_name lost its year; got {r['away_team_name']!r}"
    )
    assert r["home_score"] is None
    assert r["away_score"] is None


def test_dedup_truncates_microseconds_in_natural_key():
    """Two emissions of the same logical match with microsecond-different
    timestamps should dedup to ONE row. The ``match_date`` field on the
    surviving row is left untouched (we only truncate inside the dedup
    key, not the persisted value).
    """
    md_a = datetime(2026, 4, 11, 9, 0, 0, 0)
    md_b = datetime(2026, 4, 11, 9, 0, 0, 999999)
    rows = [
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": md_a,
            "age_group": "U15",
            "gender": "M",
        },
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": md_b,
            "age_group": "U15",
            "gender": "M",
        },
    ]
    out = _dedup_matches(rows)
    assert len(out) == 1
    # Persisted match_date is the FIRST emission's, untruncated.
    assert out[0]["match_date"] == md_a
    assert out[0]["match_date"].microsecond == 0  # md_a happened to be 0


def test_dedup_does_not_collapse_dates_a_second_apart():
    """Two matches a full second apart are different logical matches and
    must NOT collapse — guards against an over-eager truncation.
    """
    md_a = datetime(2026, 4, 11, 9, 0, 0)
    md_b = datetime(2026, 4, 11, 9, 0, 1)
    rows = [
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": md_a,
            "age_group": "U15",
            "gender": "M",
        },
        {
            "source": "gotsport",
            "platform_match_id": None,
            "home_team_name": "X",
            "away_team_name": "Y",
            "match_date": md_b,
            "age_group": "U15",
            "gender": "M",
        },
    ]
    out = _dedup_matches(rows)
    assert len(out) == 2


# ---------------------------------------------------------------------------
# Session-cookie injection (ga-matches / GOTSPORT_SESSION_COOKIE)
# ---------------------------------------------------------------------------

def test_session_cookie_injected_in_all_requests(monkeypatch):
    """When session_cookie is set, every requests.get call must include
    Cookie: <value> in its headers — both the event-home discovery fetch
    and each per-group schedule fetch.
    """
    from unittest.mock import patch, MagicMock
    from extractors.gotsport_matches import scrape_gotsport_matches

    # Minimal schedule HTML: one group link on the event home page, one
    # match row on the group schedule page.
    event_home_html = """
    <html><body>
      <a href="/org_event/events/42137/schedules?group=99">Division A</a>
    </body></html>
    """
    schedule_html = """
    <html><body>
    <table>
      <thead><tr><th>Match #</th><th>Time</th><th>Home Team</th>
                 <th>Results</th><th>Away Team</th></tr></thead>
      <tbody>
        <tr><td>1001</td><td>2025-09-07 10:00</td><td>Concorde Fire SC</td>
            <td>2-1</td><td>NTH Tophat</td></tr>
      </tbody>
    </table>
    </body></html>
    """

    calls: list = []

    def fake_get(url, headers=None, timeout=None):
        calls.append({"url": url, "headers": headers or {}})
        resp = MagicMock()
        resp.status_code = 200
        resp.text = schedule_html if "group=99" in url else event_home_html
        resp.raise_for_status = lambda: None
        return resp

    with patch("extractors.gotsport_matches.requests.get", side_effect=fake_get):
        scrape_gotsport_matches(42137, session_cookie="tok=abc123")

    assert calls, "requests.get was never called"
    for call in calls:
        assert call["headers"].get("Cookie") == "tok=abc123", (
            f"Cookie header missing or wrong on {call['url']}: {call['headers']}"
        )


def test_no_cookie_omits_cookie_header(monkeypatch):
    """When session_cookie is None, the Cookie header must be absent entirely —
    guards against Cookie: None (string) leaking into requests.
    """
    from unittest.mock import patch, MagicMock
    from extractors.gotsport_matches import scrape_gotsport_matches

    event_home_html = """
    <html><body>
      <a href="/org_event/events/99999/schedules?group=1">Div</a>
    </body></html>
    """
    schedule_html = """
    <html><body>
    <table>
      <thead><tr><th>Match #</th><th>Home Team</th><th>Away Team</th></tr></thead>
      <tbody>
        <tr><td>1</td><td>Concorde Fire SC</td><td>NTH Tophat</td></tr>
      </tbody>
    </table>
    </body></html>
    """

    calls: list = []

    def fake_get(url, headers=None, timeout=None):
        calls.append({"url": url, "headers": headers or {}})
        resp = MagicMock()
        resp.status_code = 200
        resp.text = schedule_html if "group=1" in url else event_home_html
        resp.raise_for_status = lambda: None
        return resp

    with patch("extractors.gotsport_matches.requests.get", side_effect=fake_get):
        scrape_gotsport_matches(99999, session_cookie=None)

    assert calls, "requests.get was never called"
    for call in calls:
        assert "Cookie" not in call["headers"], (
            f"Cookie header present when it should be absent on {call['url']}: "
            f"{call['headers']}"
        )


def test_auth_wall_raises_gotsport_auth_error():
    """When the event home page returns a Cloudflare CAPTCHA page (no group
    links, contains a known auth-wall marker), GotSportAuthError is raised.
    """
    from unittest.mock import patch, MagicMock
    from extractors.gotsport_matches import scrape_gotsport_matches, GotSportAuthError

    captcha_html = """
    <html><head><title>Just a moment...</title></head>
    <body>
      <div id="cf-browser-verification">
        <p>Checking your browser before accessing the site.</p>
      </div>
    </body></html>
    """

    def fake_get(url, headers=None, timeout=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.text = captcha_html
        resp.raise_for_status = lambda: None
        return resp

    with patch("extractors.gotsport_matches.requests.get", side_effect=fake_get):
        with pytest.raises(GotSportAuthError, match="auth/CAPTCHA"):
            scrape_gotsport_matches(42137, session_cookie="stale=xyz")
