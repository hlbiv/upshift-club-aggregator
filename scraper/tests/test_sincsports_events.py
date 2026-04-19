"""
Tests for the SincSports events extractor + DB writer.

HTML parsing is verified against a fixture captured from the live
TTTeamList.aspx page; DB writes are verified against a stubbed psycopg2
cursor so pytest runs without Postgres.

Run:
    python -m pytest scraper/tests/test_sincsports_events.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.sincsports_events import (  # noqa: E402
    _parse_division,
    extract_tid,
    normalize_gender,
    parse_sincsports_teamlist,
)
from events_writer import WriteResult, upsert_event_and_teams  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "sincsports" / "teamlist_GULFC.html"


# --------------------------------------------------------------------------- helpers


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- unit: parsing helpers


def test_extract_tid_from_intro_url():
    assert extract_tid("https://soccer.sincsports.com/TTIntro.aspx?tid=GULFC") == "GULFC"


def test_extract_tid_from_teamlist_url_with_extra_params():
    url = "https://soccer.sincsports.com/TTTeamList.aspx?tid=HOOVHAV&tab=5"
    assert extract_tid(url) == "HOOVHAV"


def test_extract_tid_missing_returns_none():
    assert extract_tid("https://soccer.sincsports.com/") is None


def test_normalize_gender_boys_variants():
    assert normalize_gender("Boys") == "M"
    assert normalize_gender("boy") == "M"
    assert normalize_gender("MALE") == "M"


def test_normalize_gender_girls_variants():
    assert normalize_gender("Girls") == "F"
    assert normalize_gender("female") == "F"


def test_normalize_gender_coed_returns_none():
    assert normalize_gender("Coed") is None
    assert normalize_gender("Open") is None
    assert normalize_gender(None) is None


def test_parse_division_u9_girls_gold_7v7():
    age, gender, code, birth = _parse_division("2017 (U9) Girls Gold 7v7")
    assert age == "U9"
    assert gender == "F"
    assert code == "Gold 7v7"
    assert birth == 2017


def test_parse_division_u12_boys_silver():
    age, gender, code, birth = _parse_division("2014 (U12) Boys Silver")
    assert age == "U12"
    assert gender == "M"
    assert code == "Silver"
    assert birth == 2014


def test_parse_division_u18_slash_19_band_keeps_lower_bound():
    age, gender, code, birth = _parse_division("U18/19 Boys Premier")
    assert age == "U18"
    assert gender == "M"
    assert code == "Premier"
    assert birth is None


def test_parse_division_hyphenated_u_15():
    age, gender, code, birth = _parse_division("U-15 Girls Gold")
    assert age == "U15"
    assert gender == "F"
    assert code == "Gold"


def test_parse_division_number_first_15u():
    age, gender, code, birth = _parse_division("15U Boys Silver")
    assert age == "U15"
    assert gender == "M"
    assert code == "Silver"


def test_parse_division_u_space_9():
    age, _gender, _code, _birth = _parse_division("U 9 Girls Bronze")
    assert age == "U9"


def test_parse_division_nonsense_returns_code_only():
    age, gender, code, birth = _parse_division("Schedules are final.")
    # No age match — parser returns code=None age=None
    assert age is None


# --------------------------------------------------------------------------- parsing fixture


def test_parse_fixture_extracts_event_meta():
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC", league_name="SincSports - Coastal")
    assert meta.tid == "GULFC"
    assert meta.name == "Coastal Soccer Invitational"
    assert meta.slug == "sincsports-gulfc"
    assert meta.source == "sincsports"
    assert meta.platform_event_id == "GULFC"
    assert meta.league_name == "SincSports - Coastal"
    assert meta.source_url.endswith("tid=GULFC")


def test_parse_fixture_extracts_expected_team_count():
    html = _load_fixture()
    _meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    # 3 (U9 Girls Gold) + 2 real rows (U12 Boys Silver, TBD skipped)
    # + 2 (U17 Girls Premier) = 7 teams
    assert len(teams) == 7


def test_parse_fixture_age_gender_breakdown():
    html = _load_fixture()
    _meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    by_bracket = {}
    for t in teams:
        key = (t.age_group, t.gender)
        by_bracket.setdefault(key, []).append(t)
    assert len(by_bracket[("U9", "F")]) == 3
    assert len(by_bracket[("U12", "M")]) == 2
    assert len(by_bracket[("U17", "F")]) == 2


def test_parse_fixture_skips_placeholder_team_rows():
    html = _load_fixture()
    _meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    names = [t.team_name_raw.lower() for t in teams]
    assert "tbd" not in names
    # No blank team rows either
    assert all(t.team_name_raw.strip() for t in teams)


def test_parse_fixture_populates_club_and_state():
    html = _load_fixture()
    _meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    foley = next(t for t in teams if t.team_name_raw == "Foley FC 2017 Girls")
    assert foley.club_name == "Foley FC"
    assert foley.state == "AL"
    assert foley.division_code == "Gold 7v7"


def test_parse_is_deterministic():
    """Same input → same output (idempotency-ready)."""
    html = _load_fixture()
    meta1, teams1 = parse_sincsports_teamlist(html, tid="GULFC")
    meta2, teams2 = parse_sincsports_teamlist(html, tid="GULFC")
    assert meta1 == meta2
    assert teams1 == teams2


# --------------------------------------------------------------------------- writer with stubbed cursor


class _FakeCursor:
    """Stand-in for a psycopg2 cursor that records executes and returns
    scripted fetch results. Scripted tuples: (returned_row, rowcount)."""

    def __init__(self, script: List[Tuple[Any, int]]):
        self.script = list(script)
        self.executed: List[Tuple[str, tuple]] = []
        self._last_row: Any = None
        self.rowcount: int = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params: tuple = None):
        # Transaction-control statements (SAVEPOINT / RELEASE / ROLLBACK TO)
        # are called without params by the writer. They're not part of the
        # data-level contract the tests care about, so don't record them or
        # consume scripted fetch results.
        verb = sql.strip().split()[0].upper()
        if verb in ("SAVEPOINT", "RELEASE", "ROLLBACK"):
            return
        self.executed.append((verb, params))
        # Script controls what fetchone returns for the next call.
        if self.script:
            row, rc = self.script.pop(0)
            self._last_row = row
            self.rowcount = rc
        else:
            self._last_row = None
            self.rowcount = 0

    def fetchone(self):
        return self._last_row


class _FakeConn:
    def __init__(self, cur: _FakeCursor):
        self._cur = cur
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


def test_upsert_dry_run_reports_counts_without_touching_db():
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    res = upsert_event_and_teams(meta, teams, conn=None, dry_run=True)
    assert res.events_created == 1
    assert res.teams_created == len(teams)


def test_upsert_first_run_inserts_event_and_all_teams():
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    script = [
        # events upsert — returns (event_id=100, inserted=True), rowcount=1
        ((100, True), 1),
    ] + [
        # Each team insert — row returned (inserted=True), rowcount=1
        ((True,), 1) for _ in teams
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    res = upsert_event_and_teams(meta, teams, conn=conn)

    assert res.events_created == 1
    assert res.events_updated == 0
    assert res.teams_created == len(teams)
    assert res.teams_updated == 0
    assert res.teams_skipped == 0
    assert conn.commits == 1
    assert conn.rollbacks == 0

    # First execute was the events INSERT, then one per team.
    assert cur.executed[0][0] == "INSERT"
    assert len(cur.executed) == 1 + len(teams)


def test_upsert_second_run_is_idempotent_when_nothing_changed():
    """On re-run with unchanged bracket fields: DO UPDATE ... WHERE no-ops,
    RETURNING yields no row, rowcount=0 → counted as skipped."""
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    script = [
        # event upsert — existing row, (id=100, inserted=False), rowcount=1
        ((100, False), 1),
    ] + [
        # Each team insert hits ON CONFLICT ... DO UPDATE WHERE (unchanged
        # fields fail the WHERE predicate) → no row returned, rowcount=0
        (None, 0) for _ in teams
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    res = upsert_event_and_teams(meta, teams, conn=conn)

    assert res.events_created == 0
    assert res.events_updated == 1
    assert res.teams_created == 0
    assert res.teams_updated == 0
    assert res.teams_skipped == len(teams)
    assert conn.commits == 1


def test_upsert_propagates_bracket_changes_on_reingest():
    """Re-scrape where teams have been re-bracketed: existing rows' bracket
    fields differ, so the DO UPDATE ... WHERE fires and RETURNING yields
    (inserted=False) with rowcount=1 → counted as teams_updated."""
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC")
    script = [
        # event upsert — existing row
        ((100, False), 1),
    ] + [
        # Every team is a bracket change: update fired, inserted=False
        ((False,), 1) for _ in teams
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    res = upsert_event_and_teams(meta, teams, conn=conn)

    assert res.events_updated == 1
    assert res.teams_created == 0
    assert res.teams_updated == len(teams)
    assert res.teams_skipped == 0
    assert conn.commits == 1


def test_upsert_rolls_back_on_exception():
    html = _load_fixture()
    meta, teams = parse_sincsports_teamlist(html, tid="GULFC")

    class _BoomCursor(_FakeCursor):
        def execute(self, sql, params=None):
            super().execute(sql, params)
            if len(self.executed) == 2:
                raise RuntimeError("simulated DB error")

    script = [((100, True), 1), (None, 1)]
    cur = _BoomCursor(script)
    conn = _FakeConn(cur)

    res = upsert_event_and_teams(meta, teams, conn=conn)
    # Writer swallows errors — result is zero, transaction rolled back.
    assert conn.rollbacks == 1
    assert conn.commits == 0
    assert res.teams_created == 0
