"""
Tests for the SincSports rosters extractor + snapshot writer.

Extraction tests run against fixture HTML. Writer tests stub psycopg2
with a fake cursor (same pattern as ``test_sincsports_events.py``) so
pytest runs without Postgres.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from unittest import mock  # noqa: E402

from extractors.sincsports_rosters import (  # noqa: E402
    SincSportsPageShapeChanged,
    _compute_grad_year,
    current_season_tag,
    parse_roster_html,
    parse_team_descriptors,
    scrape_sincsports_rosters,
)
from scrape_run_logger import FailureKind, classify_exception  # noqa: E402
from ingest.roster_snapshot_writer import (  # noqa: E402
    _compute_diff_rows,
    insert_roster_snapshots,
)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "sincsports"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- extractor: team descriptors


def test_team_descriptors_captures_teamid_and_metadata():
    html = _read("teamlist_ROSTR.html")
    descriptors = parse_team_descriptors(html, tid="ROSTR")
    # TBD placeholder and teams without a teamid link must be skipped.
    assert len(descriptors) == 2
    d = descriptors[0]
    assert d.tid == "ROSTR"
    assert d.teamid == "1001"
    assert d.team_name_raw == "Foley FC 14B"
    assert d.club_name == "Foley FC"
    assert d.state == "AL"
    assert d.age_group == "U12"
    assert d.gender == "M"
    assert d.division_code == "Silver"
    assert d.birth_year == 2014


def test_team_descriptors_real_world_no_teamid_returns_empty():
    """On real SincSports pages there are no TTRoster.aspx?teamid=... anchors
    inside team cells. The pure parser must return [] rather than guess."""
    html = _read("teamlist_REAL_no_teamid.html")
    descriptors = parse_team_descriptors(html, tid="GULFC")
    assert descriptors == []


def test_scrape_sincsports_rosters_detects_no_teamid_pages_and_bails():
    """Runtime guard: when the real TTTeamList HTML has zero teamid= hits,
    scrape_sincsports_rosters must raise SincSportsPageShapeChanged (with
    the offending tid in the message) without attempting per-team fetches.
    The runner catches this and logs failure_kind='parse_error' rather than
    misclassifying as 'zero_results'. This is the fix for the regression
    seen against every real SincSports seed tid in production."""
    html = _read("teamlist_REAL_no_teamid.html")
    with mock.patch(
        "extractors.sincsports_rosters._fetch", return_value=html
    ) as fetch_mock:
        with pytest.raises(SincSportsPageShapeChanged) as excinfo:
            scrape_sincsports_rosters("GULFC")
    # tid must appear in the exception message so operators can identify
    # which tournament tripped the guard.
    assert "GULFC" in str(excinfo.value)
    # Only the team-list fetch should have fired — no per-team roster fetches.
    assert fetch_mock.call_count == 1


def test_sincsports_page_shape_changed_classifies_as_parse_error():
    """The exception must route through scrape_run_logger.classify_exception
    to FailureKind.PARSE_ERROR (not UNKNOWN). The runner relies on this so
    'page shape changed' surfaces distinctly from 'tournament had 0 teams'
    in scrape_run_logs."""
    exc = SincSportsPageShapeChanged("tid=GULFC anchors=0")
    assert classify_exception(exc) is FailureKind.PARSE_ERROR


def test_warning_fires_per_call_not_once_per_process(caplog):
    """Two separate invocations against two different anchor-missing
    tournaments must each emit a warning — there is no per-process latch
    that silences subsequent misses. Operators scanning logs need to see
    every offending tid, not just the first."""
    fixture_a = _read("teamlist_REAL_no_teamid.html")
    fixture_b = _read("teamlist_GULFC.html")
    caplog.set_level("WARNING", logger="extractors.sincsports_rosters")

    with mock.patch(
        "extractors.sincsports_rosters._fetch", return_value=fixture_a
    ):
        with pytest.raises(SincSportsPageShapeChanged):
            scrape_sincsports_rosters("TIDONE")
    with mock.patch(
        "extractors.sincsports_rosters._fetch", return_value=fixture_b
    ):
        with pytest.raises(SincSportsPageShapeChanged):
            scrape_sincsports_rosters("TIDTWO")

    no_teamid_warnings = [
        rec for rec in caplog.records
        if rec.levelname == "WARNING"
        and "exposes no teamid" in rec.getMessage()
    ]
    assert len(no_teamid_warnings) == 2, (
        f"expected one warning per call, got {len(no_teamid_warnings)}: "
        f"{[r.getMessage() for r in no_teamid_warnings]}"
    )
    # Both tids must be present so log scans can identify each offender.
    messages = " ".join(r.getMessage() for r in no_teamid_warnings)
    assert "TIDONE" in messages
    assert "TIDTWO" in messages


def test_scrape_sincsports_rosters_still_works_on_pages_that_expose_teamid():
    """Inverse guard: if SincSports ever re-exposes teamid anchors, the
    existing happy path still kicks in. Uses the hypothetical fixture."""
    team_list_html = _read("teamlist_ROSTR.html")
    roster_html = _read("roster_ROSTR_team_1001.html")
    # First call → team list; subsequent calls → roster pages (same body ok).
    responses = [team_list_html, roster_html, roster_html]
    with mock.patch(
        "extractors.sincsports_rosters._fetch",
        side_effect=responses,
    ):
        rows = scrape_sincsports_rosters("ROSTR")
    assert len(rows) > 0
    assert all(r["player_name"] for r in rows)


# --------------------------------------------------------------------------- extractor: roster parsing


def test_parse_roster_happy_path_captures_name_and_jersey():
    html = _read("roster_ROSTR_team_1001.html")
    rows = parse_roster_html(html)
    assert rows == [
        ("Alex Rivera", "7"),
        ("Jordan Kim", "10"),
        ("Sam Patel", "1"),
    ]


def test_parse_roster_empty_returns_empty_list():
    html = _read("roster_ROSTR_team_1002_empty.html")
    rows = parse_roster_html(html)
    assert rows == []


def test_parse_roster_missing_jersey_column_returns_none_jersey():
    html = _read("roster_ROSTR_team_1003_no_jersey.html")
    rows = parse_roster_html(html)
    assert rows == [
        ("Chris Boyd", None),
        ("Dana Yu", None),
    ]


def test_current_season_tag_august_rolls_over():
    # August 2026 → '2026-27'
    assert current_season_tag(datetime(2026, 8, 15)) == "2026-27"
    # March 2026 → '2025-26'
    assert current_season_tag(datetime(2026, 3, 15)) == "2025-26"


# --------------------------------------------------------------------------- diff computation (pure)


def test_diff_compute_adds_new_players_removes_missing_flags_jersey_change():
    group_key = ("Foley FC", "2026-27", "U12", "M")
    current = [
        {"player_name": "Alex Rivera", "jersey_number": "7", "position": None},
        {"player_name": "Jordan Kim", "jersey_number": "9", "position": None},  # changed jersey
        {"player_name": "New Kid", "jersey_number": "22", "position": None},    # added
    ]
    prior = [
        ("Alex Rivera", "7", None),
        ("Jordan Kim", "10", None),   # old jersey
        ("Departed Kid", "3", None),  # removed
    ]
    diffs = _compute_diff_rows(group_key, current, prior)
    by_type = {d["diff_type"]: d for d in diffs}
    assert "added" in by_type and by_type["added"]["player_name"] == "New Kid"
    assert "removed" in by_type and by_type["removed"]["player_name"] == "Departed Kid"
    jc = [d for d in diffs if d["diff_type"] == "jersey_changed"]
    assert len(jc) == 1
    assert jc[0]["from_jersey_number"] == "10"
    assert jc[0]["to_jersey_number"] == "9"


# --------------------------------------------------------------------------- writer with stubbed cursor


class _FakeCursor:
    """Scriptable cursor stand-in. Each entry is ``(row, rowcount)``."""

    def __init__(self, script: List[Tuple[Any, int]]):
        self.script = list(script)
        self.executed: List[Tuple[str, Dict[str, Any]]] = []
        self._last_rows: Any = None
        self.rowcount: int = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None):
        op = sql.strip().split()[0].upper()
        # SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT are
        # transaction-control statements, not data ops. They don't
        # consume from the script and don't get logged in `executed`
        # (existing test assertions filter on op = INSERT, etc.).
        if op in ("SAVEPOINT", "RELEASE", "ROLLBACK"):
            return
        self.executed.append((op, dict(params or {})))
        if self.script:
            row, rc = self.script.pop(0)
            self._last_rows = row
            self.rowcount = rc
        else:
            self._last_rows = None
            self.rowcount = 0

    def fetchone(self):
        r = self._last_rows
        # fetchone on a list-returning script consumes the first element.
        if isinstance(r, list):
            return r[0] if r else None
        return r

    def fetchall(self):
        r = self._last_rows
        if r is None:
            return []
        if isinstance(r, list):
            return r
        return [r]


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


def _make_row(
    player_name: str,
    jersey: str,
    *,
    club: str = "Foley FC",
    snap: datetime = datetime(2026, 4, 1),
):
    return {
        "club_name_raw": club,
        "source_url": "https://x/roster?teamid=1001",
        "snapshot_date": snap,
        "season": "2025-26",
        "age_group": "U12",
        "gender": "M",
        "division": "Silver",
        "player_name": player_name,
        "jersey_number": jersey,
        "position": None,
        "event_id": None,
    }


def test_writer_first_run_inserts_and_writes_no_diffs():
    """Very first snapshot has no prior → inserts players, no diffs."""
    rows = [_make_row("Alex", "7"), _make_row("Jordan", "10")]
    script: List[Tuple[Any, int]] = [
        # prior snapshot lookup → fetchall returns empty list
        ([], 0),
    ] + [
        # two INSERT ... RETURNING (inserted=True)
        ((True,), 1),
        ((True,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["inserted"] == 2
    assert counts["updated"] == 0
    assert counts["diffs_written"] == 0
    # Caller owns the connection → writer does NOT commit.
    assert conn.commits == 0


def test_writer_idempotent_reinsert_reports_no_updates():
    """Re-scrape with no changes: DO UPDATE ... WHERE returns no row,
    rowcount=0. Neither inserted nor updated is incremented."""
    rows = [_make_row("Alex", "7"), _make_row("Jordan", "10")]
    # prior snapshot has the same two rows, so zero diffs.
    prior_rows = [("Alex", "7", None), ("Jordan", "10", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, len(prior_rows)),
        (None, 0),   # unchanged row → no fetchone result
        (None, 0),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["inserted"] == 0
    assert counts["updated"] == 0
    assert counts["diffs_written"] == 0


def test_writer_emits_removed_diff_when_prior_player_not_in_current():
    """Prior had 2 players, current has 1 → one `removed` diff."""
    rows = [_make_row("Alex", "7")]
    prior_rows = [("Alex", "7", None), ("Departed", "99", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, len(prior_rows)),
        # Alex: unchanged → rowcount 0
        (None, 0),
        # one roster_diffs INSERT ... RETURNING id → (123,), rowcount 1
        ((123,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["diffs_written"] == 1
    # Verify the diff insert was a `removed` type.
    diff_inserts = [p for op, p in cur.executed if op == "INSERT" and "diff_type" in p]
    assert len(diff_inserts) == 1
    assert diff_inserts[0]["diff_type"] == "removed"
    assert diff_inserts[0]["player_name"] == "Departed"


def test_writer_emits_jersey_changed_diff():
    rows = [_make_row("Alex", "9")]   # new jersey
    prior_rows = [("Alex", "7", None)]
    script: List[Tuple[Any, int]] = [
        (prior_rows, 1),
        # Alex upsert → WHERE predicate fires: rowcount=1, inserted=False
        ((False,), 1),
        # jersey_changed diff insert
        ((123,), 1),
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    counts = insert_roster_snapshots(rows, conn=conn)
    assert counts["updated"] == 1
    assert counts["diffs_written"] == 1
    diff_inserts = [p for op, p in cur.executed if op == "INSERT" and "diff_type" in p]
    assert diff_inserts[0]["diff_type"] == "jersey_changed"
    assert diff_inserts[0]["from_jersey_number"] == "7"
    assert diff_inserts[0]["to_jersey_number"] == "9"


def test_writer_sends_null_club_id_on_every_insert():
    """The SQL template hard-codes club_id=NULL so the linker owns it.
    Assert the compiled SQL never mentions a %(club_id)s placeholder."""
    from ingest.roster_snapshot_writer import _INSERT_SNAPSHOT_SQL
    assert "club_id" in _INSERT_SNAPSHOT_SQL
    # Must be NULL literal, never a parameter placeholder.
    assert "%(club_id)s" not in _INSERT_SNAPSHOT_SQL


# --------------------------------------------------------------------------- dedup scope: same player, two teams


_SAME_PLAYER_ROSTER_HTML = """
<html><body>
  <table>
    <tr><th>Name</th><th>Jersey</th></tr>
    <tr><td>Alex Smith</td><td>7</td></tr>
    <tr><td>Other Kid</td><td>8</td></tr>
  </table>
</body></html>
"""


def test_parse_roster_dedups_within_a_single_team_call():
    """Two rows for the same player on the SAME teamid still collapse —
    that's a parsing artifact, not a legitimate roster entry."""
    html = """
    <html><body>
      <table>
        <tr><th>Name</th><th>Jersey</th></tr>
        <tr><td>Alex Smith</td><td>7</td></tr>
        <tr><td>Alex Smith</td><td>7</td></tr>
        <tr><td>Other Kid</td><td>8</td></tr>
      </table>
    </body></html>
    """
    rows = parse_roster_html(html, teamid="1001")
    # Same player on the same team → one row only.
    names = [n for n, _ in rows]
    assert names == ["Alex Smith", "Other Kid"]


def test_scrape_sincsports_rosters_keeps_same_player_on_two_teams():
    """A U-15 player legitimately rostered as a guest on a U-17 team must
    show up TWICE — once per teamid — in the scrape output. The dedup
    key in parse_roster_html is (teamid, player_name), so two distinct
    teamid fetches yield two distinct rows for the same player name."""
    # Build a TTTeamList HTML with two team rows pointing to teamid 1001
    # and teamid 1002 under the same age-group header. Each team's
    # TTRoster page contains "Alex Smith".
    team_list_html = """
    <html><body>
      <h2>2014 (U12) Boys Silver</h2>
      <table>
        <tr><th>Team</th><th>Club</th><th>State</th></tr>
        <tr>
          <td><a href="/TTRoster.aspx?tid=ROSTR&amp;teamid=1001">Foley FC 14B</a></td>
          <td>Foley FC</td>
          <td>AL</td>
        </tr>
        <tr>
          <td><a href="/TTRoster.aspx?tid=ROSTR&amp;teamid=1002">Foley FC 14B Red</a></td>
          <td>Foley FC</td>
          <td>AL</td>
        </tr>
      </table>
    </body></html>
    """
    # Each per-team roster fetch returns the same page with "Alex Smith"
    # on it. The (teamid, player_name) dedup keeps both.
    responses = [team_list_html, _SAME_PLAYER_ROSTER_HTML, _SAME_PLAYER_ROSTER_HTML]
    with mock.patch(
        "extractors.sincsports_rosters._fetch",
        side_effect=responses,
    ):
        rows = scrape_sincsports_rosters("ROSTR")
    alex_rows = [r for r in rows if r["player_name"] == "Alex Smith"]
    # Two teams, same player → two rows survive dedup.
    assert len(alex_rows) == 2
    source_urls = sorted(r["source_url"] for r in alex_rows)
    assert "teamid=1001" in source_urls[0]
    assert "teamid=1002" in source_urls[1]


# --------------------------------------------------------------------------- grad_year computation


def test_compute_grad_year_october_birth_rolls_forward_one_year():
    """A player born in October (month=10) of 2008 turns 18 in the school
    year that starts in fall 2027 → grad_year=2027, NOT 2026."""
    assert _compute_grad_year(2008, 10) == 2027


def test_compute_grad_year_march_birth_does_not_roll():
    """A player born in March (month=3) of 2008 graduates spring 2026
    on the simple +18 formula — no rollover."""
    assert _compute_grad_year(2008, 3) == 2026


def test_compute_grad_year_unknown_month_falls_back_to_simple_plus_18():
    """When birth_month is None (the SincSports case — only birth_year is
    parsed today) the helper does NOT guess a rollover. It returns the
    base +18 formula."""
    assert _compute_grad_year(2008, None) == 2026


def test_compute_grad_year_handles_missing_birth_year():
    assert _compute_grad_year(None, None) is None
    assert _compute_grad_year(None, 10) is None


def test_compute_grad_year_august_boundary_no_rollover():
    """August (month=8) sits on the boundary — the original spec says
    rollover only for month > 8 (i.e. Sep–Dec), so August stays put."""
    assert _compute_grad_year(2008, 8) == 2026
    # September is the first rollover month.
    assert _compute_grad_year(2008, 9) == 2027
