"""
Tests for the WordPress tryouts extractor + tryouts writer.

Extraction tests use fixture HTML. Writer tests stub psycopg2 with a
fake cursor so pytest runs without Postgres.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.tryouts_wordpress import (  # noqa: E402
    extract_registration_links,
    parse_date,
    parse_tryouts_page_html,
)
from ingest.tryouts_writer import insert_tryouts  # noqa: E402


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "wordpress"


def _read(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# --------------------------------------------------------------------------- date parsing variants


def test_parse_date_month_name_format():
    assert parse_date("Aug 5, 2026") == datetime(2026, 8, 5)
    assert parse_date("August 5, 2026") == datetime(2026, 8, 5)


def test_parse_date_numeric_slash_format():
    # 2-digit year must expand to 2000s.
    assert parse_date("8/5/26") == datetime(2026, 8, 5)
    assert parse_date("08/05/2026") == datetime(2026, 8, 5)


def test_parse_date_range_picks_first_day():
    assert parse_date("August 5-7, 2026") == datetime(2026, 8, 5)


def test_parse_date_unparseable_returns_none():
    assert parse_date("every summer") is None
    assert parse_date("") is None


# --------------------------------------------------------------------------- page-level parse


def test_parse_page_happy_path_extracts_all_fields():
    html = _read("foley_fc_tryouts.html")
    rows = parse_tryouts_page_html(
        html,
        club_name_raw="Foley FC",
        source_url="https://foleyfc.example.com/tryouts/",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["club_name_raw"] == "Foley FC"
    assert row["tryout_date"] == datetime(2026, 8, 5)
    assert row["age_group"] == "U12"
    assert row["gender"] == "M"
    assert "Foley" in (row["location"] or "")


def test_parse_page_no_date_skips_and_warns(caplog):
    """A page without a parseable date AND no registration platform
    link must produce zero rows and emit a warning."""
    html = _read("no_date_tryouts.html")
    with caplog.at_level("WARNING"):
        rows = parse_tryouts_page_html(
            html,
            club_name_raw="Ghost FC",
            source_url="https://ghost.example.com/tryouts/",
        )
    assert rows == []
    # Warning text expanded — now mentions registration links too since
    # we emit reg-only rows when a date is missing but links exist.
    assert any(
        "no date or registration link" in rec.message
        for rec in caplog.records
    )


# --------------------------------------------------------------------------- registration link extraction


def test_extract_registration_links_gotsport():
    html = 'Register: <a href="https://system.gotsport.com/org_event/events/45123/teams">Click</a>'
    reg = extract_registration_links(html)
    assert reg["gotsport_event_ids"] == ["45123"]
    assert reg["primary_url"] == "https://system.gotsport.com/org_event/events/45123"


def test_extract_registration_links_tgs():
    html = '<a href="https://public.totalglobalsports.com/events/3979">Register</a>'
    reg = extract_registration_links(html)
    assert reg["tgs_event_ids"] == ["3979"]
    assert "totalglobalsports.com/events/3979" in reg["primary_url"]


def test_extract_registration_links_leagueapps():
    html = '<a href="https://myclub.leagueapps.com/clubteams/3167131-registration">Register</a>'
    reg = extract_registration_links(html)
    assert len(reg["leagueapps_urls"]) == 1
    assert reg["primary_url"].startswith("https://myclub.leagueapps.com/")


def test_extract_registration_links_filters_leagueapps_marketing():
    """LeagueApps marketing URLs (/products/, /pricing, /blog) must be
    filtered out — they're footer noise, not registration entry points."""
    html = """
    <a href="https://www.leagueapps.com/products/design/">LA Products</a>
    <a href="https://www.leagueapps.com/pricing">Pricing</a>
    <a href="https://myclub.leagueapps.com/clubteams/3167131">Real Registration</a>
    """
    reg = extract_registration_links(html)
    assert len(reg["leagueapps_urls"]) == 1
    assert "clubteams" in reg["leagueapps_urls"][0]


def test_extract_registration_links_priority_order():
    """Primary URL preference: GotSport > TGS > LeagueApps."""
    html = """
    <a href="https://myclub.leagueapps.com/clubteams/1">LA</a>
    <a href="https://system.gotsport.com/events/45123">GS</a>
    <a href="https://public.totalglobalsports.com/events/3979">TGS</a>
    """
    reg = extract_registration_links(html)
    assert reg["primary_url"].endswith("/events/45123")


def test_extract_registration_links_empty_html_returns_empty_lists():
    reg = extract_registration_links("")
    assert reg["gotsport_event_ids"] == []
    assert reg["tgs_event_ids"] == []
    assert reg["leagueapps_urls"] == []
    assert reg["primary_url"] is None


# --------------------------------------------------------------------------- registration-only row emission


def test_parse_page_registration_only_emits_row_without_date():
    """When no date is parseable but a registration platform link is
    present, emit a single row with tryout_date=None and url set."""
    html = """
    <html><body>
    <h1>Tryouts</h1>
    <p>Join us! Register here:</p>
    <a href="https://myclub.leagueapps.com/clubteams/3167131">Register</a>
    </body></html>
    """
    rows = parse_tryouts_page_html(
        html,
        club_name_raw="Test FC",
        source_url="https://testfc.example.com/tryouts/",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["tryout_date"] is None
    assert "leagueapps.com/clubteams/3167131" in row["url"]
    # Notes is a JSON blob describing the captured registration links.
    assert row["notes"] is not None
    import json as _json
    parsed = _json.loads(row["notes"])
    assert "registration" in parsed
    assert "leagueapps_urls" in parsed["registration"]


def test_parse_page_dated_row_also_captures_notes_when_link_present():
    """When BOTH a date AND a registration link are on the page, the
    dated row includes the notes JSON and prefers the registration URL
    over the source URL."""
    html = """
    <html><body>
    <h1>August 5, 2026 - U12 Boys Tryouts</h1>
    <a href="https://system.gotsport.com/org_event/events/45123/teams">Register</a>
    </body></html>
    """
    rows = parse_tryouts_page_html(
        html,
        club_name_raw="Test FC",
        source_url="https://testfc.example.com/tryouts/",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["tryout_date"] == datetime(2026, 8, 5)
    assert "gotsport" in row["url"]
    assert row["notes"] is not None


# --------------------------------------------------------------------------- writer with stubbed cursor


class _FakeCursor:
    def __init__(self, script: List[Tuple[Any, int]]):
        self.script = list(script)
        self.executed: List[Tuple[str, Dict[str, Any]]] = []
        self._last: Any = None
        self.rowcount: int = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql: str, params: Dict[str, Any]):
        op = sql.strip().split()[0].upper()
        self.executed.append((op, dict(params)))
        if self.script:
            row, rc = self.script.pop(0)
            self._last = row
            self.rowcount = rc
        else:
            self._last = None
            self.rowcount = 0

    def fetchone(self):
        return self._last


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


def _make_row(**overrides):
    base = {
        "club_name_raw": "Foley FC",
        "tryout_date": datetime(2026, 8, 5),
        "age_group": "U12",
        "gender": "M",
        "location": "Foley, AL",
        "source_url": "https://foleyfc.example.com/tryouts/",
        "notes": None,
    }
    base.update(overrides)
    return base


def test_writer_inserts_then_reinsert_is_idempotent():
    """Same (club, date, age, gender) twice with zero mutable-field
    change → 1 insert + 1 no-op (neither counted)."""
    row = _make_row()
    script: List[Tuple[Any, int]] = [
        ((True,), 1),   # first run: inserted
        (None, 0),      # second run: WHERE predicate short-circuits
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    c1 = insert_tryouts([row], conn=conn)
    assert c1["inserted"] == 1
    c2 = insert_tryouts([row], conn=conn)
    assert c2["inserted"] == 0
    assert c2["updated"] == 0


def test_writer_counts_update_when_location_changes():
    first = _make_row(location="Old Park")
    second = _make_row(location="New Park")
    script: List[Tuple[Any, int]] = [
        ((True,), 1),   # inserted
        ((False,), 1),  # updated (location changed)
    ]
    cur = _FakeCursor(script)
    conn = _FakeConn(cur)

    c1 = insert_tryouts([first], conn=conn)
    c2 = insert_tryouts([second], conn=conn)
    assert c1["inserted"] == 1
    assert c2["updated"] == 1


def test_writer_skips_row_missing_club_name_raw():
    bad = _make_row()
    del bad["club_name_raw"]
    # No cursor activity expected.
    cur = _FakeCursor([])
    conn = _FakeConn(cur)
    counts = insert_tryouts([bad], conn=conn)
    assert counts["skipped"] == 1
    assert counts["inserted"] == 0


def test_writer_sends_null_club_id_literal():
    from ingest.tryouts_writer import _INSERT_TRYOUT_SQL
    assert "club_id" in _INSERT_TRYOUT_SQL
    assert "%(club_id)s" not in _INSERT_TRYOUT_SQL
