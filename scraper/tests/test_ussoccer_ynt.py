"""
Tests for the US Soccer YNT call-up extractor.

Exercises ``parse_article_html`` against a synthesized press-release
fixture shaped like a real U-17 BNT training-camp announcement.
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.ussoccer_ynt import (  # noqa: E402
    parse_article_html,
    _detect_age_group,
    _detect_gender,
    _detect_camp_dates,
    _parse_inline_line,
)


FIXTURE = (
    Path(__file__).parent / "fixtures" / "ussoccer_ynt_sample.html"
)


def _load() -> str:
    return FIXTURE.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Helper-function coverage
# ---------------------------------------------------------------------------


def test_detect_age_group_handles_common_forms():
    assert _detect_age_group("U-17 BNT Roster") == "U-17"
    assert _detect_age_group("U19 Men's announcement") == "U-19"
    assert _detect_age_group("Under-20 camp") == "U-20"
    assert _detect_age_group("no age here") is None


def test_detect_gender_marker_priority():
    assert _detect_gender("U-17 BNT Roster") == "boys"
    assert _detect_gender("U-17 Girls' National Team") == "girls"
    assert _detect_gender("U17 WYNT January Camp") == "girls"
    assert _detect_gender("a story about nothing") is None


def test_detect_camp_dates_same_month():
    start, end = _detect_camp_dates("The camp runs January 5-12, 2026.")
    assert start == date(2026, 1, 5)
    assert end == date(2026, 1, 12)


def test_detect_camp_dates_cross_month():
    start, end = _detect_camp_dates(
        "The training window is January 28 - February 3, 2026."
    )
    assert start == date(2026, 1, 28)
    assert end == date(2026, 2, 3)


def test_parse_inline_line_em_dash():
    row = _parse_inline_line("Matteo Alvarez — GK, FC Dallas (2027)")
    assert row is not None
    assert row["player_name"] == "Matteo Alvarez"
    assert row["position"] == "GK"
    assert row["club_name_raw"] == "FC Dallas"
    assert row["graduation_year"] == 2027


def test_parse_inline_line_rejects_non_roster():
    assert _parse_inline_line("Home") is None
    assert _parse_inline_line("For more on the U-17 BNT, visit") is None


# ---------------------------------------------------------------------------
# Article-level parse on the fixture
# ---------------------------------------------------------------------------


def test_parse_article_html_fixture_yields_expected_shape():
    rows = parse_article_html(
        _load(),
        source_url=(
            "https://www.ussoccer.com/stories/2025/12/"
            "u17-bnt-announces-january-2026-roster"
        ),
    )

    assert len(rows) >= 10, f"expected ≥10 players, got {len(rows)}"

    # Every row must carry the required populated fields.
    for r in rows:
        assert r["age_group"] == "U-17"
        assert r["gender"] == "boys"
        assert r["player_name"]
        assert r["club_name_raw"], f"row missing club_name_raw: {r}"
        assert r["source_url"].startswith("https://www.ussoccer.com/")

    # Camp metadata — should propagate from the article body.
    camp_events = {r["camp_event"] for r in rows}
    assert "January 2026 Training Camp" in camp_events

    start_dates = {r["camp_start_date"] for r in rows}
    end_dates = {r["camp_end_date"] for r in rows}
    assert date(2026, 1, 5) in start_dates
    assert date(2026, 1, 12) in end_dates

    # Spot-check a row we know is in the fixture.
    by_name = {r["player_name"]: r for r in rows}
    assert "Matteo Alvarez" in by_name
    alvarez = by_name["Matteo Alvarez"]
    assert alvarez["position"] == "GK"
    assert alvarez["club_name_raw"] == "FC Dallas"
    assert alvarez["graduation_year"] == 2027


def test_parse_article_html_dedup():
    """Re-running the parser on the same HTML must be idempotent: the
    roster <table> loop + the inline-fallback must not double-count."""
    html = _load()
    url = "https://www.ussoccer.com/stories/2025/12/u17-bnt-january-roster"
    first = parse_article_html(html, source_url=url)
    second = parse_article_html(html, source_url=url)
    assert len(first) == len(second)


def test_parse_article_html_empty_returns_empty():
    assert parse_article_html("", source_url="https://x.example") == []


def test_parse_article_html_no_age_or_gender_returns_empty():
    # No U-NN / no boys/girls anywhere → parser bails rather than
    # emitting rows with bogus age_group / gender.
    html = (
        "<html><head><title>Press release</title></head>"
        "<body><article><h1>A press release</h1></article></body></html>"
    )
    assert parse_article_html(html, source_url="https://x.example") == []


def test_parse_article_html_respects_hints():
    html = (
        "<html><head><title>Press release</title></head>"
        "<body><article><h1>A release</h1>"
        "<table><thead><tr><th>Name</th><th>Club</th></tr></thead>"
        "<tbody><tr><td>Jane Doe</td><td>SoCal Blues</td></tr></tbody>"
        "</table></article></body></html>"
    )
    rows = parse_article_html(
        html,
        source_url="https://www.ussoccer.com/stories/u17-girls",
        age_group_hint="U-17",
        gender_hint="girls",
    )
    assert len(rows) == 1
    assert rows[0]["age_group"] == "U-17"
    assert rows[0]["gender"] == "girls"
    assert rows[0]["player_name"] == "Jane Doe"
    assert rows[0]["club_name_raw"] == "SoCal Blues"
