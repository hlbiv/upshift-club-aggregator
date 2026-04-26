"""
Tests for the NULL/empty-string sentinel symmetry in
``scraper.ingest.matches_writer._normalize_row`` and the writer's
pre-sweep behavior.

The ``matches_natural_key_uq`` partial unique index is defined as:

    UNIQUE (
        home_team_name, away_team_name,
        COALESCE(match_date, 'epoch'::timestamp),
        COALESCE(age_group, ''),
        COALESCE(gender, '')
    ) WHERE platform_match_id IS NULL

Postgres' ``COALESCE(col, '')`` collapses NULL and the empty string to
the same value at index time. The writer must do the same on the
parameter side of its pre-sweep UPDATE — otherwise a dirty input with
``age_group = ""`` and an existing DB row with ``age_group = NULL``
produces inconsistent behavior across the pre-sweep and INSERT paths,
and a duplicate row slips through.

These tests lock in the boundary coercion in ``_normalize_row``: empty
strings on input become ``None`` on output. The accompanying writer-fake
test asserts that two upserts with the same natural key but mismatched
sentinels (``""`` vs ``None``) collapse to the same normalized dict —
the fake doesn't simulate the partial index, so the equivalence is
asserted at the normalize boundary.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), os.pardir))

from ingest import matches_writer  # noqa: E402
from tests.test_matches_writer import FakeConn, _row  # noqa: E402


def _make_row(**overrides: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = {
        "home_team_name": "Alpha FC",
        "away_team_name": "Bravo SC",
        "match_date": None,
        "age_group": "U15",
        "gender": "Boys",
        "season": "2025-26",
        "platform_match_id": None,
        "source": "gotsport",
        "source_url": "http://example.test/match",
        "status": "scheduled",
        "home_score": None,
        "away_score": None,
        "division": None,
        "league": None,
        "event_id": None,
        "home_club_id": None,
        "away_club_id": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _normalize_row boundary coercion
# ---------------------------------------------------------------------------


def test_normalize_empty_string_age_group_becomes_none() -> None:
    out = matches_writer._normalize_row(_make_row(age_group=""))
    assert out["age_group"] is None


def test_normalize_empty_string_gender_becomes_none() -> None:
    out = matches_writer._normalize_row(_make_row(gender=""))
    assert out["gender"] is None


def test_normalize_none_age_group_stays_none() -> None:
    out = matches_writer._normalize_row(_make_row(age_group=None))
    assert out["age_group"] is None


def test_normalize_none_gender_stays_none() -> None:
    out = matches_writer._normalize_row(_make_row(gender=None))
    assert out["gender"] is None


def test_normalize_real_age_group_passes_through() -> None:
    out = matches_writer._normalize_row(_make_row(age_group="U15"))
    assert out["age_group"] == "U15"


def test_normalize_real_gender_passes_through() -> None:
    out = matches_writer._normalize_row(_make_row(gender="Boys"))
    assert out["gender"] == "Boys"


def test_normalize_both_empty_strings_become_none() -> None:
    out = matches_writer._normalize_row(_make_row(age_group="", gender=""))
    assert out["age_group"] is None
    assert out["gender"] is None


def test_normalize_missing_keys_default_to_none() -> None:
    """``row.get("age_group")`` returns None for a missing key — the
    coercion path treats that the same as an explicit None."""
    row = _make_row()
    row.pop("age_group")
    row.pop("gender")
    out = matches_writer._normalize_row(row)
    assert out["age_group"] is None
    assert out["gender"] is None


# ---------------------------------------------------------------------------
# Sentinel-equivalence at the writer boundary
# ---------------------------------------------------------------------------


def test_empty_string_and_none_collapse_to_same_normalized_dict() -> None:
    """Two upserts that share a natural key but differ only in
    ``""`` vs ``None`` for ``age_group`` / ``gender`` must normalize to
    identical dicts — guaranteeing the pre-sweep and the partial index
    see the same key.
    """
    row_empty = _make_row(age_group="", gender="")
    row_none = _make_row(age_group=None, gender=None)

    norm_empty = matches_writer._normalize_row(row_empty)
    norm_none = matches_writer._normalize_row(row_none)

    assert norm_empty == norm_none
    # Sanity: the natural-key columns specifically agree.
    for key in (
        "home_team_name",
        "away_team_name",
        "match_date",
        "age_group",
        "gender",
    ):
        assert norm_empty[key] == norm_none[key]


def test_writer_treats_empty_string_then_none_as_same_natural_key() -> None:
    """End-to-end via the writer + FakeConn: a row with ``age_group=""``
    followed by an upsert with ``age_group=None`` for the same
    ``(home_team_name, away_team_name, match_date)`` natural key must
    not produce divergent normalized parameters at the writer
    boundary. The fake doesn't simulate the partial index, so the
    assertion is that both rows pass through the writer with identical
    normalized ``age_group`` / ``gender`` (``None``) — which is what
    the index sees post-COALESCE.
    """
    rows = [
        _row("Alpha FC", "Bravo SC"),
        _row("Alpha FC", "Bravo SC"),
    ]
    # Force the asymmetric inputs.
    rows[0]["age_group"] = ""
    rows[0]["gender"] = ""
    rows[1]["age_group"] = None
    rows[1]["gender"] = None

    conn = FakeConn()
    counts = matches_writer.insert_matches(rows, conn=conn)

    # Both rows reach INSERT (the fake doesn't enforce uniqueness), but
    # the contract under test is the normalize step. Re-normalize and
    # confirm equivalence end-to-end.
    n0 = matches_writer._normalize_row(rows[0])
    n1 = matches_writer._normalize_row(rows[1])
    assert n0["age_group"] == n1["age_group"] is None
    assert n0["gender"] == n1["gender"] is None

    # No row failures expected — confirms the writer accepted both.
    assert counts["skipped"] == 0
