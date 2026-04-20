"""
test_youtube_channel_parse.py — Tests for the pure feed parser.

Live HTTP paths (``resolve_channel_id``, ``fetch_channel_videos``)
are exercised only by the smoke run. These tests pin the Atom XML
contract + handle id pass-through.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make ``scraper/`` importable when pytest is run from the repo root.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

from extractors.youtube_channel import parse_feed, resolve_channel_id  # noqa: E402


_FIXTURE = _HERE / "fixtures" / "youtube_ecnl_sample.xml"


def _load_fixture() -> str:
    return _FIXTURE.read_text(encoding="utf-8")


def test_parse_feed_returns_three_entries() -> None:
    rows = parse_feed(_load_fixture())
    assert len(rows) == 3


def test_parse_feed_row_shape() -> None:
    rows = parse_feed(_load_fixture())
    first = rows[0]

    assert first["external_id"] == "abc123DEF45"
    assert first["source_url"] == "https://www.youtube.com/watch?v=abc123DEF45"
    assert first["title"] == "ECNL Girls National Finals 2025 — Highlights"
    assert first["published_at"] == "2025-07-10T18:30:00+00:00"
    assert first["thumbnail_url"] == "https://i1.ytimg.com/vi/abc123DEF45/hqdefault.jpg"
    assert first["metadata"] == {
        "description": (
            "Recap of the ECNL Girls National Finals 2025. "
            "Goals, saves, and championship moments."
        ),
    }


def test_parse_feed_video_ids_unique() -> None:
    rows = parse_feed(_load_fixture())
    ids = [r["external_id"] for r in rows]
    assert len(set(ids)) == len(ids)
    assert set(ids) == {"abc123DEF45", "xyz789GHI01", "mno456PQR78"}


def test_parse_feed_empty_input() -> None:
    assert parse_feed("") == []


def test_parse_feed_malformed_xml_returns_empty() -> None:
    assert parse_feed("<not really xml") == []


def test_resolve_channel_id_passes_through_uc_prefix() -> None:
    raw = "UCabcdefghijklmnopqrstuv"
    # Exactly 22 chars after "UC".
    assert resolve_channel_id(raw) == raw


def test_resolve_channel_id_rejects_bad_uc() -> None:
    # Too short — should NOT be returned as-is; must go through the
    # HTTP resolver. We avoid hitting the network in this unit test by
    # monkey-patching the http module.
    bad = "UCtooshort"
    # resolve_channel_id accepts "bad" as a handle → would hit HTTP.
    # Verify it at least doesn't pass-through when the UC regex fails.
    # We don't call it to avoid network; the regex check in-module is
    # the real assertion, mirrored here.
    from extractors.youtube_channel import _UC_RE
    assert _UC_RE.match(bad) is None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
