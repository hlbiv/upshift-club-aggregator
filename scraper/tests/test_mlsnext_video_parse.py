"""
test_mlsnext_video_parse.py — Tests for the MLS NEXT video-list HTML parser.

Live HTTP paths (``fetch_page_html`` / ``fetch_mlsnext_videos``) are
exercised only by the smoke run. These tests pin the card-shape
contract so a future site-redesign breaks loudly in CI instead of
silently returning 0 videos in production.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

from extractors.mlsnext_video import parse_video_list  # noqa: E402


_FIXTURE = _HERE / "fixtures" / "mlsnext_video_sample.html"


def _load_fixture() -> str:
    return _FIXTURE.read_text(encoding="utf-8")


def test_parse_video_list_returns_expected_card_count() -> None:
    rows = parse_video_list(_load_fixture())
    # Fixture captures 4 cards — 3 Generation adidas Cup + 1 MLS NEXT All-Star.
    assert len(rows) == 4


def test_parse_video_list_row_shape() -> None:
    rows = parse_video_list(_load_fixture())
    first = rows[0]
    # Shape contract — every key a downstream writer relies on.
    for key in (
        "external_id", "source_url", "title",
        "thumbnail_url", "duration_seconds",
        "published_at", "metadata",
    ):
        assert key in first, f"missing key {key!r}"
    assert first["published_at"] is None
    assert isinstance(first["metadata"], dict)
    assert first["metadata"]["origin"] == "mlssoccer.com/mlsnext/video"
    assert first["metadata"]["detail_path"].startswith("/")


def test_parse_video_list_external_ids_are_uuids_and_unique() -> None:
    rows = parse_video_list(_load_fixture())
    ids = [r["external_id"] for r in rows]
    # All 36-char lowercase canonical UUIDs.
    for v in ids:
        assert len(v) == 36
        assert v == v.lower()
        assert v.count("-") == 4
    # Uniqueness — our dedupe step must hold.
    assert len(set(ids)) == len(ids)


def test_parse_video_list_absolute_source_urls() -> None:
    rows = parse_video_list(_load_fixture())
    for r in rows:
        assert r["source_url"].startswith("https://www.mlssoccer.com/")
        # Must include the original path.
        assert r["source_url"].endswith(r["metadata"]["detail_path"])


def test_parse_video_list_parses_duration_when_present() -> None:
    rows = parse_video_list(_load_fixture())
    # At least one card in the fixture has a M:SS duration badge.
    durations = [r["duration_seconds"] for r in rows if r["duration_seconds"]]
    assert durations, "expected at least one parsed duration in fixture"
    # Sanity bounds: no highlight is ever 0s or 99+ minutes.
    for d in durations:
        assert 1 <= d <= 99 * 60


def test_parse_video_list_thumbnail_prefers_data_src() -> None:
    rows = parse_video_list(_load_fixture())
    # The live page ships inline ``src="data:image/gif;..."`` placeholders
    # and the real URL sits on ``data-src``. Every card should end up with
    # an ``https://images.mlssoccer.com/...`` URL, not a data: URI.
    for r in rows:
        thumb = r["thumbnail_url"]
        if thumb is not None:
            assert not thumb.startswith("data:")
            assert thumb.startswith("http")


def test_parse_video_list_empty_input() -> None:
    assert parse_video_list("") == []


def test_parse_video_list_ignores_non_brightcove_anchors() -> None:
    # An anchor that isn't a brightcove card shouldn't leak into output.
    html = """
    <html><body>
      <a class="fm-card-wrap -customentity -article"
         href="/mlsnext/news/some-article" data-id="not-a-uuid"
         title="An article">Article</a>
    </body></html>
    """
    assert parse_video_list(html) == []


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-q"]))
