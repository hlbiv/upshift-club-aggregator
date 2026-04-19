"""
Fixture test for extractors.no_source.parse_html.

The no_source module is a container for stub extractors whose registered
URLs have no publicly parseable club list. parse_html honours the stub
contract: it always returns 0 records. A fixture is included purely so
the replay handler has representative HTML to dispatch against.

Run:
    python -m pytest scraper/tests/test_no_source_parse.py -v
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors import no_source  # noqa: E402


FIXTURE = Path(__file__).parent / "fixtures" / "no_source_sample.html"


def _load_fixture() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def test_parse_html_returns_empty_list_for_js_rendered_page():
    """JS-rendered HTML with no parseable club list returns []."""
    records = no_source.parse_html(
        _load_fixture(),
        source_url="https://www.clubchampions.org/clubs",
        league_name="Club Champions League",
    )
    assert records == []


def test_parse_html_returns_empty_list_for_empty_html():
    """Trivially empty HTML returns [] — stable contract across all 3 stubs."""
    records = no_source.parse_html(
        "",
        source_url="https://usclubsoccer.org/npl/",
        league_name="NPL",
    )
    assert records == []


def test_parse_html_is_idempotent_for_any_url():
    """The stub is URL-independent — all registered stubs route here."""
    for url in (
        "https://www.clubchampionsleague.com/",
        "https://clubchampions.org/",
        "https://usclubsoccer.org/npl/",
        "https://sccl.org/",
    ):
        assert no_source.parse_html("<html/>", source_url=url, league_name="L") == []
