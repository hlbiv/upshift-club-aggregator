"""
Tests for extractors.jsonld_parser.

Inline HTML fixtures only — no network. Covers:
  * SportsTeam page → athlete extraction
  * Person blocks (top-level + nested under member/coach)
  * Malformed JSON block (skipped, never raises)
  * Top-level array shape
  * Event page (find_by_type)
  * @graph wrapper unwrapping
  * @type as list
"""

from __future__ import annotations

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractors.jsonld_parser import (  # noqa: E402
    extract_athletes,
    extract_jsonld,
    extract_persons,
    find_by_type,
)


# --------------------------------------------------------------------------- fixtures


def _wrap(payload: str) -> str:
    """Wrap a JSON-LD payload into a minimal HTML document."""
    return (
        "<!doctype html><html><head>"
        f'<script type="application/ld+json">{payload}</script>'
        "</head><body></body></html>"
    )


SPORTS_TEAM_HTML = _wrap(
    """
    {
      "@context": "https://schema.org",
      "@type": "SportsTeam",
      "name": "Foley FC U12 Boys",
      "sport": "Soccer",
      "athlete": [
        {"@type": "Person", "name": "Player One", "jobTitle": "Forward"},
        {"@type": "Person", "name": "Player Two", "jobTitle": "Midfielder"}
      ],
      "coach": {"@type": "Person", "name": "Coach Smith", "jobTitle": "Head Coach"}
    }
    """
)

PERSON_PAGE_HTML = _wrap(
    """
    {
      "@context": "https://schema.org",
      "@type": "Person",
      "name": "Jane Doe",
      "jobTitle": "Director of Coaching",
      "email": "jane@example.com"
    }
    """
)

EVENT_PAGE_HTML = _wrap(
    """
    {
      "@context": "https://schema.org",
      "@type": "Event",
      "name": "Summer Tryouts 2026",
      "startDate": "2026-08-05",
      "location": {"@type": "Place", "name": "Foley Sports Complex"}
    }
    """
)

ARRAY_HTML = _wrap(
    """
    [
      {"@context": "https://schema.org", "@type": "Person", "name": "A"},
      {"@context": "https://schema.org", "@type": "Person", "name": "B"}
    ]
    """
)

GRAPH_HTML = _wrap(
    """
    {
      "@context": "https://schema.org",
      "@graph": [
        {"@type": "Organization", "name": "Foley FC", "member": [
          {"@type": "Person", "name": "Coach A"},
          {"@type": "Person", "name": "Coach B"}
        ]},
        {"@type": "Event", "name": "Tryout", "startDate": "2026-08-05"}
      ]
    }
    """
)

MALFORMED_HTML = (
    "<!doctype html><html><head>"
    '<script type="application/ld+json">{this is not, valid json</script>'
    '<script type="application/ld+json">'
    '{"@context":"https://schema.org","@type":"Person","name":"Survivor"}'
    "</script>"
    "</head><body></body></html>"
)


# --------------------------------------------------------------------------- extract_jsonld


def test_extract_jsonld_sports_team_page():
    blocks = extract_jsonld(SPORTS_TEAM_HTML)
    assert len(blocks) == 1
    assert blocks[0]["@type"] == "SportsTeam"
    assert blocks[0]["name"] == "Foley FC U12 Boys"


def test_extract_jsonld_array_shape_is_flattened():
    blocks = extract_jsonld(ARRAY_HTML)
    assert len(blocks) == 2
    names = {b["name"] for b in blocks}
    assert names == {"A", "B"}


def test_extract_jsonld_graph_wrapper_is_flattened():
    blocks = extract_jsonld(GRAPH_HTML)
    # Org + Event from the graph; the wrapper is dropped because it has
    # no payload beyond @context/@graph.
    types = sorted(b["@type"] for b in blocks)
    assert types == ["Event", "Organization"]


def test_extract_jsonld_malformed_block_skipped_and_warned(caplog):
    with caplog.at_level(logging.WARNING, logger="extractors.jsonld_parser"):
        blocks = extract_jsonld(MALFORMED_HTML)
    # Malformed block dropped; the second valid block survives.
    assert len(blocks) == 1
    assert blocks[0]["name"] == "Survivor"
    # A warning was emitted referencing JSON-LD parsing.
    assert any("malformed" in r.message.lower() for r in caplog.records)


def test_extract_jsonld_empty_inputs_return_empty_list():
    assert extract_jsonld("") == []
    assert extract_jsonld("<html></html>") == []


# --------------------------------------------------------------------------- find_by_type


def test_find_by_type_event_page():
    blocks = extract_jsonld(EVENT_PAGE_HTML)
    events = find_by_type(blocks, "Event")
    assert len(events) == 1
    assert events[0]["startDate"] == "2026-08-05"
    # Negative case — no SportsTeam on this page.
    assert find_by_type(blocks, "SportsTeam") == []


def test_find_by_type_handles_list_typed_blocks():
    html = _wrap(
        """
        {"@context":"https://schema.org","@type":["LocalBusiness","SportsOrganization"],
         "name":"Foley FC"}
        """
    )
    blocks = extract_jsonld(html)
    assert find_by_type(blocks, "SportsOrganization")[0]["name"] == "Foley FC"
    assert find_by_type(blocks, "LocalBusiness")[0]["name"] == "Foley FC"
    assert find_by_type(blocks, "Person") == []


# --------------------------------------------------------------------------- convenience helpers


def test_extract_athletes_from_sports_team():
    blocks = extract_jsonld(SPORTS_TEAM_HTML)
    athletes = extract_athletes(blocks)
    names = {a["name"] for a in athletes}
    assert names == {"Player One", "Player Two"}


def test_extract_athletes_handles_single_dict_value():
    html = _wrap(
        """
        {"@context":"https://schema.org","@type":"SportsTeam","name":"X",
         "athlete":{"@type":"Person","name":"Solo"}}
        """
    )
    athletes = extract_athletes(extract_jsonld(html))
    assert [a["name"] for a in athletes] == ["Solo"]


def test_extract_persons_finds_top_level_and_nested():
    blocks = extract_jsonld(PERSON_PAGE_HTML) + extract_jsonld(GRAPH_HTML)
    persons = extract_persons(blocks)
    names = {p["name"] for p in persons}
    # Jane (top-level), Coach A + Coach B (nested under member).
    assert {"Jane Doe", "Coach A", "Coach B"}.issubset(names)


def test_extract_persons_includes_nested_coach_property():
    blocks = extract_jsonld(SPORTS_TEAM_HTML)
    persons = extract_persons(blocks)
    names = {p["name"] for p in persons}
    # The "coach" prop on SportsTeam should surface Coach Smith.
    assert "Coach Smith" in names
