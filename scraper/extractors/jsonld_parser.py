"""
jsonld_parser.py — Generic schema.org JSON-LD extraction helpers.

Many youth-soccer club sites (Squarespace, SportsEngine, Duda/360Player,
WordPress, hand-rolled CMSes) embed structured data in
``<script type="application/ld+json">`` blocks. This module provides
shared parsing primitives so per-CMS extractors don't each re-implement
the same logic.

Design rules:
  * Pure parsing — never raises on malformed input. Returns empty list
    or skips the block. Logs a warning instead so the runner keeps going.
  * Top-level JSON arrays are flattened into the returned list. A single
    ``<script>`` block is allowed to contain ``[{...}, {...}]``.
  * ``@graph`` containers (a sibling-block convention) are also
    flattened. Schema.org publishers commonly emit
    ``{"@context": "...", "@graph": [{...}, {...}]}``.
  * ``@type`` may be a string OR a list of strings. ``find_by_type``
    handles both.

This module is intentionally dependency-light: ``BeautifulSoup`` (already
a project dep) for HTML parsing, ``json`` from stdlib, ``logging``.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------- low-level extraction


def extract_jsonld(html: str) -> List[Dict[str, Any]]:
    """Parse all ``<script type="application/ld+json">`` blocks in ``html``.

    Returns a flat list of dicts. Behavior:
      * A block whose JSON root is an array → each element is appended.
      * A block whose JSON root is a dict containing ``@graph`` →
        each element of ``@graph`` is appended (the wrapper itself
        is dropped if it has no other keys beyond ``@context`` /
        ``@graph``; otherwise the wrapper is also kept).
      * A block whose JSON root is a dict otherwise → appended as-is.
      * A block that fails to parse → logged at WARNING and skipped.

    Non-dict, non-list JSON roots (numbers, strings, etc.) are skipped
    silently — they don't carry schema.org payload.
    """
    if not html:
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as exc:  # pragma: no cover — bs4 rarely raises on str
        logger.warning("jsonld_parser: BeautifulSoup failed: %s", exc)
        return []

    blocks: List[Dict[str, Any]] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string
        if raw is None:
            # Some sites split content across child nodes; .get_text() is a fallback.
            raw = tag.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "jsonld_parser: skipping malformed JSON-LD block (%s): %s",
                exc.__class__.__name__,
                str(exc)[:160],
            )
            continue

        for item in _flatten(payload):
            if isinstance(item, dict):
                blocks.append(item)

    return blocks


def _flatten(payload: Any) -> Iterable[Any]:
    """Yield dicts from a JSON-LD payload, unwrapping arrays and ``@graph``."""
    if isinstance(payload, list):
        for elem in payload:
            yield from _flatten(elem)
        return

    if isinstance(payload, dict):
        graph = payload.get("@graph")
        if isinstance(graph, list):
            # Yield every node in @graph.
            for elem in graph:
                yield from _flatten(elem)
            # If the wrapper carries any payload beyond @context/@graph, keep it.
            extras = {k for k in payload.keys() if k not in ("@context", "@graph")}
            if extras:
                yield payload
            return
        yield payload
        return

    # Scalars / nulls — drop.
    return


# ---------------------------------------------------------------- type filtering


def _types_of(block: Dict[str, Any]) -> List[str]:
    """Return ``@type`` as a list of strings, accepting str OR list-of-str."""
    raw = block.get("@type")
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [t for t in raw if isinstance(t, str)]
    return []


def find_by_type(
    blocks: List[Dict[str, Any]], type_name: str
) -> List[Dict[str, Any]]:
    """Return all blocks whose ``@type`` matches ``type_name``.

    Match is exact-string. ``@type`` may be a single string or a list of
    strings (the spec permits both). Common schema.org types we expect
    to look up: ``SportsTeam``, ``SportsOrganization``, ``Person``,
    ``Event``, ``Organization``, ``LocalBusiness``.
    """
    if not type_name:
        return []
    out: List[Dict[str, Any]] = []
    for block in blocks:
        if type_name in _types_of(block):
            out.append(block)
    return out


# ---------------------------------------------------------------- convenience helpers


def extract_athletes(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Pull ``athlete`` arrays from any ``SportsTeam`` blocks.

    schema.org ``SportsTeam`` carries roster data under the ``athlete``
    property, which may be a single Person dict or a list of Persons.
    Returns a flat list of player dicts. Non-dict elements are dropped.
    """
    out: List[Dict[str, Any]] = []
    for team in find_by_type(blocks, "SportsTeam"):
        athletes = team.get("athlete")
        if athletes is None:
            continue
        if isinstance(athletes, dict):
            out.append(athletes)
        elif isinstance(athletes, list):
            for a in athletes:
                if isinstance(a, dict):
                    out.append(a)
    return out


# Properties that conventionally carry Person dicts on Organization /
# SportsTeam / SportsOrganization blocks. Order matters only for docs;
# we union across all of them.
_PERSON_NESTING_PROPS = ("member", "employee", "coach")


def extract_persons(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Return all Person blocks, both top-level and nested.

    Sources:
      * Top-level: any block with ``@type == "Person"``.
      * Nested: ``member`` / ``employee`` / ``coach`` properties on
        Organization-like blocks (``Organization``, ``SportsOrganization``,
        ``SportsTeam``, ``LocalBusiness``).

    Each value may be a single Person dict OR a list of dicts. Non-dict
    entries are dropped silently (some sites embed strings here, which
    isn't useful for us).

    The returned list may contain duplicates if a Person is referenced
    in multiple places — callers that care should de-dup by ``name`` or
    by URL.
    """
    out: List[Dict[str, Any]] = []

    # Top-level Person blocks.
    out.extend(find_by_type(blocks, "Person"))

    # Nested Persons under Organization-like containers.
    for block in blocks:
        types = _types_of(block)
        if not types:
            continue
        # Cheap container check — we just iterate the known props on every
        # block; if it's not an Org-like block these keys won't exist.
        for prop in _PERSON_NESTING_PROPS:
            value = block.get(prop)
            if value is None:
                continue
            if isinstance(value, dict):
                out.append(value)
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, dict):
                        out.append(v)

    return out
