"""
topdrawer_transfer_portal.py — Parse TopDrawerSoccer transfer-tracker HTML.

Pure function — no network, no DB. Callers fetch HTML via
``scraper.utils.http.get`` and hand raw strings to
``parse_topdrawer_transfer_portal_html``.

TopDrawerSoccer publishes seasonal transfer-tracker articles at::

    /college-soccer-articles/{year}-{mens|womens}-division-i-transfer-tracker_aidNNNNN

Each article is a single HTML ``<table>`` with three columns:

    Name | Outgoing College | Incoming College

Position is embedded as a prefix on the Name cell (e.g. "D/F Chloe Bryant",
"M Reece Paget", "GK Some Player"). This parser extracts the prefix tokens
as ``position`` and returns a clean ``player_name``.

Only rows with BOTH an outgoing and an incoming college are emitted. Pages
only list officially-announced destinations, so this should match 100% of
the rendered rows in practice — the filter is a belt-and-braces guard
against partial HTML.

See ``lib/db/src/schema/transfer_portal.ts`` for the companion Drizzle
table. This parser populates the writer contract::

    {
        "player_name":            str,   # required, position prefix stripped
        "position":               str|None,
        "from_college_name_raw":  str,   # required
        "to_college_name_raw":    str,   # required
        "source_url":             str,   # required
    }

``season_window``, ``gender``, ``division`` are attached by the caller
(the runner knows which tracker URL it just fetched); this parser does
NOT try to infer them from the HTML.

COVERAGE & LIMITATIONS (April 2026)
-----------------------------------
  * TDS aggressively blocks bulk crawlers. Expect HTTP 403 at volume.
    The runner caps ``--limit`` at 20 by default for the same reason as
    the commitments scraper — see commitments_runner.py.

  * Position prefix tokens observed in the wild: ``GK``, ``D``, ``M``,
    ``F``, and slash-combined (``D/F``, ``M/F``, ``D/M``). If TDS
    introduces a new token the parser will treat it as part of the
    player name rather than dropping the row — cleanup can happen in
    a follow-up.

  * No date column. The tracker URL itself carries the temporal axis
    (year + gender + division + mid-year vs summer); the parser is
    orthogonal to that metadata.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# Position prefix tokens. Multi-char first (longest-match), then singles.
# Slash-combinations are covered explicitly so "D/F Chloe Bryant" splits
# cleanly into ("D/F", "Chloe Bryant").
_POSITION_TOKENS = (
    "GK",
    "D/F", "D/M", "M/F", "M/D", "F/M", "F/D",
    "D", "M", "F",
)

# Anchored at start. First group captures the token, remainder is the
# player name. Whitespace between is required (otherwise "Dominic" would
# swallow a "D" prefix). `\s+` + `$` fallback guards against rows where
# the cell is position-only for some reason.
_POSITION_PREFIX_RE = re.compile(
    r"^(?P<pos>" + "|".join(re.escape(t) for t in _POSITION_TOKENS) + r")\s+(?P<name>.+)$"
)


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = " ".join(value.split()).strip()
    return out or None


def split_position_prefix(raw_name: str) -> tuple[Optional[str], str]:
    """Split a "POS PlayerName" cell into (position, player_name).

    Returns ``(None, raw_name)`` if no position token is found at the
    start of the string.
    """
    cleaned = _clean(raw_name) or ""
    if not cleaned:
        return None, ""
    m = _POSITION_PREFIX_RE.match(cleaned)
    if not m:
        return None, cleaned
    return m.group("pos"), _clean(m.group("name")) or ""


# --------------------------------------------------------------------------
# Table parser
# --------------------------------------------------------------------------

_HEADER_ALIASES = {
    "name": "player_name",
    "player": "player_name",
    "player name": "player_name",
    "outgoing college": "from_college_name_raw",
    "outgoing": "from_college_name_raw",
    "from": "from_college_name_raw",
    "previous college": "from_college_name_raw",
    "previous school": "from_college_name_raw",
    "incoming college": "to_college_name_raw",
    "incoming": "to_college_name_raw",
    "to": "to_college_name_raw",
    "new college": "to_college_name_raw",
    "new school": "to_college_name_raw",
}


def _map_headers(headers: List[str]) -> List[Optional[str]]:
    mapped: List[Optional[str]] = []
    for h in headers:
        key = _HEADER_ALIASES.get(h.strip().lower())
        mapped.append(key)
    return mapped


def _parse_tables(soup: BeautifulSoup) -> List[Dict]:
    rows: List[Dict] = []
    for table in soup.find_all("table"):
        header_cells: List[str] = []
        thead = table.find("thead")
        if thead:
            header_cells = [
                _clean(th.get_text(" ", strip=True)) or ""
                for th in thead.find_all(["th", "td"])
            ]
        if not header_cells:
            first_tr = table.find("tr")
            if first_tr:
                ths = first_tr.find_all("th")
                if ths:
                    header_cells = [
                        _clean(th.get_text(" ", strip=True)) or "" for th in ths
                    ]
        if not header_cells:
            continue
        field_map = _map_headers(header_cells)
        # A valid transfer-tracker table MUST have all three of
        # player_name / from_college_name_raw / to_college_name_raw.
        required = {"player_name", "from_college_name_raw", "to_college_name_raw"}
        if not required.issubset(set(k for k in field_map if k is not None)):
            continue

        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            if tr.find("th") and not tr.find("td"):
                continue
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue
            row_vals: Dict[str, Optional[str]] = {}
            for idx, cell in enumerate(cells):
                if idx >= len(field_map):
                    break
                key = field_map[idx]
                if key is None:
                    continue
                row_vals[key] = _clean(cell.get_text(" ", strip=True))
            if (
                not row_vals.get("player_name")
                or not row_vals.get("from_college_name_raw")
                or not row_vals.get("to_college_name_raw")
            ):
                continue
            rows.append(row_vals)
    return rows


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------


def parse_topdrawer_transfer_portal_html(
    html: str,
    *,
    source_url: str,
) -> List[Dict]:
    """Extract transfer-portal rows from a TopDrawerSoccer tracker page.

    Returns a list of dicts matching the `transfer_portal_entries` writer
    contract. Position prefix is split off the name cell, so the
    ``player_name`` returned here is clean.

    Pure function — no network, no DB. The caller attaches
    ``season_window``, ``gender``, ``division`` based on which tracker
    URL was fetched.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")

    raw_rows = _parse_tables(soup)

    out: List[Dict] = []
    for r in raw_rows:
        raw_name = r.get("player_name") or ""
        position, player_name = split_position_prefix(raw_name)
        from_college = _clean(r.get("from_college_name_raw"))
        to_college = _clean(r.get("to_college_name_raw"))
        if not player_name or not from_college or not to_college:
            continue
        out.append({
            "player_name": player_name,
            "position": position,
            "from_college_name_raw": from_college,
            "to_college_name_raw": to_college,
            "source_url": source_url,
        })

    logger.info(
        "[topdrawer_transfer_portal] parsed %d entries from %s",
        len(out), source_url,
    )
    return out
