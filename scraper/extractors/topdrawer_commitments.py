"""
topdrawer_commitments.py — Parse TopDrawerSoccer commitment-list HTML.

Pure function — no network, no DB. Callers fetch HTML via
``scraper.utils.http.get`` and hand raw strings to
``parse_topdrawer_commitments_html``.

TopDrawerSoccer publishes public HTML pages listing college commitments.
Each commitment row typically contains:

  - Player name
  - Graduation year
  - Position (optional)
  - Club (raw name; resolved to canonical_clubs by a separate linker pass)
  - College (raw name; scrapers may also attempt a direct colleges match
    at write time by exact name)
  - Commitment date (optional — may be just month/year on some pages)

Because the TDS page layout shifts periodically (and we don't have a
stable element ID contract), this parser is intentionally tolerant:

  - We first look for a semantic table (``<table>`` with header cells
    naming Player / Position / College / Club).
  - Failing that, we look for list-style commitment cards with
    predictable data-label attributes.
  - Rows missing a player name OR a college name are dropped (the
    natural key requires both).

COVERAGE & LIMITATIONS (April 2026)
-----------------------------------
  * TDS aggressively blocks bulk crawlers. Expect HTTP 403 at volume.
    The runner caps ``--limit`` at 20 by default for that reason; adding
    rotating residential proxies via ``proxy_config.yaml`` under
    ``topdrawersoccer.com`` will be required before scaling.

  * Commitment dates on some listing pages are month/year only. We
    best-effort to a YYYY-MM-01 date; pages with no date parse as
    ``commitment_date=None``.

  * Clubs on TDS often use short names that won't alias-match to
    canonical_clubs on the first pass. That's fine — the linker writes
    a new alias on fuzzy hit and future scrapes will match.
"""

from __future__ import annotations

import logging
import re
from datetime import date as _date, datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


_GRAD_YEAR_RE = re.compile(r"\b(20\d{2})\b")

_MONTH_TO_INT = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# "August 5, 2026" / "Aug 5 2026" / "August 5-7, 2026" (picks day 5)
_DATE_MONTH_DAY_YEAR = re.compile(
    r"\b(?P<month>" + "|".join(_MONTH_TO_INT.keys()) + r")\.?\s+(?P<day>\d{1,2})(?:\s*[-\u2013]\s*\d{1,2})?,?\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)
# "8/5/26" or "08/05/2026"
_DATE_NUMERIC = re.compile(r"\b(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{2,4})\b")
# "August 2026" / "Aug 2026" — month + year, no day
_DATE_MONTH_YEAR = re.compile(
    r"\b(?P<month>" + "|".join(_MONTH_TO_INT.keys()) + r")\.?\s+(?P<year>\d{4})\b",
    re.IGNORECASE,
)


def parse_commitment_date(text: str) -> Optional[_date]:
    """Tolerant date parser. Returns a ``datetime.date`` or None."""
    if not text:
        return None
    m = _DATE_MONTH_DAY_YEAR.search(text)
    if m:
        month = _MONTH_TO_INT.get(m.group("month").lower())
        if month:
            try:
                return _date(int(m.group("year")), month, int(m.group("day")))
            except ValueError:
                pass
    m = _DATE_NUMERIC.search(text)
    if m:
        year = int(m.group("y"))
        if year < 100:
            year += 2000
        try:
            return _date(year, int(m.group("m")), int(m.group("d")))
        except ValueError:
            return None
    m = _DATE_MONTH_YEAR.search(text)
    if m:
        month = _MONTH_TO_INT.get(m.group("month").lower())
        if month:
            try:
                return _date(int(m.group("year")), month, 1)
            except ValueError:
                return None
    return None


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = " ".join(value.split()).strip()
    return out or None


def _extract_grad_year(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    m = _GRAD_YEAR_RE.search(value)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


# --------------------------------------------------------------------------
# Table-based parser (primary path)
# --------------------------------------------------------------------------

_HEADER_ALIASES = {
    "player": "player_name",
    "name": "player_name",
    "player name": "player_name",
    "position": "position",
    "pos": "position",
    "grad year": "graduation_year",
    "graduation year": "graduation_year",
    "graduation": "graduation_year",
    "class": "graduation_year",
    "year": "graduation_year",
    "college": "college_name_raw",
    "school": "college_name_raw",
    "committed to": "college_name_raw",
    "commitment": "college_name_raw",
    "club": "club_name_raw",
    "current club": "club_name_raw",
    "team": "club_name_raw",
    "date": "commitment_date",
    "committed": "commitment_date",
    "commitment date": "commitment_date",
    "committed on": "commitment_date",
}


def _map_headers(headers: List[str]) -> List[Optional[str]]:
    """Map raw <th> labels to canonical field keys, preserving position."""
    mapped: List[Optional[str]] = []
    for h in headers:
        key = _HEADER_ALIASES.get(h.strip().lower())
        mapped.append(key)
    return mapped


def _parse_tables(soup: BeautifulSoup) -> List[Dict]:
    rows: List[Dict] = []
    for table in soup.find_all("table"):
        header_cells = []
        thead = table.find("thead")
        if thead:
            header_cells = [_clean(th.get_text(" ", strip=True)) or "" for th in thead.find_all(["th", "td"])]
        if not header_cells:
            first_tr = table.find("tr")
            if first_tr:
                ths = first_tr.find_all("th")
                if ths:
                    header_cells = [_clean(th.get_text(" ", strip=True)) or "" for th in ths]
        if not header_cells:
            continue
        field_map = _map_headers(header_cells)
        if "player_name" not in field_map or "college_name_raw" not in field_map:
            continue

        body = table.find("tbody") or table
        for tr in body.find_all("tr"):
            # Skip header rows that live inside tbody.
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
            if not row_vals.get("player_name") or not row_vals.get("college_name_raw"):
                continue
            rows.append(row_vals)
    return rows


# --------------------------------------------------------------------------
# Card-based fallback
# --------------------------------------------------------------------------

# TDS list pages sometimes render each commitment as a div/li with
# ``data-label`` attributes ("Player", "Class", "College", "Club").
_CARD_SELECTORS = (
    "li.commitment",
    "div.commitment",
    "li.commitment-item",
    "div.commitment-item",
    "tr.commitment",
)


def _parse_cards(soup: BeautifulSoup) -> List[Dict]:
    rows: List[Dict] = []
    seen_cards: set[int] = set()
    for selector in _CARD_SELECTORS:
        for card in soup.select(selector):
            if id(card) in seen_cards:
                continue
            seen_cards.add(id(card))
            row_vals: Dict[str, Optional[str]] = {}
            for labeled in card.find_all(attrs={"data-label": True}):
                key = _HEADER_ALIASES.get((labeled.get("data-label") or "").strip().lower())
                if key is None:
                    continue
                row_vals[key] = _clean(labeled.get_text(" ", strip=True))
            if not row_vals.get("player_name") or not row_vals.get("college_name_raw"):
                continue
            rows.append(row_vals)
    return rows


# --------------------------------------------------------------------------
# Public entry point
# --------------------------------------------------------------------------

def parse_topdrawer_commitments_html(
    html: str,
    *,
    source_url: str,
) -> List[Dict]:
    """Extract commitment rows from a TopDrawerSoccer commitment-list page.

    Returns a list of dicts matching the `commitments` writer contract::

        {
            "player_name":       str,       # required
            "graduation_year":   int|None,
            "position":          str|None,
            "club_name_raw":     str|None,
            "college_name_raw":  str,       # required
            "commitment_date":   date|None,
            "source_url":        str,
        }

    Pure function — no network, no DB.
    """
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")

    raw_rows: List[Dict] = []
    raw_rows.extend(_parse_tables(soup))
    if not raw_rows:
        raw_rows.extend(_parse_cards(soup))

    out: List[Dict] = []
    for r in raw_rows:
        player_name = _clean(r.get("player_name"))
        college_name_raw = _clean(r.get("college_name_raw"))
        if not player_name or not college_name_raw:
            continue
        out.append({
            "player_name": player_name,
            "graduation_year": _extract_grad_year(r.get("graduation_year")),
            "position": _clean(r.get("position")),
            "club_name_raw": _clean(r.get("club_name_raw")),
            "college_name_raw": college_name_raw,
            "commitment_date": parse_commitment_date(r.get("commitment_date") or ""),
            "source_url": source_url,
        })

    logger.info(
        "[topdrawer_commitments] parsed %d commitments from %s",
        len(out), source_url,
    )
    return out
