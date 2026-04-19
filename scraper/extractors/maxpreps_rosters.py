"""
maxpreps_rosters.py — Parse a MaxPreps high-school roster page.

Pure function. Given raw HTML, returns a list of per-player dicts. No
HTTP, no DB writes. Downstream handlers (writer, runner) do the wiring.

MaxPreps roster pages historically render the player table in one of
two common shapes:

  1. Sidearm/CBS-style table with a <thead> listing columns like
     ``#``, ``Name``, ``Pos``, ``Grade``, ``Ht``, ``Wt``.
  2. Card-style layout with ``div[data-player-id]`` and nested
     class selectors per field.

We try Strategy 1 (header-aware table) first — it's the most stable
shape and is what's shown in the public "Team" pages. On miss, we fall
through to Strategy 2. A final fallback walks any ``<tr>`` inside a
container labelled "Roster" and looks for a name-shaped cell.

The extractor is deliberately lenient: missing fields → None, not an
error. The runner treats "0 players parsed" as a parse failure, which
lets live-volume 403s and anti-bot redirect pages (which sometimes
return a 200 with a captcha body) surface cleanly.

Output schema (one dict per player):
    {
        "player_name":     str,
        "jersey_number":   str | None,    # e.g. "10", "10A", sometimes blank
        "graduation_year": int | None,
        "position":        str | None,
        "height":          str | None,    # stored as-is, e.g. "5'11\""
    }
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Grade → graduation-year mapping
# ---------------------------------------------------------------------------

_GRADE_TO_OFFSET: Dict[str, int] = {
    # Number of years until graduation, assuming a standard HS 9-12 ladder.
    # We add this to the current academic year's spring-end year.
    "fr": 4, "freshman": 4, "9": 4, "9th": 4,
    "so": 3, "sophomore": 3, "10": 3, "10th": 3,
    "jr": 2, "junior": 2, "11": 2, "11th": 2,
    "sr": 1, "senior": 1, "12": 1, "12th": 1,
}


def _current_spring_year() -> int:
    """Return the spring-end year of the current academic year.

    Academic years run Aug → May. If we're in Aug-Dec the "spring end"
    is next calendar year; Jan-Jul it's the current calendar year.
    """
    now = datetime.now(timezone.utc)
    return now.year + 1 if now.month >= 8 else now.year


def _grade_to_grad_year(raw: Optional[str]) -> Optional[int]:
    """Map 'Sr'/'12'/'Junior' etc. to an absolute YYYY graduation year.

    Returns None if the grade string is empty or unrecognized.
    """
    if not raw:
        return None
    key = raw.strip().lower().replace(".", "")
    # If the cell already contains a 4-digit year, prefer it verbatim —
    # MaxPreps sometimes renders the class year directly.
    m = re.search(r"\b(20\d{2})\b", key)
    if m:
        return int(m.group(1))
    offset = _GRADE_TO_OFFSET.get(key)
    if offset is None:
        # Try to pick up "Class of 2027" style strings.
        m2 = re.search(r"class of\s+(20\d{2})", key)
        if m2:
            return int(m2.group(1))
        return None
    return _current_spring_year() + (offset - 1)


# ---------------------------------------------------------------------------
# Header-aware column index
# ---------------------------------------------------------------------------

class _ColIdx:
    __slots__ = ("jersey", "name", "position", "grade", "height")

    def __init__(self) -> None:
        self.jersey: Optional[int] = None
        self.name: Optional[int] = None
        self.position: Optional[int] = None
        self.grade: Optional[int] = None
        self.height: Optional[int] = None


def _build_col_idx(headers: List[str]) -> _ColIdx:
    """Detect column semantics from <th> text. Header text is matched
    case-insensitively against regex alternatives common on MaxPreps."""
    idx = _ColIdx()
    for i, raw in enumerate(headers):
        stripped = raw.strip()
        h = re.sub(r"[^a-z0-9 ]", " ", stripped.lower()).strip()
        h = re.sub(r"\s+", " ", h)
        if idx.jersey is None and (stripped == "#" or re.match(r"^(no|num|number|jersey)\b", h)):
            idx.jersey = i
            continue
        if not h:
            continue
        if idx.name is None and re.search(r"\b(name|player|full name)\b", h):
            idx.name = i
        elif idx.position is None and re.search(r"\b(pos|position)\b", h):
            idx.position = i
        elif idx.grade is None and re.search(r"\b(grade|yr|year|class|cl)\b", h):
            idx.grade = i
        elif idx.height is None and re.search(r"\b(ht|height)\b", h):
            idx.height = i
    return idx


def _cell_text(td: Tag) -> str:
    return re.sub(r"\s+", " ", td.get_text()).strip()


def _clean_name(raw: str) -> str:
    """Strip leading jersey-number prefixes and trailing badges from a
    name cell. MaxPreps sometimes embeds ``#10 John Smith`` in one td."""
    name = re.sub(r"^#?\d+[A-Za-z]?\s*[-–—]?\s*", "", raw).strip()
    # Drop trailing suffixes like "(C)" for captain.
    name = re.sub(r"\s*\([A-Z]{1,3}\)\s*$", "", name).strip()
    return name


# ---------------------------------------------------------------------------
# Public parser
# ---------------------------------------------------------------------------

def parse_maxpreps_roster(html: str) -> List[Dict]:
    """Extract player rows from a MaxPreps HS roster page.

    Returns a list of dicts with keys ``player_name``, ``jersey_number``,
    ``graduation_year``, ``position``, ``height``. Unknown fields → None.
    """
    soup = BeautifulSoup(html, "html.parser")
    players: List[Dict] = []

    # ---- Strategy 1: header-aware <table> with a Name column ----
    for table in soup.find_all("table"):
        headers: List[str] = []
        thead = table.find("thead")
        if thead:
            first_tr = thead.find("tr")
            if first_tr:
                headers = [_cell_text(th) for th in first_tr.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                ths = first_tr.find_all("th")
                if ths:
                    headers = [_cell_text(th) for th in ths]
        if not headers:
            continue

        idx = _build_col_idx(headers)
        if idx.name is None:
            continue

        tbody = table.find("tbody")
        body_rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

        for tr in body_rows:
            cells = [_cell_text(td) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue

            def _get(i: Optional[int]) -> Optional[str]:
                if i is None or i >= len(cells):
                    return None
                v = cells[i].strip()
                return v or None

            raw_name = _get(idx.name) or ""
            name = _clean_name(raw_name)
            if not name or len(name) < 2:
                continue

            jersey_raw = _get(idx.jersey)
            jersey = jersey_raw.lstrip("#").strip() if jersey_raw else None

            players.append({
                "player_name": name,
                "jersey_number": jersey or None,
                "graduation_year": _grade_to_grad_year(_get(idx.grade)),
                "position": _get(idx.position),
                "height": _get(idx.height),
            })

        if players:
            return players

    # ---- Strategy 2: card-style div[data-player-id] layout ----
    card_selectors = [
        "div[data-player-id]",
        "li[data-player-id]",
        "div.roster-player",
        "li.roster-player",
    ]
    for sel in card_selectors:
        for el in soup.select(sel):
            name_el = el.select_one(
                "a.name, .player-name, .name, h3, h4"
            )
            name = _clean_name(name_el.get_text().strip()) if name_el else ""
            if not name or len(name) < 2:
                continue
            jersey_el = el.select_one(".jersey-number, .number, .num")
            pos_el = el.select_one(".position, .pos")
            grade_el = el.select_one(".grade, .class, .year")
            ht_el = el.select_one(".height, .ht")
            players.append({
                "player_name": name,
                "jersey_number": jersey_el.get_text().strip() if jersey_el else None,
                "graduation_year": _grade_to_grad_year(
                    grade_el.get_text().strip() if grade_el else None
                ),
                "position": pos_el.get_text().strip() if pos_el else None,
                "height": ht_el.get_text().strip() if ht_el else None,
            })
        if players:
            return players

    return players
