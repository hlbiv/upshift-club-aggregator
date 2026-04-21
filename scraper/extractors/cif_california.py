"""
cif_california.py — Parse CIF (California Interscholastic Federation)
state-tournament HTML.

Pure function — no network, no DB. Callers fetch HTML via
``scraper.utils.http.get`` and hand raw strings to
``parse_cif_california_html``.

CIF publishes three shapes of interest (all static HTML, no bot wall):

  1. BRACKET pages — a tournament bracket rendered as an HTML table
     with two teams per match row ("School A — School B"). Scores are
     absent for games not yet played.
  2. RESULTS pages — same shape as bracket pages but the score columns
     are filled in.
  3. RANKINGS pages — an ordered list of schools with a record column
     and sometimes a "section" grouping (CIF Northern/Southern
     Section, divisions, etc.).

COMPLEMENT TO ``hs_rosters`` (MaxPreps)
---------------------------------------
MaxPreps covers per-player HS rosters. CIF adds state-tournament
fixture + result + ranking data that MaxPreps does not expose.

Shape volatility
----------------
CIF pages carry sponsors, ads, and inline JS that drift week-to-week.
The parser is deliberately tolerant:

  * Unknown/extra table columns are ignored.
  * Rows missing the required fields (either
    ``school_name_raw``/``opponent_raw``/``gender`` for matches, or
    ``rank``/``school_name_raw`` for rankings) are dropped, not
    raised — a single malformed row never crashes the scrape.
  * ``source_url`` is always attached so the downstream writer can
    pair the row with its origin page.
  * Section context (e.g. "CIF Southern Section — Division I") is
    carried forward into each subsequent ranking row until the next
    section header.

Output contracts
----------------
The parser returns ``{"matches": [...], "rankings": [...]}`` so a
single page can emit both if it happens to carry both shapes.

Match row::

    {
        "school_name_raw":  str,       # required
        "school_state":     "CA",
        "opponent_raw":     str,       # required
        "match_date":       str|None,  # ISO "YYYY-MM-DD" when parseable
        "gender":           str,       # required, "boys" | "girls"
        "team_level":       str|None,
        "result":           str|None,  # "W" | "L" | "T" | None
        "score_for":        int|None,
        "score_against":    int|None,
        "tournament":       str|None,
        "round":            str|None,
        "season":           str|None,
        "source_url":       str,
    }

Ranking row::

    {
        "state":           "CA",
        "gender":          str,        # required, "boys" | "girls"
        "season":          str|None,
        "rank":            int,        # required
        "school_name_raw": str,        # required
        "record":          str|None,
        "points":          int|None,
        "section":         str|None,
        "source_url":      str,
    }
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")


def _clean(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    out = _WHITESPACE_RE.sub(" ", value).strip()
    return out or None


def _parse_int(value: Optional[str]) -> Optional[int]:
    cleaned = _clean(value)
    if cleaned is None:
        return None
    # Tolerate trailing punctuation like "24pts"
    m = re.match(r"^-?\d+", cleaned)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


_SCORE_RE = re.compile(r"^\s*(\d+)\s*[-–]\s*(\d+)\s*$")


def _parse_score_cell(value: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """Parse a "2-1" / "2–1" score cell into (for, against).

    Returns ``(None, None)`` if the cell is empty or unparseable. Bracket
    pages ship empty score cells until games are played.
    """
    cleaned = _clean(value)
    if not cleaned:
        return (None, None)
    m = _SCORE_RE.match(cleaned)
    if not m:
        return (None, None)
    try:
        return (int(m.group(1)), int(m.group(2)))
    except ValueError:
        return (None, None)


_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%b %d, %Y",
    "%B %d, %Y",
)


def _parse_date(value: Optional[str]) -> Optional[str]:
    cleaned = _clean(value)
    if not cleaned:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _derive_result(
    score_for: Optional[int], score_against: Optional[int]
) -> Optional[str]:
    if score_for is None or score_against is None:
        return None
    if score_for > score_against:
        return "W"
    if score_for < score_against:
        return "L"
    return "T"


# ---------------------------------------------------------------------------
# Header aliasing
# ---------------------------------------------------------------------------

_MATCH_HEADER_ALIASES = {
    "school": "school_name_raw",
    "team": "school_name_raw",
    "home": "school_name_raw",
    "home team": "school_name_raw",
    "opponent": "opponent_raw",
    "opponent school": "opponent_raw",
    "away": "opponent_raw",
    "away team": "opponent_raw",
    "vs": "opponent_raw",
    "vs.": "opponent_raw",
    "date": "match_date",
    "score": "score",
    "result": "score",
    "final": "score",
    "round": "round",
    "matchup": "round",
}


_RANK_HEADER_ALIASES = {
    "rank": "rank",
    "rk": "rank",
    "#": "rank",
    "school": "school_name_raw",
    "team": "school_name_raw",
    "record": "record",
    "w-l-t": "record",
    "overall": "record",
    "points": "points",
    "pts": "points",
}


def _map_headers(headers: List[str], aliases: Dict[str, str]) -> List[Optional[str]]:
    return [aliases.get((h or "").strip().lower()) for h in headers]


def _get_headers(table) -> Tuple[List[str], Any]:
    """Return (header_text_list, header_row_element_to_skip_or_None)."""
    thead = table.find("thead")
    if thead:
        ths = thead.find_all(["th", "td"])
        if ths:
            return (
                [(_clean(th.get_text(" ", strip=True)) or "") for th in ths],
                None,
            )

    first_tr = table.find("tr")
    if first_tr is None:
        return ([], None)

    ths = first_tr.find_all("th")
    if ths:
        return (
            [(_clean(th.get_text(" ", strip=True)) or "") for th in ths],
            first_tr,
        )
    # Fallback: first row's <td> cells as headers (CIF pages often omit
    # <thead>). Callers verify the mapping yields required fields before
    # using the row.
    tds = first_tr.find_all("td")
    if tds:
        return (
            [(_clean(td.get_text(" ", strip=True)) or "") for td in tds],
            first_tr,
        )
    return ([], None)


# ---------------------------------------------------------------------------
# Gender / season inference from page
# ---------------------------------------------------------------------------

_GENDER_KEYWORDS = (
    ("girls", "girls"),
    ("women", "girls"),
    ("female", "girls"),
    ("boys", "boys"),
    ("men", "boys"),
    ("male", "boys"),
)


def _infer_gender(text: str) -> Optional[str]:
    lower = text.lower()
    for kw, mapped in _GENDER_KEYWORDS:
        if kw in lower:
            return mapped
    return None


_SEASON_RE = re.compile(r"(20\d{2})[-–](\d{2})")
_SEASON_SINGLE_RE = re.compile(r"(20\d{2})")


def _infer_season(text: str) -> Optional[str]:
    m = _SEASON_RE.search(text)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m2 = _SEASON_SINGLE_RE.search(text)
    if m2:
        year = int(m2.group(1))
        # Soccer season convention: a single year is treated as the
        # school year ending in that year (e.g. "2026" → "2025-26").
        prev = year - 1
        return f"{prev}-{str(year)[-2:]}"
    return None


def _page_title(soup: BeautifulSoup) -> str:
    parts: List[str] = []
    for sel in ("title", "h1", "h2"):
        for el in soup.find_all(sel):
            txt = _clean(el.get_text(" ", strip=True))
            if txt:
                parts.append(txt)
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Tournament / round inference
# ---------------------------------------------------------------------------

_ROUND_KEYWORDS = (
    "state final",
    "state championship",
    "regional final",
    "regional semifinal",
    "semifinal",
    "quarterfinal",
    "first round",
    "second round",
    "third round",
    "final",
)


def _infer_tournament(text: str) -> Optional[str]:
    lower = text.lower()
    if "cif" in lower and "state" in lower:
        return "CIF State Championship"
    if "cif" in lower:
        return "CIF Championship"
    return None


def _infer_round(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in _ROUND_KEYWORDS:
        if kw in lower:
            return kw.title()
    return None


# ---------------------------------------------------------------------------
# Table classification
# ---------------------------------------------------------------------------

_MATCH_REQUIRED = {"school_name_raw", "opponent_raw"}
_RANK_REQUIRED = {"rank", "school_name_raw"}


def _classify_table(table) -> Tuple[Optional[str], List[Optional[str]], Any]:
    """Classify a <table> as "matches" / "rankings" / unknown.

    Returns ``(kind, field_map, header_row_or_None)``. ``kind`` is
    ``None`` for tables we can't confidently identify, in which case
    the caller must skip the table.
    """
    headers, header_row = _get_headers(table)
    if not headers:
        return (None, [], None)

    match_map = _map_headers(headers, _MATCH_HEADER_ALIASES)
    if _MATCH_REQUIRED.issubset({k for k in match_map if k is not None}):
        return ("matches", match_map, header_row)

    rank_map = _map_headers(headers, _RANK_HEADER_ALIASES)
    if _RANK_REQUIRED.issubset({k for k in rank_map if k is not None}):
        return ("rankings", rank_map, header_row)

    return (None, [], None)


# ---------------------------------------------------------------------------
# Match-row parser
# ---------------------------------------------------------------------------

def _parse_match_rows(
    table,
    field_map: List[Optional[str]],
    header_row,
    *,
    source_url: str,
    page_gender: Optional[str],
    page_season: Optional[str],
    page_tournament: Optional[str],
    page_round: Optional[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    body = table.find("tbody") or table
    for tr in body.find_all("tr"):
        if header_row is not None and tr is header_row:
            continue
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

        school = row_vals.get("school_name_raw")
        opponent = row_vals.get("opponent_raw")
        if not school or not opponent:
            continue

        score_for, score_against = _parse_score_cell(row_vals.get("score"))
        match_date = _parse_date(row_vals.get("match_date"))
        round_text = row_vals.get("round") or page_round

        out.append(
            {
                "school_name_raw": school,
                "school_state": "CA",
                "opponent_raw": opponent,
                "match_date": match_date,
                "gender": page_gender or "",
                "team_level": None,
                "result": _derive_result(score_for, score_against),
                "score_for": score_for,
                "score_against": score_against,
                "tournament": page_tournament,
                "round": round_text,
                "season": page_season,
                "source_url": source_url,
            }
        )
    # Drop rows missing required fields (gender is required on match
    # rows for the natural key — we surface an explicit empty string
    # above only to make the drop visible; filter now).
    return [r for r in out if r["gender"]]


# ---------------------------------------------------------------------------
# Ranking-row parser
# ---------------------------------------------------------------------------

def _find_section_for_row(tr) -> Optional[str]:
    """CIF ranking pages often insert <tr> rows containing a single
    section header cell (e.g. ``<tr><td colspan=4>Northern
    California</td></tr>``). Traverse backwards to find the most-recent
    section header above this row within the same <tbody>."""
    prev = tr.find_previous_sibling("tr")
    while prev is not None:
        tds = prev.find_all(["td", "th"])
        # Section rows have one cell that spans (colspan) the table, and
        # contain no digits (rank rows start with a number).
        if len(tds) == 1:
            text = _clean(tds[0].get_text(" ", strip=True))
            if text and not re.match(r"^\d", text):
                return text
        prev = prev.find_previous_sibling("tr")
    return None


def _parse_ranking_rows(
    table,
    field_map: List[Optional[str]],
    header_row,
    *,
    source_url: str,
    page_gender: Optional[str],
    page_season: Optional[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    body = table.find("tbody") or table
    current_section: Optional[str] = None
    for tr in body.find_all("tr"):
        if header_row is not None and tr is header_row:
            continue
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue

        # Section header row: single cell, non-numeric text.
        if len(cells) == 1:
            text = _clean(cells[0].get_text(" ", strip=True))
            if text and not re.match(r"^\d", text):
                current_section = text
                continue

        row_vals: Dict[str, Optional[str]] = {}
        for idx, cell in enumerate(cells):
            if idx >= len(field_map):
                break
            key = field_map[idx]
            if key is None:
                continue
            row_vals[key] = _clean(cell.get_text(" ", strip=True))

        rank = _parse_int(row_vals.get("rank"))
        school = row_vals.get("school_name_raw")
        if rank is None or not school:
            continue

        out.append(
            {
                "state": "CA",
                "gender": page_gender or "",
                "season": page_season,
                "rank": rank,
                "school_name_raw": school,
                "record": row_vals.get("record"),
                "points": _parse_int(row_vals.get("points")),
                "section": current_section or _find_section_for_row(tr),
                "source_url": source_url,
            }
        )
    return [r for r in out if r["gender"]]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_cif_california_html(
    html: str,
    *,
    source_url: str,
    default_gender: Optional[str] = None,
    default_season: Optional[str] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Extract CIF state-tournament matches + rankings from one page.

    Returns ``{"matches": [...], "rankings": [...]}`` — either list may
    be empty. The parser is tolerant: malformed rows drop silently,
    unrelated tables are skipped.

    ``default_gender`` / ``default_season`` override the page-level
    inference when the caller already knows the URL metadata. Page
    inference (from ``<title>``/``<h1>``) is used as a fallback.
    """
    if not html:
        return {"matches": [], "rankings": []}

    soup = BeautifulSoup(html, "lxml")
    title_text = _page_title(soup)

    page_gender = default_gender or _infer_gender(title_text)
    page_season = default_season or _infer_season(title_text)
    page_tournament = _infer_tournament(title_text)
    page_round = _infer_round(title_text)

    matches: List[Dict[str, Any]] = []
    rankings: List[Dict[str, Any]] = []

    for table in soup.find_all("table"):
        kind, field_map, header_row = _classify_table(table)
        if kind == "matches":
            matches.extend(
                _parse_match_rows(
                    table,
                    field_map,
                    header_row,
                    source_url=source_url,
                    page_gender=page_gender,
                    page_season=page_season,
                    page_tournament=page_tournament,
                    page_round=page_round,
                )
            )
        elif kind == "rankings":
            rankings.extend(
                _parse_ranking_rows(
                    table,
                    field_map,
                    header_row,
                    source_url=source_url,
                    page_gender=page_gender,
                    page_season=page_season,
                )
            )

    logger.info(
        "[cif_california] parsed matches=%d rankings=%d from %s",
        len(matches),
        len(rankings),
        source_url,
    )
    return {"matches": matches, "rankings": rankings}
