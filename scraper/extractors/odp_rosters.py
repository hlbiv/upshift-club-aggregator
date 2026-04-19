"""
odp_rosters.py — Parsers for state-association Olympic Development
Program (ODP) roster pages.

Each state ODP site ships a completely different layout, so rather
than try to write a single permissive parser, we expose a registry
keyed on a short state slug:

    PARSERS: dict[str, Callable[[BeautifulSoup], list[dict]]]

Every parser receives a parsed BeautifulSoup tree and returns a list
of dicts with the shape::

    {
        "player_name":      str,              # required
        "graduation_year":  int | None,       # optional
        "position":         str | None,       # optional
        "club_name_raw":    str | None,       # optional — linker fills FK
    }

The runner layer (`odp_runner.py`) supplies the surrounding metadata
(state, program_year, age_group, gender, source_url) externally. The
age_group/gender split is typically NOT derivable from a single-page
parse — ODP sites usually aggregate one list per (age_group, gender)
combination, but the combination itself is often implicit in the URL
or heading. The runner encodes that knowledge via the YAML seed.

All parsers are deliberately permissive — an ODP page with a shifted
DOM should return ``[]`` with a logged warning, never raise. The
runner's outer try/except then logs a parse miss to
``scrape_run_logs`` without failing the whole batch.

SHARED PARSERS
--------------
If two states ship near-identical DOM (a common pattern on USYS-hosted
subdomains), register them both to the same parser function. Today
we ship five distinct parsers because each state's site has its own
CMS, but we keep the registry indirection so a future consolidation
is a one-line change.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Callable, Dict, List, Optional

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# Graduation-year cue words ODP sites use interchangeably.
_GRAD_YEAR_RE = re.compile(r"\b(?:class\s+of\s+|co\s*|)?('?\d{2}|20\d{2})\b", re.I)

# Four-digit year in the 2005-2020 range — generous enough to cover any
# currently-live ODP age group but restrictive enough to reject random
# years like 1996 (coach birth year) or 2028 (upcoming class).
_BIRTH_YEAR_RE = re.compile(r"\b(20[0-1]\d|20[2-3][0-9])\b")

# Common position abbreviations — used to distinguish a position token
# from a trailing club name in free-text ODP listings.
_POSITION_TOKENS = {
    "GK", "D", "DEF", "M", "MID", "F", "FWD", "CB", "LB", "RB",
    "CM", "LM", "RM", "CAM", "CDM", "LW", "RW", "ST",
}


def _clean(text: str) -> str:
    return " ".join(text.split()).strip()


def _looks_like_player_name(s: str) -> bool:
    """Heuristic: a player name has 2+ whitespace-separated tokens,
    each starting with an uppercase letter and containing only letters
    / hyphens / apostrophes. Avoids matching navigation labels, age-
    group headings, etc."""
    s = s.strip()
    if not s or len(s) > 60:
        return False
    parts = s.split()
    if len(parts) < 2:
        return False
    for p in parts:
        if not p[0].isupper():
            return False
        # Allow apostrophes and hyphens inside names.
        if not re.match(r"^[A-Za-z][A-Za-z.'\-]*$", p):
            return False
    return True


def _extract_grad_year(token: Optional[str]) -> Optional[int]:
    """Normalize common grad-year formats to an int year. Returns None
    if unrecognizable."""
    if not token:
        return None
    m = _BIRTH_YEAR_RE.search(token)
    if m:
        return int(m.group(1))
    m2 = re.search(r"'(\d{2})\b", token)
    if m2:
        return 2000 + int(m2.group(1))
    return None


# ---------------------------------------------------------------------------
# Per-state parsers
# ---------------------------------------------------------------------------
#
# Each parser's job is narrow: locate the roster block, iterate player
# rows, emit the dict shape documented at the top of the file. They
# SHOULD NOT try to infer age_group/gender from the DOM — the runner
# supplies those from the YAML seed. They SHOULD tolerate missing
# optional fields and SHOULD NOT raise on a shifted layout.


def _parse_calsouth(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Cal South PRO+ ODP player pools.

    Cal South ships player pools inside ``<table>`` elements — each
    table is a single age-group pool, first column is player name,
    other columns tend to carry club and position. When a page links
    out to per-age-group news releases we still extract whatever
    pool/name data is visible on THIS page; the runner can be pointed
    at the linked releases as additional seeds.
    """
    rows: List[Dict[str, Any]] = []

    for table in soup.find_all("table"):
        headers = [
            _clean(th.get_text()) for th in table.find_all("th")
        ]
        header_lc = [h.lower() for h in headers]
        name_col = _first_header_index(header_lc, ("player", "name"))
        club_col = _first_header_index(header_lc, ("club",))
        pos_col = _first_header_index(header_lc, ("position", "pos"))

        for tr in table.find_all("tr"):
            cells = [_clean(td.get_text()) for td in tr.find_all("td")]
            if not cells:
                continue
            name = cells[name_col] if name_col is not None and name_col < len(cells) else cells[0]
            if not _looks_like_player_name(name):
                continue
            club = cells[club_col] if club_col is not None and club_col < len(cells) else None
            pos = cells[pos_col] if pos_col is not None and pos_col < len(cells) else None
            rows.append({
                "player_name": name,
                "graduation_year": None,
                "position": pos or None,
                "club_name_raw": club or None,
            })

    # Fallback for pages that embed rosters as <ul><li>Name (Club)</li></ul>
    if not rows:
        rows.extend(_parse_list_with_paren_club(soup))

    return rows


def _parse_ntxsoccer(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """North Texas ODP — Player Pools page.

    ntxsoccer pool pages typically use repeated ``<h3>AGE GROUP</h3>``
    sections followed by ``<ul>`` lists of "First Last - Club Name"
    entries or plain-name lists. Parse every bullet that looks
    like a player.
    """
    return _parse_list_with_dash_club(soup)


def _parse_fysa(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Florida YSA — ODP Selections hub.

    FYSA posts a mix of inline HTML tables per age group (the preferred
    public format) and PDF downloads. We only parse the HTML tables;
    PDF extraction is a follow-up. Layout is similar to Cal South —
    table with `Name | Club | Position` columns — so lean on the
    shared helper.
    """
    rows: List[Dict[str, Any]] = []
    for table in soup.find_all("table"):
        headers = [_clean(th.get_text()) for th in table.find_all("th")]
        header_lc = [h.lower() for h in headers]
        name_col = _first_header_index(header_lc, ("player", "name"))
        club_col = _first_header_index(header_lc, ("club",))
        pos_col = _first_header_index(header_lc, ("position", "pos"))
        grad_col = _first_header_index(header_lc, ("grad", "class", "year"))
        for tr in table.find_all("tr"):
            cells = [_clean(td.get_text()) for td in tr.find_all("td")]
            if not cells:
                continue
            name = cells[name_col] if name_col is not None and name_col < len(cells) else cells[0]
            if not _looks_like_player_name(name):
                continue
            rows.append({
                "player_name": name,
                "graduation_year": _extract_grad_year(
                    cells[grad_col] if grad_col is not None and grad_col < len(cells) else None
                ),
                "position": cells[pos_col] if pos_col is not None and pos_col < len(cells) else None,
                "club_name_raw": cells[club_col] if club_col is not None and club_col < len(cells) else None,
            })
    if not rows:
        rows.extend(_parse_list_with_paren_club(soup))
    return rows


def _parse_enysoccer(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Eastern NY — Regional/National Pool Selections.

    ENYYSA uses the classic WordPress template: each age group is an
    ``<h3>`` / ``<h4>`` followed by a ``<ul>`` of "Name, Club" bullets
    or an inline ``<p>`` listing comma-separated names. We parse both
    flavors.
    """
    rows: List[Dict[str, Any]] = []
    rows.extend(_parse_list_with_comma_club(soup))
    if not rows:
        # Some ENYYSA pages inline players inside <p> paragraphs with
        # ", " separators. Split paragraph text and look for player-
        # shaped tokens.
        for p in soup.find_all("p"):
            text = _clean(p.get_text())
            if not text:
                continue
            for piece in text.split(","):
                piece = piece.strip()
                # Skip typical prose fragments like "the following players".
                if _looks_like_player_name(piece):
                    rows.append({
                        "player_name": piece,
                        "graduation_year": None,
                        "position": None,
                        "club_name_raw": None,
                    })
    return rows


def _parse_epysa(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Eastern PA — boys/girls ODP rosters hub.

    EPYSA rosters are a WP page with a simple ``<h2>U15 Boys</h2>``
    / ``<ul>`` pattern per age group. Many ``<li>`` items are plain
    "First Last" names; club is typically in a trailing parenthesis
    or dash.
    """
    rows: List[Dict[str, Any]] = []
    rows.extend(_parse_list_with_paren_club(soup))
    if not rows:
        rows.extend(_parse_list_with_dash_club(soup))
    return rows


# ---------------------------------------------------------------------------
# Generic shared sub-parsers
# ---------------------------------------------------------------------------

def _first_header_index(headers: List[str], needles: tuple[str, ...]) -> Optional[int]:
    """Return the first header index that contains any of ``needles``."""
    for i, h in enumerate(headers):
        for n in needles:
            if n in h:
                return i
    return None


def _parse_list_with_paren_club(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse ``<ul><li>First Last (Club Name)</li></ul>`` style lists."""
    rows: List[Dict[str, Any]] = []
    for li in soup.find_all("li"):
        text = _clean(li.get_text())
        if not text:
            continue
        name, club = _split_name_and_trailer(text, delim=("(", ")"))
        if _looks_like_player_name(name):
            rows.append({
                "player_name": name,
                "graduation_year": None,
                "position": None,
                "club_name_raw": club,
            })
    return rows


def _parse_list_with_dash_club(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse ``<ul><li>First Last - Club Name</li></ul>`` style lists.
    Accepts both hyphen and en-dash separators."""
    rows: List[Dict[str, Any]] = []
    for li in soup.find_all("li"):
        text = _clean(li.get_text())
        if not text:
            continue
        # Try both dash flavors; first match wins.
        name, club = _split_on_separator(text, (" - ", " – ", " — "))
        if name is None:
            name = text
        if _looks_like_player_name(name):
            rows.append({
                "player_name": name,
                "graduation_year": None,
                "position": None,
                "club_name_raw": club,
            })
    return rows


def _parse_list_with_comma_club(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """Parse ``<ul><li>First Last, Club Name</li></ul>``."""
    rows: List[Dict[str, Any]] = []
    for li in soup.find_all("li"):
        text = _clean(li.get_text())
        if not text:
            continue
        # Don't split "Last, First" comma sequences — only if the
        # first half looks like a full "First Last" name.
        name, club = _split_on_separator(text, (", ",))
        if name and _looks_like_player_name(name):
            rows.append({
                "player_name": name,
                "graduation_year": None,
                "position": None,
                "club_name_raw": club,
            })
    return rows


def _split_on_separator(
    text: str, separators: tuple[str, ...]
) -> tuple[Optional[str], Optional[str]]:
    for sep in separators:
        if sep in text:
            left, right = text.split(sep, 1)
            return _clean(left), _clean(right) or None
    return None, None


def _split_name_and_trailer(
    text: str, *, delim: tuple[str, str]
) -> tuple[str, Optional[str]]:
    """Split ``"First Last (Club Name)"`` into ``("First Last", "Club Name")``."""
    open_d, close_d = delim
    if open_d in text and close_d in text:
        name = text.split(open_d, 1)[0]
        trailer = text.split(open_d, 1)[1].rsplit(close_d, 1)[0]
        return _clean(name), _clean(trailer) or None
    return _clean(text), None


# ---------------------------------------------------------------------------
# Public registry
# ---------------------------------------------------------------------------

PARSERS: Dict[str, Callable[[BeautifulSoup], List[Dict[str, Any]]]] = {
    "calsouth": _parse_calsouth,
    "ntxsoccer": _parse_ntxsoccer,
    "fysa": _parse_fysa,
    "enysoccer": _parse_enysoccer,
    "epysa": _parse_epysa,
}


def parse_odp_page(parser_key: str, html: str) -> List[Dict[str, Any]]:
    """Run a named parser over ``html``. Returns [] with a warning when
    the parser is unknown or raises — never propagates the exception.
    """
    parser = PARSERS.get(parser_key)
    if parser is None:
        logger.warning("[odp] unknown parser key: %s", parser_key)
        return []
    soup = BeautifulSoup(html, "html.parser")
    try:
        return parser(soup)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("[odp] parser %s raised: %s", parser_key, exc)
        return []
