"""
NCAA roster scraper for D1/D2/D3 — writes to ``college_roster_history``.

Ported from the TypeScript scrapers in the sibling player-platform repo
(``ncaa-d2-roster-scraper.ts``, ``ncaa-d3-roster-scraper.ts``,
``college-roster-scraper.ts``). Key design decisions preserved:

- **Header-aware table parsing**: column positions are detected from
  ``<th>`` text, never hardcoded offsets.
- **Multi-strategy HTML extraction**: Sidearm roster elements, generic
  ``<table>`` with header detection, and card/div layouts.
- **Year/class normalization**: Fr, So, Jr, Sr, Gr, RS-Fr, R-So, 5th,
  etc. all map to the ``year`` enum values expected by the schema.
- **Rate limiting**: >= 1 s between HTTP requests.
- **Graceful degradation**: 404s, timeouts, and unparseable pages are
  logged and skipped, not fatal.

Two entry modes
---------------

1. ``scrape_college_rosters`` — bulk mode. Iterates DB-seeded colleges
   (filtered by division/gender) and writes directly to
   ``college_roster_history``. Requires a populated ``colleges`` table.
2. ``scrape_school_url`` — single-school MVP (used by
   ``run.py --source ncaa-rosters --school-url``). Takes a roster URL
   plus inline metadata (name, division, gender, state...) and returns
   a structured dict the writer can upsert. Does not require any DB
   seed row — the writer upserts the ``colleges`` row on the fly via
   ``colleges_name_division_gender_uq``.

CSS selectors used for SIDEARM roster pages (strategy 1 in
``parse_roster_html``)::

    li.sidearm-roster-player, div.sidearm-roster-player   # card root
      h3 a, h4 a, .sidearm-roster-player-name a            # player name
      .sidearm-roster-player-jersey-number                 # jersey number
      .sidearm-roster-player-position                      # position
      .sidearm-roster-player-academic-year                 # class year
      .sidearm-roster-player-hometown                      # hometown
      .sidearm-roster-player-highschool,
      .sidearm-roster-player-previous-school               # prev club / HS

CLI::

    python -m scraper.extractors.ncaa_soccer_rosters \\
        [--division D1|D2|D3] [--gender mens|womens] \\
        [--limit 5] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Sibling package imports (scraper.*)
# ---------------------------------------------------------------------------

# Ensure the parent ``scraper/`` package is importable when invoked as
# ``python -m scraper.extractors.ncaa_soccer_rosters``.
_SCRAPER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from scrape_run_logger import ScrapeRunLogger, FailureKind, classify_exception  # noqa: E402
from alerts import alert_scraper_failure  # noqa: E402
from ingest import ncaa_roster_writer as _ncaa_roster_writer  # noqa: E402

try:
    import psycopg2  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 15  # seconds
RETRY_ATTEMPTS = 2
RETRY_DELAY = 1.0  # seconds between retries
RATE_LIMIT_DELAY = 1.5  # seconds between schools

# Year/class normalization — matches the TS ACADEMIC_YEAR_MAP exactly
YEAR_MAP: Dict[str, str] = {
    "fr": "freshman",
    "fr.": "freshman",
    "freshman": "freshman",
    "r-fr": "freshman",
    "rs-fr": "freshman",
    "rs fr": "freshman",
    "r-fr.": "freshman",
    "so": "sophomore",
    "so.": "sophomore",
    "sophomore": "sophomore",
    "r-so": "sophomore",
    "rs-so": "sophomore",
    "rs so": "sophomore",
    "r-so.": "sophomore",
    "jr": "junior",
    "jr.": "junior",
    "junior": "junior",
    "r-jr": "junior",
    "rs-jr": "junior",
    "rs jr": "junior",
    "r-jr.": "junior",
    "sr": "senior",
    "sr.": "senior",
    "senior": "senior",
    "r-sr": "senior",
    "rs-sr": "senior",
    "rs sr": "senior",
    "r-sr.": "senior",
    "gr": "grad",
    "gr.": "grad",
    "grad": "grad",
    "graduate": "grad",
    "5th": "grad",
    "5th yr": "grad",
    "5th year": "grad",
}

# Soccer-specific path segments tried when discovering roster URL
MENS_PATHS = ["mens-soccer", "msoc", "m-soccer", "soccer"]
WOMENS_PATHS = ["womens-soccer", "wsoc", "w-soccer", "soccer"]

SCRAPER_KEY_MAP = {
    "D1": "ncaa-d1-rosters",
    "D2": "ncaa-d2-rosters",
    "D3": "ncaa-d3-rosters",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RosterPlayer:
    player_name: str
    position: Optional[str] = None
    year: Optional[str] = None
    hometown: Optional[str] = None
    prev_club: Optional[str] = None
    jersey_number: Optional[str] = None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })
    return s


def fetch_with_retry(
    session: requests.Session,
    url: str,
    retries: int = RETRY_ATTEMPTS,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[str]:
    """Fetch a URL with retry + backoff. Returns HTML text or None."""
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except requests.RequestException:
            if attempt == retries:
                return None
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Year normalization
# ---------------------------------------------------------------------------

def normalize_year(raw: Optional[str]) -> Optional[str]:
    """Map free-text year/class to the schema enum value.

    Handles: Fr, So, Jr, Sr, Gr, RS-Fr, R-So, 5th, etc.
    Returns one of: freshman, sophomore, junior, senior, grad, or None.
    """
    if not raw:
        return None
    key = raw.strip().lower().replace(".", "")
    # Direct lookup
    if key in YEAR_MAP:
        return YEAR_MAP[key]
    # Try stripping leading "rs " or "r-" prefix for redshirt variants
    for prefix in ("rs-", "rs ", "r-"):
        if key.startswith(prefix):
            base = key[len(prefix):]
            if base in YEAR_MAP:
                return YEAR_MAP[base]
    return None


# ---------------------------------------------------------------------------
# Academic year (season string)
# ---------------------------------------------------------------------------

def current_academic_year() -> str:
    """Return season string like '2025-26'."""
    now = datetime.now(timezone.utc)
    y = now.year
    m = now.month
    if m >= 8:
        return f"{y}-{str(y + 1)[-2:]}"
    else:
        return f"{y - 1}-{str(y)[-2:]}"


# ---------------------------------------------------------------------------
# Header-aware column index — ported from college-roster-scraper.ts
# ---------------------------------------------------------------------------

@dataclass
class ColumnIndex:
    jersey_number: Optional[int] = None
    player_name: Optional[int] = None
    position: Optional[int] = None
    class_year: Optional[int] = None
    height: Optional[int] = None
    hometown: Optional[int] = None
    high_school: Optional[int] = None


def build_column_index(headers: List[str]) -> ColumnIndex:
    """Detect column semantics from header text. Never hardcodes positions."""
    idx = ColumnIndex()
    for i, raw in enumerate(headers):
        raw_stripped = raw.strip()
        h = re.sub(r"[^a-z0-9 ]", " ", raw_stripped.lower()).strip()
        h = re.sub(r"\s+", " ", h)

        # Bare "#" becomes empty after cleanup — detect it from raw
        if idx.jersey_number is None and raw_stripped == "#":
            idx.jersey_number = i
            continue

        if not h:
            continue

        if idx.jersey_number is None and re.match(r"^(no|num|number|jersey)\b", h):
            idx.jersey_number = i
        elif idx.player_name is None and re.search(r"\b(name|player|full name)\b", h):
            idx.player_name = i
        elif idx.position is None and re.search(r"\b(pos|position)\b", h):
            idx.position = i
        elif idx.class_year is None and re.search(r"\b(yr|year|class|cl|academic)\b", h):
            idx.class_year = i
        elif idx.height is None and re.search(r"\b(ht|height)\b", h):
            idx.height = i
        elif idx.hometown is None and re.search(r"\b(hometown|home town|from)\b", h):
            idx.hometown = i
        elif idx.high_school is None and re.search(
            r"\b(high school|hs|previous|prev|club|school|last school)\b", h
        ):
            idx.high_school = i

    return idx


def _cell_text(td: Tag) -> str:
    return re.sub(r"\s+", " ", td.get_text()).strip()


# ---------------------------------------------------------------------------
# Sidearm Vue-embedded JSON helpers (Strategy 5)
# ---------------------------------------------------------------------------

# Regex locates the start of ``data: () => ({ ... roster: {`` where the
# final ``{`` marks the first character of the JSON object we want to
# extract. Captures the position of that brace in group 1.
#
# The `[^}]*` before ``roster:`` is intentional — other keys may appear
# on the same factory (e.g. ``loading: true``, ``sport: "MSOC"``) but
# none of them contain a ``}`` themselves because they are scalars. If
# a site ever inserts a nested object before ``roster:``, the regex
# will still work as long as that nested object doesn't contain the
# string ``roster:`` (which would be an ambiguous match regardless).
_SIDEARM_VUE_ROSTER_RE = re.compile(
    r"data:\s*\(\s*\)\s*=>\s*\(\s*\{[^}]*roster:\s*(\{)",
    re.DOTALL,
)


def _find_balanced_json_end(src: str, start: int) -> Optional[int]:
    """Given ``src`` and the index of an opening ``{``, return the index
    just past the matching closing ``}`` (i.e. the end-exclusive slice
    bound).

    Uses a simple state machine that understands JSON string literals
    (including escapes) so that braces inside strings don't confuse the
    depth counter. Returns ``None`` if no balanced closer is found.
    """
    depth = 0
    in_str = False
    escape = False
    i = start
    n = len(src)
    while i < n:
        c = src[i]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return i + 1
        i += 1
    return None


def _parse_sidearm_vue_embedded_json(html: str) -> List[RosterPlayer]:
    """Extract players from the Sidearm Vue data factory JSON blob.

    Returns ``[]`` if the page isn't this shape, or if the embedded JSON
    can't be parsed (malformed, truncated, etc.) — callers fall through
    to their existing zero-player SKIP path. Never raises.
    """
    match = _SIDEARM_VUE_ROSTER_RE.search(html)
    if not match:
        return []
    start = match.start(1)
    end = _find_balanced_json_end(html, start)
    if end is None:
        return []
    blob = html[start:end]
    try:
        roster_obj: Any = json.loads(blob)
    except (ValueError, TypeError):
        return []
    raw_players = roster_obj.get("players") if isinstance(roster_obj, dict) else None
    if not isinstance(raw_players, list):
        return []

    parsed: List[RosterPlayer] = []
    for rp in raw_players:
        if not isinstance(rp, dict):
            continue
        first = (rp.get("first_name") or "").strip()
        last = (rp.get("last_name") or "").strip()
        name = f"{first} {last}".strip()
        if not name or len(name) < 2:
            continue

        jersey_raw = rp.get("jersey_number_2") or rp.get("jersey_number")
        # Jersey fields are typed inconsistently (str on some sites, int
        # on others). Normalize to stripped string; None for blanks.
        if jersey_raw is None or jersey_raw == "":
            jersey: Optional[str] = None
        else:
            jersey = str(jersey_raw).strip() or None

        position_raw = rp.get("position_short") or rp.get("position_long")
        position = position_raw.strip() if isinstance(position_raw, str) and position_raw.strip() else None

        year_raw = rp.get("academic_year_short") or rp.get("academic_year_long")
        year = normalize_year(year_raw) if isinstance(year_raw, str) else None

        hometown_raw = rp.get("hometown")
        hometown = hometown_raw.strip().rstrip(".") if isinstance(hometown_raw, str) and hometown_raw.strip() else None

        prev_raw = rp.get("previous_school") or rp.get("highschool") or rp.get("high_school")
        prev_club = prev_raw.strip() if isinstance(prev_raw, str) and prev_raw.strip() else None

        parsed.append(RosterPlayer(
            player_name=name,
            position=position,
            year=year,
            hometown=hometown,
            prev_club=prev_club,
            jersey_number=jersey,
        ))
    return parsed


def _parse_sidearm_vue_embedded_head_coach(html: str) -> Optional[Dict[str, Optional[str]]]:
    """Extract the head coach from the Sidearm Vue data factory JSON blob.

    Sibling of ``_parse_sidearm_vue_embedded_json`` (players). Locates
    the same ``data: () => ({ roster: {...} })`` prelude and reads a
    ``coaches`` / ``coaching_staff`` / ``staff`` array instead of
    ``players``. Filters to strict head coach via
    ``_is_strict_head_coach``.

    Recovers programs where DOM-based Strategy 4 (``.roster-card-item``)
    missed because either (a) the site's Vue store has ``show_coaches_under_roster``
    / ``display_coaches`` toggled off so the staff cards never render,
    or (b) the coach data is in the JSON but rendered in a different
    DOM subtree the parser doesn't reach.

    Returns the same coach-dict shape as strategies 1-4 (plus the
    ``_strategy`` tag for per-run instrumentation). Returns ``None``
    when the blob is absent, malformed, or contains no strict head
    coach.

    Key-name tolerance: Sidearm's Rails backend is consistent on the
    player side but the coach side has drifted across tenants. We try
    multiple title + name keys and take the first non-empty hit.
    Matches the resilience pattern used in ``_parse_sidearm_vue_embedded_json``
    for player fields.
    """
    match = _SIDEARM_VUE_ROSTER_RE.search(html)
    if not match:
        return None
    start = match.start(1)
    end = _find_balanced_json_end(html, start)
    if end is None:
        return None
    blob = html[start:end]
    try:
        roster_obj: Any = json.loads(blob)
    except (ValueError, TypeError):
        return None
    if not isinstance(roster_obj, dict):
        return None

    # Likely coach-array keys (ordered by observed prevalence).
    raw_coaches = None
    for key in ("coaches", "coaching_staff", "staff"):
        candidate = roster_obj.get(key)
        if isinstance(candidate, list) and candidate:
            raw_coaches = candidate
            break
    if raw_coaches is None:
        return None

    for rc in raw_coaches:
        if not isinstance(rc, dict):
            continue

        # Title can live under multiple keys depending on CMS version.
        title_raw: Any = (
            rc.get("title")
            or rc.get("position_long")
            or rc.get("position_short")
            or rc.get("position")
            or ""
        )
        if not isinstance(title_raw, str):
            continue
        title = title_raw.strip()
        if not _is_strict_head_coach(title):
            continue

        first = (rc.get("first_name") or "").strip() if isinstance(rc.get("first_name"), str) else ""
        last = (rc.get("last_name") or "").strip() if isinstance(rc.get("last_name"), str) else ""
        name = f"{first} {last}".strip()
        if not name:
            # Fall back to a single "name" / "full_name" / "display_name"
            # field if first+last weren't populated.
            for k in ("name", "full_name", "display_name"):
                candidate = rc.get(k)
                if isinstance(candidate, str) and candidate.strip():
                    name = candidate.strip()
                    break
        if not name or len(name) < 3:
            continue

        email_raw = rc.get("email") or rc.get("email_address")
        email = (
            email_raw.strip().lower()
            if isinstance(email_raw, str) and email_raw.strip()
            else None
        )

        phone_raw = rc.get("phone") or rc.get("phone_number")
        phone = (
            phone_raw.strip()
            if isinstance(phone_raw, str) and phone_raw.strip()
            else None
        )

        return {
            "name": name,
            "title": title,
            "email": email,
            "phone": phone,
            "is_head_coach": True,
            "_strategy": "vue-embedded-json",
        }

    return None


# ---------------------------------------------------------------------------
# HTML parsing — five strategies, matching the TS scrapers
# ---------------------------------------------------------------------------

def parse_roster_html(html: str) -> List[RosterPlayer]:
    """Extract player rows from an NCAA roster page.

    Six strategies are tried in order; first non-empty result wins.

    1. **Sidearm card template** — ``li.sidearm-roster-player`` or
       ``div.sidearm-roster-player`` with ``.sidearm-roster-player-*`` fields.
    2. **Header-aware table** — any ``<table>`` whose ``<th>`` row contains
       a "Name" column. Column positions are detected from headers.
    3. **Card/div layout** — ``.s-person-card``, ``.roster-card``,
       ``.s-person`` containers with nested class selectors.
    4. **Nuxt template** — ``.roster-card-item`` cards with
       ``roster-player-card-profile-field`` label/value rows. Used by
       ~20 D1 programs whose sites aren't SIDEARM (Stanford, USC, etc.).
    5. **Sidearm Vue-embedded JSON** — a classic-SIDEARM variant where
       the roster template is a Vue ``v-for`` and the full player list
       is shipped inline inside the Vue instance's ``data: () => ({
       roster: {...} })`` factory. Non-DOM strategy — delegates to
       ``_parse_sidearm_vue_embedded_json`` which extracts + parses the
       JSON blob directly.
    6. **WMT Digital / WordPress** — ``.roster__list_item`` figure/figcaption
       cards. Paired with a ``.roster__table`` on desktop (caught by Strategy 2
       first) but a standalone fallback when the table is absent. Observed on
       ramblinwreck.com (Georgia Tech) — the only WMT/WordPress athletics site
       in the current NCAA soccer directory. Fallback kept cheap so it doesn't
       cost anything when the page is a different platform.
    7. **Sidearm list template (DOM)** — ``.sidearm-roster-list-item``
       rows. DOM fallback for the same operator-toggled list display
       that Strategy 5 targets via JSON — catches sites where the Vue
       JSON blob is absent or malformed but the hydrated list renders
       correctly under Playwright. Used by George Mason (when JSON
       extraction misses), Richmond, etc.
    """
    soup = BeautifulSoup(html, "html.parser")
    players: List[RosterPlayer] = []

    # --- Strategy 1: Sidearm roster elements ---
    sidearm_els = soup.select("li.sidearm-roster-player, div.sidearm-roster-player")
    for el in sidearm_els:
        name_el = el.select_one("h3 a, h4 a, .sidearm-roster-player-name a")
        name = name_el.get_text().strip() if name_el else ""
        if not name or len(name) < 2:
            continue

        jersey_el = el.select_one(".sidearm-roster-player-jersey-number")
        jersey = jersey_el.get_text().strip() if jersey_el else None

        pos_el = (
            el.select_one(".sidearm-roster-player-position-long-short.hide-on-small-down")
            or el.select_one(".sidearm-roster-player-position span.text-bold")
        )
        position = pos_el.get_text().strip() if pos_el else None

        year_el = el.select_one(".sidearm-roster-player-academic-year")
        year_raw = year_el.get_text().strip() if year_el else None

        hometown_el = el.select_one(".sidearm-roster-player-hometown")
        hometown = hometown_el.get_text().rstrip(".").strip() if hometown_el else None

        prev_el = el.select_one(
            ".sidearm-roster-player-highschool, .sidearm-roster-player-previous-school"
        )
        prev_club = prev_el.get_text().strip() if prev_el else None

        players.append(RosterPlayer(
            player_name=name,
            position=position or None,
            year=normalize_year(year_raw),
            hometown=hometown or None,
            prev_club=prev_club or None,
            jersey_number=jersey or None,
        ))

    if players:
        return players

    # --- Strategy 2: Header-aware <table> parsing ---
    for table in soup.find_all("table"):
        # Find header row
        headers: List[str] = []
        thead = table.find("thead")
        if thead:
            first_tr = thead.find("tr")
            if first_tr:
                headers = [th.get_text().strip() for th in first_tr.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                ths = first_tr.find_all("th")
                if ths:
                    headers = [th.get_text().strip() for th in ths]

        if not headers:
            continue

        idx = build_column_index(headers)
        if idx.player_name is None:
            continue

        # Parse body rows
        tbody = table.find("tbody")
        body_rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

        for tr in body_rows:
            cells = [_cell_text(td) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue

            def _get(col_idx: Optional[int]) -> Optional[str]:
                if col_idx is None or col_idx >= len(cells):
                    return None
                v = cells[col_idx].strip()
                return v if v else None

            name = _get(idx.player_name) or ""
            # Strip leading jersey number that some sites embed in the name cell
            name = re.sub(r"^#?\d+\s*", "", name).strip()
            if not name or len(name) < 2:
                continue

            jersey_raw = _get(idx.jersey_number)
            jersey = jersey_raw.lstrip("#").strip() if jersey_raw else None

            players.append(RosterPlayer(
                player_name=name,
                position=_get(idx.position),
                year=normalize_year(_get(idx.class_year)),
                hometown=_get(idx.hometown),
                prev_club=_get(idx.high_school),
                jersey_number=jersey or None,
            ))

        if players:
            return players

    # --- Strategy 3: Card/div layout ---
    card_selectors = [
        ".s-person-card",
        ".roster-card",
        ".s-person",
        "tr.s-table-body__row",
    ]
    for sel in card_selectors:
        for el in soup.select(sel):
            if sel == "tr.s-table-body__row":
                # Presto-style table rows without proper <th> headers
                cells = [_cell_text(td) for td in el.find_all("td")]
                if len(cells) < 2:
                    continue
                has_jersey = bool(re.match(r"^\d+$", cells[0]))
                offset = 1 if has_jersey else 0
                n = cells[offset] if offset < len(cells) else ""
                if not n or len(n) < 2:
                    continue
                players.append(RosterPlayer(
                    player_name=n,
                    position=cells[offset + 1] if offset + 1 < len(cells) else None,
                    year=normalize_year(cells[offset + 2] if offset + 2 < len(cells) else None),
                    hometown=(
                        (cells[offset + 4] if offset + 4 < len(cells) else "")
                        or (cells[offset + 3] if offset + 3 < len(cells) else "")
                    ).split("/")[0].strip() or None,
                    jersey_number=cells[0] if has_jersey else None,
                ))
            else:
                name_el = el.select_one(
                    ".s-person-card__name, .roster-card__name, h3, h4, .name"
                )
                n = name_el.get_text().strip() if name_el else ""
                if not n or len(n) < 2:
                    continue
                num_el = el.select_one(".s-person-card__number, .number")
                pos_el = el.select_one(".s-person-card__position, .position")
                yr_el = el.select_one(".s-person-card__year, .year")
                ht_el = el.select_one(".s-person-card__hometown, .hometown")
                players.append(RosterPlayer(
                    player_name=n,
                    position=pos_el.get_text().strip() if pos_el else None,
                    year=normalize_year(yr_el.get_text().strip() if yr_el else None),
                    hometown=ht_el.get_text().strip() if ht_el else None,
                    jersey_number=num_el.get_text().strip() if num_el else None,
                ))

        if players:
            return players

    # --- Strategy 4: Nuxt-based roster template (card / list / player-list) ---
    # Non-SIDEARM template used by a meaningful chunk of D1 programs.
    # Observed container classes (driven by the NextGen/Nuxt view_template
    # field — ``card`` / ``list`` / Penn-State-custom-``list``):
    #
    #   .roster-card-item      — card view  (Stanford, Georgia Tech, Auburn, ...)
    #   .roster-list-item      — list view  (Virginia Tech / hokiesports, San Diego State)
    #   .player-list-item      — Penn State ``list`` variant (gopsusports)
    #
    # For each container, child classes are the stem + a known suffix:
    #   {stem}__title                → player name (via <a>, else element text)
    #   {stem}__jersey-number        → jersey
    #   {stem}__position             — Nuxt card only; list/player variants put
    #                                   the position into a labeled profile-field
    #
    # Two labeled-field conventions coexist, both supported below:
    #
    #   Nuxt card:
    #     .roster-player-card-profile-field / __label / __value
    #     basic block (unlabeled ordered) + --additional block (labeled)
    #
    #   Nuxt list (hokiesports / goaztecs):
    #     .roster-player-list-profile-field with BEM modifier classes
    #     --class-level / --height / --position / --hometown / --high-school
    #     / --previous-school
    #
    #   Penn-State variant:
    #     .profile-field-content with
    #       .profile-field-content__title (label) +
    #       .profile-field-content__value (value)
    #     labels: "Year" / "Height" / "Hometown" / "High School" / "Club Team"
    #
    # Staff cards (same container class but with ``-staff-members-`` in the
    # class list) are filtered out up front so we don't return the head coach
    # as a player.
    NUXT_CONTAINERS: list = [
        # (container selector, stem for __title / __jersey-number / __position)
        (".roster-card-item", "roster-card-item"),
        (".roster-list-item", "roster-list-item"),
        (".player-list-item", "player-list-item"),
    ]

    def _is_nuxt_staff_card(el) -> bool:
        cls = el.get("class") or []
        return any("staff-members" in c for c in cls)

    for container_sel, stem in NUXT_CONTAINERS:
        cards = [c for c in soup.select(container_sel) if not _is_nuxt_staff_card(c)]
        if not cards:
            continue

        for card in cards:
            name_el = card.select_one(f".{stem}__title")
            # Some variants nest the name inside an <a> child of the title wrapper
            # (e.g. hokiesports: ``a.roster-list-item__title`` directly).
            # Others nest <a> inside an <h3> .__title-wrapper.
            if name_el is None:
                name_el = card.select_one(f".{stem}__title-wrapper a, .{stem}__title-link")
            name = name_el.get_text().strip() if name_el else ""
            if not name or len(name) < 2:
                continue

            jersey_el = card.select_one(f".{stem}__jersey-number")
            jersey = jersey_el.get_text().strip() if jersey_el else None

            position_el = card.select_one(f".{stem}__position")
            position = position_el.get_text().strip() if position_el else None

            year: Optional[str] = None
            hometown: Optional[str] = None
            prev_club: Optional[str] = None

            # --- Nuxt card: --basic (unlabeled) + --additional (labeled) ---
            for value_el in card.select(
                ".roster-players-cards-item__profile-fields--basic "
                ".roster-player-card-profile-field__value"
            ):
                normalized = normalize_year(value_el.get_text().strip())
                if normalized:
                    year = normalized
                    break

            for field in card.select(
                ".roster-players-cards-item__profile-fields--additional "
                ".roster-player-card-profile-field"
            ):
                label_el = field.select_one(".roster-player-card-profile-field__label")
                value_el = field.select_one(".roster-player-card-profile-field__value")
                if not (label_el and value_el):
                    continue
                label = label_el.get_text().strip().lower().rstrip(":")
                value = value_el.get_text().strip()
                if "hometown" in label and not hometown:
                    hometown = value
                elif (("previous" in label) or ("high school" in label)
                      or ("last school" in label) or ("club team" in label)):
                    if not prev_club:
                        prev_club = value

            # --- Nuxt list: BEM-modifier classes on profile-list-field ---
            #
            # Each field's text itself IS the value — no label element — because
            # the DOM uses the BEM --<kind> class as the semantic marker.
            # Position sometimes lands here instead of the card-item's __position.
            #
            # Prev-club precedence: ``--previous-school`` outranks
            # ``--high-school`` when both are present. On the live hokiesports
            # DOM, high-school renders first in source order; without the
            # explicit precedence we'd stick with the HS name and throw away
            # the more-informative last-collegiate-program name (e.g. Sam
            # Joseph → "Saint Augustine HS" instead of "UCLA").
            hs_fallback: Optional[str] = None
            for field in card.select(".roster-player-list-profile-field"):
                cls = " ".join(field.get("class") or [])
                txt = field.get_text().strip()
                if not txt:
                    continue
                if "--class-level" in cls and year is None:
                    year = normalize_year(txt)
                elif "--position" in cls and not position:
                    position = txt
                elif "--hometown" in cls and not hometown:
                    hometown = txt
                elif "--previous-school" in cls and not prev_club:
                    prev_club = txt
                elif "--high-school" in cls and hs_fallback is None:
                    hs_fallback = txt
            if not prev_club and hs_fallback:
                prev_club = hs_fallback

            # --- Penn-State variant: profile-field-content label/value pairs ---
            for field in card.select(".profile-field-content"):
                label_el = field.select_one(".profile-field-content__title")
                value_el = field.select_one(".profile-field-content__value")
                if not (label_el and value_el):
                    continue
                label = label_el.get_text().strip().lower().rstrip(":")
                value = value_el.get_text().strip()
                if not value:
                    continue
                if "year" in label or "class" in label:
                    if year is None:
                        year = normalize_year(value)
                elif "hometown" in label and not hometown:
                    hometown = value
                elif (("previous" in label) or ("high school" in label)
                      or ("last school" in label) or ("club team" in label)):
                    if not prev_club:
                        prev_club = value
                elif "position" in label and not position:
                    position = value

            players.append(RosterPlayer(
                player_name=name,
                position=position or None,
                year=year,
                hometown=hometown or None,
                prev_club=prev_club or None,
                jersey_number=jersey or None,
            ))

        if players:
            return players

    # --- Strategy 5: Sidearm Vue-embedded roster JSON ---
    # A cluster of classic-SIDEARM programs (e.g. gomason.com) ship a Vue
    # roster template whose player list is never rendered server-side, but
    # the full roster JSON is embedded inline inside the Vue instance's
    # ``data: () => ({ roster: {...} })`` initializer — a sibling script
    # to the ``<template v-for="player in computedPlayers">`` block.
    # The <li> shells DO appear in the HTML (strategy 1 picks them up
    # when SSR hydration is complete), but the underlying requests fetch
    # only sees the un-hydrated Vue template with zero player elements.
    # This shape has both the static-0 and Playwright-0 outcome observed
    # in the operational cluster.
    #
    # Markup signature (classic SIDEARM / Vue mount):
    #   <script>
    #     ... new Vue({
    #       el: '#vue-rosters',
    #       data: () => ({
    #         roster: {"id":...,"players":[{"rp_id":..., "first_name":...,
    #                  "last_name":..., "jersey_number":..., "position_short":...,
    #                  "academic_year_short":..., "hometown":...,
    #                  "highschool":..., "previous_school":..., ...}, ...]}
    #       }),
    #       ...
    #     })
    #   </script>
    #
    # The JSON lives inside JS source (not a ``<script type="application/json">``
    # tag), so we locate the ``data: () => ({ ... roster: {`` prelude via
    # regex and then walk the brace depth to find the balanced end of the
    # roster object.
    players = _parse_sidearm_vue_embedded_json(html)
    if players:
        return players

    # --- Strategy 6: WMT Digital / WordPress roster cards ---
    # Ramblinwreck.com (Georgia Tech) and other WMT Digital themes ship a
    # roster template with two sibling containers:
    #
    #   <section class="wrapper roster">
    #     <ul class="roster__list">
    #       <li class="roster__list_item">   <!-- or div.roster__list_item -->
    #         <figure>
    #           <a href="/sports/.../roster/season/YYYY-YY/firstname-lastname/">
    #             <div class="thumb" title="First Last">...
    #               <div class="icon"><span>#12</span></div>
    #             </div>
    #           </a>
    #           <figcaption>
    #             <span>INF</span>
    #             <a href="...">First Last</a>
    #             <ul>
    #               <li>6-2</li>          <!-- height -->
    #               <li>174 lbs.</li>     <!-- weight -->
    #               <li>Freshman</li>     <!-- class year -->
    #               <li>Business Administration</li>  <!-- major -->
    #             </ul>
    #           </figcaption>
    #         </figure>
    #       </li>
    #     </ul>
    #     <section class="roster__table"><table>...</table></section>  <!-- Strategy 2 -->
    #   </section>
    #
    # Strategy 2 normally wins because the sibling ``<table class="roster__table">``
    # has a proper ``<th>Name</th>`` header row. Strategy 6 is the belt-and-
    # suspenders fallback for WMT variants that ship only the card list (e.g.
    # mobile-first themes or sports pages that suppress the table). It also
    # keeps the scraper resilient to ramblinwreck.com DOM churn — if the
    # WordPress theme drops the table, cards still work.
    #
    # Hometown and prev_club are not in the card — only the table exposes them,
    # so when the fallback runs the row is jersey+name+position+year only. That
    # still satisfies the roster_diffs contract (``player_name`` is required;
    # all others are optional) and keeps future-season head-count tracking
    # working even in degraded mode.
    for card in soup.select(
        "li.roster__list_item, div.roster__list_item, .roster__list_item"
    ):
        figcaption = card.select_one("figcaption")
        if not figcaption:
            continue

        # Player name: first <a> inside the figcaption (not the figure's image
        # link — that one has no visible text, just an <img>). Fall back to
        # figure anchor title attribute if text-only anchor missing.
        name: Optional[str] = None
        for anchor in figcaption.find_all("a"):
            txt = anchor.get_text().strip()
            if txt and len(txt) >= 2:
                name = txt
                break
        if not name:
            # Some themes put the name only as the image `title=` attribute
            thumb = card.select_one(".thumb[title]")
            if thumb:
                name = thumb.get("title", "").strip() or None
        if not name or len(name) < 2:
            continue

        # Position: first <span> child of figcaption that isn't nested inside
        # the social-wrapper. WMT Digital ships it as a bare <span> sibling
        # of the name anchor.
        position: Optional[str] = None
        for span in figcaption.find_all("span", recursive=False):
            txt = span.get_text().strip()
            if txt:
                position = txt
                break

        # Jersey number: lives in .icon > span on the figure (with leading #)
        jersey: Optional[str] = None
        icon_span = card.select_one(".icon span")
        if icon_span:
            jersey_raw = icon_span.get_text().strip()
            jersey = jersey_raw.lstrip("#").strip() or None

        # Class year: scan each <li> inside figcaption and try normalize_year.
        # The list is an unlabeled mix of height/weight/year/major; year wins
        # on the first match.
        year_val: Optional[str] = None
        for li in figcaption.select("ul li"):
            normalized = normalize_year(li.get_text().strip())
            if normalized:
                year_val = normalized
                break

        players.append(RosterPlayer(
            player_name=name,
            position=position,
            year=year_val,
            hometown=None,  # not present in card; table has it
            prev_club=None,  # not present in card; table has it
            jersey_number=jersey,
        ))

    if players:
        return players

    # --- Strategy 7: SIDEARM list-template (DOM fallback) ---
    # Catches the same operator-toggled list display Strategy 5 targets
    # via JSON, but via post-hydration DOM instead — protection for
    # sites where the Vue JSON blob is absent/malformed but the rendered
    # <li class="sidearm-roster-list-item"> elements are populated.
    # Playwright fallback (enabled via NCAA_PLAYWRIGHT_FALLBACK=true)
    # is the typical source of hydrated HTML for these programs.
    #
    # Programs observed in this cluster (from PR-5 + PR-8 diagnostic
    # runs): George Mason (when JSON extraction misses), Pepperdine mens,
    # Richmond, USC mens, Virginia Tech, Minnesota mens, San Diego State,
    # Tulane, Penn State womens. Georgia Tech now caught by Strategy 6
    # (WMT) ahead of this.
    #
    # Structural signature:
    #   <li class="sidearm-roster-list-item">
    #     .sidearm-roster-list-item-name a       → player name
    #     .sidearm-roster-list-item-photo-number → jersey (nested <span>)
    #     .sidearm-roster-list-item-position     → position (GK/M/F/D)
    #     .sidearm-roster-list-item-year         → class (Sr./Jr./Fr./So.)
    #     .sidearm-roster-list-item-hometown     → hometown
    #     .sidearm-roster-list-item-previous-school
    #     .sidearm-roster-list-item-highschool
    for el in soup.select("li.sidearm-roster-list-item, div.sidearm-roster-list-item"):
        name_el = el.select_one(
            ".sidearm-roster-list-item-name a, "
            ".sidearm-roster-list-item-name"
        )
        name = name_el.get_text().strip() if name_el else ""
        if not name or len(name) < 2:
            continue

        # Jersey lives inside .sidearm-roster-list-item-photo-number > span
        # on list-template sites. Fall back to the standalone number
        # class in case it's used as a sibling.
        jersey_el = el.select_one(
            ".sidearm-roster-list-item-photo-number span, "
            ".sidearm-roster-list-item-photo-number, "
            ".sidearm-roster-list-item-number"
        )
        jersey = jersey_el.get_text().strip() if jersey_el else None

        pos_el = el.select_one(".sidearm-roster-list-item-position")
        position = pos_el.get_text().strip() if pos_el else None

        year_el = el.select_one(".sidearm-roster-list-item-year")
        year = normalize_year(year_el.get_text().strip()) if year_el else None

        hometown_el = el.select_one(".sidearm-roster-list-item-hometown")
        hometown = hometown_el.get_text().rstrip(".").strip() if hometown_el else None

        prev_el = el.select_one(
            ".sidearm-roster-list-item-previous-school, "
            ".sidearm-roster-list-item-highschool"
        )
        prev_club = prev_el.get_text().strip() if prev_el else None

        players.append(RosterPlayer(
            player_name=name,
            position=position or None,
            year=year,
            hometown=hometown or None,
            prev_club=prev_club or None,
            jersey_number=jersey or None,
        ))

    if players:
        return players

    return players


# ---------------------------------------------------------------------------
# Head coach extraction (single-school MVP)
# ---------------------------------------------------------------------------

# Head-coach detection keywords on SIDEARM staff blocks that sometimes
# appear inline on a roster page (e.g. a small "Coaching Staff" aside)
# or on the associated /coaches page.
_HEAD_COACH_RE = re.compile(
    r"\b(head\s+(?:men'?s|women'?s)?\s*(?:soccer\s+)?coach|head\s+coach)\b",
    re.IGNORECASE,
)


def _extract_email_from_el(el) -> Optional[str]:
    mailto = el.find("a", href=re.compile(r"^mailto:", re.IGNORECASE))
    if mailto:
        return (
            mailto.get("href", "")
            .replace("mailto:", "")
            .split("?")[0]
            .strip()
            .lower()
        )
    return None


def _extract_phone_from_el(el) -> Optional[str]:
    m = re.search(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}", el.get_text())
    return m.group(0) if m else None


# Stricter "Head Coach" regex used by the new s-person-card / inline
# strategies so we can distinguish a true head coach from "Associate
# Head Coach" / "Assistant Head Coach" / "Director of ..." titles. The
# original ``_HEAD_COACH_RE`` is left intact for backward compat with
# the legacy sidearm-staff-member strategy below.
_STRICT_HEAD_COACH_RE = re.compile(
    r"\bhead\s+(?:men'?s?|women'?s?)?\s*(?:soccer\s+)?coach\b",
    re.IGNORECASE,
)

# Matches any subordinate variant of "head coach" we have seen on staff
# pages — these must be filtered out so a real Head Coach card isn't
# clobbered by a sibling Associate / Assistant card in the same grid.
# Covers: Associate / Assoc / Assoc., Assistant / Asst / Asst.,
# "Assistant to the Head Coach", and prefixes like "Volunteer" or
# "Interim" before any of the above.
_NON_HEAD_COACH_RE = re.compile(
    r"\b(?:assoc(?:iate|\.?)|assistant|asst\.?)"
    r"(?:\s+to\s+the)?"
    r"\s+head\s+coach\b",
    re.IGNORECASE,
)


def _is_strict_head_coach(title: Optional[str]) -> bool:
    """True only for plain "Head Coach" variants. Excludes every
    subordinate form we've observed on D1-D3 staff cards: Associate /
    Assoc. / Assoc, Assistant / Asst. / Asst, and "Assistant to the
    Head Coach". This guard keeps a real Head Coach card from being
    promoted by mistake when an Associate / Assistant card sits next
    to it in the same staff grid."""
    if not title:
        return False
    t = title.strip()
    if _NON_HEAD_COACH_RE.search(t):
        return False
    return bool(_STRICT_HEAD_COACH_RE.search(t))


def extract_head_coach_from_html(html: str) -> Optional[Dict[str, Optional[str]]]:
    """Extract a single head-coach entry from a roster-page HTML.

    Strategy order (each returns the first hit; later strategies are
    tried only if the prior ones miss):

      1. ``.sidearm-staff-member`` (legacy SIDEARM staff page markup).
      2. ``.s-person-card`` (modern SIDEARM nextgen markup — the
         dominant pattern on current D1 roster pages, where the head
         coach is rendered alongside players in the same card grid).
      3. ``.sidearm-roster-coach`` (legacy SIDEARM roster pages that
         embed the coaching staff in a small ``<ul>`` block beneath
         the player list).
      4. ``.roster-staff-members-card-item`` / ``.roster-card-item``
         (WMT/Vue-style staff card layout used by Stanford and other
         non-SIDEARM CMSs).
      5. Sidearm Vue-embedded JSON — the same ``data: () => ({
         roster: {...} })`` factory that Strategy 5 of ``parse_roster_html``
         reads for players also ships a ``coaches`` / ``coaching_staff``
         / ``staff`` array on many tenants. Non-DOM fallback for sites
         where the staff cards never render (Vue template flags hide
         them, or the pre-hydration HTML omits staff even when the
         JSON has it).

    Returns a dict ``{name, title, email, phone, is_head_coach, _strategy}`` or
    ``None`` if the page does not expose a head coach block (typical
    for roster-only pages — callers should fall back to fetching the
    coaches page).

    The ``_strategy`` key names which of the five strategies fired:
    ``sidearm-staff-member`` / ``s-person-card`` /
    ``sidearm-roster-coach`` / ``roster-staff-members-card-item`` /
    ``vue-embedded-json``. Not semantically part of the coach record —
    the bulk enumerator uses it for per-run instrumentation + diagnostic
    logging; the writers (``upsert_coaches`` / ``upsert_coach_tenures``)
    read only the explicit named fields, so the tag passes through
    harmlessly.
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Strategy 1: legacy ``.sidearm-staff-member`` -----------------
    for el in soup.select(".sidearm-staff-member, [class*='staff-member']"):
        title_el = el.select_one(
            ".sidearm-staff-member-title, [class*='title'], [class*='position']"
        )
        title = title_el.get_text().strip() if title_el else ""
        if not title or not _HEAD_COACH_RE.search(title):
            continue
        # Filter out associate / assistant head coach so the legacy
        # strategy matches the strict semantics of strategies 2-4.
        if not _is_strict_head_coach(title):
            continue
        name_el = el.select_one(
            ".sidearm-staff-member-name a, .sidearm-staff-member-name, "
            "h3 a, h4 a, h3, h4"
        )
        name = name_el.get_text().strip() if name_el else ""
        if not name or len(name) < 3:
            continue
        return {
            "name": name,
            "title": title,
            "email": _extract_email_from_el(el),
            "phone": _extract_phone_from_el(el),
            "is_head_coach": True,
            "_strategy": "sidearm-staff-member",
        }

    # --- Strategy 2: modern SIDEARM ``.s-person-card`` ----------------
    # The card grid contains every player AND the staff. Head coach is
    # the card whose ``.s-person-details__position`` text matches the
    # strict "Head Coach" regex. Name comes from
    # ``.s-person-details__personal-single-line``; aria-label fallback
    # ("<Name> full bio") covers older payload variants.
    for el in soup.select(".s-person-card"):
        title_el = el.select_one(
            ".s-person-details__position, [class*='position']"
        )
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        if not _is_strict_head_coach(title):
            continue
        name_el = el.select_one(
            "[data-test-id='s-person-details__personal-single-line'], "
            ".s-person-details__personal-single-line, "
            ".s-text-paragraph-large-bold"
        )
        name = name_el.get_text(" ", strip=True) if name_el else ""
        if not name:
            aria_link = el.select_one("a[aria-label]")
            if aria_link:
                aria = aria_link.get("aria-label", "")
                # aria-label is consistently "<Name> full bio"
                m = re.match(r"^(.+?)\s+full\s+bio\s*$", aria, re.I)
                if m:
                    name = m.group(1).strip()
                elif aria:
                    name = aria.strip()
        if not name or len(name) < 3:
            continue
        return {
            "name": name,
            "title": title,
            "email": _extract_email_from_el(el),
            "phone": _extract_phone_from_el(el),
            "is_head_coach": True,
            "_strategy": "s-person-card",
        }

    # --- Strategy 3: legacy ``.sidearm-roster-coach`` (inline list) ---
    for el in soup.select(".sidearm-roster-coach"):
        title_el = el.select_one(".sidearm-roster-coach-title")
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        if not _is_strict_head_coach(title):
            continue
        name_el = el.select_one(".sidearm-roster-coach-name")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        if not name or len(name) < 3:
            continue
        return {
            "name": name,
            "title": title,
            "email": _extract_email_from_el(el),
            "phone": _extract_phone_from_el(el),
            "is_head_coach": True,
            "_strategy": "sidearm-roster-coach",
        }

    # --- Strategy 4: WMT/Vue ``.roster-staff-members-card-item`` ------
    for el in soup.select(".roster-staff-members-card-item, .roster-card-item"):
        title_el = el.select_one(
            ".roster-card-item__position, [class*='position']"
        )
        if not title_el:
            continue
        title = title_el.get_text(" ", strip=True)
        if not _is_strict_head_coach(title):
            continue
        name_el = el.select_one(
            ".roster-card-item__title, .roster-card-item__title--link, "
            "[class*='title']"
        )
        name = name_el.get_text(" ", strip=True) if name_el else ""
        if not name or len(name) < 3:
            continue
        return {
            "name": name,
            "title": title,
            "email": _extract_email_from_el(el),
            "phone": _extract_phone_from_el(el),
            "is_head_coach": True,
            "_strategy": "roster-staff-members-card-item",
        }

    # --- Strategy 5: Sidearm Vue-embedded roster JSON -----------------
    # Fallback for sites where DOM strategies all miss but the Vue
    # ``data: () => ({ roster: {...} })`` factory carries a coaches
    # array. Observed on Stanford (gostanford.com) and related Vue
    # NextGen tenants where ``show_coaches_under_roster`` /
    # ``display_coaches`` Vue flags can hide the staff section from the
    # rendered DOM while the JSON still carries the data.
    head_coach_from_json = _parse_sidearm_vue_embedded_head_coach(html)
    if head_coach_from_json is not None:
        return head_coach_from_json

    return None


# ---------------------------------------------------------------------------
# Coaches-page fallback (PR-9)
#
# When ``extract_head_coach_from_html`` returns None against the roster
# page itself, a small ordered list of likely staff URLs (``/coaches``,
# ``/staff``, ``/staff-directory``, ``/coaches-and-staff``) is probed and
# the same extractor is run against each. Most JS-rendered roster pages
# (Pepperdine, George Mason, Virginia Tech) ship a server-rendered staff
# page even when the roster itself is hydrated client-side.
#
# Per-host caching is critical: many programs share an athletics host
# (e.g. all Stanford teams live at gostanford.com) and the school-wide
# ``/staff-directory`` is identical for every team. We also cache
# negative results (404 / no head coach found) so we don't re-probe the
# same host once for every sport on every run.
# ---------------------------------------------------------------------------

# Ordered candidate paths appended to the program base. Order matters:
# program-scoped staff pages first (most precise — return *that* sport's
# head coach), then athletics-wide staff directory as a last resort.
# Conservative deliberately: we'd rather miss than mis-attribute (e.g.
# pulling the women's coach onto the men's program).
_COACHES_PATH_CANDIDATES: tuple = (
    "coaches",
    "coaches-and-staff",
    "staff",
    "staff-directory",
)

# Maximum number of candidate URLs probed per call. Caps worst-case
# extra HTTP load at 4 fetches per cache-miss school. With per-host
# caching this is a one-time cost across all of that host's programs.
_MAX_COACHES_PROBES_PER_CALL = 4

# Polite inter-fetch delay when probing multiple candidates against the
# same host. Same cadence as the inter-school RATE_LIMIT_DELAY but
# applied between candidate URLs within a single school's probe.
_COACHES_PROBE_DELAY = 0.75


def compose_coaches_urls(roster_url: str) -> List[str]:
    """Pure: derive an ordered list of candidate staff-page URLs.

    Given a roster URL like ``https://host/sports/mens-soccer/roster``,
    returns candidates in priority order:

      1. ``https://host/sports/mens-soccer/coaches``
      2. ``https://host/sports/mens-soccer/coaches-and-staff``
      3. ``https://host/sports/mens-soccer/staff``
      4. ``https://host/sports/mens-soccer/staff-directory``

    Sport-scoped paths only — we deliberately do NOT fall back to the
    athletics-wide ``/staff-directory`` because pulling the wrong sport's
    head coach is worse than missing the row entirely (downstream
    upserts are keyed on ``college_id`` and would mis-attribute).

    Strips any trailing ``/roster`` (case-insensitive, with or without
    trailing slash) and any embedded query / fragment to derive the
    program base. Raises ``ValueError`` for empty input.
    """
    if not roster_url:
        raise ValueError("roster_url must be non-empty")
    # Drop query + fragment before path manipulation
    base = roster_url.split("?", 1)[0].split("#", 1)[0]
    base = re.sub(r"/roster/?$", "", base, flags=re.IGNORECASE)
    base = base.rstrip("/")
    if not base:
        raise ValueError(f"could not derive base from {roster_url!r}")
    return [f"{base}/{path}" for path in _COACHES_PATH_CANDIDATES]


def probe_coaches_pages(
    session: requests.Session,
    roster_url: str,
    *,
    cache: Optional[Dict[str, Optional[Dict[str, Optional[str]]]]] = None,
) -> Optional[Dict[str, Optional[str]]]:
    """Fetch candidate /coaches pages and run the inline extractor.

    Returns the first head-coach dict produced by
    ``extract_head_coach_from_html`` against any candidate URL, or
    ``None`` if every candidate either 404s, fails to fetch, or yields
    no head coach. Reuses ``extract_head_coach_from_html`` so all
    semantic guards (`_is_strict_head_coach`, `_NON_HEAD_COACH_RE`)
    apply identically.

    The returned dict has ``_strategy`` rewritten to
    ``coaches-page-fallback:<original_strategy>`` and adds a
    ``_source_url`` key naming the page that hit, so the caller can
    record provenance distinct from the roster page.

    ``cache`` is an optional dict keyed by the cache key returned by
    ``_coaches_cache_key`` (host + program path), used to short-circuit
    repeat probes within a single run. The same value type is stored
    in the cache as is returned: a coach dict on hit, ``None`` on
    confirmed miss. Pass an empty dict to enable caching across
    multiple calls; omit to disable.
    """
    if cache is not None:
        key = _coaches_cache_key(roster_url)
        if key in cache:
            return cache[key]

    candidates = compose_coaches_urls(roster_url)[:_MAX_COACHES_PROBES_PER_CALL]
    result: Optional[Dict[str, Optional[str]]] = None

    for i, candidate in enumerate(candidates):
        if i > 0:
            time.sleep(_COACHES_PROBE_DELAY)
        try:
            html = fetch_with_retry(session, candidate)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "[ncaa-coaches-fallback] fetch error for %s: %s", candidate, exc
            )
            continue
        if not html or len(html) < 500:
            continue
        coach = extract_head_coach_from_html(html)
        rendered_hit = False
        # Last-bucket fallback: a small residual of schools (D1
        # SIDEARM NextGen, some Nuxt tenants) ship JS-only /coaches
        # pages too — the static fetch returns a shell with no
        # staff markup, so the inline extractor misses identically
        # to how it would on the JS-only roster page. When the
        # roster-side Playwright fallback is enabled (same env
        # flag, same renderer), re-run the candidate through
        # headless Chromium and re-extract from the hydrated DOM.
        # Mirrors the pattern in ``_fetch_and_parse_with_fallback``.
        if coach is None and _playwright_fallback_enabled():
            logger.info(
                "[ncaa-coaches-fallback] inline miss on %s; trying "
                "Playwright render",
                candidate,
            )
            rendered = _render_with_playwright(candidate)
            if rendered:
                coach = extract_head_coach_from_html(rendered)
                if coach is not None:
                    rendered_hit = True
                    logger.info(
                        "[ncaa-coaches-fallback] Playwright render "
                        "recovered head coach: %s", candidate,
                    )
        if coach is None:
            continue
        coach = dict(coach)
        original_strategy = coach.get("_strategy", "unknown")
        if rendered_hit:
            original_strategy = f"rendered:{original_strategy}"
        coach["_strategy"] = f"coaches-page-fallback:{original_strategy}"
        coach["_source_url"] = candidate
        result = coach
        break

    if cache is not None:
        cache[_coaches_cache_key(roster_url)] = result
    return result


def _coaches_cache_key(roster_url: str) -> str:
    """Cache key for ``probe_coaches_pages``.

    Keys on host + program path (everything before ``/roster``). This
    means men's soccer and women's soccer at the same school get
    independent cache entries (they have distinct program paths) but
    multiple runs against the same program reuse the result.
    """
    base = roster_url.split("?", 1)[0].split("#", 1)[0]
    base = re.sub(r"/roster/?$", "", base, flags=re.IGNORECASE)
    return base.rstrip("/").lower()


# ---------------------------------------------------------------------------
# Playwright fallback for JS-rendered rosters
# ---------------------------------------------------------------------------

# Many D1 SIDEARM sites (Stanford, Notre Dame, Virginia, Penn State,
# Georgia Tech, Vanderbilt, etc.) ship a shell HTML and hydrate the
# roster client-side via React/Vue. The static ``requests`` fetch sees
# the shell → ``parse_roster_html`` returns 0 players.
#
# This fallback renders the page via headless Chromium and re-parses
# the hydrated DOM. Guarded by an env flag so CI / sandbox environments
# without Playwright don't blow up, and so the operator can disable it
# without code changes if bulk runs get too slow.

_PLAYWRIGHT_FALLBACK_ENV = "NCAA_PLAYWRIGHT_FALLBACK"
_PLAYWRIGHT_RENDER_TIMEOUT_MS = 25_000
_PLAYWRIGHT_SELECTOR_TIMEOUT_MS = 5_000
# Wait up to this long for the NextGen/Nuxt roster XHR to fire during the
# extra Playwright pass. Longer than the selector-wait because the XHR
# typically fires only after all Nuxt JS chunks have loaded.
_PLAYWRIGHT_XHR_TIMEOUT_MS = 15_000

# Regex predicate matching the Sidearm NextGen / Nuxt roster JSON endpoint.
# Observed on hokiesports.com (sport_id=8), goaztecs.com (sport_id=18),
# gopsusports.com (sport_id=28), richmondspiders.com, soonersports.com,
# gowyo.com, utahstateaggies.com. Shape:
#   https://<host>/website-api/rosters?filter[sport_id]=<N>&include=season&...
#
# Firing this response is the deterministic "roster is about to render"
# signal on NextGen — the Nuxt store populates its ``rosterPlayers``
# key right after. Waiting on the response (rather than a DOM selector)
# gets us in front of hydration flakes where the card DOM lags behind
# the data by a few hundred ms.
_NUXT_XHR_PATH_RE = re.compile(
    r"/website-api/rosters(?:\?|/|$)",
    re.IGNORECASE,
)


def _playwright_fallback_enabled() -> bool:
    """True when ``NCAA_PLAYWRIGHT_FALLBACK`` is set to a truthy value.

    Kept off by default: Playwright adds ~3-5s per page for the render,
    which matters at 185+ programs × 2 genders. Operator turns it on
    on Replit after PR-4 merges.
    """
    return os.environ.get(_PLAYWRIGHT_FALLBACK_ENV, "").strip().lower() in (
        "1", "true", "yes", "on",
    )


def _render_with_playwright(url: str) -> Optional[str]:
    """Render ``url`` in headless Chromium; return post-hydration HTML or None.

    Returns None on any of: Playwright not installed, launch failure,
    navigation timeout, or unexpected exception. Caller treats None as
    'no further fallback available' and skips the program.

    Two-phase hydration wait. The NextGen / Nuxt shell cluster
    (hokiesports.com, goaztecs.com, gopsusports.com, ...) fetches its
    roster JSON from ``/website-api/rosters?filter[sport_id]=<N>``
    *after* DOMContentLoaded — the DOM selector for a player card does
    not appear until that response lands and the Nuxt store hydrates.
    The classic SIDEARM fallback's 5-second selector wait is too short
    for that path and times out before the cards ever paint.

    Phase 1: ``page.wait_for_response`` against the NextGen roster
    endpoint predicate. Matches on path (``/website-api/rosters``),
    not host — same predicate works across every NextGen tenant.
    Treated as best-effort: SIDEARM classic sites never fire this XHR,
    so a timeout here is expected, not fatal.

    Phase 2: ``page.wait_for_selector`` for any known roster card
    container (SIDEARM, Nuxt card, Nuxt list, Penn-State player-list,
    or a plain ``<tbody> <tr>``). This catches sites where phase 1
    matched (NextGen post-XHR DOM paint) AND sites that skipped
    phase 1 (SIDEARM classic, which already hydrates before the
    selector wait).

    If both waits time out the function still returns ``page.content()``
    — the parser tries all strategies and may still succeed on an
    off-template site. Returning None only on hard failure (launch,
    navigation) preserves diagnostic value.
    """
    try:
        from playwright.sync_api import (  # type: ignore
            sync_playwright,
            TimeoutError as PlaywrightTimeout,
        )
    except ImportError:
        logger.warning(
            "[ncaa-rosters] NCAA_PLAYWRIGHT_FALLBACK set but playwright is not "
            "installed — skipping fallback for %s",
            url,
        )
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                ctx = browser.new_context(user_agent=USER_AGENT)
                page = ctx.new_page()
                page.goto(
                    url,
                    timeout=_PLAYWRIGHT_RENDER_TIMEOUT_MS,
                    wait_until="domcontentloaded",
                )

                # Phase 1: NextGen / Nuxt roster XHR. Best-effort — SIDEARM
                # classic + WMT + Vue-embedded-JSON sites never fire this.
                # Note: Playwright sync API exposes XHR waits via
                # ``page.wait_for_event("response", predicate=...)`` — the
                # ``page.wait_for_response(...)`` symbol wraps ``event_info``
                # return semantics and is not present on every Playwright
                # version. ``wait_for_event`` is stable across versions.
                try:
                    page.wait_for_event(
                        "response",
                        predicate=lambda r: (
                            r.status == 200
                            and bool(_NUXT_XHR_PATH_RE.search(r.url))
                        ),
                        timeout=_PLAYWRIGHT_XHR_TIMEOUT_MS,
                    )
                    # Give Nuxt a tick to paint the card DOM after the
                    # response lands. Observed ~200-400 ms gap on
                    # hokiesports.com / goaztecs.com between XHR ack and
                    # first ``.roster-list-item`` appearing.
                    page.wait_for_timeout(750)
                except PlaywrightTimeout:
                    pass

                # Phase 2: DOM selector across every known template.
                # Widened vs pre-XHR version to include Nuxt list/card
                # containers and Penn-State's ``player-list-item`` —
                # previously only SIDEARM + raw table rows.
                try:
                    page.wait_for_selector(
                        "li.sidearm-roster-player, "
                        "div.sidearm-roster-player, "
                        ".roster-card-item:not(.roster-staff-members-card-item), "
                        ".roster-list-item, "
                        ".player-list-item, "
                        "table tr[data-player-id], "
                        "table tbody tr",
                        timeout=_PLAYWRIGHT_SELECTOR_TIMEOUT_MS,
                    )
                except PlaywrightTimeout:
                    # Still return the content; parser will try every
                    # strategy and may catch a non-standard shape.
                    pass
                return page.content()
            finally:
                browser.close()
    except Exception as exc:
        logger.warning(
            "[ncaa-rosters] Playwright render failed for %s: %s", url, exc
        )
        return None


def _fetch_and_parse_with_fallback(
    session: requests.Session,
    url: str,
) -> Tuple[Optional[str], List["RosterPlayer"]]:
    """Fetch + parse a roster URL, with optional Playwright fallback.

    Returns ``(html, players)``. If the requests-based parse returns 0
    players AND the Playwright fallback is enabled AND the render
    succeeds AND the rendered parse returns ≥1 player, the rendered
    HTML + player list replace the shell ones.

    ``html`` is returned (rather than re-fetching) so the caller can
    run ``extract_head_coach_from_html`` against the same DOM that
    produced the players — staff blocks are also JS-rendered on the
    same pages.
    """
    html = fetch_with_retry(session, url)
    if not html:
        return None, []

    players = parse_roster_html(html)
    if players or not _playwright_fallback_enabled():
        return html, players

    logger.info(
        "[ncaa-rosters] 0 players from static HTML; trying Playwright fallback: %s",
        url,
    )
    rendered = _render_with_playwright(url)
    if rendered is None:
        return html, players  # still 0; caller handles SKIP

    rendered_players = parse_roster_html(rendered)
    if not rendered_players:
        logger.info(
            "[ncaa-rosters] Playwright render also yielded 0 players: %s", url
        )
        return rendered, rendered_players  # DOM captured, just no players

    logger.info(
        "[ncaa-rosters] Playwright fallback recovered %d player(s): %s",
        len(rendered_players), url,
    )
    return rendered, rendered_players


# ---------------------------------------------------------------------------
# Historical-season roster URLs (PR-6)
#
# SIDEARM and Nuxt both expose prior-season rosters via the same base URL
# plus a year segment. Patterns discovered from live probes + dropdown
# ``<option value>`` attributes:
#
#   SIDEARM : /sports/<sport>/roster/<YYYY>          (Georgetown's dropdown)
#   Nuxt    : /sports/<sport>/roster/season/<YYYY>   (Stanford: roster-season-<YYYY>
#                                                     class on root confirms)
#
# Year is the 4-digit *start year* — "2023-24" season → "2023". Academic-year
# strings throughout the scraper use the "YYYY-YY" range form; this helper
# extracts the start year.
#
# Neither platform accepts the range form (``/roster/2023-24`` on SIDEARM
# returned current-season HTML; ``/roster/season/2023-24`` on Nuxt 404'd).
# Same-shape patterns apply to D2/D3/NAIA/NJCAA — both vendors use
# consistent URL conventions across divisions.
# ---------------------------------------------------------------------------

# Ordered templates. First 200-with-players wins. Same "try multiple,
# first match wins" pattern as the PR-3 multi-path resolver.
_HISTORICAL_URL_TEMPLATES: tuple = (
    "{base}/roster/{start_year}",         # SIDEARM
    "{base}/roster/season/{start_year}",  # Nuxt
)


def _start_year_from_academic_year(academic_year: str) -> str:
    """Given ``"2023-24"`` return ``"2023"``. Validates format strictly."""
    if not re.match(r"^\d{4}-\d{2}$", academic_year or ""):
        raise ValueError(
            f"academic_year must be 'YYYY-YY' (got {academic_year!r})"
        )
    return academic_year.split("-", 1)[0]


def compose_historical_roster_urls(
    current_roster_url: str,
    academic_year: str,
) -> List[str]:
    """Pure: return candidate historical roster URLs for a prior season.

    ``current_roster_url`` is the live /roster page (from
    ``colleges.soccer_program_url`` after PR-2's resolver ran — typically
    shaped ``https://host/sports/<sport>/roster``). ``academic_year`` is the
    "YYYY-YY" season string; the start year is substituted into each
    template.

    The caller probes the returned URLs in order; first that returns ≥1
    player wins. Order is SIDEARM-first because SIDEARM is the majority of
    D1 athletics sites (~130/145 programs with rosters).
    """
    if not current_roster_url:
        raise ValueError("current_roster_url must be non-empty")
    start_year = _start_year_from_academic_year(academic_year)
    # Strip trailing /roster or /roster/ so we can re-append the templated
    # path. Case-insensitive for sites that capitalize 'Roster'.
    base = re.sub(r"/roster/?$", "", current_roster_url, flags=re.IGNORECASE)
    return [tmpl.format(base=base, start_year=start_year) for tmpl in _HISTORICAL_URL_TEMPLATES]


def _find_historical_roster(
    session: requests.Session,
    current_roster_url: str,
    academic_year: str,
) -> Tuple[Optional[str], str, List["RosterPlayer"]]:
    """Probe historical URL candidates; return (url, html, players) on hit.

    Falls back through ``_HISTORICAL_URL_TEMPLATES`` in order; first
    candidate that returns HTML with ≥1 parseable player wins.
    HEAD-probing is insufficient here because malformed SIDEARM URLs
    can return 200 with current-season HTML (observed on
    ``/roster/2023-24`` — 200 but same content as /roster). A proper
    decision requires parsing.

    Returns ``(None, "", [])`` if every candidate fails to produce players.
    The caller treats that as "skip this season for this college".
    """
    candidates = compose_historical_roster_urls(current_roster_url, academic_year)
    for candidate in candidates:
        html, players = _fetch_and_parse_with_fallback(session, candidate)
        if html is None:
            continue
        if not players:
            # URL returned HTML but parser couldn't extract anything.
            # Could be a false 200 serving current-season shell, or a
            # real historical page with markup we don't handle — either
            # way, try the next candidate.
            continue
        return candidate, html, players
    return None, "", []


# ---------------------------------------------------------------------------
# Single-school entry point — used by ``run.py --source ncaa-rosters``
# ---------------------------------------------------------------------------


def scrape_school_url(
    school_url: str,
    *,
    name: str,
    division: str = "D1",
    gender_program: str = "mens",
    conference: Optional[str] = None,
    state: Optional[str] = None,
    city: Optional[str] = None,
    website: Optional[str] = None,
    ncaa_id: Optional[str] = None,
    academic_year: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> Dict:
    """Scrape one NCAA roster page end-to-end.

    Fetches ``school_url``, parses the roster via ``parse_roster_html``,
    and (best-effort) extracts a head coach from the same page. Returns
    a structured dict the writer consumes::

        {
          "college": { name, division, gender_program, conference,
                       state, city, website, soccer_program_url,
                       ncaa_id, scrape_confidence },
          "players": [RosterPlayer, ...],
          "coaches": [ { name, title, ... } ],  # 0 or 1 entries (head coach)
          "academic_year": "2025-26",
          "source_url": school_url,
          "sidearm": bool,  # True if the HTML appears to be SIDEARM-hosted
        }

    The extractor does NOT write anything. The caller hands the dict to
    ``scraper.ingest.ncaa_roster_writer`` to upsert rows.

    Raises ``RuntimeError`` if the page cannot be fetched or the HTML
    contains no parseable roster (caller decides whether to log-and-skip
    or propagate).
    """
    if division not in ("D1", "D2", "D3", "NAIA", "NJCAA"):
        raise ValueError(f"invalid division: {division!r}")
    if gender_program not in ("mens", "womens", "both"):
        raise ValueError(f"invalid gender_program: {gender_program!r}")

    sess = session or _get_session()
    html, players = _fetch_and_parse_with_fallback(sess, school_url)
    if html is None:
        raise RuntimeError(f"failed to fetch roster page: {school_url}")
    if not players:
        raise RuntimeError(f"parsed 0 players from {school_url}")

    head_coach = extract_head_coach_from_html(html)
    coaches: List[Dict] = []
    if head_coach:
        head_coach["source_url"] = school_url
        head_coach["source"] = "ncaa_roster_page"
        coaches.append(head_coach)

    is_sidearm = "sidearm" in html.lower()[:5000]

    # soccer_program_url is the page base (strip trailing /roster so the
    # DB row points at the program landing rather than the roster itself).
    soccer_program_url = re.sub(r"/roster/?$", "", school_url, flags=re.IGNORECASE) or school_url

    return {
        "college": {
            "name": name,
            "division": division,
            "gender_program": gender_program,
            "conference": conference,
            "state": state,
            "city": city,
            "website": website,
            "soccer_program_url": soccer_program_url,
            "ncaa_id": ncaa_id,
            "scrape_confidence": 0.95 if is_sidearm else 0.80,
        },
        "players": players,
        "coaches": coaches,
        "academic_year": academic_year or current_academic_year(),
        "source_url": school_url,
        "sidearm": is_sidearm,
    }


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def discover_roster_url(
    session: requests.Session,
    college: Dict,
    gender: str,
) -> Optional[str]:
    """Try to find the roster page URL for a college.

    Tries, in order:
    1. ``soccer_program_url`` from the DB (if it looks like a roster page)
    2. ``soccer_program_url`` as a base + ``/roster`` suffix
    3. Candidate path segments appended to the program URL base
    4. ``website`` field + sport path candidates
    """
    program_url = college.get("soccer_program_url")
    website = college.get("website")
    paths = WOMENS_PATHS if gender == "womens" else MENS_PATHS

    # If program_url already ends in /roster, try it directly
    if program_url and "/roster" in program_url.lower():
        html = fetch_with_retry(session, program_url)
        if html and len(html) > 500:
            return program_url

    # Try program_url as a base
    if program_url:
        base = program_url.rstrip("/")
        # Strip trailing /roster or /schedule if present
        base = re.sub(r"/(roster|schedule)$", "", base, flags=re.IGNORECASE)
        url = f"{base}/roster"
        html = fetch_with_retry(session, url)
        if html and len(html) > 500:
            return url

    # Try website + sport path candidates
    if website:
        base = website.rstrip("/")
        for path in paths:
            url = f"{base}/sports/{path}/roster"
            html = fetch_with_retry(session, url)
            if html and len(html) > 500:
                return url
            time.sleep(0.3)

    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_connection():
    """Get a psycopg2 connection from DATABASE_URL. Returns None if unavailable."""
    if psycopg2 is None:
        return None
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        conn = psycopg2.connect(url)
        return conn
    except Exception as exc:
        logger.warning("DB connect failed: %s", exc)
        return None


def _fetch_colleges(
    conn,
    division: Optional[str] = None,
    gender: Optional[str] = None,
    skip_unresolved: bool = True,
) -> List[Dict]:
    """Query the colleges table. Returns list of dicts.

    Parameters
    ----------
    skip_unresolved : bool (default True)
        When True (default), exclude rows with
        ``soccer_program_url IS NULL``. Each ``colleges`` row is
        gender-scoped via ``gender_program`` (mens/womens/both), and
        the ``soccer_program_url`` column on that row is the roster
        URL for that specific gender's program. A NULL value means
        the ``ncaa-resolve-urls`` job (PR-2 resolver) probed every
        candidate SIDEARM path and found none responding — i.e., the
        school does not field that sport. Fetching a roster for
        those rows wastes ~6s per school in network + Playwright
        fallback and then SKIPs with "no players parsed", producing
        misleading error counts. The seed data itself over-lists
        men's programs for Big Ten newcomers (Minnesota, Oregon,
        USC) and a handful of others (Richmond, Pepperdine); this
        filter is the single chokepoint that suppresses those
        wasteful attempts.

        Set to False only for debugging / audit scenarios where you
        want to see every seeded row regardless of resolver state.
        Scrapers should always run with the default (True) after the
        ``ncaa-resolve-urls`` job has populated URLs for real
        programs.
    """
    clauses = []
    params: List = []

    if division:
        clauses.append("division = %s")
        params.append(division)
    if gender:
        clauses.append("gender_program = %s")
        params.append(gender)
    if skip_unresolved:
        clauses.append("soccer_program_url IS NOT NULL")

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)

    query = f"""
        SELECT id, name, slug, division, conference, state, city,
               website, soccer_program_url, gender_program,
               last_scraped_at
        FROM colleges
        {where}
        ORDER BY division, name
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _upsert_roster_row(
    cur,
    college_id: int,
    player: RosterPlayer,
    academic_year: str,
) -> str:
    """Insert or update a single roster row. Returns 'inserted' or 'updated'."""
    cur.execute(
        """
        INSERT INTO college_roster_history
            (college_id, player_name, position, year, academic_year,
             hometown, prev_club, jersey_number, scraped_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (college_id, player_name, academic_year)
        DO UPDATE SET
            position       = EXCLUDED.position,
            year           = EXCLUDED.year,
            hometown       = EXCLUDED.hometown,
            prev_club      = EXCLUDED.prev_club,
            jersey_number  = EXCLUDED.jersey_number,
            scraped_at     = NOW()
        RETURNING (xmax = 0) AS is_insert
        """,
        (
            college_id,
            player.player_name,
            player.position,
            player.year,
            academic_year,
            player.hometown,
            player.prev_club,
            player.jersey_number,
        ),
    )
    row = cur.fetchone()
    return "inserted" if row and row[0] else "updated"


def _update_last_scraped(cur, college_id: int) -> None:
    cur.execute(
        "UPDATE colleges SET last_scraped_at = NOW() WHERE id = %s",
        (college_id,),
    )


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def _prior_academic_years(current: str, n: int) -> List[str]:
    """Return ``[current, current-1, ..., current-n]`` as 'YYYY-YY' strings.

    Example: ``_prior_academic_years("2025-26", 2)`` →
    ``["2025-26", "2024-25", "2023-24"]``. Pure; used by backfill loop.
    """
    if n < 0:
        raise ValueError(f"n must be >= 0 (got {n})")
    start = int(_start_year_from_academic_year(current))
    seasons: List[str] = []
    for i in range(n + 1):
        s = start - i
        seasons.append(f"{s}-{str(s + 1)[-2:]}")
    return seasons


_MAX_HISTORICAL_ATTEMPTS = 3

# Seasons with widespread programme cancellations — skip Playwright fallback entirely
LIKELY_COVID_SEASONS = {"2020-21"}


def should_scrape(
    college: Dict,
    season: str,
    current_season: str,
    *,
    conn,
    skip_fresh_days: int = 30,
    force_rescrape: bool = False,
    force_historical: Optional[str] = None,
) -> tuple:
    """Decide whether to scrape a (college, season) pair.

    Returns ``(go: bool, reason: str)``.

    Decision tree:
      1. force_rescrape=True → always scrape (override everything)
      2. force_historical matches this season → always scrape
      3. COVID season (LIKELY_COVID_SEASONS) → skip (likely_covid_cancelled)
      4. Current season: skip if last_scraped_at < skip_fresh_days ago
      5. Historical with ≥10 existing players → NEVER re-scrape (data is done)
      6. Historical with unresolved url_needs_review flag → skip (operator must triage)
      7. Historical with ≥3 failed attempts (tracked in flag metadata) → skip permanently
      8. Otherwise: scrape

    Degrades gracefully if ``college_roster_quality_flags`` doesn't exist
    (pre-PR-24 DB push) — skips flag checks and proceeds.
    """
    if force_rescrape:
        return (True, "force_rescrape")
    if force_historical and force_historical == season:
        return (True, "force_historical")
    if season in LIKELY_COVID_SEASONS and not force_rescrape and force_historical != season:
        return (False, "likely_covid_cancelled")

    college_id = college["id"]
    is_current = season == current_season

    try:
        with conn.cursor() as cur:
            if is_current:
                # Freshness gate: skip if scraped within skip_fresh_days
                cur.execute(
                    "SELECT last_scraped_at FROM colleges WHERE id = %s",
                    (college_id,),
                )
                row = cur.fetchone()
                if row and row[0] is not None:
                    import datetime
                    threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=skip_fresh_days)
                    scraped_at = row[0]
                    if scraped_at.tzinfo is None:
                        scraped_at = scraped_at.replace(tzinfo=datetime.timezone.utc)
                    if scraped_at > threshold:
                        return (False, f"fresh:scraped_at={scraped_at.date()}")
            else:
                # Historical: check existing player count
                cur.execute(
                    "SELECT COUNT(*) FROM college_roster_history "
                    "WHERE college_id = %s AND academic_year = %s",
                    (college_id, season),
                )
                count_row = cur.fetchone()
                if count_row and (count_row[0] or 0) >= 10:
                    return (False, f"historical_has_data:count={count_row[0]}")

                # Historical: check flag table (may not exist pre-PR-24)
                try:
                    cur.execute(
                        """
                        SELECT metadata
                        FROM college_roster_quality_flags
                        WHERE college_id = %s
                          AND academic_year = %s
                          AND flag_type = %s
                          AND resolved_at IS NULL
                        LIMIT 1
                        """,
                        (college_id, season, "url_needs_review"),
                    )
                    flag_row = cur.fetchone()
                    if flag_row is not None:
                        return (False, "unresolved_url_needs_review")

                    # Attempt cap: check historical_no_data flags for attempt count
                    cur.execute(
                        """
                        SELECT metadata
                        FROM college_roster_quality_flags
                        WHERE college_id = %s
                          AND academic_year = %s
                          AND flag_type = %s
                        LIMIT 1
                        """,
                        (college_id, season, "historical_no_data"),
                    )
                    attempt_row = cur.fetchone()
                    if attempt_row is not None:
                        meta = attempt_row[0] or {}
                        attempts = int(meta.get("attempts", 1))
                        if attempts >= _MAX_HISTORICAL_ATTEMPTS:
                            return (False, f"max_attempts:{attempts}")

                except Exception as flag_exc:
                    # UndefinedTable or any other flag-table error — skip flag checks
                    _is_undefined = type(flag_exc).__name__ == "UndefinedTable"
                    if not _is_undefined:
                        try:
                            import psycopg2.errors as _pge
                            _is_undefined = isinstance(flag_exc, _pge.UndefinedTable)
                        except (ImportError, AttributeError):
                            pass
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    if _is_undefined:
                        logger.debug("[should_scrape] flag table not yet pushed; skipping flag checks")
                    else:
                        logger.debug("[should_scrape] flag check error (non-fatal): %s", flag_exc)

    except Exception as exc:
        logger.warning("[should_scrape] DB check failed (proceeding): %s", exc)

    return (True, "ok")


def scrape_college_rosters(
    division: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    backfill_seasons: int = 0,
    skip_unresolved: bool = True,
    skip_fresh_days: int = 30,
    force_rescrape: bool = False,
    force_historical: Optional[str] = None,
    force_covid: bool = False,
) -> Dict:
    """Scrape NCAA rosters and write to college_roster_history.

    Parameters
    ----------
    division : 'D1', 'D2', 'D3', or None (all)
    gender   : 'mens', 'womens', or None (all)
    limit    : max number of colleges to process (for testing)
    dry_run  : if True, parse pages but skip DB writes
    backfill_seasons : 0 = current season only (default). N > 0 = also
        pull rosters for each of the prior N seasons via the
        ``/roster/<YYYY>`` (SIDEARM) or ``/roster/season/<YYYY>`` (Nuxt)
        URL pattern. Writes land in ``college_roster_history`` keyed on
        ``(college_id, player_name, academic_year)`` — same natural key
        as current-season rows, so re-runs are idempotent.
    skip_unresolved : bool (default True)
        Skip colleges with ``soccer_program_url IS NULL`` — the
        ``ncaa-resolve-urls`` resolver leaves that NULL when every
        SIDEARM candidate path 404'd, which means the school doesn't
        field the sport. See ``_fetch_colleges`` for detail.
    skip_fresh_days : int (default 30)
        Skip current-season scrapes where ``last_scraped_at`` is within
        this many days. Passed through to ``should_scrape()``.
    force_rescrape : bool (default False)
        Bypass all ``should_scrape`` guards for every (college, season).
    force_historical : str or None
        Bypass guards for this specific academic year (e.g. ``"2023-24"``).
    force_covid : bool (default False)
        Bypass the 2020-21 COVID season skip guard. Normally the scraper
        skips the 2020-21 season entirely (NCAA cancelled soccer that
        year) to avoid wasting Playwright retries on pages that don't
        exist. Pass True to attempt the scrape anyway (e.g. for a
        targeted investigation or if the season data ever surfaces).

    Returns
    -------
    dict with keys: scraped, rows_inserted, rows_updated, errors, covid_skipped
    """
    current_season = current_academic_year()
    seasons = _prior_academic_years(current_season, backfill_seasons)
    logger.info(
        "Starting NCAA roster scrape: division=%s gender=%s limit=%s dry_run=%s "
        "seasons=%s",
        division, gender, limit, dry_run, seasons,
    )

    conn = _get_connection()
    if conn is None:
        if dry_run:
            logger.warning("No DB connection in dry-run mode; cannot fetch colleges list")
            return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 0, "covid_skipped": 0}
        logger.error("DATABASE_URL not set or connection failed; aborting (use --dry-run for no-DB mode)")
        return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 1, "covid_skipped": 0}

    colleges = _fetch_colleges(
        conn,
        division=division,
        gender=gender,
        skip_unresolved=skip_unresolved,
    )
    if limit:
        colleges = colleges[:limit]

    logger.info("Processing %d colleges", len(colleges))

    session = _get_session()
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    total_scraped = 0
    total_covid_skipped = 0

    # Per-strategy instrumentation for ``extract_head_coach_from_html``.
    # Tracks which of the four strategies produced the hit (or "miss" for
    # pages that returned None). End-of-run logline surfaces the
    # distribution so we can tell whether the 82% probe-set hit rate
    # generalizes to production. Bias sharp: if one strategy accounts
    # for >90% of hits, the others add marginal coverage; if misses
    # dominate, the extractor isn't the right tool for the remaining
    # pages and a separate staff-page scraper (PR-9) may be warranted.
    coach_strategy_hits: Dict[str, int] = {
        "sidearm-staff-member": 0,
        "s-person-card": 0,
        "sidearm-roster-coach": 0,
        "roster-staff-members-card-item": 0,
        "vue-embedded-json": 0,
        "coaches-page-fallback": 0,
        "miss": 0,
    }

    # Per-host coaches-page probe cache (PR-9). Shared across all
    # colleges in the run so multi-program hosts (Stanford fields ~30
    # sports at gostanford.com) only pay the probe cost once per
    # program. Negative results are cached too — see
    # ``probe_coaches_pages`` docstring.
    coaches_probe_cache: Dict[str, Optional[Dict[str, Optional[str]]]] = {}

    # Set up per-division run loggers
    divisions_seen: set = set()

    for i, college in enumerate(colleges):
        college_div = college["division"]
        college_gender = college["gender_program"]
        tag = f"[{i + 1}/{len(colleges)}] {college['name']} ({college_div} {college_gender})"

        # Start run logger on first encounter of this division
        scraper_key = SCRAPER_KEY_MAP.get(college_div, f"ncaa-{college_div.lower()}-rosters")
        if college_div not in divisions_seen:
            divisions_seen.add(college_div)
            run_logger = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NCAA {college_div}",
            )
            run_logger.start()

        try:
            # Discover the live (current-season) URL once per college. The
            # historical URL composer derives from this by replacing the
            # trailing /roster with /roster/<YYYY> (SIDEARM) or
            # /roster/season/<YYYY> (Nuxt). Doing it once avoids N extra
            # discover_roster_url calls per college when backfilling.
            current_roster_url = discover_roster_url(session, college, college_gender)
            if not current_roster_url:
                logger.info("  SKIP %s - no roster URL found", tag)
                total_errors += 1
                continue

            for season in seasons:
                is_current = season == current_season
                season_tag = tag if is_current else f"{tag} [{season}]"

                # NCAA cancelled soccer for the 2020-21 season due to COVID.
                # Attempting to fetch those pages wastes Playwright retries on
                # pages that don't exist. Skip unless --force-covid is passed.
                if season == "2020-21" and not force_covid:
                    logger.info("  SKIP %s - COVID cancelled season (2020-21)", season_tag)
                    total_covid_skipped += 1
                    if not dry_run and college.get("id"):
                        from ingest.college_flag_writer import write_college_flag
                        try:
                            write_college_flag(
                                college_id=college["id"],
                                academic_year=season,
                                flag_type="historical_no_data",
                                metadata={"reason": "covid_cancelled"},
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning(
                                "  flag write failed for %s (2020-21): %s",
                                college["name"], exc,
                            )
                    continue

                go, guard_reason = should_scrape(
                    college,
                    season,
                    current_season,
                    conn=conn,
                    skip_fresh_days=skip_fresh_days,
                    force_rescrape=force_rescrape,
                    force_historical=force_historical,
                )
                if not go:
                    logger.debug("  GUARD SKIP %s reason=%s", season_tag, guard_reason)
                    continue

                if is_current:
                    target_url = current_roster_url
                    html, players = _fetch_and_parse_with_fallback(session, target_url)
                else:
                    target_url, html, players = _find_historical_roster(
                        session, current_roster_url, season
                    )

                if not target_url or html is None:
                    logger.info(
                        "  SKIP %s - no historical URL hit for %s", season_tag, season
                    )
                    total_errors += 1
                    time.sleep(RATE_LIMIT_DELAY)
                    continue
                if not players:
                    logger.info(
                        "  SKIP %s - no players parsed from %s", season_tag, target_url
                    )
                    total_errors += 1
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                total_scraped += 1
                inserted = 0
                updated = 0

                if not dry_run:
                    for p in players:
                        try:
                            result = _upsert_roster_row(
                                conn.cursor(), college["id"], p, season
                            )
                            if result == "inserted":
                                inserted += 1
                            else:
                                updated += 1
                        except Exception as exc:
                            logger.warning(
                                "  DB error for %s / %s (%s): %s",
                                college["name"], p.player_name, season, exc,
                            )
                            conn.rollback()
                            continue
                    conn.commit()

                    # Head-coach capture (PR-7). Uses the already-fetched
                    # roster HTML — no extra HTTP hit. Writes to
                    # college_coach_tenures always; writes to
                    # college_coaches ONLY on the current-season pass so
                    # out-of-order historical runs can't regress the
                    # current-directory view.
                    head_coach = extract_head_coach_from_html(html)
                    fallback_source_url: Optional[str] = None
                    # PR-9: when inline extraction misses on the
                    # current-season roster, probe a small allowlist of
                    # /coaches and /staff URLs derived from the same
                    # base. Restricted to current-season because
                    # historical staff pages are extremely rare and the
                    # extra HTTP cost isn't worth the near-zero hit
                    # rate. Per-host caching (via coaches_probe_cache)
                    # ensures multi-program hosts only pay the probe
                    # cost once per program across the whole run.
                    if head_coach is None and is_current:
                        fallback = probe_coaches_pages(
                            session, target_url, cache=coaches_probe_cache,
                        )
                        if fallback is not None:
                            head_coach = fallback
                    if head_coach:
                        head_coach = dict(head_coach)
                        strategy_hit = head_coach.pop("_strategy", "unknown")
                        fallback_source_url = head_coach.pop("_source_url", None)
                        # Bucket all coaches-page-fallback variants
                        # (regardless of which inline strategy fired
                        # against the staff HTML) into a single counter
                        # so the end-of-run breakdown remains readable.
                        bucket = (
                            "coaches-page-fallback"
                            if strategy_hit.startswith("coaches-page-fallback")
                            else strategy_hit
                        )
                        coach_strategy_hits[bucket] = (
                            coach_strategy_hits.get(bucket, 0) + 1
                        )
                        head_coach.setdefault(
                            "source_url", fallback_source_url or target_url,
                        )
                        head_coach.setdefault(
                            "source",
                            "ncaa_coaches_page" if fallback_source_url
                            else "ncaa_roster_page",
                        )
                        try:
                            _ncaa_roster_writer.upsert_coach_tenures(
                                [head_coach],
                                college_id=college["id"],
                                academic_year=season,
                                conn=conn,
                            )
                        except Exception as exc:
                            logger.warning(
                                "  coach tenure upsert failed for %s (%s): %s",
                                college["name"], season, exc,
                            )
                            try:
                                conn.rollback()
                            except Exception:
                                pass

                        if is_current:
                            try:
                                _ncaa_roster_writer.upsert_coaches(
                                    [head_coach],
                                    college_id=college["id"],
                                    conn=conn,
                                )
                            except Exception as exc:
                                logger.warning(
                                    "  coach directory upsert failed for %s: %s",
                                    college["name"], exc,
                                )
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass

                            # PR-#56: clear any stale "miss" row now that
                            # we have a head coach for this (college,
                            # gender_program) pair. Keeps the dashboard
                            # honest as a current-state queue, not a
                            # historical audit log.
                            try:
                                cur = conn.cursor()
                                cur.execute(
                                    """
                                    DELETE FROM coach_misses
                                    WHERE college_id = %s
                                      AND gender_program = %s
                                    """,
                                    (college["id"], college_gender),
                                )
                                conn.commit()
                            except Exception as exc:
                                logger.warning(
                                    "  coach_misses resolve failed for %s: %s",
                                    college["name"], exc,
                                )
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
                    else:
                        # No head coach extracted from this page — track so
                        # the end-of-run breakdown accurately reflects
                        # production hit rate.
                        coach_strategy_hits["miss"] += 1

                        # PR-#56: optional miss report for the dashboard.
                        # Gated so the table stays empty for ad-hoc runs;
                        # the scheduled deployments set the env var so
                        # operators can see "which schools still have no
                        # head coach" from /data-quality/coach-misses.
                        # Only record on the current-season pass — a
                        # historical pull failing to find a 2017 head
                        # coach is expected and not actionable.
                        if (
                            is_current
                            and os.environ.get("COACH_MISSES_REPORT_ENABLED", "").lower()
                            in ("true", "1", "yes")
                        ):
                            try:
                                probed = compose_coaches_urls(target_url)[
                                    :_MAX_COACHES_PROBES_PER_CALL
                                ]
                                cur = conn.cursor()
                                cur.execute(
                                    """
                                    INSERT INTO coach_misses
                                        (scrape_run_log_id, college_id, division,
                                         gender_program, roster_url, probed_urls)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (college_id, gender_program)
                                    DO UPDATE SET
                                        scrape_run_log_id = EXCLUDED.scrape_run_log_id,
                                        division = EXCLUDED.division,
                                        roster_url = EXCLUDED.roster_url,
                                        probed_urls = EXCLUDED.probed_urls,
                                        recorded_at = now()
                                    """,
                                    (
                                        run_logger.run_id,
                                        college["id"],
                                        college_div,
                                        college_gender,
                                        target_url,
                                        "\n".join(probed),
                                    ),
                                )
                                conn.commit()
                            except Exception as exc:
                                logger.warning(
                                    "  coach_misses write failed for %s: %s",
                                    college["name"], exc,
                                )
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass

                    # Only bump last_scraped_at on the current-season pass —
                    # historical pulls shouldn't make an 8-year-old roster
                    # look like it was just refreshed.
                    if is_current:
                        _update_last_scraped(conn.cursor(), college["id"])
                        conn.commit()

                total_inserted += inserted
                total_updated += updated

                logger.info(
                    "  OK   %s - %d players (%d new, %d updated) from %s",
                    season_tag, len(players), inserted, updated, target_url,
                )

                # Rate-limit between seasons for the same host. Same 1.5s
                # cadence as between colleges — politer to spread the
                # historical backfill load across time rather than burst 5
                # requests back-to-back at one athletics site.
                time.sleep(RATE_LIMIT_DELAY)

        except Exception as exc:
            logger.error("  ERROR %s - %s", tag, exc)
            total_errors += 1
            kind = classify_exception(exc)
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                league_name=f"NCAA {college_div}",
            )
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass

    # Finish run loggers
    for div in divisions_seen:
        key = SCRAPER_KEY_MAP.get(div, f"ncaa-{div.lower()}-rosters")
        run_logger = ScrapeRunLogger(scraper_key=key, league_name=f"NCAA {div}")
        run_logger.start()
        if total_errors > 0 and total_scraped == 0:
            run_logger.finish_failed(
                FailureKind.ZERO_RESULTS,
                error_message=f"{total_errors} colleges failed with no results",
            )
        else:
            run_logger.finish_ok(
                records_created=total_inserted,
                records_updated=total_updated,
                records_failed=total_errors,
            )

    if conn:
        conn.close()

    summary = {
        "scraped": total_scraped,
        "rows_inserted": total_inserted,
        "rows_updated": total_updated,
        "errors": total_errors,
        "covid_skipped": total_covid_skipped,
    }
    logger.info("NCAA roster scrape complete: %s", summary)

    # Strategy-hit breakdown for ``extract_head_coach_from_html``. Grep
    # friendly one-liner so the operator can pipe through ``grep 'coach
    # extraction hits'`` to get the production hit distribution post-run.
    # See the plan's "Diagnostic SQL" section for the decision logic.
    total_hits = sum(v for k, v in coach_strategy_hits.items() if k != "miss")
    total_pages = total_hits + coach_strategy_hits["miss"]
    hit_pct = (100.0 * total_hits / total_pages) if total_pages else 0.0
    logger.info(
        "coach extraction hits: sidearm-staff-member=%d s-person-card=%d "
        "sidearm-roster-coach=%d roster-staff-members-card-item=%d "
        "vue-embedded-json=%d coaches-page-fallback=%d misses=%d  "
        "(hit_rate=%.1f%% of %d pages)",
        coach_strategy_hits["sidearm-staff-member"],
        coach_strategy_hits["s-person-card"],
        coach_strategy_hits["sidearm-roster-coach"],
        coach_strategy_hits["roster-staff-members-card-item"],
        coach_strategy_hits["vue-embedded-json"],
        coach_strategy_hits["coaches-page-fallback"],
        coach_strategy_hits["miss"],
        hit_pct,
        total_pages,
    )
    summary["coach_strategy_hits"] = dict(coach_strategy_hits)

    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Scrape NCAA D1/D2/D3 soccer rosters into college_roster_history",
    )
    parser.add_argument(
        "--division",
        choices=["D1", "D2", "D3"],
        default=None,
        help="Filter to a single division (default: all)",
    )
    parser.add_argument(
        "--gender",
        choices=["mens", "womens"],
        default=None,
        help="Filter to a single gender program (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of colleges to process (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse pages but skip DB writes",
    )
    args = parser.parse_args()

    result = scrape_college_rosters(
        division=args.division,
        gender=args.gender,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(1 if result["errors"] > 0 and result["scraped"] == 0 else 0)


if __name__ == "__main__":
    main()
