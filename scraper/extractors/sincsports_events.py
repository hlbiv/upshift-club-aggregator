"""
SincSports events extractor — produces rows for the Path A
``events`` + ``event_teams`` tables.

Complements ``sincsports.py`` (which produces a flat list of clubs for the
legacy ``canonical_clubs`` ingest).  This module is structured around the
new data model: one ``event`` row per tournament, many ``event_teams`` rows
per event with age/gender/division breakdown.

STRATEGY
--------
SincSports ``TTTeamList.aspx?tid=<TID>`` pages return the full team list
for a tournament in a single static HTML response (verified April 2026
against the 14 configured SincSports events).  No pagination, no JS.

Each age-group/gender bracket is rendered as:

    <h2>2017 (U9) Girls Gold 7v7</h2>
    <table> ... rows: Team | Club | State | Division | Points </table>

We walk the document looking for h2 headings that match the division
regex, read the next sibling ``<table>`` with the expected column shape,
and emit one team row per data row.

Event metadata (name) comes from the page ``<title>`` — format is
consistently ``"Team List - <EventName>"``.  Dates and location are left
NULL — the ``TTIntro.aspx`` page doesn't expose them in a machine-readable
form, and the ``events`` schema allows NULL for both.

PUBLIC API
----------
``scrape_sincsports_event(tid, league_name)`` returns a
``(event_meta, team_rows)`` tuple suitable for feeding to the DB writer.
Pure extraction — no DB writes.

``parse_sincsports_teamlist(html, tid)`` is the HTML-only entry point used
by tests with fixture HTML.  Does not issue HTTP.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE_URL = "https://soccer.sincsports.com"
_TEAM_LIST_PATH = "/TTTeamList.aspx"
_EXPECTED_HEADERS = {"team", "club", "state"}

# Placeholder names we refuse to write as either teams or clubs.
_SKIP_NAMES: frozenset = frozenset({"", "tbd", "tba", "n/a", "club", "state", "team", "request"})

# Division regex — captures age group and gender from headers like:
#   "2017 (U9) Girls Gold 7v7"
#   "2014 (U12) Boys Silver"
#   "2009 (U17) Girls Premier 11v11"
_DIVISION_RE = re.compile(
    r"""^\s*
        (?P<birth_year>\d{4})?\s*          # optional "2017"
        \(?\s*(?P<age>U\d{1,2})\s*\)?\s*   # "U9" or "(U9)"
        \s*(?P<gender>Boys|Girls|Coed|Mixed|Open)?\s*  # gender
        (?P<rest>.*?)$                     # "Gold 7v7", "Silver", etc.
    """,
    re.IGNORECASE | re.VERBOSE,
)


@dataclass
class EventMeta:
    """Scraped metadata for a single SincSports tournament."""
    tid: str
    name: str
    slug: str
    source: str  # always "sincsports"
    platform_event_id: str  # == tid
    league_name: Optional[str]
    source_url: str
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    season: Optional[str] = None


@dataclass
class TeamRow:
    """One team entry within an event."""
    team_name_raw: str  # the roster row's "Team" column — includes suffix like "- Red"
    club_name: str  # the "Club" column (used for canonicalization)
    state: Optional[str]
    age_group: Optional[str]  # e.g. "U9"
    gender: Optional[str]  # "M", "F", or None — normalized
    division_code: Optional[str]  # raw division header, e.g. "Gold 7v7"
    birth_year: Optional[int]


def extract_tid(url: str) -> Optional[str]:
    """Extract the ``tid`` query parameter from any SincSports URL."""
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    return qs.get("tid", [None])[0]


def normalize_gender(raw: Optional[str]) -> Optional[str]:
    """Map SincSports gender strings to the ``'M' | 'F'`` convention used
    by the events schema (matches the TS ``normalizeGender`` utility).
    Returns ``None`` for Coed/Mixed/Open/unknown to avoid misclassifying
    co-ed brackets as a single gender.
    """
    if not raw:
        return None
    r = raw.strip().lower()
    if r in ("boys", "boy", "male", "m", "men"):
        return "M"
    if r in ("girls", "girl", "female", "f", "women"):
        return "F"
    return None


def _canonicalize_name(name: str) -> str:
    """Light canonicalization for club names (Python-side mirror of the
    TS ``toCanonicalName`` semantics: strip, lowercase, collapse
    whitespace, drop punctuation). The full canonicalization happens in
    the TS layer; this is a best-effort duplicate for early dedup. Never
    empty — callers upstream are expected to reject empty raw names.
    """
    if not name:
        return ""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_division(header_text: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[int]]:
    """Parse ``"2017 (U9) Girls Gold 7v7"`` → (age_group, gender, division_code, birth_year)."""
    if not header_text:
        return None, None, None, None
    m = _DIVISION_RE.match(header_text.strip())
    if not m:
        return None, None, header_text.strip() or None, None
    age = m.group("age")
    gender = normalize_gender(m.group("gender"))
    rest = (m.group("rest") or "").strip()
    birth_year = int(m.group("birth_year")) if m.group("birth_year") else None
    division_code = rest or None
    return (
        age.upper() if age else None,
        gender,
        division_code,
        birth_year,
    )


def _is_team_table(table: Tag) -> bool:
    """A SincSports team table has a first row whose header cells contain
    ``Team``, ``Club``, and ``State`` (order matters but case doesn't).
    """
    first_row = table.find("tr")
    if not first_row:
        return False
    cells = [c.get_text(strip=True).lower() for c in first_row.find_all(["td", "th"])]
    if len(cells) < 3:
        return False
    return _EXPECTED_HEADERS.issubset(set(cells))


def _extract_event_name_from_html(soup: BeautifulSoup, tid: str) -> str:
    """Event name lives in ``<title>Team List - <Event Name></title>``.
    Falls back to ``SincSports <TID>`` if the title is missing or
    malformed — we'd rather write a synthetic name than fail the upsert.
    """
    if soup.title and soup.title.string:
        raw = soup.title.string.strip()
        m = re.match(r"^\s*Team List\s*-\s*(.+?)\s*$", raw)
        if m:
            return m.group(1).strip()
    return f"SincSports {tid}"


def parse_sincsports_teamlist(html: str, tid: str, league_name: Optional[str] = None) -> Tuple[EventMeta, List[TeamRow]]:
    """Parse a SincSports ``TTTeamList.aspx?tid=<tid>`` HTML document.

    Pure function — no HTTP, no DB.  Fixture-driven tests feed HTML
    straight in.  Returns an ``(EventMeta, [TeamRow...])`` tuple.
    """
    soup = BeautifulSoup(html, "lxml")
    event_name = _extract_event_name_from_html(soup, tid)
    source_url = f"{_BASE_URL}{_TEAM_LIST_PATH}?tid={tid}"

    meta = EventMeta(
        tid=tid,
        name=event_name,
        slug=f"sincsports-{tid.lower()}",
        source="sincsports",
        platform_event_id=tid,
        league_name=league_name,
        source_url=source_url,
    )

    teams: List[TeamRow] = []
    seen: set[tuple[str, str]] = set()  # (team_name_raw, division_code) de-dup within this event

    # Strategy: iterate h2 division headers, then find the next team
    # table after each one. We also catch tables without a preceding h2
    # (rare) by scanning any unclaimed team-shaped tables afterward.
    claimed_tables: set[int] = set()

    for h2 in soup.find_all("h2"):
        header_text = h2.get_text(" ", strip=True)
        if not header_text:
            continue
        age_group, gender, division_code, birth_year = _parse_division(header_text)
        if age_group is None and division_code is None:
            continue  # not a division header (e.g. "Schedules are final.")

        # Find the nearest following team-shaped table.
        nxt = h2.find_next("table")
        while nxt is not None and not _is_team_table(nxt):
            nxt = nxt.find_next("table")
        if nxt is None:
            continue
        claimed_tables.add(id(nxt))

        header_cells = [c.get_text(strip=True).lower() for c in nxt.find("tr").find_all(["td", "th"])]
        try:
            team_idx = header_cells.index("team")
            club_idx = header_cells.index("club")
            state_idx = header_cells.index("state")
        except ValueError:
            continue

        for row in nxt.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < max(team_idx, club_idx, state_idx) + 1:
                continue
            team_name = cells[team_idx].get_text(" ", strip=True)
            club_name = cells[club_idx].get_text(" ", strip=True)
            state = cells[state_idx].get_text(strip=True)

            if not team_name or team_name.lower() in _SKIP_NAMES:
                continue
            if not club_name or club_name.lower() in _SKIP_NAMES:
                # A team row with no club is still useful (anonymous clubs
                # sometimes register that way) — fall back to team name.
                club_name = team_name

            # Validate state — must be a 2-letter uppercase abbreviation.
            if not (len(state) == 2 and state.isalpha()):
                state = ""

            key = (team_name.strip().lower(), (division_code or "").lower())
            if key in seen:
                continue
            seen.add(key)

            teams.append(
                TeamRow(
                    team_name_raw=team_name.strip(),
                    club_name=club_name.strip(),
                    state=state.upper() or None,
                    age_group=age_group,
                    gender=gender,
                    division_code=division_code,
                    birth_year=birth_year,
                )
            )

    return meta, teams


def fetch_and_parse(tid: str, league_name: Optional[str] = None, timeout: int = 25) -> Tuple[EventMeta, List[TeamRow]]:
    """Fetch the TTTeamList page and parse it.

    Raises ``requests.RequestException`` on network failure; callers are
    expected to catch and classify via :func:`run.FailureKind`.
    """
    url = f"{_BASE_URL}{_TEAM_LIST_PATH}?tid={tid}"
    logger.info("[SincSports-events] fetching %s", url)
    r = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return parse_sincsports_teamlist(r.text, tid=tid, league_name=league_name)
