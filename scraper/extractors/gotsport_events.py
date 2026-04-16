"""
GotSport events extractor — produces rows for the Path A
``events`` + ``event_teams`` tables.

Complements ``gotsport_matches.py`` (which handles D5 match/schedule
data).  This module populates event metadata and team rosters per
division from GotSport's tournament pages.

STRATEGY
--------
GotSport exposes team lists at:

    https://system.gotsport.com/org_event/events/{event_id}/teams?showall=clean

The ``?showall=clean`` variant returns a minimal HTML page with a
``<select>`` element whose ``<option>`` values encode division codes
(e.g. ``m_12`` for Male U12, ``f_15`` for Female U15).

For each division, fetching:

    ?search[group]={div_code}&showall=clean

returns a table with columns: Club | Team | State.

Event name is extracted from the page ``<title>``.

PUBLIC API
----------
``scrape_gotsport_event(event_id, league_name)`` returns a
``(EventMeta, List[TeamRow])`` tuple.

``parse_gotsport_teams_page(html, event_id)`` is the HTML-only entry
point for tests with fixture HTML.

``parse_gotsport_division_page(html, event_id, div_code)`` parses a
single division's team table from fixture HTML.
"""

from __future__ import annotations

import html as html_module
import logging
import re
import time
from typing import List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.retry import retry_with_backoff, TransientError  # noqa: E402

# Re-use the same dataclasses from sincsports_events so the writer
# accepts both sources without any adapter.
from extractors.sincsports_events import EventMeta, TeamRow  # noqa: E402

# Retry settings — inline defaults match config.py values.  Imported
# lazily to avoid the config-package-vs-config-module shadowing issue
# that breaks pytest collection when running from the repo root.
MAX_RETRIES = 3
RETRY_BASE_DELAY_SECONDS = 2.0
try:
    from config import MAX_RETRIES as _mr, RETRY_BASE_DELAY_SECONDS as _rbd  # type: ignore
    MAX_RETRIES = _mr
    RETRY_BASE_DELAY_SECONDS = _rbd
except ImportError:
    pass

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE = "https://system.gotsport.com"
_TEAMS_PATH = "/org_event/events/{event_id}/teams"
_CLUBS_PATH = "/org_event/events/{event_id}/clubs"
_CLUB_DETAIL_PATH = "/org_event/events/{event_id}/clubs/{club_id}"
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

# Placeholder team names to skip.
_SKIP_NAMES = frozenset({"", "tbd", "tba", "bye", "n/a", "club", "state", "team"})

# Division code pattern: m_12, f_15, etc.
_DIV_CODE_RE = re.compile(r"^([mf])_(\d+)$", re.IGNORECASE)

# Option value pattern inside the select element.
_OPTION_VALUE_RE = re.compile(r'value="([mf]_\d+)"', re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS_CODES
    return False


def _get_with_retry(url: str, timeout: int = 20) -> requests.Response:
    def _fetch() -> requests.Response:
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if _is_retryable(exc):
                raise TransientError(str(exc)) from exc
            raise

    return retry_with_backoff(
        _fetch,
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
        label=f"gotsport-events:{url}",
    )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def decode_html_entities(s: str) -> str:
    """Decode HTML entities like &#39; &amp; etc."""
    return html_module.unescape(s).strip()


def parse_division_code(code: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse ``m_12`` → (gender="M", age_group="U12").

    Returns (None, None) if the code doesn't match.
    """
    m = _DIV_CODE_RE.match(code.strip())
    if not m:
        return None, None
    gender = "M" if m.group(1).lower() == "m" else "F"
    age_group = f"U{m.group(2)}"
    return gender, age_group


def extract_division_codes(html: str) -> List[str]:
    """Extract division option values from the teams page HTML.

    Looks for ``value="m_12"`` or ``value="f_15"`` patterns in
    ``<option>`` elements (or anywhere in the HTML — GotSport's
    ``showall=clean`` pages sometimes render options outside a
    proper ``<select>``).
    """
    matches = _OPTION_VALUE_RE.findall(html)
    # Deduplicate while preserving order.
    seen = set()
    result = []
    for code in matches:
        lc = code.lower()
        if lc not in seen:
            seen.add(lc)
            result.append(code)
    return sorted(result)


def extract_event_name(html: str, event_id: str) -> str:
    """Extract event name from the page ``<title>`` tag.

    GotSport titles are typically ``"Event Name | GotSport"`` or
    ``"Teams - Event Name"``.  Falls back to ``"GotSport {event_id}"``.
    """
    soup = BeautifulSoup(html, "html.parser")
    if soup.title and soup.title.string:
        raw = soup.title.string.strip()
        # Try common patterns
        # "Teams - Event Name"
        m = re.match(r"^\s*Teams?\s*[-–|]\s*(.+?)\s*$", raw)
        if m:
            name = m.group(1).strip()
            # Remove trailing " | GotSport" or similar
            name = re.sub(r"\s*\|\s*GotSport\s*$", "", name, flags=re.IGNORECASE).strip()
            if name:
                return decode_html_entities(name)
        # "Event Name | GotSport"
        m = re.match(r"^(.+?)\s*\|\s*GotSport\s*$", raw, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            if name:
                return decode_html_entities(name)
        # Plain title without separators
        if raw and raw.lower() != "gotsport":
            return decode_html_entities(raw)
    return f"GotSport {event_id}"


def parse_team_rows(html: str, div_code: str) -> List[Tuple[str, str, str]]:
    """Parse team rows from a division page.

    Each row has ``<td>Club</td><td>Team</td><td>State</td>``.
    Returns list of (club_name, team_name, state) tuples.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Tuple[str, str, str]] = []

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        # Find the header row to locate column indices.
        header_cells = [
            c.get_text(strip=True).lower()
            for c in trs[0].find_all(["td", "th"])
        ]

        # Need at least Club, Team, State columns.
        try:
            club_idx = header_cells.index("club")
            team_idx = header_cells.index("team")
            state_idx = header_cells.index("state")
        except ValueError:
            continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < max(club_idx, team_idx, state_idx) + 1:
                continue

            club = decode_html_entities(cells[club_idx].get_text(" ", strip=True))
            team = decode_html_entities(cells[team_idx].get_text(" ", strip=True))
            state = decode_html_entities(cells[state_idx].get_text(strip=True))

            rows.append((club, team, state))

    return rows


# ---------------------------------------------------------------------------
# Pure parsing entry points (for tests)
# ---------------------------------------------------------------------------

def parse_gotsport_teams_page(
    html: str,
    event_id: str,
    league_name: Optional[str] = None,
) -> Tuple[EventMeta, List[str]]:
    """Parse the main teams page to get event metadata + division codes.

    Returns ``(EventMeta, [div_codes])`` — no team rows yet (those
    require per-division HTTP fetches or fixture HTML).
    """
    event_name = extract_event_name(html, event_id)
    div_codes = extract_division_codes(html)
    source_url = f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}?showall=clean"

    meta = EventMeta(
        tid=str(event_id),
        name=event_name,
        slug=f"gotsport-{event_id}",
        source="gotsport",
        platform_event_id=str(event_id),
        league_name=league_name,
        source_url=source_url,
    )
    return meta, div_codes


def parse_gotsport_division_page(
    html: str,
    event_id: str,
    div_code: str,
    league_name: Optional[str] = None,
) -> List[TeamRow]:
    """Parse a single division's team table from HTML.

    Pure function — no HTTP.  Returns ``[TeamRow...]``.
    """
    gender, age_group = parse_division_code(div_code)
    raw_rows = parse_team_rows(html, div_code)
    source_url = (
        f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}"
        f"?search%5Bgroup%5D={div_code}&showall=clean"
    )

    teams: List[TeamRow] = []
    seen: set = set()

    for club, team, state in raw_rows:
        if not team or team.lower() in _SKIP_NAMES:
            continue
        if not club or club.lower() in _SKIP_NAMES:
            club = team  # fall back to team name

        # Validate state — must be 2-letter alpha.
        if not (len(state) == 2 and state.isalpha()):
            state = ""

        key = (team.strip().lower(), div_code.lower())
        if key in seen:
            continue
        seen.add(key)

        teams.append(TeamRow(
            team_name_raw=team.strip(),
            club_name=club.strip(),
            state=state.upper() or None,
            age_group=age_group,
            gender=gender,
            division_code=div_code,
            birth_year=None,
        ))

    return teams


# ---------------------------------------------------------------------------
# Live scraper entry point
# ---------------------------------------------------------------------------

def scrape_gotsport_event(
    event_id: int | str,
    league_name: Optional[str] = None,
    timeout: int = 20,
) -> Tuple[EventMeta, List[TeamRow]]:
    """Fetch and parse a GotSport event's team list.

    1. Fetch the main teams page to get division codes + event name.
    2. For each division, fetch the filtered team table.
    3. Return ``(EventMeta, [TeamRow...])``.

    Raises ``requests.RequestException`` on network failure.
    """
    base_url = f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}"
    main_url = f"{base_url}?showall=clean"
    logger.info("[gotsport-events] fetching %s", main_url)

    r = _get_with_retry(main_url, timeout=timeout)
    meta, div_codes = parse_gotsport_teams_page(
        r.text, str(event_id), league_name=league_name,
    )

    if not div_codes:
        logger.warning(
            "[gotsport-events] event %s — no division codes found", event_id,
        )
        return meta, []

    logger.info(
        "[gotsport-events] event %s — %d divisions: %s",
        event_id, len(div_codes), ", ".join(div_codes),
    )

    all_teams: List[TeamRow] = []
    for div_code in div_codes:
        div_url = f"{base_url}?search%5Bgroup%5D={div_code}&showall=clean"
        logger.info("[gotsport-events] fetching division %s", div_code)

        try:
            dr = _get_with_retry(div_url, timeout=timeout)
            teams = parse_gotsport_division_page(
                dr.text, str(event_id), div_code, league_name=league_name,
            )
            all_teams.extend(teams)
            logger.info(
                "[gotsport-events] division %s → %d teams", div_code, len(teams),
            )
        except Exception as exc:
            logger.warning(
                "[gotsport-events] division %s fetch failed: %s", div_code, exc,
            )

        # Polite delay between division fetches.
        time.sleep(0.4)

    return meta, all_teams


# ---------------------------------------------------------------------------
# League-style scraper (via /clubs endpoint)
# ---------------------------------------------------------------------------
# GotSport league events (NPL sub-leagues, GA, DPL, etc.) expose club
# rosters at /clubs instead of /teams. The /clubs page lists clubs with
# links to /clubs/{club_id}, where team tables appear.

_CLUB_LINK_RE = re.compile(
    r"/org_event/events/\d+/clubs/(\d+)[^\"]*\"[^>]*>([^<]+)", re.IGNORECASE,
)


def parse_clubs_page(html: str) -> List[Tuple[str, str]]:
    """Parse the /clubs page to extract (club_id, club_name) pairs."""
    clubs: List[Tuple[str, str]] = []
    seen: set = set()
    for m in _CLUB_LINK_RE.finditer(html):
        club_id = m.group(1)
        name = decode_html_entities(m.group(2))
        if name.lower() in ("schedule", "teams", "") or len(name) < 2:
            continue
        if club_id not in seen:
            seen.add(club_id)
            clubs.append((club_id, name))
    return clubs


def parse_club_detail_page(
    html: str, club_name: str, event_id: str,
) -> List[TeamRow]:
    """Parse a club's team table from /clubs/{club_id}.

    Returns TeamRow list.  The table has columns like:
    Team | Gender | Age Group | Division  (or similar).
    """
    soup = BeautifulSoup(html, "html.parser")
    teams: List[TeamRow] = []

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        header_cells = [
            c.get_text(strip=True).lower()
            for c in trs[0].find_all(["td", "th"])
        ]

        # Try to find key columns — different events may have different headers
        team_idx = None
        gender_idx = None
        age_idx = None
        div_idx = None
        state_idx = None

        for i, h in enumerate(header_cells):
            if h in ("team", "team name"):
                team_idx = i
            elif h in ("gender", "sex"):
                gender_idx = i
            elif h in ("age group", "age", "age_group"):
                age_idx = i
            elif h in ("division", "div", "group"):
                div_idx = i
            elif h in ("state", "st"):
                state_idx = i

        if team_idx is None:
            # If no "team" header, try Club | Team | State fallback
            try:
                club_idx = header_cells.index("club")
                team_idx = header_cells.index("team") if "team" in header_cells else club_idx
            except ValueError:
                continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) <= team_idx:
                continue

            team_name = decode_html_entities(cells[team_idx].get_text(" ", strip=True))
            if not team_name or team_name.lower() in _SKIP_NAMES:
                continue

            gender_raw = ""
            if gender_idx is not None and gender_idx < len(cells):
                gender_raw = cells[gender_idx].get_text(strip=True).lower()

            gender = None
            if gender_raw in ("male", "m", "boys", "boy"):
                gender = "M"
            elif gender_raw in ("female", "f", "girls", "girl"):
                gender = "F"

            age_group = None
            if age_idx is not None and age_idx < len(cells):
                raw_age = cells[age_idx].get_text(strip=True)
                if raw_age:
                    # Normalize: "12" → "U12", "U-14" → "U14"
                    raw_age = raw_age.replace("-", "").strip()
                    if raw_age.isdigit():
                        age_group = f"U{raw_age}"
                    elif raw_age.upper().startswith("U"):
                        age_group = raw_age.upper()

            division = None
            if div_idx is not None and div_idx < len(cells):
                division = cells[div_idx].get_text(strip=True) or None

            state = None
            if state_idx is not None and state_idx < len(cells):
                st = cells[state_idx].get_text(strip=True)
                if len(st) == 2 and st.isalpha():
                    state = st.upper()

            teams.append(TeamRow(
                team_name_raw=team_name,
                club_name=club_name,
                state=state,
                age_group=age_group,
                gender=gender,
                division_code=division,
                birth_year=None,
            ))

    # If no table found, just return one entry for the club itself
    if not teams:
        teams.append(TeamRow(
            team_name_raw=club_name,
            club_name=club_name,
            state=None,
            age_group=None,
            gender=None,
            division_code=None,
            birth_year=None,
        ))

    return teams


def scrape_gotsport_league_event(
    event_id: int | str,
    league_name: Optional[str] = None,
    timeout: int = 20,
) -> Tuple[EventMeta, List[TeamRow]]:
    """Scrape a GotSport league event via the /clubs endpoint.

    League events (NPL sub-leagues, GA, DPL) don't expose /teams.
    Instead:
      1. Fetch /clubs → list of club links
      2. For each club, fetch /clubs/{id} → team table

    Returns ``(EventMeta, [TeamRow...])``.
    """
    clubs_url = f"{_BASE}{_CLUBS_PATH.format(event_id=event_id)}"
    logger.info("[gotsport-league] fetching clubs page %s", clubs_url)

    r = _get_with_retry(clubs_url, timeout=timeout)

    # Extract event name from clubs page
    event_name = extract_event_name(r.text, str(event_id))
    clubs = parse_clubs_page(r.text)

    source_url = clubs_url
    meta = EventMeta(
        tid=str(event_id),
        name=event_name,
        slug=f"gotsport-{event_id}",
        source="gotsport",
        platform_event_id=str(event_id),
        league_name=league_name,
        source_url=source_url,
    )

    if not clubs:
        logger.warning("[gotsport-league] event %s — no clubs found", event_id)
        return meta, []

    logger.info(
        "[gotsport-league] event %s — %d clubs found", event_id, len(clubs),
    )

    all_teams: List[TeamRow] = []
    for club_id, club_name in clubs:
        detail_url = (
            f"{_BASE}{_CLUB_DETAIL_PATH.format(event_id=event_id, club_id=club_id)}"
        )

        try:
            dr = _get_with_retry(detail_url, timeout=timeout)
            teams = parse_club_detail_page(dr.text, club_name, str(event_id))
            all_teams.extend(teams)
        except Exception as exc:
            logger.warning(
                "[gotsport-league] club %s (%s) fetch failed: %s",
                club_id, club_name, exc,
            )

        # Polite delay between club fetches.
        time.sleep(0.3)

    logger.info(
        "[gotsport-league] event %s — %d total team entries from %d clubs",
        event_id, len(all_teams), len(clubs),
    )

    return meta, all_teams
