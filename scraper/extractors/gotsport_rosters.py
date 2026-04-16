"""
GotSport per-event roster (player-level) extractor.

For a given GotSport event, fetches division codes (reusing the
``gotsport_events`` division discovery), then for each team in each
division, attempts to follow the team's roster link and extract
individual player rows.

GotSport team pages live at:

    https://system.gotsport.com/org_event/events/{event_id}/teams
        ?search[group]={div_code}&showall=clean

Each team row may contain a link to a roster page. The roster page
contains a table with columns like: Name, #, Pos, DOB (columns vary
by event configuration).

Output shape (one dict per player row):

    {
        "club_name_raw": str,
        "player_name": str,
        "jersey_number": Optional[str],
        "position": Optional[str],
        "snapshot_date": str,          # ISO date of scrape
        "season": Optional[str],
        "age_group": Optional[str],
        "gender": Optional[str],       # "M" / "F"
        "division": Optional[str],
        "source_url": str,
        "event_id": Optional[int],     # DB FK — stamped by runner
    }

These dicts are shaped for ``roster_snapshot_writer.insert_roster_snapshots()``.
"""

from __future__ import annotations

import html as html_module
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from utils.retry import retry_with_backoff, TransientError  # noqa: E402

# Retry settings — inline defaults, overridden by config if available.
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
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

# Placeholder names to skip.
_SKIP_NAMES = frozenset({"", "tbd", "tba", "bye", "n/a", "name", "player"})

# Division code pattern: m_12, f_15.
_DIV_CODE_RE = re.compile(r"^([mf])_(\d+)$", re.IGNORECASE)
_OPTION_VALUE_RE = re.compile(r'value="([mf]_\d+)"', re.IGNORECASE)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def _grad_year_from_dob(raw: str) -> Optional[int]:
    """Compute high-school graduation year from a DOB string.

    Convention: a player born in year Y graduates in May of Y+18.
    If born Aug 1 or later, they graduate in Y+19 (fall cutoff).
    """
    if not raw:
        return None
    # Try common date formats: MM/DD/YYYY, YYYY-MM-DD, MM-DD-YYYY
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            # Fall birthday cutoff: Aug 1
            if dt.month >= 8:
                return dt.year + 19
            return dt.year + 18
        except ValueError:
            continue
    # Try year-only (e.g., "2008")
    m = re.match(r"^(19|20)\d{2}$", raw)
    if m:
        return int(raw) + 18
    return None


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
        label=f"gotsport-rosters:{url}",
    )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def decode_html_entities(s: str) -> str:
    return html_module.unescape(s).strip()


def parse_division_code(code: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse ``m_12`` -> (gender="M", age_group="U12")."""
    m = _DIV_CODE_RE.match(code.strip())
    if not m:
        return None, None
    gender = "M" if m.group(1).lower() == "m" else "F"
    age_group = f"U{m.group(2)}"
    return gender, age_group


def extract_division_codes(html: str) -> List[str]:
    """Extract division option values from the teams page HTML."""
    matches = _OPTION_VALUE_RE.findall(html)
    seen: set = set()
    result: List[str] = []
    for code in matches:
        lc = code.lower()
        if lc not in seen:
            seen.add(lc)
            result.append(code)
    return sorted(result)


# ---------------------------------------------------------------------------
# Team + roster extraction
# ---------------------------------------------------------------------------

def _extract_team_roster_links(
    html: str,
    event_id: str,
    div_code: str,
) -> List[Tuple[str, str, Optional[str]]]:
    """Parse team rows from a division page, extracting roster links.

    Returns list of (club_name, team_name, roster_url_or_None).
    """
    soup = BeautifulSoup(html, "html.parser")
    results: List[Tuple[str, str, Optional[str]]] = []

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        header_cells = [
            c.get_text(strip=True).lower()
            for c in trs[0].find_all(["td", "th"])
        ]

        # Need at least Club + Team columns.
        try:
            club_idx = header_cells.index("club")
            team_idx = header_cells.index("team")
        except ValueError:
            continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) < max(club_idx, team_idx) + 1:
                continue

            club = decode_html_entities(cells[club_idx].get_text(" ", strip=True))
            team_cell = cells[team_idx]
            team = decode_html_entities(team_cell.get_text(" ", strip=True))

            if not team or team.lower() in _SKIP_NAMES:
                continue
            if not club or club.lower() in _SKIP_NAMES:
                club = team

            # Look for a roster link in the team cell.
            roster_url: Optional[str] = None
            link = team_cell.find("a", href=True)
            if link:
                href = link["href"]
                if href.startswith("/"):
                    roster_url = f"{_BASE}{href}"
                elif href.startswith("http"):
                    roster_url = href

            results.append((club.strip(), team.strip(), roster_url))

    return results


def parse_roster_page(html: str) -> List[Dict[str, Optional[str]]]:
    """Parse individual player rows from a GotSport roster page.

    Returns list of dicts with keys: player_name, jersey_number, position.
    """
    soup = BeautifulSoup(html, "html.parser")
    players: List[Dict[str, Optional[str]]] = []

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        # Build header index map.
        headers = [
            c.get_text(strip=True).lower()
            for c in trs[0].find_all(["td", "th"])
        ]
        if not headers:
            continue

        name_idx = _find_header_index(headers, ("name", "player"))
        num_idx = _find_header_index(headers, ("no", "num", "number", "#", "jersey"))
        pos_idx = _find_header_index(headers, ("pos", "position"))
        dob_idx = _find_header_index(headers, ("dob", "date of birth", "birth", "birthday"))
        ht_idx = _find_header_index(headers, ("hometown", "home town", "city"))
        state_idx = _find_header_index(headers, ("state", "st"))

        if name_idx is None:
            continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            texts = [decode_html_entities(c.get_text(" ", strip=True)) for c in cells]
            if len(texts) <= name_idx:
                continue

            player_name = texts[name_idx].strip()
            if not player_name or len(player_name) < 2:
                continue
            if player_name.lower() in _SKIP_NAMES:
                continue

            jersey_number = None
            if num_idx is not None and num_idx < len(texts):
                raw_num = texts[num_idx].strip().lstrip("#")
                if raw_num:
                    jersey_number = raw_num

            position = None
            if pos_idx is not None and pos_idx < len(texts):
                raw_pos = texts[pos_idx].strip()
                if raw_pos:
                    position = raw_pos

            grad_year = None
            if dob_idx is not None and dob_idx < len(texts):
                grad_year = _grad_year_from_dob(texts[dob_idx].strip())

            hometown = None
            if ht_idx is not None and ht_idx < len(texts):
                raw_ht = texts[ht_idx].strip()
                if raw_ht:
                    hometown = raw_ht

            state = None
            if state_idx is not None and state_idx < len(texts):
                raw_st = texts[state_idx].strip()
                if raw_st and len(raw_st) <= 3:
                    state = raw_st.upper()

            players.append({
                "player_name": player_name,
                "jersey_number": jersey_number,
                "position": position,
                "grad_year": grad_year,
                "hometown": hometown,
                "state": state,
            })

        if players:
            return players

    return players


def _find_header_index(
    headers: List[str],
    candidates: Tuple[str, ...],
) -> Optional[int]:
    """Find the first header that matches any candidate (substring or exact)."""
    for i, h in enumerate(headers):
        for c in candidates:
            if c == h or re.search(rf"\b{re.escape(c)}\b", h):
                return i
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def scrape_gotsport_rosters(
    event_id: int | str,
    *,
    default_season: Optional[str] = None,
    snapshot_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch rosters for all teams in a GotSport event.

    Returns a list of dicts shaped for
    ``roster_snapshot_writer.insert_roster_snapshots()``.
    """
    snap_date = snapshot_date or date.today().isoformat()
    base_url = f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}"
    main_url = f"{base_url}?showall=clean"

    logger.info("[gotsport-rosters] fetching divisions for event %s", event_id)
    try:
        r = _get_with_retry(main_url)
    except (TransientError, requests.RequestException) as exc:
        logger.error("[gotsport-rosters] failed to fetch teams page for event %s: %s", event_id, exc)
        return []

    div_codes = extract_division_codes(r.text)
    if not div_codes:
        logger.warning("[gotsport-rosters] event %s -- no division codes found", event_id)
        return []

    logger.info(
        "[gotsport-rosters] event %s -- %d divisions: %s",
        event_id, len(div_codes), ", ".join(div_codes),
    )

    all_players: List[Dict[str, Any]] = []

    for div_code in div_codes:
        gender, age_group = parse_division_code(div_code)
        div_url = f"{base_url}?search%5Bgroup%5D={div_code}&showall=clean"

        try:
            dr = _get_with_retry(div_url)
        except Exception as exc:
            logger.warning("[gotsport-rosters] division %s fetch failed: %s", div_code, exc)
            continue

        team_entries = _extract_team_roster_links(dr.text, str(event_id), div_code)
        logger.info(
            "[gotsport-rosters] division %s -> %d teams", div_code, len(team_entries),
        )

        for club_name, team_name, roster_url in team_entries:
            if not roster_url:
                continue

            try:
                rr = _get_with_retry(roster_url)
            except Exception as exc:
                logger.warning(
                    "[gotsport-rosters] roster fetch failed for %s (%s): %s",
                    team_name, roster_url, exc,
                )
                continue

            players = parse_roster_page(rr.text)
            for p in players:
                all_players.append({
                    "club_name_raw": club_name,
                    "player_name": p["player_name"],
                    "jersey_number": p.get("jersey_number"),
                    "position": p.get("position"),
                    "grad_year": p.get("grad_year"),
                    "hometown": p.get("hometown"),
                    "state": p.get("state"),
                    "snapshot_date": snap_date,
                    "season": default_season,
                    "age_group": age_group,
                    "gender": gender,
                    "division": div_code,
                    "source_url": roster_url,
                    "event_id": None,  # stamped by runner
                })

            # Polite delay between roster page fetches.
            time.sleep(0.4)

        # Polite delay between divisions.
        time.sleep(0.3)

    logger.info("[gotsport-rosters] event %s -> %d player rows", event_id, len(all_players))
    return all_players
