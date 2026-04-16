"""
GotSport tryout/combine event extractor.

For a given GotSport event ID, fetches the main teams page, discovers
division codes, then walks each division page extracting club rows
shaped for ``tryouts_writer.insert_tryouts()``.

GotSport teams pages live at:

    https://system.gotsport.com/org_event/events/{event_id}/teams
        ?search[group]={div_code}&showall=clean

Output shape (one dict per tryout row):

    {
        "club_name_raw": str,
        "tryout_date": Optional[str],       # ISO date if found
        "age_group": Optional[str],         # e.g. "U12"
        "gender": Optional[str],            # "M" / "F"
        "location_name": Optional[str],
        "location_city": Optional[str],
        "location_state": Optional[str],
        "url": str,
        "source": "gotsport",
        "status": "upcoming",
        "notes": Optional[str],
    }

These dicts are shaped for ``tryouts_writer.insert_tryouts()``.
"""

from __future__ import annotations

import html as html_module
import logging
import os
import re
import sys
import time
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

# Keywords that signal a tryout/combine event.
TRYOUT_KEYWORDS = re.compile(
    r"\b(tryout|try[\-\s]?out|combine|id[\-\s]?camp|identification[\-\s]?camp"
    r"|open[\-\s]?practice|player[\-\s]?evaluation|showcase)\b",
    re.IGNORECASE,
)

# Month name map for date parsing.
_MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


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
        label=f"gotsport-tryouts:{url}",
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


def extract_event_name(html: str) -> Optional[str]:
    """Extract the event name from a GotSport page (h1 or title tag)."""
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    if h1:
        text = h1.get_text(strip=True)
        if text:
            return decode_html_entities(text)
    title = soup.find("title")
    if title:
        raw = title.get_text(strip=True)
        # GotSport titles are often "GotSport - Event Name"
        raw = decode_html_entities(raw)
        if " - " in raw:
            raw = raw.split(" - ", 1)[1].strip()
        return raw if raw else None
    return None


def parse_date_from_text(text: str) -> Optional[str]:
    """Try to extract an ISO date from free-form text.

    Handles: YYYY-MM-DD, MM/DD/YYYY, "Month DD, YYYY", "Mon DD, YYYY".
    """
    if not text:
        return None
    text = text.strip()

    # ISO: 2026-08-10
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # US: 08/10/2026
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

    # Month name: August 10, 2026 or Aug 10, 2026
    m = re.search(
        r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", text
    )
    if m:
        month_str = m.group(1).lower()
        month = _MONTH_MAP.get(month_str)
        if month:
            day = int(m.group(2))
            year = int(m.group(3))
            return f"{year}-{month:02d}-{day:02d}"

    return None


def _extract_location(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Try to extract location info from page text.

    Returns (location_name, city, state).
    """
    soup = BeautifulSoup(html, "html.parser")
    # Look for paragraphs or divs containing "Location" or address-like text
    for el in soup.find_all(["p", "div", "span"]):
        text = el.get_text(" ", strip=True)
        if not text:
            continue
        # Pattern: "Location: Place Name, City, ST"
        loc_match = re.search(
            r"(?:location|venue|address)[:\s]+(.+)", text, re.IGNORECASE
        )
        if loc_match:
            parts = [p.strip() for p in loc_match.group(1).split(",")]
            name = parts[0] if parts else None
            city = parts[1] if len(parts) > 1 else None
            state = parts[2].strip()[:2].upper() if len(parts) > 2 else None
            return name, city, state

        # Pattern: "City, ST" in text
        st_match = re.search(r"([A-Za-z\s]+),\s+([A-Z]{2})\b", text)
        if st_match:
            return None, st_match.group(1).strip(), st_match.group(2)

    return None, None, None


# ---------------------------------------------------------------------------
# Page-level parsers (pure functions — no HTTP, testable)
# ---------------------------------------------------------------------------

def parse_gotsport_tryout_page(
    html: str,
    event_id: str,
) -> Dict[str, Any]:
    """Parse the main teams page for an event.

    Returns a dict with:
      - event_name: str or None
      - event_date: str (ISO) or None
      - division_codes: list of division code strings
      - location_name, location_city, location_state: optional
    """
    event_name = extract_event_name(html)
    division_codes = extract_division_codes(html)

    # Try to find a date in the page text
    soup = BeautifulSoup(html, "html.parser")
    event_date = None
    for el in soup.find_all(["p", "div", "span", "h2", "h3"]):
        text = el.get_text(" ", strip=True)
        d = parse_date_from_text(text)
        if d:
            event_date = d
            break

    location_name, location_city, location_state = _extract_location(html)

    return {
        "event_name": event_name,
        "event_date": event_date,
        "division_codes": division_codes,
        "location_name": location_name,
        "location_city": location_city,
        "location_state": location_state,
    }


def parse_gotsport_tryout_division(
    html: str,
    event_id: str,
    div_code: str,
) -> List[Dict[str, Any]]:
    """Parse a division page and extract tryout rows.

    Returns a list of dicts shaped for ``tryouts_writer.insert_tryouts()``.
    """
    gender, age_group = parse_division_code(div_code)
    soup = BeautifulSoup(html, "html.parser")
    results: List[Dict[str, Any]] = []

    # Try to extract date and location from the page
    event_date = None
    for el in soup.find_all(["p", "div", "span", "h2", "h3"]):
        text = el.get_text(" ", strip=True)
        d = parse_date_from_text(text)
        if d:
            event_date = d
            break

    location_name, location_city, location_state = _extract_location(html)

    base_url = f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}"
    source_url = f"{base_url}?search%5Bgroup%5D={div_code}&showall=clean"

    for table in soup.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue

        header_cells = [
            c.get_text(strip=True).lower()
            for c in trs[0].find_all(["td", "th"])
        ]

        # Need at least Club + Team columns
        try:
            club_idx = header_cells.index("club")
        except ValueError:
            continue

        for tr in trs[1:]:
            cells = tr.find_all(["td", "th"])
            if len(cells) <= club_idx:
                continue

            club = decode_html_entities(cells[club_idx].get_text(" ", strip=True))
            if not club or club.lower() in _SKIP_NAMES:
                continue

            # Also check team column if it exists
            team_idx = None
            try:
                team_idx = header_cells.index("team")
            except ValueError:
                pass

            team_name = None
            if team_idx is not None and team_idx < len(cells):
                team_name = decode_html_entities(
                    cells[team_idx].get_text(" ", strip=True)
                )
                if team_name and team_name.lower() in _SKIP_NAMES:
                    continue

            results.append({
                "club_name_raw": club.strip(),
                "tryout_date": event_date,
                "age_group": age_group,
                "gender": gender,
                "location_name": location_name,
                "location_city": location_city,
                "location_state": location_state,
                "url": source_url,
                "source": "gotsport",
                "status": "upcoming",
                "notes": f"GotSport event {event_id}, division {div_code}"
                         + (f", team: {team_name}" if team_name else ""),
            })

    return results


# ---------------------------------------------------------------------------
# Public entry point (live HTTP)
# ---------------------------------------------------------------------------

def scrape_gotsport_tryouts(
    event_id: int | str,
) -> List[Dict[str, Any]]:
    """Fetch tryout listings from a GotSport event.

    Returns a list of dicts shaped for ``tryouts_writer.insert_tryouts()``.
    """
    base_url = f"{_BASE}{_TEAMS_PATH.format(event_id=event_id)}"
    main_url = f"{base_url}?showall=clean"

    logger.info("[gotsport-tryouts] fetching divisions for event %s", event_id)
    try:
        r = _get_with_retry(main_url)
    except (TransientError, requests.RequestException) as exc:
        logger.error(
            "[gotsport-tryouts] failed to fetch teams page for event %s: %s",
            event_id, exc,
        )
        return []

    page_info = parse_gotsport_tryout_page(r.text, str(event_id))
    div_codes = page_info["division_codes"]

    if not div_codes:
        logger.warning(
            "[gotsport-tryouts] event %s -- no division codes found", event_id
        )
        return []

    logger.info(
        "[gotsport-tryouts] event %s -- %d divisions: %s",
        event_id, len(div_codes), ", ".join(div_codes),
    )

    all_rows: List[Dict[str, Any]] = []

    for div_code in div_codes:
        div_url = f"{base_url}?search%5Bgroup%5D={div_code}&showall=clean"

        try:
            dr = _get_with_retry(div_url)
        except Exception as exc:
            logger.warning(
                "[gotsport-tryouts] division %s fetch failed: %s", div_code, exc
            )
            continue

        rows = parse_gotsport_tryout_division(dr.text, str(event_id), div_code)
        all_rows.extend(rows)
        logger.info(
            "[gotsport-tryouts] division %s -> %d rows", div_code, len(rows)
        )

        # Polite delay between division fetches.
        time.sleep(0.3)

    logger.info(
        "[gotsport-tryouts] event %s -> %d total tryout rows",
        event_id, len(all_rows),
    )
    return all_rows
