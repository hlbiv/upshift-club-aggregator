"""
SincSports USA Rank club rankings extractor.

SincSports publishes youth soccer club rankings at:
    https://soccer.sincsports.com/rank.aspx

URL parameters:
    gender  — 'B' (Boys/Men) or 'G' (Girls/Women)
    age     — 'U08' through 'U19'
    state   — 'National' (default) or a 2-letter US state code
    level   — '' | 'G' (Gold) | 'S' (Silver) | 'B' (Bronze) |
               'R' (Red) | 'BL' (Blue) | 'GR' (Green)

DYNAMIC LOADING NOTE:
    The rankings table is loaded dynamically by JavaScript — the initial
    server response contains a skeleton page with filter controls but no
    ranking rows. Rankings are fetched via XHR (the specifics of the
    endpoint are not publicly documented). To work around this limitation
    the scraper uses two strategies:

    Strategy A (primary): Parse any ranking rows the server embeds in the
        initial HTML response. Some server configurations pre-render the
        first page of results. The parser looks for <table> elements whose
        headers include rank-like column names.

    Strategy B (fallback): If the initial HTML has no rows, the scraper
        is a no-op and returns an empty list. Operators are notified via
        the logger. A future PR can swap in a Playwright-based fetch
        (scraper_js) to handle the AJAX surface properly.

Output shape (one dict per ranking row):
    {
        "club_name_raw": str,
        "rank_value": Optional[int],
        "rating_value": Optional[str],  # tier label if numeric rank absent
        "age_group": str,               # e.g. 'U13'
        "gender": str,                  # 'B' | 'G'
        "season": Optional[str],        # e.g. '2024-25'
        "division": Optional[str],      # tier label e.g. 'Gold'
        "source_url": str,
    }

``canonical_club_id`` is intentionally NOT populated — the canonical-club
linker resolves it in a separate pass.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS, USER_AGENT  # noqa: E402
from utils.retry import retry_with_backoff, TransientError  # noqa: E402

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": USER_AGENT}
_BASE_URL = "https://soccer.sincsports.com"
_RANK_URL = f"{_BASE_URL}/rank.aspx"

_RETRYABLE_STATUS = {500, 502, 503, 504}

# Tier label → numeric rank range midpoint for division tagging.
_TIER_LABELS = {
    "G": "Gold",
    "S": "Silver",
    "B": "Bronze",
    "R": "Red",
    "BL": "Blue",
    "GR": "Green",
}

# Default age groups and genders to scrape when no explicit filter is given.
DEFAULT_AGE_GROUPS = [
    "U09", "U10", "U11", "U12", "U13", "U14", "U15", "U16", "U17", "U18", "U19",
]
DEFAULT_GENDERS = ["B", "G"]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS
    return False


def _fetch_page(url: str, params: Optional[Dict[str, str]] = None) -> str:
    """Fetch a rankings page with retry, return HTML text."""
    def _do_fetch() -> requests.Response:
        try:
            r = requests.get(url, headers=_HEADERS, params=params, timeout=25)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if _is_retryable(exc):
                raise TransientError(str(exc)) from exc
            raise

    r = retry_with_backoff(
        _do_fetch,
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
        label=f"sincsports-rankings:{url}",
    )
    return r.text


# ---------------------------------------------------------------------------
# HTML parser
# ---------------------------------------------------------------------------

def _row_text(cell) -> str:
    """Extract clean text from a BeautifulSoup tag."""
    if cell is None:
        return ""
    return re.sub(r"\s+", " ", cell.get_text(" ", strip=True)).strip()


def _is_rank_table(headers: List[str]) -> bool:
    """Return True if the table looks like a rankings table."""
    joined = " ".join(h.lower() for h in headers)
    return any(k in joined for k in ("rank", "team", "club", "rating", "level"))


def _parse_rankings_html(
    html: str,
    age_group: str,
    gender: str,
    season: Optional[str],
    source_url: str,
) -> List[Dict[str, Any]]:
    """Parse server-rendered ranking rows out of a SincSports rank page.

    SincSports dynamically loads the rankings table via JavaScript/AJAX,
    so in many cases the initial HTML will contain no data rows. When rows
    ARE present they appear inside a standard <table> element whose first
    row or <thead> contains column headers like Rank, Team Name, State,
    Rating/Level.

    Returns an empty list when no parseable rows are found (normal for
    fully AJAX-driven pages).
    """
    soup = BeautifulSoup(html, "lxml")
    rows_out: List[Dict[str, Any]] = []

    for table in soup.find_all("table"):
        # Build header list from <th> or first <tr>.
        th_cells = table.find_all("th")
        if th_cells:
            headers = [_row_text(th) for th in th_cells]
        else:
            first_tr = table.find("tr")
            if not first_tr:
                continue
            headers = [_row_text(td) for td in first_tr.find_all("td")]

        if not _is_rank_table(headers):
            continue

        # Map header name → column index (case-insensitive, first match).
        h_lower = [h.lower() for h in headers]
        idx: Dict[str, int] = {}
        for i, h in enumerate(h_lower):
            if "rank" in h and "rank" not in idx:
                idx["rank"] = i
            if ("team" in h or "club" in h or "name" in h) and "name" not in idx:
                idx["name"] = i
            if ("rating" in h or "level" in h or "tier" in h) and "rating" not in idx:
                idx["rating"] = i
            if "state" in h and "state" not in idx:
                idx["state"] = i
            if "division" in h and "division" not in idx:
                idx["division"] = i

        if "name" not in idx:
            logger.debug(
                "[sincsports-rankings] table has no name/team column: headers=%s",
                headers,
            )
            continue

        data_rows = table.find_all("tr")
        # Skip header row(s) — rows with <th> elements are header rows.
        for tr in data_rows:
            if tr.find("th"):
                continue
            tds = tr.find_all("td")
            if not tds:
                continue
            min_col = max(idx.values())
            if len(tds) <= min_col:
                continue

            club_name = _row_text(tds[idx["name"]]) if "name" in idx else ""
            if not club_name:
                continue

            rank_raw = _row_text(tds[idx["rank"]]) if "rank" in idx else ""
            rank_value: Optional[int] = None
            try:
                rank_value = int(re.sub(r"[^\d]", "", rank_raw)) if rank_raw else None
            except ValueError:
                rank_value = None

            rating_value: Optional[str] = None
            if "rating" in idx:
                rating_value = _row_text(tds[idx["rating"]]) or None

            division: Optional[str] = None
            if "division" in idx:
                division = _row_text(tds[idx["division"]]) or None

            rows_out.append({
                "club_name_raw": club_name,
                "rank_value": rank_value,
                "rating_value": rating_value,
                "age_group": age_group,
                "gender": gender,
                "season": season,
                "division": division,
                "source_url": source_url,
            })

    return rows_out


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def scrape_sincsports_rankings_page(
    age_group: str,
    gender: str,
    *,
    season: Optional[str] = None,
    state: str = "National",
    level: str = "",
) -> List[Dict[str, Any]]:
    """Fetch and parse one SincSports rankings page.

    Args:
        age_group: Age-group string such as 'U13', 'U15'.
        gender:    'B' (Boys/Men) or 'G' (Girls/Women).
        season:    Optional season tag e.g. '2024-25' (not present in the URL;
                   stored as metadata).
        state:     State filter — 'National' or 2-letter US state code.
        level:     Tier filter — '' (all), 'G' (Gold), 'S' (Silver), etc.

    Returns:
        List of ranking-row dicts. Typically empty when the page is
        fully AJAX-driven (normal behaviour for SincSports). Rows are
        populated only when the server pre-renders ranking data.
    """
    params: Dict[str, str] = {
        "gender": gender,
        "age": age_group,
    }
    if state and state != "National":
        params["state"] = state
    if level:
        params["level"] = level

    source_url = f"{_RANK_URL}?" + "&".join(f"{k}={v}" for k, v in params.items())

    logger.info(
        "[sincsports-rankings] fetching age=%s gender=%s state=%s level=%s",
        age_group, gender, state, level,
    )

    try:
        html = _fetch_page(_RANK_URL, params=params)
    except Exception as exc:
        logger.error(
            "[sincsports-rankings] fetch failed age=%s gender=%s: %s",
            age_group, gender, exc,
        )
        return []

    rows = _parse_rankings_html(
        html,
        age_group=age_group,
        gender=gender,
        season=season,
        source_url=source_url,
    )

    if not rows:
        logger.info(
            "[sincsports-rankings] no rows parsed for age=%s gender=%s "
            "(page likely AJAX-rendered — Playwright integration needed for "
            "full data capture)",
            age_group, gender,
        )

    return rows


def scrape_sincsports_rankings(
    *,
    age_groups: Optional[List[str]] = None,
    genders: Optional[List[str]] = None,
    season: Optional[str] = None,
    state: str = "National",
) -> List[Dict[str, Any]]:
    """Scrape SincSports rankings across a matrix of age groups + genders.

    Iterates the cartesian product of ``age_groups`` × ``genders``,
    fetches each rankings page, and returns all parsed rows combined.

    Args:
        age_groups: List of age-group strings (default: U09–U19).
        genders:    List of gender codes (default: ['B', 'G']).
        season:     Season tag stored as metadata (e.g. '2024-25').
        state:      State filter applied to every page fetch.

    Returns:
        Combined list of ranking-row dicts across all age/gender combos.
    """
    if age_groups is None:
        age_groups = DEFAULT_AGE_GROUPS
    if genders is None:
        genders = DEFAULT_GENDERS

    all_rows: List[Dict[str, Any]] = []

    for gender in genders:
        for age in age_groups:
            rows = scrape_sincsports_rankings_page(
                age_group=age,
                gender=gender,
                season=season,
                state=state,
            )
            all_rows.extend(rows)

    logger.info(
        "[sincsports-rankings] total rows scraped: %d "
        "(across %d age groups × %d genders)",
        len(all_rows), len(age_groups), len(genders),
    )
    return all_rows
