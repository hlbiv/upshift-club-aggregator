"""
Static page scraper — uses requests + BeautifulSoup.
Handles plain HTML pages that don't require JavaScript.
"""

from __future__ import annotations

import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS
from utils.retry import retry_with_backoff, TransientError

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
    )
}

_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


def _extract_clubs_from_table(soup: BeautifulSoup, url: str, league_name: str) -> List[Dict]:
    """Extract clubs from HTML <table> elements."""
    records = []
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if not cells or all(c.name == "th" for c in cells):
                continue
            values = [c.get_text(strip=True) for c in cells]
            if not values[0]:
                continue
            record = _build_record(values, headers, url, league_name)
            if record:
                records.append(record)
    return records


def _extract_clubs_from_lists(soup: BeautifulSoup, url: str, league_name: str) -> List[Dict]:
    """Extract clubs from <ul>/<ol> lists and definition lists."""
    records = []
    for ul in soup.find_all(["ul", "ol"]):
        for li in ul.find_all("li"):
            text = li.get_text(strip=True)
            if len(text) < 3:
                continue
            records.append({
                "club_name": text,
                "league_name": league_name,
                "city": "",
                "state": "",
                "source_url": url,
            })
    return records


def _extract_clubs_from_links(soup: BeautifulSoup, url: str, league_name: str) -> List[Dict]:
    """
    Fall-through: pull club names from anchor text when no table/list is present.
    Only keeps links that look like club names (title-case, > 3 chars).
    """
    records = []
    seen = set()
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        if (
            len(text) > 3
            and text not in seen
            and any(c.isupper() for c in text)
            and not text.lower().startswith(("home", "about", "contact", "log", "sign", "menu", "search"))
        ):
            seen.add(text)
            records.append({
                "club_name": text,
                "league_name": league_name,
                "city": "",
                "state": "",
                "source_url": url,
            })
    return records


def _build_record(values: List[str], headers: List[str], url: str, league_name: str) -> Dict | None:
    """Map table row values onto the target schema using detected column headers."""
    if not values:
        return None

    def safe_get(condition_fn) -> str:
        for i, h in enumerate(headers):
            if condition_fn(h) and i < len(values):
                return values[i]
        return ""

    field_map = {
        "club_name": safe_get(lambda h: "club" in h or "name" in h or "team" in h) or values[0],
        "city": safe_get(lambda h: "city" in h or "town" in h),
        "state": safe_get(lambda h: "state" in h or "province" in h),
    }
    if not field_map["club_name"]:
        return None
    return {
        **field_map,
        "league_name": league_name,
        "source_url": url,
    }


def _is_retryable(exc: Exception) -> bool:
    """Return True if this requests exception is transient and worth retrying."""
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS_CODES
    return False


def scrape_static(url: str, league_name: str) -> List[Dict]:
    """
    Fetch a static HTML page and extract clubs from tables, lists, or links.

    Retries up to MAX_RETRIES times on transient network errors (connection
    errors, timeouts, 5xx responses) using exponential backoff.

    Returns a list of raw club dicts (pre-normalization).
    """
    logger.info("Static scrape: %s", url)

    def _fetch() -> requests.Response:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if _is_retryable(exc):
                raise TransientError(str(exc)) from exc
            raise

    try:
        response = retry_with_backoff(
            _fetch,
            max_retries=MAX_RETRIES,
            base_delay=RETRY_BASE_DELAY_SECONDS,
            label=f"static:{url}",
        )
    except TransientError as exc:
        logger.error("Failed to fetch %s after retries (transient): %s", url, exc)
        raise
    except requests.RequestException as exc:
        logger.error("Failed to fetch %s: %s", url, exc)
        raise

    soup = BeautifulSoup(response.text, "lxml")

    for tag in soup.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()

    records = _extract_clubs_from_table(soup, url, league_name)
    if records:
        logger.info("Table extraction yielded %d clubs from %s", len(records), url)
        return records

    records = _extract_clubs_from_lists(soup, url, league_name)
    if records:
        logger.info("List extraction yielded %d clubs from %s", len(records), url)
        return records

    records = _extract_clubs_from_links(soup, url, league_name)
    logger.info("Link extraction yielded %d clubs from %s", len(records), url)
    return records
