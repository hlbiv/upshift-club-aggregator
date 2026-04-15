"""
tryouts_wordpress.py — Scrape club-website tryout announcements.

Target: the big long tail of WordPress-hosted club sites (the vast
majority of youth-soccer club sites are WordPress). We probe a small
set of common paths (``/tryouts/``, ``/register/``, etc.) and extract
best-effort structured data from whatever the page contains.

Rules:
  - Permissive input, STRICT validation. A row without a parseable
    date is dropped (and a WARNING is logged).
  - No Playwright — static HTML only. WordPress sites render server-side.
  - No partial writes: every row we emit has at least ``club_name_raw``
    and ``tryout_date``.

Output rows match the ``tryouts`` writer contract:

    {
        "club_name_raw": str,         # required
        "tryout_date":   datetime,    # required (first date if a range)
        "age_group":     str | None,  # "U12"
        "gender":        str | None,  # "M" | "F" | None
        "location":      str | None,  # becomes `location_name` at the writer
        "source_url":    str | None,
        "notes":         str | None,
    }
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}

# Common URL suffixes WordPress clubs put tryout info under.
_TRYOUT_PATHS = (
    "/tryouts/",
    "/tryouts",
    "/register/",
    "/registration/",
    "/join/",
    "/join",
)

_MONTHS = (
    "january|february|march|april|may|june|july|august|september|"
    "october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
)

# "August 5, 2026" or "Aug 5, 2026". Also handles ranges: "Aug 5-7, 2026"
# (we capture the first day and the year).
_DATE_MONTH_DAY_YEAR = re.compile(
    rf"\b(?P<month>{_MONTHS})\.?\s+(?P<day>\d{{1,2}})(?:\s*[-–]\s*\d{{1,2}})?,?\s+(?P<year>\d{{4}})\b",
    re.IGNORECASE,
)
# "8/5/26" or "08/05/2026"
_DATE_NUMERIC = re.compile(
    r"\b(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{2,4})\b"
)

_MONTH_TO_INT = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

_AGE_RE = re.compile(r"\b[Uu]-?(?P<age>\d{1,2})\b")
_BIRTH_YEAR_AGE_RE = re.compile(r"\b(?P<year>20\d{2})\s+(?P<g>Boys|Girls)\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b(?P<g>Boys|Girls|Coed|Co-ed)\b", re.IGNORECASE)


@dataclass
class WordPressClubSite:
    club_name_raw: str
    website: str


def parse_date(text: str) -> Optional[datetime]:
    """Parse the first recognizable date from a blob of text.

    Handles:
      - "August 5, 2026" / "Aug 5, 2026"
      - "August 5-7, 2026" (returns Aug 5)
      - "8/5/26" / "08/05/2026"

    Returns ``None`` if nothing parses.
    """
    if not text:
        return None
    m = _DATE_MONTH_DAY_YEAR.search(text)
    if m:
        month = _MONTH_TO_INT.get(m.group("month").lower())
        if month:
            try:
                return datetime(int(m.group("year")), month, int(m.group("day")))
            except ValueError:
                pass
    m = _DATE_NUMERIC.search(text)
    if m:
        year = int(m.group("y"))
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(m.group("m")), int(m.group("d")))
        except ValueError:
            return None
    return None


def parse_age_group(text: str) -> Optional[str]:
    """Return ``"U<n>"`` if the text mentions one, else None.

    Also recognizes birth-year form (``"2015 Boys"`` → ``"U11"`` relative
    to Aug 2026). We stick to the explicit ``U<n>`` match in the common
    path; the birth-year form requires knowing the current seasonal age
    cutoff which we don't thread here.
    """
    if not text:
        return None
    m = _AGE_RE.search(text)
    if m:
        return f"U{int(m.group('age'))}"
    return None


def parse_gender(text: str) -> Optional[str]:
    """Free-text → ``"M" | "F" | None``. Co-ed becomes None."""
    if not text:
        return None
    m = _GENDER_RE.search(text)
    if not m:
        return None
    g = m.group("g").lower()
    if g in ("boys", "boy"):
        return "M"
    if g in ("girls", "girl"):
        return "F"
    return None


def parse_location(soup: BeautifulSoup) -> Optional[str]:
    """Best-effort location extractor. Prefers <address>, then anything
    near a "Location:" label, then None.
    """
    addr = soup.find("address")
    if addr:
        text = addr.get_text(" ", strip=True)
        if text:
            return text
    # Look for a label.
    for tag in soup.find_all(["p", "li", "div", "h3", "h4"]):
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        low = txt.lower()
        if low.startswith("location") or low.startswith("where:"):
            # Strip the leading label.
            cleaned = re.sub(r"^(location|where)\s*[:\-]?\s*", "", txt, flags=re.IGNORECASE)
            return cleaned.strip() or None
    return None


def parse_tryouts_page_html(
    html: str,
    *,
    club_name_raw: str,
    source_url: str,
) -> List[Dict]:
    """Extract zero-or-more tryout rows from a single page's HTML.

    Pure function — fixture-driven. Only emits rows that have a
    parseable date; otherwise logs a WARNING.
    """
    soup = BeautifulSoup(html, "lxml")

    # Kill site chrome noise.
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    tryout_date = parse_date(text)
    if tryout_date is None:
        logger.warning(
            "[tryouts-wordpress] no date parsed for %s @ %s",
            club_name_raw, source_url,
        )
        return []

    age_group = parse_age_group(text)
    gender = parse_gender(text)
    location = parse_location(soup)

    return [{
        "club_name_raw": club_name_raw,
        "tryout_date": tryout_date,
        "age_group": age_group,
        "gender": gender,
        "location": location,
        "source_url": source_url,
        "notes": None,
    }]


def _fetch(url: str, timeout: int = 20) -> Optional[str]:
    """HEAD+GET with retry. Returns None on non-200 / network failure."""
    def _do() -> Optional[str]:
        r = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None
        return r.text
    try:
        return retry_with_backoff(
            _do,
            max_retries=1,
            base_delay=1.5,
            retryable_exceptions=(requests.exceptions.RequestException,),
            label=f"tryouts-wordpress:{url}",
        )
    except Exception as exc:
        logger.info("[tryouts-wordpress] fetch failed %s: %s", url, exc)
        return None


def scrape_tryouts_wordpress(club_sites: Iterable[Dict]) -> List[Dict]:
    """Walk each club's website probing the known tryout paths.

    ``club_sites`` iterable items are dicts with keys ``club_name_raw``
    and ``website`` (absolute URL, trailing slash optional). Stops
    probing paths for a given site on the first HTTP 200 that yields
    a parseable date.
    """
    rows: List[Dict] = []
    for entry in club_sites:
        club_name = (entry.get("club_name_raw") or "").strip()
        website = (entry.get("website") or "").strip()
        if not club_name or not website:
            continue
        base = website.rstrip("/")
        found_for_site = False
        for path in _TRYOUT_PATHS:
            if found_for_site:
                break
            url = urljoin(base + "/", path.lstrip("/"))
            html = _fetch(url)
            if not html:
                continue
            page_rows = parse_tryouts_page_html(
                html,
                club_name_raw=club_name,
                source_url=url,
            )
            if page_rows:
                rows.extend(page_rows)
                found_for_site = True
        if not found_for_site:
            logger.info(
                "[tryouts-wordpress] no tryout page found for %s (%s)",
                club_name, website,
            )
    logger.info("[tryouts-wordpress] produced %d tryout rows", len(rows))
    return rows
