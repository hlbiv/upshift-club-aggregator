"""
ncaa_discover_urls_ncsa.py — Fill ``colleges.soccer_program_url`` by scraping
the NCSA Sports college soccer directory pages.

Replaces / complements ``ncaa-discover-urls-google`` for installs where the
Google CSE engine cannot search the entire web (engines created after
Google's January 20 2026 policy change are permanently site-restricted).

How it works
------------

NCSA Sports maintains public, server-side-rendered directory pages at:

    https://www.ncsasports.org/womens-soccer/colleges
    https://www.ncsasports.org/mens-soccer/colleges

Each page lists every college soccer program NCSA tracks, grouped by
division (D1 / D2 / D3 / NAIA).  Clicking a school opens a profile page
whose canonical URL contains the NCAA-facing program URL in an outbound
"Official Website" or "Team Website" link.

Scraper strategy
----------------

1. Fetch the directory page (one per gender).
2. Parse the school list:  name, division label, profile URL.
3. For each school whose ``colleges.soccer_program_url IS NULL``,
   fetch its NCSA profile page.
4. Extract the outbound program/athletic-site link.
5. If the outbound link matches soccer-path patterns → write to
   ``colleges.soccer_program_url`` directly.
   If it looks like an athletics homepage → SIDEARM-probe it first;
   on success write both ``website`` + ``soccer_program_url``.

Robustness
----------

- Returns gracefully (with a clear log message) if the directory page
  404s or returns HTML that doesn't match the expected structure.  This
  handles NCSA changing their URL scheme without crashing the pipeline.
- Delays 1.5 s between profile fetches so we don't hammer NCSA.
- Respects ``--limit N`` so the operator can do incremental runs.

Usage
-----

    python3 run.py --source ncaa-discover-urls-ncsa \\
        --division D1 --gender womens [--limit 50] [--dry-run]

    python3 run.py --source ncaa-discover-urls-ncsa  # all divisions + genders
"""

from __future__ import annotations

import logging
import re
import sys
import os
from typing import List, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_discover_urls_ncsa")

_REQUEST_TIMEOUT = 20
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# NCSA directory URLs
# ---------------------------------------------------------------------------

_NCSA_BASE = "https://www.ncsasports.org"

_DIRECTORY_URLS: dict[str, str] = {
    "womens": f"{_NCSA_BASE}/womens-soccer/colleges",
    "mens":   f"{_NCSA_BASE}/mens-soccer/colleges",
}

# ---------------------------------------------------------------------------
# Soccer-program URL patterns (same as in ncaa_discover_urls_google)
# ---------------------------------------------------------------------------

_SOCCER_PATH_RE = re.compile(
    r"/sports/(?:mens?-?soccer|womens?-?soccer|[mw]soc|[mw]-soccer)"
    r"|/sports/soccer"
    r"|/sports/soccer-[mw]",
    re.IGNORECASE,
)

_SKIP_DOMAINS = re.compile(
    r"(?:^|\.)(?:wikipedia|topdrawersoccer|247sports|rivals|on3|fieldlevel"
    r"|ncaa|twitter|instagram|facebook|youtube|linkedin|hudl|ncsasports)\.(?:org|com)$",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Division label normalisation
# ---------------------------------------------------------------------------

_DIV_MAP: dict[str, str] = {
    "division i": "D1",
    "division 1": "D1",
    "ncaa division i": "D1",
    "ncaa d1": "D1",
    "d1": "D1",
    "division ii": "D2",
    "division 2": "D2",
    "ncaa division ii": "D2",
    "ncaa d2": "D2",
    "d2": "D2",
    "division iii": "D3",
    "division 3": "D3",
    "ncaa division iii": "D3",
    "ncaa d3": "D3",
    "d3": "D3",
    "naia": "NAIA",
    "njcaa": "NJCAA",
}


def _normalise_division(raw: str) -> Optional[str]:
    return _DIV_MAP.get(raw.strip().lower())


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class NcsaSchoolListing:
    __slots__ = ("name", "division", "profile_url")

    def __init__(self, name: str, division: Optional[str], profile_url: str) -> None:
        self.name = name
        self.division = division  # may be None if NCSA doesn't show it inline
        self.profile_url = profile_url


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _fetch_html(url: str, session: requests.Session) -> Optional[str]:
    """Fetch ``url``; return HTML text or ``None`` on non-200."""
    def _do() -> requests.Response:
        return session.get(url, timeout=_REQUEST_TIMEOUT, allow_redirects=True)

    try:
        resp = retry_with_backoff(
            _do,
            max_retries=2,
            base_delay=2.0,
            retryable_exceptions=(requests.ConnectionError, requests.Timeout),
            label=f"ncsa-fetch-{url[:60]}",
        )
    except Exception as exc:
        log.warning("[ncsa] network error fetching %s: %s", url, exc)
        return None

    if resp.status_code != 200:
        log.warning("[ncsa] HTTP %d for %s", resp.status_code, url)
        return None
    return resp.text


def parse_directory_page(html: str, gender: str) -> List[NcsaSchoolListing]:
    """Extract school listings from an NCSA college soccer directory page.

    NCSA's directory renders school cards or table rows.  We look for
    anchor tags whose href starts with /[mens|womens]-soccer/ and whose
    text looks like a school name (title-case, not a nav label).

    Falls back gracefully: if no school links are found the caller logs a
    warning and the caller skips to the next source.
    """
    soup = BeautifulSoup(html, "html.parser")
    listings: List[NcsaSchoolListing] = []
    seen: set[str] = set()

    # NCSA school profile links look like:
    #   /womens-soccer/colleges/stanford-university
    #   /mens-soccer/colleges/duke-university
    # They may also appear as /college-athletic-scholarships/soccer/...
    sport_slug = "womens-soccer" if gender == "womens" else "mens-soccer"
    href_pattern = re.compile(
        rf"/{re.escape(sport_slug)}/colleges/[a-z0-9-]+",
        re.IGNORECASE,
    )
    # Broader fallback in case URL structure differs:
    broader_pattern = re.compile(
        r"/(?:college-athletic-scholarships/soccer|[a-z-]+-soccer)/colleges/[a-z0-9-]+",
        re.IGNORECASE,
    )

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"]
        if not (href_pattern.match(href) or broader_pattern.match(href)):
            continue
        name = anchor.get_text(strip=True)
        # Skip nav labels and empty anchors
        if not name or len(name) < 3 or name.lower() in (
            "view all", "see all", "learn more", "apply", "home", "contact",
        ):
            continue
        # Build absolute URL
        full_url = urljoin(_NCSA_BASE, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        # Division is not reliably inline on the directory page.
        # We'll resolve it during DB matching.
        listings.append(NcsaSchoolListing(name=name, division=None, profile_url=full_url))

    log.info("[ncsa] parsed %d %s school listings from directory", len(listings), gender)
    return listings


def extract_program_url_from_profile(html: str) -> Optional[str]:
    """Extract the official athletics / soccer program URL from an NCSA profile page.

    NCSA profile pages typically contain outbound links labelled
    "Official Website", "Team Website", or similar.  We rank candidates:

      1. Any link whose URL contains a soccer-path pattern → direct hit.
      2. Any outbound link to a .edu domain or known athletics domain.
      3. Any link in an element labelled "website" / "official" / "athletics".

    Returns the best candidate URL or ``None``.
    """
    soup = BeautifulSoup(html, "html.parser")

    candidates: List[Tuple[int, str]] = []  # (priority, url)

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"]
        if not href.startswith("http"):
            continue
        parsed = urlparse(href.lower())
        host = parsed.netloc
        path = parsed.path

        # Never return NCSA's own domain or generic skip-list domains
        if _SKIP_DOMAINS.search(host):
            continue

        # Priority 1: soccer program path
        if _SOCCER_PATH_RE.search(path):
            candidates.append((1, href))
            continue

        # Priority 2: .edu domain
        if host.endswith(".edu"):
            candidates.append((2, href))
            continue

        # Priority 3: link text suggests "official" / "website" / "athletics"
        link_text = anchor.get_text(strip=True).lower()
        parent_text = ""
        parent = anchor.parent
        if parent:
            parent_text = parent.get_text(strip=True).lower()

        if any(kw in link_text or kw in parent_text for kw in (
            "official website", "team website", "athletics", "official site",
        )):
            candidates.append((3, href))

    if not candidates:
        return None

    # Return the highest-priority (lowest number) candidate.
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# ---------------------------------------------------------------------------
# URL classification (mirrors ncaa_discover_urls_google)
# ---------------------------------------------------------------------------


def classify_url(url: str) -> Optional[str]:
    """Return ``'soccer_program_url'``, ``'website'``, or ``None``."""
    if not url:
        return None
    parsed = urlparse(url.lower())
    host = parsed.netloc
    path = parsed.path

    if _SKIP_DOMAINS.search(host):
        return None

    if _SOCCER_PATH_RE.search(path):
        return "soccer_program_url"

    if host.endswith(".edu"):
        return "website"

    # Generic athletics-looking URL (short path, non-.edu)
    if path in ("", "/") or re.search(r"^/athletics/?$", path):
        return "website"

    return None


# ---------------------------------------------------------------------------
# Name fuzzy-matching
# ---------------------------------------------------------------------------


def _best_match(
    ncsa_name: str,
    candidates: List[Tuple[int, str, str, str]],  # (id, name, division, gender_program)
    *,
    threshold: int = 88,
) -> Optional[Tuple[int, str, str, str]]:
    """Return the best-matching college row for ``ncsa_name`` or ``None``."""
    best_score = 0
    best: Optional[Tuple[int, str, str, str]] = None
    for row in candidates:
        score = fuzz.token_set_ratio(ncsa_name.lower(), row[1].lower())
        if score > best_score:
            best_score = score
            best = row
    if best_score >= threshold:
        return best
    return None
