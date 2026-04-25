"""
ncaa_crawl_athletics.py — Find ``colleges.soccer_program_url`` by fetching
the athletics homepage stored in ``colleges.website`` and extracting soccer
links from the page HTML.

Why this exists
---------------

``ncaa-resolve-urls`` probes well-known SIDEARM / PrestoSports URL paths
(``/sports/wsoc/roster`` etc.) via HEAD requests.  That covers ~60-70% of
D1 schools but fails for D2/D3 because those divisions use a wide variety
of CMS platforms (BlueStar, AthleticNet, FinalForms, custom `.edu/athletics`
pages, etc.) with no predictable URL structure.

This module takes a different approach: GET the athletics homepage and look
for any anchor whose href contains soccer-related keywords.  Works for any
CMS because it reads what the site actually links to rather than guessing
path patterns.

Strategy
--------

1. Fetch the ``website`` URL (full GET, with redirects).
2. Parse HTML with BeautifulSoup; collect all ``<a href="…">`` anchors.
3. Score each anchor by:
     - Does the href path contain "soccer", "wsoc", "msoc", "w-soccer",
       "m-soccer", "womens-soccer", "mens-soccer"?  → hard hit.
     - Does the anchor text (or its parent element text) contain "soccer"?
       → soft hit (lower confidence, logged but still returned if it's the
       only candidate).
4. From the hard-hit candidates, prefer gender-matching paths (for a womens
   program, prefer "wsoc" or "womens-soccer" over "msoc").
5. Return the best candidate URL, or ``None`` if nothing soccer-related found.

The caller (run.py handler) then writes the URL to
``colleges.soccer_program_url`` and commits.

No SIDEARM probe is run by this module — the URL discovered is what the site
actually uses, not a guessed path.
"""

from __future__ import annotations

import logging
import re
import sys
import os
from typing import List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_crawl_athletics")

_REQUEST_TIMEOUT = 20
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Soccer keyword patterns
# ---------------------------------------------------------------------------

# Path-level patterns — high confidence: URL itself contains sport slug.
_SOCCER_PATH_RE = re.compile(
    r"/(?:sports?/)?(?:womens?[-_]soccer|mens?[-_]soccer|[wm]-soccer"
    r"|wsoc|msoc|w_soc|m_soc|soccer)",
    re.IGNORECASE,
)

# Exclude social / aggregator URLs that happen to mention "soccer".
_SKIP_HOSTS = re.compile(
    r"(?:^|\.)(?:twitter|instagram|facebook|youtube|linkedin|tiktok"
    r"|topdrawersoccer|on3|247sports|rivals|ncaa|wikipedia)\.(?:org|com)$",
    re.IGNORECASE,
)

# Gender preference scoring: womens paths score higher for womens programs.
_WOMENS_SIGNALS = re.compile(r"(?:womens?|wsoc|w[-_]soccer|w-soc)", re.IGNORECASE)
_MENS_SIGNALS   = re.compile(r"(?<![wo])(?:mens?|msoc|m[-_]soccer|m-soc)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def fetch_html(url: str, session: requests.Session) -> Optional[str]:
    """Fetch URL; return HTML text or None on non-200 / network error."""
    def _do() -> requests.Response:
        return session.get(url, timeout=_REQUEST_TIMEOUT, allow_redirects=True)

    try:
        resp = retry_with_backoff(
            _do,
            max_retries=2,
            base_delay=2.0,
            retryable_exceptions=(requests.ConnectionError, requests.Timeout),
            label=f"ncaa-crawl-{url[-50:]}",
        )
    except Exception as exc:
        log.debug("[ncaa-crawl] network error for %s: %s", url, exc)
        return None

    if resp.status_code in (403, 429):
        log.debug("[ncaa-crawl] HTTP %d for %s", resp.status_code, url)
        return None
    if resp.status_code != 200:
        log.debug("[ncaa-crawl] HTTP %d for %s", resp.status_code, url)
        return None
    return resp.text


# ---------------------------------------------------------------------------
# Soccer link extraction
# ---------------------------------------------------------------------------


def _score_anchor(href: str, text: str, gender: str) -> int:
    """Return a score for a candidate anchor (higher = better match).

    0  = not a soccer link
    1  = anchor text mentions soccer (soft)
    10 = href path matches soccer pattern (hard)
    +5 = gender-matched (href signals correct gender)
    """
    parsed = urlparse(href)
    host = parsed.netloc.lower()
    path = parsed.path

    if _SKIP_HOSTS.search(host):
        return 0

    # Hard hit: soccer in path
    if _SOCCER_PATH_RE.search(path):
        score = 10
        if gender == "womens" and _WOMENS_SIGNALS.search(path):
            score += 5
        elif gender == "mens" and _MENS_SIGNALS.search(path):
            score += 5
        # Small penalty for combined gender pages so gendered pages win
        if re.search(r"(?<![wm])/soccer(?!/[wm])", path, re.IGNORECASE):
            score -= 1
        return score

    # Soft hit: anchor text mentions soccer
    if re.search(r"\bsoccer\b", text, re.IGNORECASE):
        return 1

    return 0


def find_soccer_url(
    html: str,
    base_url: str,
    gender: str,
) -> Optional[str]:
    """Return the best soccer-program URL found in the page, or None.

    ``base_url`` is used to resolve relative hrefs.  ``gender`` is
    ``'mens'`` or ``'womens'`` and is used to prefer gender-specific links.
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates: List[Tuple[int, str]] = []

    for anchor in soup.find_all("a", href=True):
        raw_href: str = anchor["href"].strip()
        if not raw_href or raw_href.startswith(("#", "javascript:", "mailto:")):
            continue

        # Build absolute URL
        href = urljoin(base_url, raw_href)
        parsed = urlparse(href)
        if parsed.scheme not in ("http", "https"):
            continue

        text = anchor.get_text(strip=True)
        score = _score_anchor(href, text, gender)
        if score > 0:
            candidates.append((score, href))

    if not candidates:
        return None

    # Return the highest-scored candidate (stable sort preserves page order
    # as tiebreaker so first-in-page wins among equals).
    candidates.sort(key=lambda x: -x[0])
    best_score, best_url = candidates[0]

    log.debug(
        "[ncaa-crawl] best candidate score=%d url=%s  (total %d candidates)",
        best_score, best_url, len(candidates),
    )

    # Soft-hit-only (score=1) is too noisy — require at least a path hit.
    if best_score < 10:
        log.debug("[ncaa-crawl] no hard soccer-path hit — skipping (score=%d)", best_score)
        return None

    return best_url
