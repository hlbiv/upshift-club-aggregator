"""
ncaa_discover_urls_google.py — Find ``colleges.soccer_program_url`` via
Google Custom Search Engine for schools that have no URL yet.

Complements ``ncaa-resolve-urls`` (requires ``website IS NOT NULL``) and
``ncaa-resolve-urls-wikipedia`` (requires a Wikipedia article with an
infobox ``| website = ...`` value). This module handles the residual tail
that neither approach reaches — schools with no Wikipedia article or whose
article lacks an infobox website.

Usage pattern
-------------

    python3 run.py --source ncaa-discover-urls-google \\
        --division D1 --gender womens --limit 50 [--dry-run]

Environment variables required
-------------------------------

    GOOGLE_CSE_API_KEY — Google Custom Search JSON API key.
    GOOGLE_CSE_CX      — Custom Search Engine ID (cx).

    The CSE should be configured as a general web search (not site-
    restricted) so queries like "Purdue Fort Wayne womens soccer roster"
    can surface both .edu athletics sites and third-party roster pages.
    Restrict to ``Search entire web`` in CSE settings.

Query strategy
--------------

Two-pass per school:

  Pass 1 — ``"<name> <state> womens soccer roster"``
    Targets the roster page directly. ~60-70% of schools have their SIDEARM
    or WMT roster indexed under this query.

  Pass 2 (fallback, only if pass 1 yields nothing) — ``"<name> athletics"``
    Broader query to find the athletics homepage (``website`` fill only).

Result classification
---------------------

For each result URL we score:

  - ``soccer_program_url`` candidate: URL path contains ``/sports/`` and
    any of ``soccer``, ``msoc``, ``wsoc``, ``m-soccer``, ``w-soccer``.
  - ``website`` candidate: URL has an ``.edu`` origin and path is short
    (no obvious sport path), or URL matches a known athletics-domain pattern.
  - Skipped: everything else (Wikipedia, social media, TDS/247sports pages
    that would confuse the SIDEARM probe downstream).

Rate limits
-----------

Google CSE free tier: 100 queries/day. Paid tier: $5 per 1000 queries.
For the expected 1,504-row gap that's ~$7.50 total. The handler defaults
to ``--limit 100`` per run and emits a cost estimate in the summary logline.

API shape
---------

    GET https://www.googleapis.com/customsearch/v1
        ?key=<GOOGLE_CSE_API_KEY>
        &cx=<GOOGLE_CSE_CX>
        &q=<query>
        &num=3          # top 3 results per query; rarely need more

Response: JSON with ``items[].link`` for hit URLs, ``items[].title``.
On quota exhaustion the API returns HTTP 429; the handler surfaces it
as a warning and stops cleanly.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse, quote_plus

import requests

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_discover_urls_google")

_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"
_REQUEST_TIMEOUT = 15

# Patterns that indicate a URL is a direct soccer program / roster page.
# Checked against the URL path (lowercased).
_SOCCER_PATH_PATTERNS = re.compile(
    r"/sports/(?:mens?-?soccer|womens?-?soccer|[mw]soc|[mw]-soccer)"
    r"|/sports/soccer"          # combined gender page (Purdue, Nebraska)
    r"|/sports/soccer-[mw]",    # rare reversed form
    re.IGNORECASE,
)

# Domains that are recognised athletics homepages (not sport-specific URLs).
_KNOWN_ATHLETICS_DOMAINS = re.compile(
    r"(?:^|\.)(?:gostanford|goaztecs|hokiesports|gopsusports|goduke"
    r"|tarheelblue|virginiasports|ndathletics|creightonbluejays"
    r"|wakeforest|gauchos|seahawks|goheels|georgetownhoyas"
    r"|minutemenandwomen|ukathletics|und\.com|texassports"
    r"|huskers|msuspartans|michiganwolverines)\.com$",
    re.IGNORECASE,
)

# Domains to skip outright — not athletics sites.
_SKIP_DOMAINS = re.compile(
    r"(?:^|\.)(?:wikipedia|topdrawersoccer|247sports|rivals|on3|fieldlevel"
    r"|ncaa|twitter|instagram|facebook|youtube|linkedin|hudl)\.(?:org|com)$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Query builders
# ---------------------------------------------------------------------------


def _pass1_query(school_name: str, state: Optional[str], gender: str) -> str:
    """Direct roster-page query."""
    g = "mens" if gender == "mens" else "womens"
    location = f" {state}" if state else ""
    return f"{school_name}{location} {g} soccer roster"


def _pass2_query(school_name: str, state: Optional[str]) -> str:
    """Athletics-homepage fallback query."""
    location = f" {state}" if state else ""
    return f"{school_name}{location} athletics"


# ---------------------------------------------------------------------------
# Result classification
# ---------------------------------------------------------------------------


def classify_url(url: str, gender: str) -> Optional[str]:
    """Return ``'soccer_program_url'``, ``'website'``, or ``None`` (skip).

    ``'soccer_program_url'`` — the URL looks like a direct soccer roster page.
    ``'website'`` — the URL looks like an athletics homepage.
    ``None`` — not useful for our purposes (Wikipedia, social, TDS, etc.).
    """
    if not url:
        return None
    parsed = urlparse(url.lower())
    host = parsed.netloc
    path = parsed.path

    # Reject known non-athletics domains immediately.
    if _SKIP_DOMAINS.search(host):
        return None

    # A URL with a soccer program path is a direct hit.
    if _SOCCER_PATH_PATTERNS.search(path):
        return "soccer_program_url"

    # .edu domain with short path or root → athletics homepage.
    if host.endswith(".edu"):
        if not path or path == "/" or path.count("/") <= 1:
            return "website"
        # .edu path that includes "athletics" or "sports" in a short segment
        if re.search(r"^/(?:athletics|sports)/?$", path):
            return "website"
        return None

    # Known athletics domains without a sport path → website.
    if _KNOWN_ATHLETICS_DOMAINS.search(host):
        if not _SOCCER_PATH_PATTERNS.search(path):
            return "website"

    return None


# ---------------------------------------------------------------------------
# Google CSE fetch
# ---------------------------------------------------------------------------


def _search(
    query: str,
    *,
    api_key: str,
    cx: str,
    num: int = 3,
    session: Optional[requests.Session] = None,
) -> List[dict]:
    """Call Google CSE JSON API; return ``items`` list (may be empty).

    Returns ``[]`` on any non-200 response (including 429 quota exhaustion).
    Caller inspects ``[]`` and can surface the issue.

    Raises ``requests.RequestException`` on network failure; ``retry_with_backoff``
    in the caller handles transient errors.
    """
    own_session = session is None
    if own_session:
        session = requests.Session()
    try:
        def _do_call() -> requests.Response:
            resp = session.get(
                _CSE_ENDPOINT,
                params={
                    "key": api_key,
                    "cx": cx,
                    "q": query,
                    "num": str(num),
                    "fields": "items(link,title)",
                },
                timeout=_REQUEST_TIMEOUT,
            )
            return resp

        resp = retry_with_backoff(
            _do_call,
            max_retries=2,
            base_delay=2.0,
            retryable_exceptions=(requests.ConnectionError, requests.Timeout),
            label=f"google-cse-{query[:40]}",
        )
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    if resp.status_code == 429:
        log.warning("[google-cse] 429 quota exhausted — stopping")
        raise _QuotaExhausted()
    if resp.status_code != 200:
        log.warning("[google-cse] non-200 %d for query %r", resp.status_code, query)
        return []

    return resp.json().get("items") or []


class _QuotaExhausted(Exception):
    pass


# ---------------------------------------------------------------------------
# High-level discovery
# ---------------------------------------------------------------------------


def discover_soccer_url(
    school_name: str,
    state: Optional[str],
    gender: str,
    *,
    api_key: str,
    cx: str,
    session: Optional[requests.Session] = None,
) -> Optional[Tuple[str, str]]:
    """Return ``(url, kind)`` or ``None`` for a single school.

    ``kind`` is ``'soccer_program_url'`` or ``'website'``.

    Two passes: pass 1 targets the roster page directly; pass 2 (fallback)
    targets the athletics homepage when pass 1 finds nothing classifiable.
    """
    # Pass 1 — direct roster query
    q1 = _pass1_query(school_name, state, gender)
    try:
        items = _search(q1, api_key=api_key, cx=cx, session=session)
    except _QuotaExhausted:
        raise
    except Exception as exc:
        log.warning("[google-cse] pass 1 search error for %r: %s", school_name, exc)
        items = []

    for item in items:
        link = item.get("link") or ""
        kind = classify_url(link, gender)
        if kind:
            log.debug(
                "[google-cse] pass-1 hit for %s: %s → %s", school_name, link, kind
            )
            return (link, kind)

    # Pass 2 — athletics homepage fallback
    q2 = _pass2_query(school_name, state)
    try:
        items2 = _search(q2, api_key=api_key, cx=cx, session=session)
    except _QuotaExhausted:
        raise
    except Exception as exc:
        log.warning("[google-cse] pass 2 search error for %r: %s", school_name, exc)
        return None

    for item in items2:
        link = item.get("link") or ""
        kind = classify_url(link, gender)
        if kind == "website":
            log.debug(
                "[google-cse] pass-2 website hit for %s: %s", school_name, link
            )
            return (link, kind)

    return None
