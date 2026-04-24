"""
ncaa_enrich_websites.py — Fill ``colleges.website`` using each school's
``ncaa_id`` to fetch its stats.ncaa.org team page, then extract the
outbound athletics-site link.

Background
----------

Three prior sources leave D1 schools without a ``website``:
  * ``ncaa-seed-d1`` — seeds name / ncaa_id but NO website.
  * ``ncaa-resolve-urls-wikipedia`` — fills website from Wikipedia infoboxes
    (~50-60% coverage; remainder lack infoboxes or have wrong URLs).
  * ``ncaa-discover-urls-google`` — requires a Google CSE engine that can
    "Search the entire web"; engines created after Jan 20 2026 are
    permanently restricted to site-specific search only.

This module closes the gap by using the ``ncaa_id`` we already have stored.
Each school's stats.ncaa.org team page (static, server-side HTML) contains a
link back to the school's official athletics homepage.  Once ``website`` is
populated, the existing ``ncaa-resolve-urls`` SIDEARM probe fills in
``soccer_program_url``.

URL pattern
-----------

    https://stats.ncaa.org/team/<ncaa_id>

Fetching without a year_id follows a redirect to the team's current season
page, which contains school metadata including the outbound athletics link.

Outbound link detection
-----------------------

stats.ncaa.org team pages embed a link to the school's official site in the
page header / info panel.  We look for:
  1. An ``<a>`` whose href is an absolute URL ending in ``.edu`` or a
     known athletics sub-domain pattern — highest priority.
  2. An ``<a>`` inside an element with text "Official Site", "School Site",
     or similar.
  3. An ``<a>`` with ``rel="nofollow"`` pointing to a non-NCAA domain
     (stats.ncaa.org wraps outbound links this way in some skins).

Domains to skip outright: social media, wikipedia, ncaa.org itself,
espn, cbssports, etc.

Fallback / robustness
---------------------

If no outbound athletics link is found on the stats.ncaa.org page, the
function returns ``None`` and the caller logs a debug-level miss.  No writes
are made for misses — the row stays in the NULL pool for manual entry.

Usage (via run.py)
------------------

    python3 run.py --source ncaa-enrich-websites-ncaaid \\
        --division D1 [--limit 100] [--dry-run]

After this source runs, execute ``--source ncaa-resolve-urls`` to probe
SIDEARM on the newly-filled ``website`` rows.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from typing import Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_enrich_websites")

_REQUEST_TIMEOUT = 20
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_NCAA_BASE = "https://stats.ncaa.org"

# ---------------------------------------------------------------------------
# Domains we never want to write as a school's ``website``
# ---------------------------------------------------------------------------

_SKIP_DOMAINS = re.compile(
    r"(?:^|\.)(?:"
    r"ncaa|stats\.ncaa|espn|cbssports|foxsports|247sports|rivals|on3"
    r"|wikipedia|wikimedia|twitter|instagram|facebook|youtube|linkedin"
    r"|topdrawersoccer|fieldlevel|hudl|maxpreps|athletic\.net"
    r")\.(?:org|com|net)$",
    re.IGNORECASE,
)

# Path fragments that indicate a soccer-specific page rather than the
# athletics homepage.  We want the homepage here — soccer URLs come from
# the SIDEARM probe later.
_SOCCER_PATH_RE = re.compile(
    r"/sports/(?:mens?-?soccer|womens?-?soccer|[mw]soc|[mw]-soccer)"
    r"|/sports/soccer",
    re.IGNORECASE,
)


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _team_url(ncaa_id: str) -> str:
    return f"{_NCAA_BASE}/team/{ncaa_id}"


def _fetch_html(url: str, session: requests.Session) -> Optional[str]:
    def _do() -> requests.Response:
        return session.get(url, timeout=_REQUEST_TIMEOUT, allow_redirects=True)

    try:
        resp = retry_with_backoff(
            _do,
            max_retries=2,
            base_delay=2.0,
            retryable_exceptions=(requests.ConnectionError, requests.Timeout),
            label=f"ncaa-enrich-{url[-40:]}",
        )
    except Exception as exc:
        log.debug("[ncaa-enrich] network error fetching %s: %s", url, exc)
        return None

    if resp.status_code == 403:
        log.debug("[ncaa-enrich] 403 for %s (UA may need rotation)", url)
        return None
    if resp.status_code != 200:
        log.debug("[ncaa-enrich] HTTP %d for %s", resp.status_code, url)
        return None
    return resp.text


def _is_usable_athletics_url(url: str) -> bool:
    """Return True if ``url`` looks like a school athletics homepage."""
    if not url or not url.startswith("http"):
        return False
    parsed = urlparse(url.lower())
    host = parsed.netloc
    path = parsed.path

    # Never write social / media / aggregator domains.
    if _SKIP_DOMAINS.search(host):
        return False

    # Never write a direct soccer-sport path — we want the homepage,
    # not the soccer page; the SIDEARM probe handles that step.
    if _SOCCER_PATH_RE.search(path):
        return False

    return True


def extract_school_website(html: str, ncaa_id: str) -> Optional[str]:
    """Parse a stats.ncaa.org team page and return the school's athletics URL.

    Returns the best candidate URL, or ``None`` if nothing usable found.

    Priority order:
      1. Any absolute outbound link to a ``.edu`` domain (short path ≤ 2 segments).
      2. Any link in an element containing "official site", "school website",
         or "visit" anchor text.
      3. Any ``rel="nofollow"`` outbound link to a non-NCAA domain with a
         short path (stats.ncaa.org typically adds rel=nofollow on outbound
         links).
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[tuple[int, str]] = []  # (priority, url)

    for anchor in soup.find_all("a", href=True):
        href: str = anchor["href"]
        if not href.startswith("http"):
            continue
        if not _is_usable_athletics_url(href):
            continue

        parsed = urlparse(href.lower())
        host = parsed.netloc
        path = parsed.path
        path_depth = len([p for p in path.split("/") if p])
        link_text = anchor.get_text(strip=True).lower()

        # Priority 1: .edu short-path link
        if host.endswith(".edu") and path_depth <= 2:
            candidates.append((1, href))
            continue

        # Priority 2: anchor text hints at "official site" / school link
        if any(kw in link_text for kw in (
            "official site", "official website", "school website",
            "visit site", "team website",
        )):
            candidates.append((2, href))
            continue

        # Priority 3: rel=nofollow outbound with short path
        rels = anchor.get("rel") or []
        if "nofollow" in rels and path_depth <= 2:
            candidates.append((3, href))
            continue

        # Priority 4: any short-path outbound to a non-ncaa domain that
        # hasn't been rejected yet — e.g., "gostanford.com", "goduke.com"
        if path_depth <= 1 and "ncaa" not in host:
            candidates.append((4, href))

    if not candidates:
        log.debug("[ncaa-enrich] no outbound athletics link found for ncaa_id=%s", ncaa_id)
        return None

    candidates.sort(key=lambda x: x[0])
    best = candidates[0][1]
    log.debug("[ncaa-enrich] ncaa_id=%s → website=%s (priority=%d)", ncaa_id, best, candidates[0][0])
    return best
