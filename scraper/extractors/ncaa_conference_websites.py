"""
ncaa_conference_websites.py — Fill ``colleges.website`` by scraping D2/D3
conference member-school directories.

Why this exists
---------------

stats.ncaa.org D2/D3 directory pages 403 from Replit, and Wikipedia's
``| website =`` infobox field is stale (schools migrate CMS platforms every
few years without updating their Wikipedia article). Conference websites are
maintained by the conference office and reflect the school's current
athletics URL.

Most D2/D3 conferences use SIDEARM Sports for their own conference site,
which means their member-school pages follow a consistent pattern:
a listing of schools each with a link to the school's athletics homepage.

Strategy
--------

1. Walk ``CONFERENCE_DIRECTORY_URLS`` — a curated mapping of
   ``conference_name → member_directory_page_url``.
2. Fetch each directory page and extract ``(school_name, athletics_url)``
   pairs via ``extract_member_schools()``.
3. Fuzzy-match each extracted school name to a ``colleges`` row using
   ``rapidfuzz.fuzz.token_set_ratio >= 88`` within the same division.
4. Write ``website`` for matched rows where it is currently NULL
   (never overwrites existing data).

Conference URL mapping
----------------------

Keys are normalised conference name strings as stored in
``colleges.conference``. Both full name and abbreviation are included for
coverage. If a conference's member page returns no usable links the handler
logs a warning and continues — a wrong URL fails gracefully.

Generic extraction
------------------

``extract_member_schools()`` looks for anchors on the page whose href:
  - Points to an external domain (not the conference site itself)
  - Is not a social-media or aggregator URL
  - Has a short path (≤ 2 segments) — we want athletics homepages, not
    sport-specific sub-pages

The anchor text (or its enclosing ``<td>`` / ``<li>`` text) is used as the
candidate school name passed to the fuzzy matcher.

If that yields < 3 pairs the function tries a second pass looking for any
external link in the page that isn't a social or NCAA link and whose
nearby heading / strong text looks like a school name.
"""

from __future__ import annotations

import logging
import re
import sys
import os
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_conference_websites")

_REQUEST_TIMEOUT = 20
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Conference → member directory URL mapping
# ---------------------------------------------------------------------------

# Keys: normalised conference name substrings (lowercased match via `in`).
# Values: URL of the conference's member school listing page.
#
# Many conference sites use SIDEARM Sports and share the /schools/ pattern.
# Those that don't are noted inline. Wrong/dead URLs fail gracefully
# (no results returned, logged at WARNING level).

CONFERENCE_DIRECTORY_URLS: dict[str, str] = {
    # ── D2 ──────────────────────────────────────────────────────────────
    "pennsylvania state athletic":  "https://psacsports.org/schools/",
    "psac":                         "https://psacsports.org/schools/",
    "mountain east":                "https://mecathletics.com/schools/",
    "mec":                          "https://mecathletics.com/schools/",
    "northern sun":                 "https://nsic.org/schools/",
    "nsic":                         "https://nsic.org/schools/",
    "great lakes valley":           "https://theglvc.com/schools/",
    "glvc":                         "https://theglvc.com/schools/",
    "gulf south":                   "https://gulfsouthconference.org/schools/",
    "gsc":                          "https://gulfsouthconference.org/schools/",
    "south atlantic":               "https://thesac.com/schools/",
    "california collegiate":        "https://goccaa.org/schools/",
    "ccaa":                         "https://goccaa.org/schools/",
    "sunshine state":               "https://sunshinestateconference.com/schools/",
    "ssc":                          "https://sunshinestateconference.com/schools/",
    "northeast-10":                 "https://ne10sports.com/schools/",
    "ne-10":                        "https://ne10sports.com/schools/",
    "ne10":                         "https://ne10sports.com/schools/",
    "rocky mountain":               "https://rmac-usa.com/schools/",
    "rmac":                         "https://rmac-usa.com/schools/",
    "peach belt":                   "https://peachbeltconference.com/schools/",
    "great lakes intercollegiate":  "https://gliac.org/schools/",
    "gliac":                        "https://gliac.org/schools/",
    "central atlantic":             "https://caccathletics.org/schools/",
    "cacc":                         "https://caccathletics.org/schools/",
    "east coast conference":        "https://eastcoastconference.com/schools/",
    "central intercollegiate":      "https://theciaa.com/schools/",
    "ciaa":                         "https://theciaa.com/schools/",
    "southern intercollegiate":     "https://thesinc.org/schools/",
    "siac":                         "https://thesinc.org/schools/",
    "great american":               "https://greatamericanconference.com/schools/",
    "great northwest":              "https://gnacsports.com/schools/",
    "gnac":                         "https://gnacsports.com/schools/",
    "lone star":                    "https://lonestarconference.org/schools/",
    "lsc":                          "https://lonestarconference.org/schools/",
    "mid-america intercollegiate":  "https://miaa.org/schools/",
    "miaa":                         "https://miaa.org/schools/",
    "heartland":                    "https://heartlandconference.org/schools/",
    "conference carolinas":         "https://conferencecarolinas.com/schools/",
    "carolinas":                    "https://conferencecarolinas.com/schools/",

    # ── D3 ──────────────────────────────────────────────────────────────
    "new england small college":    "https://nescac.com/schools/",
    "nescac":                       "https://nescac.com/schools/",
    "liberty league":               "https://libertyleagueathletics.com/schools/",
    "old dominion athletic":        "https://odacsports.com/schools/",
    "odac":                         "https://odacsports.com/schools/",
    "new jersey athletic":          "https://theNJAC.com/schools/",
    "njac":                         "https://theNJAC.com/schools/",
    "presidents' athletic":         "https://pacathletics.com/schools/",
    "presidents athletic":          "https://pacathletics.com/schools/",
    "centennial":                   "https://centennialconference.com/schools/",
    "empire 8":                     "https://empire8.com/schools/",
    "empire8":                      "https://empire8.com/schools/",
    "little east":                  "https://littleeastconference.com/schools/",
    "middle atlantic":              "https://macathletics.com/schools/",
    "new england women's":          "https://newlathletics.com/schools/",
    "commonwealth coast":           "https://commonwealthcoast.com/schools/",
    "american rivers":              "https://americanriversconference.com/schools/",
    "midwest conference":           "https://midwestconference.org/schools/",
    "upper midwest athletic":       "https://umac.org/schools/",
    "umac":                         "https://umac.org/schools/",
    "northern athletics":           "https://nacsports.org/schools/",
    "nacc":                         "https://nacsports.org/schools/",
    "southern athletic":            "https://southernathleticassociation.com/schools/",
    "saa":                          "https://southernathleticassociation.com/schools/",
    "usa south":                    "https://usasouth.net/schools/",
    "heartland collegiate":         "https://hcac.org/schools/",
    "hcac":                         "https://hcac.org/schools/",
    "michigan intercollegiate":     "https://miaa.org/schools/",
    "north coast athletic":         "https://ncac.com/schools/",
    "ncac":                         "https://ncac.com/schools/",
    "great south athletic":         "https://gsac.org/schools/",
    "gsac":                         "https://gsac.org/schools/",
    "st. louis intercollegiate":    "https://sliac.org/schools/",
    "sliac":                        "https://sliac.org/schools/",
    "northwestern":                 "https://nwcathletics.org/schools/",
    "nwc":                          "https://nwcathletics.org/schools/",
    "northeast conference":         "https://northeastconference.org/schools/",
    "colonial states":              "https://csacsports.com/schools/",
}

# Hosts to skip when extracting athletics links (social, aggregators, NCAA)
_SKIP_HOSTS = re.compile(
    r"(?:^|\.)(?:twitter|instagram|facebook|youtube|linkedin|tiktok"
    r"|topdrawersoccer|on3|247sports|rivals|ncaa|wikipedia|espn"
    r"|cbssports|foxsports|hudl|maxpreps)\.(?:org|com|net)$",
    re.IGNORECASE,
)

# Short text that isn't a school name
_JUNK_TEXT = re.compile(
    r"^(?:home|visit|website|official|site|athletics|sports|click here|more|"
    r"view|go|link|school|member|team|\s*)$",
    re.IGNORECASE,
)


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
    """Fetch URL; return HTML text or None on error."""
    def _do() -> requests.Response:
        return session.get(url, timeout=_REQUEST_TIMEOUT, allow_redirects=True)

    try:
        resp = retry_with_backoff(
            _do,
            max_retries=1,
            base_delay=2.0,
            retryable_exceptions=(requests.Timeout,),
            label=f"ncaa-conf-{url[-40:]}",
        )
    except requests.ConnectionError as exc:
        log.debug("[ncaa-conf] DNS/connection error for %s: %s", url, exc)
        return None
    except Exception as exc:
        log.debug("[ncaa-conf] error for %s: %s", url, exc)
        return None

    if resp.status_code in (403, 404, 429):
        log.debug("[ncaa-conf] HTTP %d for %s", resp.status_code, url)
        return None
    if resp.status_code != 200:
        log.debug("[ncaa-conf] HTTP %d for %s", resp.status_code, url)
        return None
    return resp.text


# ---------------------------------------------------------------------------
# Conference member page extraction
# ---------------------------------------------------------------------------


def _is_athletics_url(href: str, conference_host: str) -> bool:
    """Return True if href looks like a school athletics homepage."""
    if not href.startswith("http"):
        return False
    parsed = urlparse(href.lower())
    host = parsed.netloc
    path = parsed.path
    path_depth = len([p for p in path.split("/") if p])

    if conference_host and conference_host in host:
        return False
    if _SKIP_HOSTS.search(host):
        return False
    if path_depth > 2:
        return False
    return True


def extract_member_schools(html: str, conference_url: str) -> list[tuple[str, str]]:
    """Parse a conference member-schools page → list of (school_name, athletics_url).

    Tries two passes:
    1. Any anchor whose href is an external short-path URL; use anchor
       text or enclosing ``<td>``/``<li>`` text as the school name.
    2. If pass 1 yields < 3 pairs, scan for external links near headings
       (``<h3>``, ``<h4>``, ``<strong>``) — handles card-grid layouts.
    """
    soup = BeautifulSoup(html, "html.parser")
    conf_host = urlparse(conference_url).netloc.lower()
    results: list[tuple[str, str]] = []
    seen_hosts: set[str] = set()

    def _best_name(anchor) -> str:
        """Prefer anchor text; fall back to enclosing cell/list-item text."""
        name = anchor.get_text(strip=True)
        if len(name) > 3 and not _JUNK_TEXT.match(name):
            return name
        # Walk up to find a meaningful container text
        for parent_tag in ("td", "li", "div", "article"):
            parent = anchor.find_parent(parent_tag)
            if parent:
                t = parent.get_text(strip=True)
                if 3 < len(t) < 80 and not _JUNK_TEXT.match(t):
                    return t
        return name

    # Pass 1 — anchor-level scan
    for anchor in soup.find_all("a", href=True):
        raw = anchor["href"].strip()
        href = urljoin(conference_url, raw)
        if not _is_athletics_url(href, conf_host):
            continue
        host = urlparse(href).netloc.lower()
        if host in seen_hosts:
            continue
        name = _best_name(anchor)
        if len(name) > 3:
            seen_hosts.add(host)
            results.append((name, href))

    if len(results) >= 3:
        return results

    # Pass 2 — heading-adjacent scan for card layouts
    for heading in soup.find_all(["h2", "h3", "h4", "strong"]):
        text = heading.get_text(strip=True)
        if len(text) < 4 or len(text) > 80:
            continue
        # Find first external link sibling or child
        container = heading.find_parent(["div", "article", "li", "section"])
        if not container:
            continue
        for anchor in container.find_all("a", href=True):
            raw = anchor["href"].strip()
            href = urljoin(conference_url, raw)
            if not _is_athletics_url(href, conf_host):
                continue
            host = urlparse(href).netloc.lower()
            if host in seen_hosts:
                continue
            seen_hosts.add(host)
            results.append((text, href))
            break

    return results


# ---------------------------------------------------------------------------
# Conference name → directory URL resolver
# ---------------------------------------------------------------------------


def conference_directory_url(conference_name: str) -> Optional[str]:
    """Return the member-directory URL for a conference, or None if unknown."""
    if not conference_name:
        return None
    norm = conference_name.lower().strip()
    for key, url in CONFERENCE_DIRECTORY_URLS.items():
        if key in norm or norm in key:
            return url
    return None
