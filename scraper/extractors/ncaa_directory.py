"""
ncaa_directory.py — Seed ``colleges`` from stats.ncaa.org's D1 team
directory + resolve ``colleges.soccer_program_url`` via SIDEARM probing.

PR-1 (merged) — ``CollegeSeed`` + ``parse_directory_html`` +
``fetch_d1_programs``: walks stats.ncaa.org's sport-code=MSO/WSO
listings and writes seed rows via ``ingest.ncaa_roster_writer.upsert_college``.

PR-2 (this extension) — ``compose_sidearm_roster_url`` +
``resolve_soccer_program_url``: given a school's ``website`` (athletics
site homepage), probe ``/sports/{mens,womens}-soccer/roster`` and
return the valid URL if the athletics site serves it. Pre-flight check
before this PR measured a 10/10 hit rate on the reference D1 sample
(Georgetown, UNC, UVA, Stanford, Indiana, Duke, Maryland, Notre Dame,
Creighton, Wake Forest), so the resolver is Try-1-only by design. The
fallback conditional branches were deliberately not written; if the
operator-observed miss rate climbs, a Try-2 strategy is a follow-up PR
against the same function.

URL format
----------

    https://stats.ncaa.org/team/inst_team_list?sport_code=MSO&division=1
    https://stats.ncaa.org/team/inst_team_list?sport_code=WSO&division=1

The page renders a single HTML ``<table>`` where each row is a D1
program. The program name is an ``<a href="/team/<org_id>/<year_id>">``
anchor in the first cell; conference (when shown) is in the second
cell.

Out of scope for this module
----------------------------

- State / city — stats.ncaa.org doesn't expose these on inst_team_list
- Non-D1 divisions — same parser extends trivially but out of scope
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass, field, asdict
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402

log = logging.getLogger("ncaa_directory")


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 20  # seconds

_SPORT_CODE = {"mens": "MSO", "womens": "WSO"}
_BASE_URL = "https://stats.ncaa.org/team/inst_team_list"
_TEAM_HREF_RE = re.compile(r"/team/(\d+)(?:/|$)")


@dataclass
class CollegeSeed:
    """One D1 program row scraped from stats.ncaa.org."""

    name: str
    division: str  # always "D1" for this module
    gender_program: str  # "mens" | "womens"
    ncaa_id: Optional[str] = None
    conference: Optional[str] = None
    state: Optional[str] = None  # not available from inst_team_list

    def to_upsert_row(self) -> dict:
        """Shape expected by ``ingest.ncaa_roster_writer.upsert_college``."""
        row = asdict(self)
        row["scrape_confidence"] = 0.9
        return row


def directory_url(gender: str) -> str:
    """Return the stats.ncaa.org D1 directory URL for ``gender``."""
    code = _SPORT_CODE.get(gender)
    if code is None:
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")
    return f"{_BASE_URL}?sport_code={code}&division=1"


def parse_directory_html(html: str, gender: str) -> List[CollegeSeed]:
    """Parse a stats.ncaa.org inst_team_list page into seed rows.

    Logic:
      - Walk every ``<a href="/team/<org_id>/...">`` anchor.
      - Skip ones with empty text (page navigation, sort links).
      - For each hit, find the enclosing ``<tr>``; the team name comes
        from the anchor, conference from the next ``<td>`` if present.
      - Dedup by (lowercased name, gender_program) in the rare case the
        page re-renders the same program twice (historical variant).
    """
    if gender not in ("mens", "womens"):
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    seeds: List[CollegeSeed] = []
    seen: set = set()

    for anchor in soup.find_all("a", href=_TEAM_HREF_RE):
        href = anchor.get("href") or ""
        match = _TEAM_HREF_RE.search(href)
        if not match:
            continue
        name = anchor.get_text(strip=True)
        if not name:
            continue
        ncaa_id = match.group(1)

        conference: Optional[str] = None
        tr = anchor.find_parent("tr")
        if tr is not None:
            tds = tr.find_all("td", recursive=False)
            if len(tds) >= 2:
                for td in tds[1:]:
                    text = re.sub(r"\s+", " ", td.get_text()).strip()
                    if text and text != name:
                        conference = text
                        break

        dedup_key = (name.lower(), gender)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        seeds.append(
            CollegeSeed(
                name=name,
                division="D1",
                gender_program=gender,
                ncaa_id=ncaa_id,
                conference=conference,
            )
        )

    return seeds


def fetch_d1_programs(
    gender: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[CollegeSeed]:
    """Fetch + parse the D1 directory page for the given gender.

    Retries once on transient errors (``requests.RequestException``).
    stats.ncaa.org sometimes 403s on unconfigured UAs; a realistic
    browser UA is set on the session.
    """
    url = directory_url(gender)
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*",
            }
        )

    try:
        def _do_fetch() -> requests.Response:
            resp = session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            return resp

        response = retry_with_backoff(
            _do_fetch,
            max_retries=2,
            base_delay=2.0,
            retryable_exceptions=(requests.RequestException,),
            label=f"ncaa-directory-{gender}",
        )
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    seeds = parse_directory_html(response.text, gender)
    log.info("[ncaa-directory] fetched %d %s D1 programs", len(seeds), gender)
    return seeds


# ---------------------------------------------------------------------------
# soccer_program_url resolver (PR-2)
# ---------------------------------------------------------------------------

_SIDEARM_PATH = {
    "mens": "/sports/mens-soccer/roster",
    "womens": "/sports/womens-soccer/roster",
}


def compose_sidearm_roster_url(website: str, gender_program: str) -> str:
    """Pure: return the conventional SIDEARM roster URL for a site + gender.

    The athletics-site `website` may include a scheme or a trailing slash
    or path; we normalize to the origin + canonical SIDEARM path. The
    result is *candidate*, not verified — ``resolve_soccer_program_url``
    is the function that probes it.
    """
    if not website:
        raise ValueError("website must be non-empty")
    if gender_program not in _SIDEARM_PATH:
        raise ValueError(
            f"gender_program must be 'mens' or 'womens' (got {gender_program!r})"
        )

    normalized = website.strip()
    if not re.match(r"^https?://", normalized, re.IGNORECASE):
        normalized = f"https://{normalized}"
    normalized = normalized.rstrip("/")
    # Strip any trailing path — we want scheme://host only
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(normalized)
    origin = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    return f"{origin}{_SIDEARM_PATH[gender_program]}"


def resolve_soccer_program_url(
    website: Optional[str],
    gender_program: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 10,
) -> Optional[str]:
    """Probe the SIDEARM roster URL for a college website.

    Pre-flight check on the reference D1 sample returned 10/10 hits, so
    this is intentionally Try-1-only: if the HEAD returns 200, return
    the candidate URL; any other status (404, 4xx, 5xx, redirect-away,
    or network error) returns ``None``. Missed rows are left for the
    operator to fill manually — the caller logs them.

    A ``HEAD`` request is cheaper than ``GET`` and sufficient: the
    athletics site returns a 200 for a valid roster path and a 404 for
    an invalid one. A handful of sites don't support HEAD cleanly; the
    caller treats connection errors the same as a 404 miss.
    """
    if not website:
        return None
    try:
        candidate = compose_sidearm_roster_url(website, gender_program)
    except ValueError:
        return None

    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*",
            }
        )

    try:
        try:
            resp = session.head(
                candidate, timeout=timeout, allow_redirects=True
            )
        except requests.RequestException as exc:
            log.debug("[ncaa-resolver] HEAD %s failed: %s", candidate, exc)
            return None

        if resp.status_code != 200:
            return None

        # Guard against a 200 that landed on the site's homepage after a
        # catch-all redirect. If the final URL doesn't still end with the
        # canonical path, treat it as a miss.
        final_url = resp.url or candidate
        if _SIDEARM_PATH[gender_program] not in final_url:
            return None

        return candidate
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass
