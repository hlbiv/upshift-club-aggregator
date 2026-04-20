"""
ncaa_directory.py — Seed ``colleges`` from stats.ncaa.org's D1 team directory.

Solves the PR-1 gap from the NCAA enumeration plan: the bulk enumerator
``scrape_college_rosters()`` in ``ncaa_rosters.py`` iterates the
``colleges`` table, so an empty table means nothing to iterate. This
module walks stats.ncaa.org's sport-code=MSO/WSO D1 listings and writes
seed rows via ``ingest.ncaa_roster_writer.upsert_college`` (same natural
key ``colleges_name_division_gender_uq`` the single-school path uses —
idempotent across re-runs).

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

- ``soccer_program_url`` resolution (athletics-site roster URL) — PR-2
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
