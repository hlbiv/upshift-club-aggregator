"""
naia_directory.py — Seed ``colleges`` from naia.org's 2021-22 soccer
teams index.

Wikipedia doesn't have consolidated "List of NAIA ... soccer programs"
pages (unlike NCAA D2/D3). naia.org is the authoritative source, but
their current-season list endpoint (``/sports/msoc/<current>/teams``)
broke after 2021-22 — subsequent seasons 302 to the first team detail
page instead of rendering the full list.

The 2021-22 endpoint still renders the complete index with one
``<a href="/sports/(m|w)soc/2021-22/teams/<slug>">NAME (STATE)</a>``
anchor per program. That's our seed source. NAIA program churn is
roughly ~5 programs/year, so a 2021-22 snapshot covers ~95% of
current (2025-26) membership; the kid flags any gaps during manual
data entry (PR #195 workflow).

Same ``CollegeSeed`` dataclass + ``upsert_college`` writer as the D1
(stats.ncaa.org) and D2/D3 (Wikipedia) seeders.

Out of scope
------------

- Conference data — naia.org's index page doesn't show it. Would
  require fetching each team detail page (217*2 = 434 extra requests).
  Defer to a follow-up if/when consumers need conference linking.
- ``soccer_program_url`` resolution — the existing
  ``--source ncaa-resolve-urls --division NAIA`` step handles it
  afterward, same as every other division.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import List, Optional

import requests
from bs4 import BeautifulSoup

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402
from extractors.ncaa_directory import CollegeSeed, USER_AGENT, REQUEST_TIMEOUT  # noqa: E402

log = logging.getLogger("naia_directory")


# ---------------------------------------------------------------------------
# Gender → URL map
# ---------------------------------------------------------------------------

# naia.org's 2021-22 is the last season with a rendered team index.
# Pinning the season here rather than the current one is deliberate —
# even though the listing is stale by ~4 seasons, it still covers the
# vast majority of current NAIA membership and *does* render, whereas
# later seasons don't.
_NAIA_SEASON = "2021-22"
_NAIA_BASE = "https://www.naia.org/sports"

_GENDER_SOURCES: dict[str, str] = {
    "mens":   f"{_NAIA_BASE}/msoc/{_NAIA_SEASON}/teams",
    "womens": f"{_NAIA_BASE}/wsoc/{_NAIA_SEASON}/teams",
}

# Team-link pattern. Matches both m-soc and w-soc, both the pinned
# 2021-22 season and (defensively) any future season naia.org starts
# rendering again. Captures the slug for dedup.
_TEAM_HREF_RE = re.compile(
    r"/sports/(?:m|w)soc/[\d]{4}-[\d]{2}/teams/([a-z0-9_-]+)"
)


def supported_genders() -> list[str]:
    return sorted(_GENDER_SOURCES.keys())


def directory_url(gender: str) -> str:
    if gender not in _GENDER_SOURCES:
        raise ValueError(
            f"gender must be 'mens' or 'womens' (got {gender!r})"
        )
    return _GENDER_SOURCES[gender]


# ---------------------------------------------------------------------------
# State abbreviation lookup
# ---------------------------------------------------------------------------

# naia.org uses abbreviated-with-period forms in the parenthetical
# ("Calif.", "Neb.", "Ariz."). Also supports 2-letter codes. The map
# below is keyed lowercased with trailing period stripped — the lookup
# function normalizes before matching.
_NAIA_STATE_ABBREV: dict[str, str] = {
    # 2-letter codes — passthrough via the isalpha/len==2 branch below
    # Abbreviated-with-period forms
    "ala": "AL",
    "alaska": "AK",
    "ariz": "AZ",
    "ark": "AR",
    "calif": "CA",
    "colo": "CO",
    "conn": "CT",
    "dc": "DC",
    "d.c": "DC",
    "del": "DE",
    "fla": "FL",
    "ga": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "ill": "IL",
    "ind": "IN",
    "iowa": "IA",
    "kan": "KS",
    "kans": "KS",
    "ky": "KY",
    "la": "LA",
    "maine": "ME",
    "md": "MD",
    "mass": "MA",
    "mich": "MI",
    "minn": "MN",
    "miss": "MS",
    "mo": "MO",
    "mont": "MT",
    "neb": "NE",
    "nev": "NV",
    "n.h": "NH",
    "n.j": "NJ",
    "n.m": "NM",
    "n.y": "NY",
    "n.c": "NC",
    "n.d": "ND",
    "ohio": "OH",
    "okla": "OK",
    "ore": "OR",
    "pa": "PA",
    "r.i": "RI",
    "s.c": "SC",
    "s.d": "SD",
    "tenn": "TN",
    "tex": "TX",
    "utah": "UT",
    "vt": "VT",
    "va": "VA",
    "wash": "WA",
    "w.va": "WV",
    "wis": "WI",
    "wyo": "WY",
    # Full-name spellings — rare on naia.org but inexpensive to support
    "alabama": "AL", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL",
    "georgia": "GA", "illinois": "IL", "indiana": "IN", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE",
    "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}


def _parse_state_parenthetical(raw: str) -> Optional[str]:
    """Convert a naia.org state parenthetical to a 2-letter state code.

    Accepts inputs like ``"Calif."``, ``"KS"``, ``"N.Y."``,
    ``"California"``, ``"Neb."``. Returns None if nothing recognizable.
    """
    if not raw:
        return None
    cleaned = raw.strip().rstrip(".").strip()
    # Two-letter passthrough: "KS", "NY"
    if len(cleaned) == 2 and cleaned.isalpha():
        return cleaned.upper()
    key = cleaned.lower()
    # Also try key with periods stripped ("N.Y." → "n.y" → "ny" key
    # isn't in the map; "n.y" is)
    return _NAIA_STATE_ABBREV.get(key) or _NAIA_STATE_ABBREV.get(
        key.replace(".", "")
    )


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# School-name cells look like "Antelope Valley (Calif.)" or
# "Benedictine (KS)" or "Aquinas" (no parenthetical). Parser splits
# on the final '(' to pull out the state; falls back to the raw text
# if no parens.
_NAME_STATE_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<state>[^()]+)\)\s*$")


def _name_and_state_from_anchor_text(text: str) -> tuple[str, Optional[str]]:
    """Split '<Name> (<State>)' anchor text into (name, state_or_None).

    Idempotent on inputs without parentheses. Aggressive on whitespace.
    """
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return "", None
    match = _NAME_STATE_RE.match(cleaned)
    if match:
        return match.group("name").strip(), _parse_state_parenthetical(
            match.group("state")
        )
    return cleaned, None


def parse_naia_index(html: str, gender: str) -> List[CollegeSeed]:
    """Parse a naia.org teams index HTML page into seed rows.

    Logic:
      - Walk every ``<a>`` whose ``href`` matches the per-team URL
        pattern (``/sports/(m|w)soc/<season>/teams/<slug>``).
      - Pull name + state from the anchor text ("Antelope Valley
        (Calif.)" → name="Antelope Valley", state="CA").
      - Dedup by (lowercased name, gender) — the index page
        sometimes repeats a program (e.g., alphabetical + conference
        indexes on the same page).
      - Skip empty / structural anchors.
    """
    if gender not in _GENDER_SOURCES:
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    seeds: List[CollegeSeed] = []
    seen: set = set()

    for anchor in soup.find_all("a", href=_TEAM_HREF_RE):
        href = anchor.get("href") or ""
        match = _TEAM_HREF_RE.search(href)
        if not match:
            continue
        raw_text = anchor.get_text()
        name, state = _name_and_state_from_anchor_text(raw_text)
        if not name or len(name) < 2:
            continue

        dedup_key = (name.lower(), gender)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        seeds.append(
            CollegeSeed(
                name=name,
                division="NAIA",
                gender_program=gender,
                state=state,
            )
        )

    return seeds


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_naia_programs(
    gender: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[CollegeSeed]:
    """Fetch + parse the naia.org index page for one gender.

    Retries once on transient errors. naia.org tolerates a realistic
    browser UA and doesn't rate-limit a single page fetch.
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
            label=f"naia-directory-{gender}",
        )
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    seeds = parse_naia_index(response.text, gender)
    log.info("[naia-directory] fetched %d NAIA %s programs", len(seeds), gender)
    return seeds
