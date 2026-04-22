"""
ncaa_wikipedia_directory.py — Seed ``colleges`` from Wikipedia's D2/D3/NAIA/NJCAA
soccer-program list pages.

PR-1 (ncaa_directory.py) shipped a stats.ncaa.org-backed D1 seeder.
Stats.ncaa.org 403s our scraper for non-D1 divisions too, so for
D2/D3/NAIA we use Wikipedia's public
``List_of_NCAA_Division_*_soccer_programs`` / ``List_of_NAIA_*``
tables. Same ``CollegeSeed`` dataclass + ``upsert_college`` writer —
just a different source module.

D1 coverage: the canonical D1 seeder is still stats.ncaa.org
(richer conference metadata). Wikipedia is a **fallback** for D1
when stats.ncaa.org IP/UA-blocks us — confirmed-failing state as
of April 2026, manifesting as D1 mens seed coverage dropping to
~30% of the real universe. Operator runs
``--source ncaa-seed-wikipedia --division D1`` to top up.

Wikipedia's "List of ..." tables are consistently ``<table
class="wikitable">`` with a header row (``<th>Institution/School``,
``<th>Location``, ``<th>Conference``, ``<th>Nickname`` variants) and
one ``<tr>`` per program. The parser is header-aware: column
positions are inferred from ``<th>`` text, not hardcoded offsets, so
minor table-shape variance across division pages doesn't require
per-division parsers.

Out of scope
------------

- Conference linking — conference cell is captured but not resolved
  to a conference row in any other table. D1 seeder has the same
  behavior; this matches.
- Soccer-program URL resolution — same as D1, PR-2's
  ``ncaa-resolve-urls`` runs afterward per division.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import asdict
from typing import List, Optional
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup, Tag

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402
from extractors.ncaa_directory import CollegeSeed, USER_AGENT, REQUEST_TIMEOUT  # noqa: E402

log = logging.getLogger("ncaa_wikipedia_directory")


# ---------------------------------------------------------------------------
# Division → Wikipedia URL map
# ---------------------------------------------------------------------------

# Wikipedia URL format. Pages have separate men's and women's lists
# for NCAA divisions; NAIA has them too. Encoded apostrophe (%27)
# matches Wikipedia's canonical URL form.
_WIKIPEDIA_BASE = "https://en.wikipedia.org/wiki"

_DIVISION_SOURCES: dict[tuple[str, str], str] = {
    ("D1", "mens"):   f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_I_men%27s_soccer_programs",
    ("D1", "womens"): f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_I_women%27s_soccer_programs",
    ("D2", "mens"):   f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_II_men%27s_soccer_programs",
    ("D2", "womens"): f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_II_women%27s_soccer_programs",
    ("D3", "mens"):   f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_III_men%27s_soccer_programs",
    ("D3", "womens"): f"{_WIKIPEDIA_BASE}/List_of_NCAA_Division_III_women%27s_soccer_programs",
    ("NAIA", "mens"):   f"{_WIKIPEDIA_BASE}/List_of_NAIA_men%27s_soccer_programs",
    ("NAIA", "womens"): f"{_WIKIPEDIA_BASE}/List_of_NAIA_women%27s_soccer_programs",
    # D1 note: the canonical seed source for D1 is stats.ncaa.org
    # (``_handle_ncaa_seed_d1``), which carries richer conference
    # metadata. Wikipedia support is a fallback for when stats.ncaa.org
    # IP/UA-blocks the scraper — confirmed-failing state as of April
    # 2026. Operator runs ``--source ncaa-seed-wikipedia --division D1``
    # in that case. The D1 Wikipedia pages are just "List of NCAA
    # Division I ... soccer programs", same shape as D2/D3.
    # NJCAA Wikipedia coverage is fragmented (per-region pages, no
    # single consolidated "List of NJCAA ... soccer programs"). If
    # the operator wants NJCAA they'll need to supply a curated CSV
    # or ship a per-region parser as a follow-up. Intentionally not
    # listed here so the scraper fails loudly on request rather than
    # silently hitting a 404 page.
}


def supported_divisions() -> list[str]:
    """Return the list of division strings this module knows how to seed."""
    return sorted({div for (div, _gender) in _DIVISION_SOURCES})


def directory_url(division: str, gender: str) -> str:
    """Return the Wikipedia URL for a (division, gender) pair."""
    key = (division, gender)
    if key not in _DIVISION_SOURCES:
        raise ValueError(
            f"No Wikipedia source registered for ({division!r}, {gender!r}). "
            f"Supported divisions: {supported_divisions()}"
        )
    return _DIVISION_SOURCES[key]


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# Header keyword → canonical column key. Parser walks the `<th>` row
# and maps each header to one of these keys via case-insensitive
# substring match. Order matters: "Conference" before "State" so that
# a header reading "Conference (State)" maps to conference, not state.
_HEADER_KEYWORD_TO_KEY: list[tuple[str, str]] = [
    ("institution", "name"),
    ("school",      "name"),
    ("college",     "name"),
    ("university",  "name"),
    ("team",        "name"),
    ("program",     "name"),
    ("location",    "location"),
    ("city",        "location"),
    ("conference",  "conference"),
    ("primary",     "conference"),  # "Primary conference" on some pages
    ("state",       "state"),
]


def _detect_columns(header_cells: list[Tag]) -> dict[str, int]:
    """Given a list of ``<th>`` cells from the table header, return a
    mapping of canonical key → column index. Missing keys mean the
    table doesn't expose that field; the parser tolerates missing
    everything except ``name``.
    """
    columns: dict[str, int] = {}
    for idx, th in enumerate(header_cells):
        text = re.sub(r"\s+", " ", th.get_text()).strip().lower()
        for keyword, key in _HEADER_KEYWORD_TO_KEY:
            if keyword in text and key not in columns:
                columns[key] = idx
                break
    return columns


def _cell_plain_text(td: Tag) -> str:
    """Extract clean text from a table cell. Strips footnote markers
    (``[1]`` superscripts) that Wikipedia inserts on many rows."""
    # Remove <sup> footnote markers before extracting text
    for sup in td.find_all("sup"):
        sup.extract()
    return re.sub(r"\s+", " ", td.get_text()).strip()


def _name_from_cell(td: Tag) -> str:
    """School-name cell text, preferring the first ``<a>`` title (cleaner
    than the surrounding text that may include annotations). Falls back
    to raw cell text. Dewikified — URL-decoded apostrophes."""
    anchor = td.find("a")
    if anchor:
        name = anchor.get_text().strip()
        if name and len(name) >= 2:
            return unquote(name)
    return unquote(_cell_plain_text(td))


def _state_from_location(location: str) -> Optional[str]:
    """Pull a 2-letter state abbreviation out of a Wikipedia location
    cell like ``"Waco, Texas"`` or ``"Collegeville, Pennsylvania"``.
    Returns None if nothing recognizable is present.

    The list of US states matches Wikipedia's own spellings; we don't
    attempt territories (PR, Guam, etc.) because D2/D3 don't include
    any at the moment.
    """
    if not location:
        return None
    # Last token after a comma, lowercased, trimmed
    if "," in location:
        tail = location.rsplit(",", 1)[1].strip()
    else:
        tail = location.strip()
    tail = tail.split("[")[0].strip()  # strip any trailing footnote

    if len(tail) == 2 and tail.isalpha():
        return tail.upper()
    # Full state name → abbrev. Only a partial map — enough for the
    # overwhelming majority of D2/D3/NAIA programs.
    abbrev = _STATE_NAME_TO_ABBREV.get(tail.lower())
    return abbrev


_STATE_NAME_TO_ABBREV: dict[str, str] = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}


def parse_wikipedia_table(html: str, division: str, gender: str) -> List[CollegeSeed]:
    """Parse a Wikipedia "List of ... soccer programs" page into seed rows.

    Logic:
      - Find every ``<table class="wikitable">`` (most pages have just
        one; some have per-conference sub-tables that we process too).
      - For each table, detect column positions from the header row.
        Skip the table if it has no recognizable "name" column —
        probably a navigation or reference table, not the program list.
      - Iterate body rows; extract name + conference + state into
        ``CollegeSeed``.
      - Dedup by (name.lower(), gender). Some programs appear in
        multiple sub-tables on the same page; one row per program.
    """
    if division not in supported_divisions():
        raise ValueError(f"unsupported division: {division!r}")
    if gender not in ("mens", "womens"):
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    seeds: List[CollegeSeed] = []
    seen: set = set()

    for table in soup.select("table.wikitable"):
        header_row = table.find("tr")
        if header_row is None:
            continue
        header_cells = header_row.find_all(["th", "td"])
        columns = _detect_columns(header_cells)
        if "name" not in columns:
            # Not a program table — skip.
            continue
        name_col = columns["name"]
        conference_col = columns.get("conference")
        location_col = columns.get("location")
        state_col = columns.get("state")

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= name_col:
                continue
            name = _name_from_cell(cells[name_col])
            if not name or len(name) < 2:
                continue
            # Skip rows that are clearly footer / section-header artifacts:
            # bare numbers, column headers, "Total" rollups.
            if name.lower() in ("total", "totals", "name", "school", "institution"):
                continue
            if re.match(r"^\d+$", name):
                continue

            conference: Optional[str] = None
            if conference_col is not None and len(cells) > conference_col:
                conf_text = _cell_plain_text(cells[conference_col])
                conference = conf_text or None

            state: Optional[str] = None
            if state_col is not None and len(cells) > state_col:
                state = _state_from_location(_cell_plain_text(cells[state_col]))
            if state is None and location_col is not None and len(cells) > location_col:
                state = _state_from_location(_cell_plain_text(cells[location_col]))

            dedup_key = (name.lower(), gender)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            seeds.append(CollegeSeed(
                name=name,
                division=division,
                gender_program=gender,
                conference=conference,
                state=state,
            ))

    return seeds


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_division_programs(
    division: str,
    gender: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[CollegeSeed]:
    """Fetch + parse the Wikipedia program list for one (division, gender).

    Retries twice on transient errors (``requests.RequestException``).
    Wikipedia is generally forgiving on non-bot UAs and doesn't
    rate-limit single page fetches, but the retry + realistic UA is
    cheap insurance.
    """
    url = directory_url(division, gender)
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

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
            label=f"wikipedia-{division}-{gender}",
        )
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    seeds = parse_wikipedia_table(response.text, division, gender)
    log.info("[wikipedia] fetched %d %s %s programs", len(seeds), division, gender)
    return seeds
