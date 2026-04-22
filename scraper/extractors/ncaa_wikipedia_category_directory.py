"""
ncaa_wikipedia_category_directory.py — Seed ``colleges`` from MediaWiki
category pages for divisions where Wikipedia's ``List_of_...`` pages
don't exist.

D3 case
-------

``List_of_NCAA_Division_III_<gender>'s_soccer_programs`` pages now 404
— they existed at PR #189 merge time and have since been deleted or
moved (confirmed April 2026 during the multi-division diagnostic
session). Wikipedia opensearch returns no match for "List of Division
III ... soccer programs" today. The 154/154 rows already in ``colleges``
are frozen at whatever the first production run managed to seed.

Wikipedia's ``Category:NCAA_Division_III_men's_soccer_teams`` (and the
womens counterpart) returns 200 and is stable. Each category member is
a team's Wikipedia article title, e.g. ``"Stanford Cardinal men's
soccer"``. This module walks the MediaWiki API to get those titles and
upserts the school portion into ``colleges``.

Coverage is partial — only D3 schools with their own Wikipedia article.
Expect 60-80% of the ~400 D3 universe. The remaining long tail comes
in via the manual-entry workflow (PR #195 / #200).

Why a separate module (not extending ``ncaa_wikipedia_directory.py``)
--------------------------------------------------------------------

The access shape is fundamentally different:

- ``ncaa_wikipedia_directory.py`` fetches one HTML page per
  (division, gender), parses ``<table class="wikitable">`` rows.
- This module walks the MediaWiki API's
  ``action=query&list=categorymembers`` endpoint, gets JSON,
  parses article titles.

Forcing both into one module would muddy the parser. Sibling modules
match the pattern ``naia_directory.py`` already set for the NAIA
naia.org seed source.

Naming convention
-----------------

Seeded names include the team nickname, e.g. ``"Stanford Cardinal"``,
not ``"Stanford"``. Stripping nicknames reliably requires a per-school
map we don't have. Consequence: D3 rows seeded this way may not dedup
against D1/D2 rows of the same university, but D3 schools are rarely
multi-divisional so the collision rate is near zero in practice.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from typing import Iterable, List, Optional
from urllib.parse import quote

import requests

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402
from extractors.ncaa_directory import CollegeSeed, USER_AGENT, REQUEST_TIMEOUT  # noqa: E402

log = logging.getLogger("ncaa_wikipedia_category_directory")


# ---------------------------------------------------------------------------
# (division, gender) → category title
# ---------------------------------------------------------------------------

# Raw category titles (no "Category:" prefix; the API takes the "title"
# form with the prefix). Curly-apostrophe form ``'`` is what Wikipedia
# canonicalizes to in article titles and categories; the URL encoder
# handles %27 vs %E2%80%99 transparently via ``quote``.
#
# D2 / D1 / NAIA intentionally NOT listed here — they have working
# list-page or naia.org seeders. This module is specifically a
# fallback for divisions whose list page doesn't exist.
_CATEGORY_SOURCES: dict[tuple[str, str], str] = {
    ("D3", "mens"):   "Category:NCAA Division III men's soccer teams",
    ("D3", "womens"): "Category:NCAA Division III women's soccer teams",
}

_MEDIAWIKI_API = "https://en.wikipedia.org/w/api.php"

# Max allowed by MediaWiki for anonymous requests; plenty for a single
# category (D3 has < 500 teams).
_API_PAGE_SIZE = 500


def supported_divisions_categories() -> list[str]:
    """Return divisions this module knows how to seed via category walk."""
    return sorted({div for (div, _gender) in _CATEGORY_SOURCES})


def category_title(division: str, gender: str) -> str:
    """Return the MediaWiki category title for a (division, gender) pair."""
    key = (division, gender)
    if key not in _CATEGORY_SOURCES:
        raise ValueError(
            f"No Wikipedia category registered for ({division!r}, {gender!r}). "
            f"Supported divisions (category-based): {supported_divisions_categories()}"
        )
    return _CATEGORY_SOURCES[key]


# ---------------------------------------------------------------------------
# Article-title → school name parser
# ---------------------------------------------------------------------------

# Matches "<School [Nickname]> <gender>'s soccer" at the end of a title.
# Accepts curly (U+2019) or straight apostrophes. Case-insensitive on
# the sport phrase.
_MENS_SOCCER_SUFFIX_RE = re.compile(
    r"\s+men[’']s\s+soccer\s*$",
    re.IGNORECASE,
)
_WOMENS_SOCCER_SUFFIX_RE = re.compile(
    r"\s+women[’']s\s+soccer\s*$",
    re.IGNORECASE,
)


def _school_name_from_article_title(title: str, gender: str) -> Optional[str]:
    """Strip the sport suffix from a team-page title.

    ``"Stanford Cardinal men's soccer"`` → ``"Stanford Cardinal"``.
    Curly-apostrophe and straight-apostrophe variants both accepted.
    Case-insensitive on the sport word.

    Returns ``None`` if the suffix doesn't match the requested gender
    — guards against category noise (disambiguation articles, pages
    that happen to be in the category but aren't a team page).
    """
    if not title:
        return None
    if gender == "mens":
        primary = _MENS_SOCCER_SUFFIX_RE
        wrong = _WOMENS_SOCCER_SUFFIX_RE
    elif gender == "womens":
        primary = _WOMENS_SOCCER_SUFFIX_RE
        wrong = _MENS_SOCCER_SUFFIX_RE
    else:
        return None

    # Reject a wrong-gender match before accepting the primary. A
    # title like "Stanford Cardinal women's soccer" with gender="mens"
    # must return None, NOT accept by stripping the women's suffix.
    if wrong.search(title):
        return None
    m = primary.search(title)
    if not m:
        return None
    name = title[:m.start()].strip()
    if not name:
        return None
    return name


def parse_article_titles_to_seeds(
    titles: Iterable[str],
    division: str,
    gender: str,
) -> List[CollegeSeed]:
    """Map an iterable of article titles to ``CollegeSeed`` rows.

    Dedups by (name.lower(), gender) since the same school could
    theoretically appear twice via categorization quirks.
    """
    if division not in supported_divisions_categories():
        raise ValueError(f"unsupported division: {division!r}")
    if gender not in ("mens", "womens"):
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    seeds: List[CollegeSeed] = []
    seen: set = set()
    for title in titles:
        name = _school_name_from_article_title(title, gender)
        if not name:
            continue
        dedup_key = (name.lower(), gender)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        seeds.append(
            CollegeSeed(
                name=name,
                division=division,
                gender_program=gender,
            )
        )
    return seeds


# ---------------------------------------------------------------------------
# MediaWiki API walker
# ---------------------------------------------------------------------------


def fetch_category_members(
    category_title_str: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[str]:
    """Walk the MediaWiki API's categorymembers endpoint with pagination.

    Returns the raw list of member article titles (no "Category:"
    prefix — just the team pages). Each API request retries twice on
    transient HTTP errors.
    """
    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            }
        )

    titles: List[str] = []
    cmcontinue: Optional[str] = None
    label_base = f"wikipedia-category-{quote(category_title_str, safe='')[:40]}"

    try:
        while True:
            params: dict[str, str] = {
                "action": "query",
                "format": "json",
                "list": "categorymembers",
                "cmtitle": category_title_str,
                "cmtype": "page",
                "cmlimit": str(_API_PAGE_SIZE),
            }
            if cmcontinue:
                params["cmcontinue"] = cmcontinue

            def _do_fetch() -> requests.Response:
                resp = session.get(_MEDIAWIKI_API, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp

            response = retry_with_backoff(
                _do_fetch,
                max_retries=2,
                base_delay=2.0,
                retryable_exceptions=(requests.RequestException,),
                label=label_base,
            )
            payload = response.json()
            members = (payload.get("query") or {}).get("categorymembers") or []
            for m in members:
                t = m.get("title")
                if isinstance(t, str) and t:
                    titles.append(t)

            cont = payload.get("continue") or {}
            cmcontinue = cont.get("cmcontinue")
            if not cmcontinue:
                break
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    return titles


# ---------------------------------------------------------------------------
# Top-level orchestrator — matches ncaa_wikipedia_directory's signature
# ---------------------------------------------------------------------------


def fetch_division_programs(
    division: str,
    gender: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[CollegeSeed]:
    """Fetch + parse the MediaWiki category for one (division, gender).

    Signature-compatible with
    ``ncaa_wikipedia_directory.fetch_division_programs`` so callers can
    swap implementations via a source-key dispatch.
    """
    title = category_title(division, gender)
    raw_titles = fetch_category_members(title, session=session)
    seeds = parse_article_titles_to_seeds(raw_titles, division, gender)
    log.info(
        "[wikipedia-category] fetched %d %s %s programs (from %d raw titles)",
        len(seeds), division, gender, len(raw_titles),
    )
    return seeds
