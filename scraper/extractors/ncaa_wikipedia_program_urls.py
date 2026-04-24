"""
ncaa_wikipedia_program_urls.py — Discover ``colleges.soccer_program_url``
for D1/D2 NCAA programs by walking each program's own Wikipedia article.

Why this module exists
----------------------

The 2026-04-22 production measurement
(``scraper/notes/inline_coach_production_measure.md``) showed that the
inline head-coach extractor now hits 80–95% of-URL for every NCAA cell,
but ``colleges.soccer_program_url`` is populated for only ~22-24% of
D1/D2 rows in both genders. URL coverage — not parsing quality — is now
the binding constraint on overall head-coach coverage.

The existing ``ncaa-resolve-urls`` handler probes SIDEARM paths starting
from ``colleges.website``, but the Wikipedia + stats.ncaa.org seeders
that populate D1/D2 don't write a ``website`` column, so the resolver
skips ~76% of those rows (its ``WHERE website IS NOT NULL`` guard).

This module closes that gap by going one hop further into Wikipedia:

  1. Walk the same ``List_of_NCAA_Division_*_<gender>'s_soccer_programs``
     pages used by ``ncaa_wikipedia_directory.py`` — but capture the
     per-row article href as well as the school name.
  2. Batch-fetch each program article's wikitext via the MediaWiki API
     and pull the ``| website = ...`` infobox field.
  3. Hand the discovered website to the existing
     ``ncaa_directory.resolve_soccer_program_url`` to probe the SIDEARM
     paths and verify the roster URL.

The same Wikipedia category fallback used for D3 (``Category:NCAA
Division III men's soccer teams`` etc.) is **not** needed here — D1/D2
have working ``List_of_...`` pages. D3 already has ~65% URL coverage
via that category seeder; this module is scoped to D1 and D2 where
URL coverage sits at ~22-24%.

Out of scope
------------

- Updating ``colleges.website`` is a side benefit (the run handler
  backfills it when found), but the primary write is
  ``soccer_program_url``.
- Schools without a dedicated Wikipedia article (no ``<a href="/wiki/
  ...">`` in their list-page row) cannot be resolved here. Those still
  fall through to manual fill / future enrichment passes.
- D1 schools whose ``colleges.name`` was seeded from stats.ncaa.org
  may not match the name spelled on the Wikipedia list page (e.g.
  "USC" vs "USC Trojans"). Name matching uses a normalized lowercase
  comparison plus a small alias map; misses are logged for operator
  review and don't block other rows.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup, Tag

_EXTRACTORS_DIR = os.path.dirname(os.path.abspath(__file__))
_SCRAPER_ROOT = os.path.dirname(_EXTRACTORS_DIR)
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from utils.retry import retry_with_backoff  # noqa: E402
from extractors.ncaa_directory import (  # noqa: E402
    USER_AGENT,
    REQUEST_TIMEOUT,
    resolve_soccer_program_url,
)
from extractors.ncaa_wikipedia_directory import (  # noqa: E402
    directory_url as _list_directory_url,
    supported_divisions as _list_supported_divisions,
    _cell_plain_text,
    _detect_columns,
)

log = logging.getLogger("ncaa_wikipedia_program_urls")


# Divisions this module is scoped to. D3 has its own (working) URL
# coverage path via the category seeder; D1/D2 are the cells where URL
# coverage is the binding constraint per
# ``inline_coach_production_measure.md``.
_SUPPORTED_DIVISIONS: tuple[str, ...] = ("D1", "D2")

# MediaWiki API endpoint and per-request batching limit. ``titles=`` on
# ``action=query`` accepts up to 50 titles per request for anonymous
# clients, so we batch in chunks of 50.
_MEDIAWIKI_API = "https://en.wikipedia.org/w/api.php"
_API_TITLE_BATCH = 50


def supported_divisions() -> list[str]:
    """Return the divisions this module knows how to discover URLs for."""
    return list(_SUPPORTED_DIVISIONS)


# ---------------------------------------------------------------------------
# List-page → (school_name, article_title) parser
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProgramArticleRef:
    """One program parsed from a Wikipedia list page.

    ``article_title`` is the MediaWiki-canonical page title (no /wiki/
    prefix, spaces instead of underscores, percent-decoded).
    """

    school_name: str
    article_title: str


_WIKI_HREF_RE = re.compile(r"^/wiki/(?!File:|Category:|Help:|Special:)([^#?]+)")


def _article_title_from_href(href: str) -> Optional[str]:
    """Convert an internal ``/wiki/Foo_Bar`` href to a MediaWiki title.

    Returns ``None`` for external hrefs, special-namespace hrefs, or
    redlinks (``index.php?title=...&action=edit&redlink=1``) — those
    are pages that don't exist and have no infobox to read.
    """
    if not href:
        return None
    m = _WIKI_HREF_RE.match(href)
    if not m:
        return None
    raw = m.group(1)
    # Reject redlink-style hrefs disguised as /wiki/... (rare but
    # observed when an editor manually writes a stub link).
    if "action=edit" in raw or "redlink=1" in raw:
        return None
    title = unquote(raw).replace("_", " ").strip()
    if not title:
        return None
    return title


def parse_program_articles(html: str, division: str, gender: str) -> List[ProgramArticleRef]:
    """Parse a Wikipedia "List of ... soccer programs" page into article refs.

    Mirrors ``ncaa_wikipedia_directory.parse_wikipedia_table`` row
    detection but additionally captures the per-row article href so
    each program can be looked up on its own Wikipedia page.

    Rows whose name cell has no ``<a href="/wiki/...">`` link are
    skipped — those programs don't have their own Wikipedia article and
    therefore have no infobox we can read.
    """
    if division not in _SUPPORTED_DIVISIONS:
        raise ValueError(
            f"unsupported division: {division!r} (supported: {supported_divisions()})"
        )
    if gender not in ("mens", "womens"):
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    refs: List[ProgramArticleRef] = []
    seen: set = set()

    for table in soup.select("table.wikitable"):
        header_row = table.find("tr")
        if header_row is None:
            continue
        header_cells = header_row.find_all(["th", "td"])
        columns = _detect_columns(header_cells)
        if "name" not in columns:
            continue
        name_col = columns["name"]

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= name_col:
                continue
            name_cell = cells[name_col]
            anchor: Optional[Tag] = name_cell.find("a")
            if anchor is None:
                continue
            href = anchor.get("href") or ""
            article_title = _article_title_from_href(href)
            if not article_title:
                continue
            name = anchor.get_text().strip()
            if not name:
                # Fallback to cell text if anchor is image-only
                name = _cell_plain_text(name_cell)
            if not name or len(name) < 2:
                continue
            if name.lower() in ("total", "totals", "name", "school", "institution"):
                continue
            if re.match(r"^\d+$", name):
                continue

            dedup_key = (name.lower(), gender)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            refs.append(ProgramArticleRef(school_name=name, article_title=article_title))

    return refs


def fetch_program_articles(
    division: str,
    gender: str,
    *,
    session: Optional[requests.Session] = None,
) -> List[ProgramArticleRef]:
    """Fetch the Wikipedia list page for one (division, gender) and parse refs.

    Uses ``ncaa_wikipedia_directory.directory_url`` so the canonical
    URL set stays single-source-of-truth across both modules.
    """
    if division not in _SUPPORTED_DIVISIONS:
        raise ValueError(
            f"unsupported division: {division!r} (supported: {supported_divisions()})"
        )
    if division not in _list_supported_divisions():
        raise ValueError(
            f"upstream ncaa_wikipedia_directory has no URL for {division!r}"
        )

    url = _list_directory_url(division, gender)
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
            label=f"wikipedia-program-urls-{division}-{gender}",
        )
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    refs = parse_program_articles(response.text, division, gender)
    log.info(
        "[wikipedia-program-urls] %s %s: parsed %d article refs",
        division, gender, len(refs),
    )
    return refs


# ---------------------------------------------------------------------------
# Infobox website extraction
# ---------------------------------------------------------------------------


# Matches ``| athletics_website = ...`` or similar athletics-specific infobox
# fields.  Checked FIRST in extract_website_from_wikitext — an athletics URL
# is always preferable to the university's main homepage (which is what
# ``| website =`` typically contains for university articles, not program
# articles).  Field aliases observed in the wild:
#   athletics_website, athletics_site, athletics, sports_website, sports_site
_INFOBOX_ATHLETICS_FIELD_RE = re.compile(
    r"^\s*\|\s*(?:athletics_website|athletics_site|athletics|sports_website|sports_site"
    r"|athletic_website|athletic_site)\s*=\s*(?P<value>.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Matches ``| website = ...`` (or ``| url = ...``).  Used as fallback when
# no athletics-specific field is found.  For program-article pages (i.e. the
# Wikipedia article is specifically about the soccer program, not the whole
# university) this IS the athletics/program URL.  For university articles it
# is the main homepage — callers should layer an athletics-subdomain probe on
# top.
_INFOBOX_WEBSITE_FIELD_RE = re.compile(
    r"^\s*\|\s*(?:website|url)\s*=\s*(?P<value>.+?)\s*$",
    re.IGNORECASE | re.MULTILINE,
)

# Pulls a URL out of a wikitext value. Handles three common shapes:
#   1. Bare URL:                 https://gostanford.com/sports/m-soccer
#   2. {{URL|...}}:              {{URL|gostanford.com/sports/m-soccer}}
#   3. External-link form:       [https://gostanford.com/... text]
_BARE_URL_RE = re.compile(r"https?://[^\s\]\|\}<]+")
_URL_TEMPLATE_RE = re.compile(r"\{\{\s*URL\s*\|\s*([^\}\|]+?)\s*(?:\|[^\}]*)?\}\}", re.IGNORECASE)
_EXTERNAL_LINK_RE = re.compile(r"\[\s*(https?://[^\s\]]+)")


def extract_website_from_wikitext(wikitext: str) -> Optional[str]:
    """Pull the best athletics/program URL from a Wikipedia infobox.

    Priority:
      1. ``| athletics_website =`` (or alias) — direct athletics URL.
      2. ``| website =`` (or ``| url =``) — falls back to this; may be
         the university's main homepage rather than the athletics portal.

    Returns the first usable URL found at the highest priority level,
    normalized to ``scheme://host…`` form.  ``None`` if no infobox website
    field is present or the value is unparseable.
    """
    if not wikitext:
        return None

    def _extract_from_pattern(pattern: re.Pattern) -> Optional[str]:
        for m in pattern.finditer(wikitext):
            value = m.group("value").strip()
            if not value:
                continue
            value = re.sub(r"<!--.*?-->", "", value, flags=re.DOTALL).strip()
            value = re.sub(r"<ref[\s>].*?(</ref>|/>)", "", value, flags=re.DOTALL).strip()
            url = _value_to_url(value)
            if url:
                return url
        return None

    # Try athletics-specific field first
    athletics_url = _extract_from_pattern(_INFOBOX_ATHLETICS_FIELD_RE)
    if athletics_url:
        return athletics_url

    # Fall back to generic website field
    return _extract_from_pattern(_INFOBOX_WEBSITE_FIELD_RE)


def _value_to_url(value: str) -> Optional[str]:
    """Normalize an infobox field value to a canonical URL string."""
    if not value:
        return None

    # 1) {{URL|host/path}} (most common style on athletics articles)
    tpl = _URL_TEMPLATE_RE.search(value)
    if tpl:
        candidate = tpl.group(1).strip()
        if candidate and not candidate.lower().startswith(("http://", "https://")):
            candidate = f"https://{candidate}"
        if _looks_like_http_url(candidate):
            return candidate

    # 2) [https://... text]  — external-link form
    ext = _EXTERNAL_LINK_RE.search(value)
    if ext:
        candidate = ext.group(1).strip()
        if _looks_like_http_url(candidate):
            return candidate

    # 3) Bare URL
    bare = _BARE_URL_RE.search(value)
    if bare:
        candidate = bare.group(0).strip().rstrip(",.;)")
        if _looks_like_http_url(candidate):
            return candidate

    return None


def _looks_like_http_url(s: str) -> bool:
    """True iff ``s`` parses as an http(s) URL with a non-empty host."""
    try:
        parsed = urlparse(s)
    except Exception:
        return False
    return parsed.scheme in ("http", "https") and bool(parsed.netloc)


# ---------------------------------------------------------------------------
# MediaWiki API: batch wikitext fetcher
# ---------------------------------------------------------------------------


def fetch_program_websites(
    article_titles: Iterable[str],
    *,
    session: Optional[requests.Session] = None,
    batch_size: int = _API_TITLE_BATCH,
) -> Dict[str, Optional[str]]:
    """Batch-fetch Wikipedia article wikitext and extract infobox websites.

    Returns ``{title: url_or_None}`` for every input title (titles with
    no extractable website map to ``None`` so the caller can
    distinguish "tried, no URL" from "not tried"). Titles with a 404 /
    missing-page response also map to ``None``.

    Uses ``action=query&prop=revisions&rvprop=content&rvslots=main``
    which returns the latest revision's wikitext for up to 50 titles
    per request (anonymous limit). Pagination across batches is the
    caller's concern (we slice locally rather than using
    ``query-continue``, since each batch is self-contained).

    Each batch retries twice on transient HTTP errors. A failed batch
    leaves all of its titles as ``None`` in the result map and is
    logged at WARNING — the run handler can then re-attempt or skip
    those rows on its next pass.
    """
    titles = [t for t in dict.fromkeys(article_titles) if t]
    if not titles:
        return {}
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    own_session = session is None
    if own_session:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
            }
        )

    out: Dict[str, Optional[str]] = {t: None for t in titles}

    try:
        for chunk_start in range(0, len(titles), batch_size):
            chunk = titles[chunk_start:chunk_start + batch_size]
            params = {
                "action": "query",
                "format": "json",
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
                "titles": "|".join(chunk),
                # ``redirects=1`` so a renamed program article still
                # returns content under its current title.
                "redirects": "1",
            }
            try:
                def _do_fetch() -> requests.Response:
                    resp = session.get(_MEDIAWIKI_API, params=params, timeout=REQUEST_TIMEOUT)
                    resp.raise_for_status()
                    return resp

                response = retry_with_backoff(
                    _do_fetch,
                    max_retries=2,
                    base_delay=2.0,
                    retryable_exceptions=(requests.RequestException,),
                    label=f"wikipedia-wikitext-batch-{chunk_start}",
                )
            except Exception as exc:
                log.warning(
                    "[wikipedia-program-urls] wikitext batch starting at %d failed: %s",
                    chunk_start, exc,
                )
                continue

            try:
                payload = response.json()
            except ValueError as exc:
                log.warning(
                    "[wikipedia-program-urls] non-JSON response for batch %d: %s",
                    chunk_start, exc,
                )
                continue

            # ``query.redirects`` maps the original requested title to
            # the canonicalized one. Build a reverse map so we can put
            # the extracted URL back under the requested-title key.
            redirects = (payload.get("query") or {}).get("redirects") or []
            canonical_to_requested: Dict[str, str] = {}
            normalized_to_requested: Dict[str, str] = {}
            for r in redirects:
                src = r.get("from")
                dst = r.get("to")
                if src and dst:
                    canonical_to_requested[dst] = src
            normalizations = (payload.get("query") or {}).get("normalized") or []
            for n in normalizations:
                src = n.get("from")
                dst = n.get("to")
                if src and dst:
                    normalized_to_requested[dst] = src

            pages = (payload.get("query") or {}).get("pages") or {}
            for page in pages.values():
                title = page.get("title")
                if not title:
                    continue
                # Map back through redirects + normalizations to the
                # caller-supplied title so the output dict keys match.
                requested = canonical_to_requested.get(title, title)
                requested = normalized_to_requested.get(requested, requested)
                if "missing" in page:
                    # Page doesn't exist; leave as None
                    continue
                revisions = page.get("revisions") or []
                if not revisions:
                    continue
                slots = (revisions[0].get("slots") or {})
                main = slots.get("main") or {}
                wikitext = main.get("*") or main.get("content") or ""
                website = extract_website_from_wikitext(wikitext)
                if website and requested in out:
                    out[requested] = website
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass

    return out


# ---------------------------------------------------------------------------
# Name-matching helper (Wikipedia name → colleges.name lookup)
# ---------------------------------------------------------------------------


# Tokens that we strip when normalizing school names for matching. The
# Wikipedia list page may say "Adelphi University" while
# ``colleges.name`` (seeded by stats.ncaa.org or a curated list) may
# say "Adelphi". Stripping these institutional-suffix tokens makes the
# match more forgiving without introducing false positives — none of
# these tokens disambiguate two different schools.
_NAME_NORMALIZER_STRIP_TOKENS: tuple[str, ...] = (
    "university",
    "the",
    "of",
)


def normalize_school_name(name: str) -> str:
    """Lowercase, strip punctuation + institutional suffixes for matching.

    The output is a stable join key; not for display. Used to match
    Wikipedia row labels against ``colleges.name`` rows seeded from
    stats.ncaa.org / curated lists, where the same school may appear
    with or without "University" / "College" / "The ".
    """
    if not name:
        return ""
    s = name.lower()
    # Replace common punctuation with spaces so tokenization works
    s = re.sub(r"[\.,&'\u2019\(\)/]+", " ", s)
    s = re.sub(r"-", " ", s)
    tokens = [t for t in s.split() if t and t not in _NAME_NORMALIZER_STRIP_TOKENS]
    return " ".join(tokens)


def build_name_index(refs: Iterable[ProgramArticleRef]) -> Dict[str, ProgramArticleRef]:
    """Return ``normalized_name -> ProgramArticleRef`` for fast lookup.

    Later refs with the same normalized name overwrite earlier ones —
    the parser already dedups within a single page so this only fires
    on cross-page name collisions, which are rare and benign for the
    URL-discovery use case.
    """
    out: Dict[str, ProgramArticleRef] = {}
    for ref in refs:
        key = normalize_school_name(ref.school_name)
        if key:
            out[key] = ref
    return out


# ---------------------------------------------------------------------------
# Top-level discovery orchestrator (used by run.py handler + tests)
# ---------------------------------------------------------------------------


@dataclass
class ProgramUrlDiscovery:
    """Result row from ``discover_program_urls`` per (school_name, gender)."""

    school_name: str
    article_title: str
    website: Optional[str]
    soccer_program_url: Optional[str]


def discover_program_urls(
    refs: Iterable[ProgramArticleRef],
    gender: str,
    *,
    session: Optional[requests.Session] = None,
    websites_override: Optional[Dict[str, Optional[str]]] = None,
) -> List[ProgramUrlDiscovery]:
    """For each program ref, discover (website, soccer_program_url).

    ``websites_override`` lets tests inject a pre-built
    ``article_title -> website`` map to skip the live MediaWiki API
    call. Passing ``None`` triggers the normal batched API fetch.

    Probing is done sequentially through
    ``ncaa_directory.resolve_soccer_program_url`` (the same SIDEARM
    multi-path probe that ``ncaa-resolve-urls`` uses), so success
    semantics here exactly match the existing resolver.
    """
    refs_list = list(refs)
    if gender not in ("mens", "womens"):
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    if websites_override is not None:
        websites = websites_override
    else:
        websites = fetch_program_websites(
            (r.article_title for r in refs_list),
            session=session,
        )

    out: List[ProgramUrlDiscovery] = []
    for ref in refs_list:
        website = websites.get(ref.article_title)
        program_url: Optional[str] = None
        if website:
            try:
                program_url = resolve_soccer_program_url(
                    website, gender, session=session
                )
            except Exception as exc:
                log.debug(
                    "[wikipedia-program-urls] SIDEARM probe failed for %s (%s): %s",
                    ref.school_name, website, exc,
                )
                program_url = None
        out.append(
            ProgramUrlDiscovery(
                school_name=ref.school_name,
                article_title=ref.article_title,
                website=website,
                soccer_program_url=program_url,
            )
        )

    return out
