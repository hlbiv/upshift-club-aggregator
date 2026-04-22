"""
naia_directory.py — Seed ``colleges`` from naia.org's 2021-22 soccer
teams index, and discover roster/athletics URLs from naia.org's per-team
detail pages.

Wikipedia doesn't have consolidated "List of NAIA ... soccer programs"
pages (unlike NCAA D2/D3). naia.org is the authoritative source, but
their current-season list endpoint (``/sports/(m|w)soc/<current>/teams``)
broke after different seasons per gender:

  - mens:   2021-22 is the last working index. 2023-24 onward
            302-redirects to the first team's detail page; 2025-26
            404s.
  - womens: 2020-21 is the last working index. 2021-22 already renders
            a generic "Women's Soccer" landing page with zero team
            anchors (this was surfaced by the first production run —
            the womens parser returned 0 seeds).

Each working-season endpoint renders the complete index as one
``<a href="/sports/(m|w)soc/<season>/teams/<slug>">NAME (STATE)</a>``
anchor per program. NAIA program churn is ~5/year, so each snapshot
covers ~95% of current (2025-26) membership; the rest come in via
the manual-entry workflow (PR #195).

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

# naia.org's teams-list endpoint broke at different seasons per gender:
#   - mens:   2021-22 is the last season that renders a full index
#             (2023-24 onward 302-redirects to the first team's detail
#             page; 2025-26 404s)
#   - womens: 2020-21 is the last working index (2021-22 renders a
#             generic "Women's Soccer" landing page with zero team
#             anchors)
# The per-gender map encodes that asymmetry. Staleness is ~4-5 seasons,
# but NAIA program churn (~5/year) means each snapshot still covers
# the vast majority of current membership; the rest come in via the
# manual-entry workflow (#195).
_NAIA_SEASONS: dict[str, str] = {
    "mens":   "2021-22",
    "womens": "2020-21",
}
_NAIA_BASE = "https://www.naia.org/sports"

_GENDER_SOURCES: dict[str, str] = {
    "mens":   f"{_NAIA_BASE}/msoc/{_NAIA_SEASONS['mens']}/teams",
    "womens": f"{_NAIA_BASE}/wsoc/{_NAIA_SEASONS['womens']}/teams",
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


# ---------------------------------------------------------------------------
# Slug capture (for the URL-discovery flow — see ``parse_naia_team_page`` and
# the ``naia-resolve-urls`` run.py handler). The directory parser already
# walks the same anchors; this helper returns the slug instead of building
# a CollegeSeed so the URL-resolver can join NAIA `colleges` rows to their
# detail-page slug without a DB schema change.
# ---------------------------------------------------------------------------


# Common school-name suffixes / qualifiers that drift between naia.org's
# short-form anchor text and ``colleges.name`` in our DB. Stripped during
# the slug-map normalization so "Wayland Baptist University" in the DB
# still joins to "Wayland Baptist" on naia.org.
# Conservative suffix list: only qualifiers that NAIA's short anchor
# text reliably drops. NOT included: "State" (real names like "Kansas
# State"), "Christian" (real names like "Arizona Christian"), "Tech",
# "A&M" — stripping any of those would create false-positive joins.
_NAIA_NAME_SUFFIXES = re.compile(
    r"\s+(university|college|institute)$",
    re.IGNORECASE,
)
_NAIA_PUNCT_RE = re.compile(r"[^\w\s]")
_NAIA_WS_RE = re.compile(r"\s+")


def _normalize_naia_name(name: str) -> str:
    """Loose normalization for fuzzy slug-map joins.

    Lowercases, strips non-word punctuation (St. vs St, O'Connell vs
    OConnell), collapses whitespace, and removes common trailing
    qualifiers ("University", "College") that naia.org's short anchor
    text drops but our DB ``colleges.name`` may include. Stops short of
    fuzzywuzzy / FUZZY_THRESHOLD — that's a separate follow-up — but
    cheaply resolves the common suffix-drift cases that block ~10% of
    NAIA programs from joining at all.
    """
    if not name:
        return ""
    s = name.lower().strip()
    s = _NAIA_PUNCT_RE.sub(" ", s)
    s = _NAIA_WS_RE.sub(" ", s).strip()
    # Strip trailing qualifiers iteratively (handles
    # "Wayland Baptist University" -> "Wayland Baptist" then a no-op).
    prev = None
    while prev != s:
        prev = s
        s = _NAIA_NAME_SUFFIXES.sub("", s).strip()
    return s


def parse_naia_index_slug_records(
    html: str, gender: str
) -> list[dict]:
    """Per-program slug records from one gender's naia.org index page.

    Walks the same anchors as ``parse_naia_index_slugs`` but emits a
    list of dicts ``{slug, name, normalized, state}`` so callers can
    fuzzy-match across name drift while gating on state. ``state`` may
    be None when the anchor lacks a parenthetical (rare on naia.org).
    First occurrence per (lowercased name) wins, mirroring
    ``parse_naia_index_slugs``.
    """
    if gender not in _GENDER_SOURCES:
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    records: list[dict] = []
    seen_keys: set[str] = set()
    for anchor in soup.find_all("a", href=_TEAM_HREF_RE):
        href = anchor.get("href") or ""
        match = _TEAM_HREF_RE.search(href)
        if not match:
            continue
        slug = match.group(1)
        raw_text = anchor.get_text()
        name, state = _name_and_state_from_anchor_text(raw_text)
        if not name or len(name) < 2:
            continue
        key = name.lower()
        if key in seen_keys:
            continue
        seen_keys.add(key)
        records.append({
            "slug": slug,
            "name": name,
            "normalized": _normalize_naia_name(name),
            "state": state,
        })
    return records


_NAIA_ABBREV_MAP = {
    "st": "saint",
    "mt": "mount",
    "ft": "fort",
}


def _expand_naia_abbreviations(normalized: str) -> str:
    """Expand St./Mt./Ft. → Saint/Mount/Fort on a normalized name.

    Run AFTER ``_normalize_naia_name`` (lowercased, punctuation
    stripped). This is the small extra step that lets a token-sort
    fuzzy ratio actually clear FUZZY_THRESHOLD on the abbreviation
    drift cases the task calls out — "St. Ambrose"/"Saint Ambrose"
    only score ~70 on token_sort_ratio without it.
    """
    if not normalized:
        return ""
    tokens = normalized.split()
    return " ".join(_NAIA_ABBREV_MAP.get(t, t) for t in tokens)


def fuzzy_match_naia_slug(
    name: str,
    state: Optional[str],
    records: list[dict],
    threshold: int = 88,
) -> Optional[tuple[str, str, Optional[str], int]]:
    """Best fuzzy slug match for ``name`` across ``records``.

    Returns ``(slug, matched_name, matched_state, score)`` for the
    highest-scoring record whose ``token_sort_ratio`` against the
    normalized name is ``>= threshold``, or None if nothing clears the
    bar. Gated by state when state is known on both sides — records
    with a state different from ``state`` (case-insensitive 2-letter
    compare) are excluded entirely. Records with state=None are always
    eligible (fall back to name-only match).

    Used by ``naia-resolve-urls`` as the second-pass after exact + suffix
    normalization both miss. Threshold defaults to project-wide
    FUZZY_THRESHOLD=88.
    """
    if not name or not records:
        return None
    try:
        from rapidfuzz import fuzz  # type: ignore
    except ImportError:
        return None

    normalized = _expand_naia_abbreviations(_normalize_naia_name(name))
    if not normalized:
        return None

    db_state = (state or "").strip().upper() or None

    best: Optional[tuple[str, str, Optional[str], int]] = None
    for rec in records:
        rec_state = (rec.get("state") or "").strip().upper() or None
        # State gate: only enforce when BOTH sides have a state.
        if db_state and rec_state and db_state != rec_state:
            continue
        rec_norm = _expand_naia_abbreviations(rec.get("normalized") or "")
        if not rec_norm:
            continue
        score = int(fuzz.token_sort_ratio(normalized, rec_norm))
        if score < threshold:
            continue
        if best is None or score > best[3]:
            best = (rec["slug"], rec["name"], rec.get("state"), score)
    return best


def parse_naia_index_slugs(
    html: str, gender: str
) -> dict[str, str]:
    """Map name → slug for one gender's naia.org index page.

    Walks the same anchors as ``parse_naia_index`` but returns the
    ``/sports/(m|w)soc/<season>/teams/<slug>`` slug captured by
    ``_TEAM_HREF_RE``. The first occurrence of each name wins (the index
    sometimes repeats programs across alphabetical + conference subindexes).

    The returned dict contains BOTH the lowercased original name AND
    the normalized form (see ``_normalize_naia_name``) as keys, all
    pointing at the same slug. The run.py handler tries the lowercased
    DB name first, then the normalized form — handles the common
    "University"/"College" suffix drift between naia.org's short anchor
    text and our DB ``colleges.name``. Empty anchors and nav links are
    filtered with the same logic as ``parse_naia_index``.
    """
    if gender not in _GENDER_SOURCES:
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")

    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, str] = {}
    for anchor in soup.find_all("a", href=_TEAM_HREF_RE):
        href = anchor.get("href") or ""
        match = _TEAM_HREF_RE.search(href)
        if not match:
            continue
        slug = match.group(1)
        raw_text = anchor.get_text()
        name, _state = _name_and_state_from_anchor_text(raw_text)
        if not name or len(name) < 2:
            continue
        keys = [name.lower(), _normalize_naia_name(name)]
        for key in keys:
            if key and key not in out:
                out[key] = slug
    return out


# ---------------------------------------------------------------------------
# Per-team detail-page URL discovery
#
# The directory seed gives us name + state but no athletics website. The
# canonical NAIA detail page (``/sports/(m|w)soc/<season>/teams/<slug>``)
# embeds an "Official Site" / "Athletics Website" outbound link to the
# school's athletics homepage; from there ``resolve_soccer_program_url``
# handles the SIDEARM probe just like NCAA.
#
# The detail-page HTML structure is not stable across the ~5-year-old
# snapshot vs. any newer template naia.org may serve, so the extractor
# below tries multiple selector strategies in priority order. Misses are
# logged for operator review — manual fill is the last-resort fallback.
# ---------------------------------------------------------------------------

# Outbound-link label patterns we'll accept as "this is the school's
# athletics homepage". Ordered loosely from most-specific to most-generic;
# the parser scans every anchor and matches against this regex.
_NAIA_OFFICIAL_SITE_LABELS = re.compile(
    r"\b("
    r"official\s+(?:athletic[s]?\s+)?site"
    r"|athletic[s]?\s+(?:website|home(?:page)?|site)"
    r"|team\s+website"
    r"|visit\s+(?:athletic[s]?\s+)?site"
    r"|school\s+website"
    r")\b",
    re.IGNORECASE,
)

# Outbound-link href filters: exclude obvious non-athletics destinations
# (social media, naia.org self-links, ticketing, mailto, javascript hooks).
# A real athletics URL is an http(s) URL whose host is NOT one of these.
_NAIA_LINK_BLOCKLIST = re.compile(
    r"^(?:mailto:|tel:|javascript:|#|/)|"
    r"(?:naia\.org|facebook\.com|twitter\.com|x\.com|instagram\.com|"
    r"youtube\.com|tiktok\.com|linkedin\.com|flickr\.com|pinterest\.com|"
    r"snapchat\.com|google\.com/maps|maps\.google\.com)",
    re.IGNORECASE,
)


def naia_team_detail_url(slug: str, gender: str) -> str:
    """Return the canonical naia.org detail-page URL for a slug + gender.

    Same season pinning as the index page (``_NAIA_SEASONS``) — naia.org
    keeps historical detail pages live indefinitely, so the 4-5 year-old
    snapshot is fine for athletics-website discovery (the school's own
    homepage rarely changes).
    """
    if gender not in _NAIA_SEASONS:
        raise ValueError(f"gender must be 'mens' or 'womens' (got {gender!r})")
    season = _NAIA_SEASONS[gender]
    code = "msoc" if gender == "mens" else "wsoc"
    return f"{_NAIA_BASE}/{code}/{season}/teams/{slug}"


def parse_naia_team_page(html: str) -> Optional[str]:
    """Extract the school's athletics-homepage URL from a NAIA team page.

    Strategy (first hit wins):
      1. Anchor whose visible text matches ``_NAIA_OFFICIAL_SITE_LABELS``
         ("Official Site", "Athletics Website", etc.) and whose href is
         an absolute http(s) URL not in ``_NAIA_LINK_BLOCKLIST``.
      2. Anchor whose ``title`` / ``aria-label`` attribute matches the
         same label set (some NAIA templates put the label on the
         attribute and use an icon for visible content).
      3. None — caller logs the miss for operator review.

    Returned URL is normalized to ``scheme://host`` (no trailing path).
    The downstream SIDEARM probe re-composes the roster path on top.
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1+2 unified: walk every <a>, score by label match
    # against (visible text, title, aria-label).
    for anchor in soup.find_all("a"):
        href = anchor.get("href") or ""
        if not href or _NAIA_LINK_BLOCKLIST.search(href):
            continue
        if not re.match(r"^https?://", href, re.IGNORECASE):
            continue

        candidates = [
            anchor.get_text() or "",
            anchor.get("title") or "",
            anchor.get("aria-label") or "",
        ]
        if not any(_NAIA_OFFICIAL_SITE_LABELS.search(c) for c in candidates):
            continue

        # Normalize to scheme://host (drop path/query/fragment) — the
        # SIDEARM resolver re-composes /sports/.../roster on its own.
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(href.strip())
        if not parsed.netloc:
            continue
        return urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))

    return None


def discover_naia_program_url(
    slug: str,
    gender: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 15,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve (athletics_website, soccer_program_url) for a NAIA program.

    Two-phase lookup:
      1. Fetch the naia.org detail page for ``slug`` + ``gender`` and
         extract the athletics homepage via ``parse_naia_team_page``.
      2. Probe SIDEARM/common roster paths on that homepage via
         ``resolve_soccer_program_url`` (same code path NCAA uses).

    Both halves can fail independently — a program with a working
    athletics homepage but a non-SIDEARM roster CMS will return
    ``(website, None)`` so the caller can still backfill
    ``colleges.website`` (useful input for future probe strategies).
    Returns ``(None, None)`` if the detail-page fetch fails or has no
    extractable athletics link.
    """
    # Lazy import — keeps this function callable without dragging in
    # ncaa_directory's resolver at import time (matters for unit tests
    # that mock the module).
    from extractors.ncaa_directory import resolve_soccer_program_url  # noqa: E402
    # Route the naia.org detail-page GET through the proxy-aware
    # wrapper. Replit egress IPs hit naia.org's WAF (HTTP 405); the
    # wrapper transparently rotates through ``proxy_config.yaml`` when
    # populated and falls back to a direct call otherwise — so this
    # path is correct in dev (empty config) AND prod (proxy pool).
    # The ``session`` kwarg is still accepted (and used for the SIDEARM
    # probe under the school's own domain — proxy not needed there).
    from utils import http as proxy_http  # noqa: E402

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
        url = naia_team_detail_url(slug, gender)
        try:
            resp = proxy_http.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,*/*",
                },
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            log.debug("[naia-resolver] GET %s failed: %s", url, exc)
            return None, None
        if resp.status_code != 200:
            log.debug(
                "[naia-resolver] GET %s -> HTTP %d", url, resp.status_code
            )
            return None, None

        website = parse_naia_team_page(resp.text)
        if not website:
            return None, None

        program_url = resolve_soccer_program_url(
            website, gender, session=session
        )
        return website, program_url
    finally:
        if own_session:
            try:
                session.close()
            except Exception:
                pass


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
