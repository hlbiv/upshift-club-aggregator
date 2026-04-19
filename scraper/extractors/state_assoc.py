"""
State Association extractor for all 54 USYS tier-4 state associations.

Strategy (in priority order):
1. GotSport event clubs endpoint   — system.gotsport.com/org_event/events/{id}/clubs
2. Google My Maps KML feed         — google.com/maps/d/kml?forcekml=1&mid={id}
3. js_club_list                    — JS variable `const clubs=[{n:'...'}]` on assoc page
4. html_club_list                  — plain-text club list scraped from assoc page HTML
5. no_source_found / unknown       — returns [] with a warning

All data sources are declared in data/state_assoc_config.json, keyed by the
canonical URL listed in leagues_master.csv (no trailing slash).

Coverage (Task #12 complete — April 2026; 5 more states added):
  GotSport    (34 states): AL, AK, AR, AZ, CA-North, CA-South, CO, DE, E-NY,
                           FL, GA, ID, IL, IA, KS, KY, ME, MD, MI, MN, MT,
                           NV, NH, NJ, NM, NT, NY-West, OH, OK, VT, VA,
                           WA, WV, WY
  Google Maps ( 6 states): CT, Eastern PA, IN, MO, TN, TX-South
  JS club list ( 1 state):  NC  (ncsoccer.org/find-my-club/ JS variable)
  HTML club list (2 states): OR (oregonyouthsoccer.org/find-a-club/),
                             PA-West (pawest-soccer.org/club-list/)
  SoccerWire  ( 8 states): HI, LA, MA, MS, NE, RI, SC, WI
    — SoccerWire WP REST API + individual club pages (Task #22, April 2026)
  HTML club page (2 states): UT (utahyouthsoccer.net/youth-members/ — 71 clubs),
                             ND (northdakotasoccer.org/club-info/ — 8 clubs)
  Curated seed ( 1 state):  SD (no public directory; 28 clubs from third-party sources)

Replay coverage (April 2026):
  The live ``scrape_state_association`` path is a dispatch over many small
  per-state sub-scrapers, each of which fetches + parses its own HTML. For
  the ``--source replay-html`` pipeline we expose:

  * ``parse_html(html, source_url, league_name)`` — module-level dispatcher
    that inspects the source URL hostname to decide which sub-parser to run.
  * ``PARSERS`` — a dict mapping a source-type key (``gotsport`` /
    ``html_club_list`` / ``soccerwire``) to the pure-function parser.
  * ``parse_gotsport_html`` / ``parse_html_club_list_html`` /
    ``parse_soccerwire_html`` — the sub-parsers themselves, each taking
    pre-fetched HTML and returning normaliser-shaped records.

  Migrated paths: ``gotsport``, ``html_club_list``, ``soccerwire``. Skipped
  (no HTML to replay from or require JS runtime): ``google_maps`` (XML/KML
  feed — not HTML, Playwright not involved), ``js_club_list`` (a plain
  HTTP page but the payload is a JS array; captured HTML can be parsed
  with the existing regex but this hasn't been wired into the dispatcher
  yet — single-state coverage, low volume), ``html_club_page`` and
  ``curated_seed`` (delegate to per-state handler modules with bespoke
  logic — refactor separately if those sources start archiving HTML).
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Callable, List, Dict
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from config import FUZZY_THRESHOLD
from extractors.registry import register
from extractors.gotsport import parse_gotsport_event_html, scrape_gotsport_event
from extractors import soccerwire as _soccerwire
from normalizer import _canonical
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0)"}

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "state_assoc_config.json")
with open(_CONFIG_PATH) as _f:
    _STATE_CONFIG: dict = json.load(_f)

_CONFIG_BY_DOMAIN: dict[str, dict] = {}
for _url, _cfg in _STATE_CONFIG.items():
    _domain = _url.rstrip("/").replace("https://", "").replace("http://", "")
    _CONFIG_BY_DOMAIN[_domain] = {**_cfg, "_url": _url}


def _lookup_config(url: str) -> dict | None:
    url_clean = url.rstrip("/")
    if url_clean in _STATE_CONFIG:
        return _STATE_CONFIG[url_clean]
    domain = url_clean.replace("https://", "").replace("http://", "").lstrip("www.")
    for key, cfg in _STATE_CONFIG.items():
        if key.rstrip("/").replace("https://", "").replace("http://", "").lstrip("www.") == domain:
            return cfg
    return None


def _multi_event_dedup(clubs_list: List[Dict]) -> List[Dict]:
    """Deduplicate across multiple GotSport events using fuzzy matching."""
    seen_canonical: list[str] = []
    out: List[Dict] = []
    for club in clubs_list:
        canon = _canonical(club["club_name"])
        is_dup = False
        for seen in seen_canonical:
            if fuzz.token_sort_ratio(canon, seen) >= FUZZY_THRESHOLD:
                is_dup = True
                break
        if not is_dup:
            seen_canonical.append(canon)
            out.append(club)
    return out


def parse_gotsport_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
    state: str = "",
    multi_state: bool = False,
) -> List[Dict]:
    """
    Pure-function parser for a GotSport ``/org_event/events/{id}/clubs``
    page in the state-assoc dispatch context.

    Thin adapter over :func:`extractors.gotsport.parse_gotsport_event_html`
    so the replay dispatcher can route a single captured page through this
    extractor without knowing GotSport internals. ``state`` is ignored when
    ``multi_state=True`` (same contract as the live path).

    A live state-assoc scrape usually stitches records from multiple
    GotSport events with :func:`_multi_event_dedup`; replay works off one
    captured page at a time, so this returns the raw records for that one
    page and leaves dedup to the caller (or ``parse_html`` below when the
    full dispatch is needed).
    """
    return parse_gotsport_event_html(
        html,
        url=source_url,
        league_name=league_name,
        state=state,
        multi_state=multi_state,
    )


def _scrape_gotsport(
    event_ids: List[int],
    league_name: str,
    state: str,
    multi_state: bool = False,
) -> List[Dict]:
    raw: List[Dict] = []
    for eid in event_ids:
        clubs = scrape_gotsport_event(eid, league_name, state=state, multi_state=multi_state)
        raw.extend(clubs)
        logger.info("  GotSport event %s: %d clubs", eid, len(clubs))
    return _multi_event_dedup(raw)


def _scrape_google_maps(map_ids: List[str], league_name: str, state: str) -> List[Dict]:
    """Fetch Google My Maps KML and extract place names as club names."""
    raw: List[Dict] = []
    SKIP_PHRASES = {"layer", "sheet", "csv", "directory", "map", "find a place", "member"}

    for mid in map_ids:
        url = f"https://www.google.com/maps/d/kml?forcekml=1&mid={mid}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                logger.warning("Google Maps KML %s returned %s", mid, r.status_code)
                continue

            names = re.findall(r"<name>([^<]+)</name>", r.text)
            for raw_name in names:
                name = raw_name.strip()
                if len(name) < 4 or len(name) > 100:
                    continue
                lower = name.lower()
                if any(phrase in lower for phrase in SKIP_PHRASES):
                    continue
                raw.append({
                    "club_name": name,
                    "league_name": league_name,
                    "city": "",
                    "state": state,
                    "source_url": url,
                })
            logger.info("  Google Maps KML %s: %d raw places", mid, len(raw))
        except Exception as exc:
            logger.warning("Google Maps KML %s error: %s", mid, exc)

    return _multi_event_dedup(raw)


def _scrape_js_club_list(page_url: str, js_var: str, league_name: str, state: str) -> List[Dict]:
    """Extract club names from a JavaScript array variable embedded in a webpage.

    Handles the pattern used by NCYSA's find-my-club page:
        const clubs=[{n:'Club Name', lat:..., ...}, ...]

    Args:
        page_url: Full URL of the page containing the JS variable.
        js_var:   Name of the JavaScript variable (e.g. "clubs").
        league_name: League name to tag on each record.
        state:    State name to tag on each record.
    """
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("JS club list page %s returned %s", page_url, r.status_code)
            return []
    except Exception as exc:
        logger.warning("JS club list page %s error: %s", page_url, exc)
        return []

    pattern = rf"const\s+{re.escape(js_var)}\s*=\s*\[(.*?)\];"
    m = re.search(pattern, r.text, re.DOTALL)
    if not m:
        logger.warning("JS variable '%s' not found on %s", js_var, page_url)
        return []

    clubs_js = m.group(0)
    names = re.findall(r"\{n:'([^']+)'", clubs_js)
    if not names:
        logger.warning("No club names found in JS variable '%s' on %s", js_var, page_url)
        return []

    records = []
    for name in names:
        name = name.strip()
        if not name or len(name) < 2:
            continue
        records.append({
            "club_name": name,
            "league_name": league_name,
            "city": "",
            "state": state,
            "source_url": page_url,
        })
    logger.info("  JS club list %s ('%s'): %d clubs", page_url, js_var, len(records))
    return _multi_event_dedup(records)


_CLUB_KEYWORDS = frozenset({
    "fc", "sc", "soccer", "club", "united", "academy", "futbol", "athletic",
    "youth", "sports", "association", "assoc", "football", "warriors", "force",
    "rush", "storm", "elite", "premier", "lightning", "select", "heat", "fire",
    "rangers", "eagles", "hawks", "stars", "knights", "tigers",
    "wolves", "falcons", "thunder", "impact", "fusion", "cosmos", "dynamo",
})


def parse_html_club_list_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
    state: str = "",
    skip_phrases: List[str] | None = None,
) -> List[Dict]:
    """
    Pure-function parser for state-assoc ``html_club_list`` pages.

    Handles the pattern used by OYSA (Oregon Youth Soccer) and PA West,
    where club names appear as plain text within the page's content area.
    Lines that are all-uppercase section headers, very short, or match
    ``skip_phrases`` are filtered out.

    Args:
        html:         Raw HTML body of the club-list page.
        source_url:   Canonical URL of the page; stamped on each record.
        league_name:  League name to tag on each record.
        state:        State name to tag on each record.
        skip_phrases: Lower-cased substrings that disqualify a line as a
                      club name. When ``None`` (replay case) we fall back
                      to a conservative default that matches the OYSA/PA
                      West page chrome.

    Returns:
        A list of normaliser-shaped club dicts. De-duplication is left to
        the caller; see :func:`_multi_event_dedup`.
    """
    if skip_phrases is None:
        skip_phrases = [
            "skip to", "privacy policy", "sportsengine", "club list",
            "find a club", "member clubs", "photo gallery", "about us",
            "home", "contact", "login",
        ]

    soup = BeautifulSoup(html, "lxml")
    content = (
        soup.find("div", class_="entry-content")
        or soup.find("main")
        or soup.find("article")
        or soup.body
    )
    if not content:
        logger.warning("No content area found on %s", source_url)
        return []

    raw_lines = [
        line.strip()
        for line in content.get_text(separator="\n").splitlines()
        if line.strip()
    ]

    records: List[Dict] = []
    for line in raw_lines:
        if len(line) < 3 or len(line) > 120:
            continue
        lower = line.lower()
        if any(phrase in lower for phrase in skip_phrases):
            continue
        if line.startswith("–") or line.startswith("-"):
            continue
        if line.isupper():
            if not any(kw in lower for kw in _CLUB_KEYWORDS):
                continue
        records.append({
            "club_name": line,
            "league_name": league_name,
            "city": "",
            "state": state,
            "source_url": source_url,
        })

    logger.info(
        "  HTML club list %s: %d raw lines, %d kept",
        source_url, len(raw_lines), len(records),
    )
    return records


def _fetch_html_club_list(page_url: str) -> str | None:
    """Fetch the HTML club-list page body. Returns ``None`` on any error."""
    try:
        r = requests.get(page_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("HTML club list page %s returned %s", page_url, r.status_code)
            return None
    except Exception as exc:
        logger.warning("HTML club list page %s error: %s", page_url, exc)
        return None
    return r.text


def _scrape_html_club_list(
    page_url: str,
    skip_phrases: List[str],
    league_name: str,
    state: str,
) -> List[Dict]:
    """Live-path wrapper: fetch + parse + dedup the HTML club list page."""
    html = _fetch_html_club_list(page_url)
    if html is None:
        return []
    records = parse_html_club_list_html(
        html,
        source_url=page_url,
        league_name=league_name,
        state=state,
        skip_phrases=skip_phrases,
    )
    return _multi_event_dedup(records)


def parse_soccerwire_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
    state: str = "",
) -> List[Dict]:
    """
    Pure-function parser for a SoccerWire club page in the state-assoc
    dispatch context.

    Live state-assoc ``soccerwire`` scrapes walk the WP REST API to build
    a slug candidate list, then fetch each individual club page and filter
    by state. Replay works one captured page at a time, so this adapter:

    * delegates the actual HTML parse to
      :func:`extractors.soccerwire.parse_html` (returns a one-element
      list or ``[]``); and
    * drops the record when ``state`` is set and the parsed record's
      ``state`` doesn't match — keeps parity with the live filter that
      only returns clubs where the page's location matches the target.
    """
    records = _soccerwire.parse_html(
        html,
        source_url=source_url,
        league_name=league_name,
    )
    if not records:
        return []

    target_abbr = _soccerwire._state_abbr(state) if state else ""
    if target_abbr:
        records = [r for r in records if r.get("state") == target_abbr]
    return records


def _scrape_state(url: str, league_name: str) -> List[Dict]:
    cfg = _lookup_config(url)
    if not cfg:
        logger.warning("No state_assoc_config entry for URL: %s — skipping", url)
        return []

    if cfg.get("disabled"):
        logger.info(
            "State assoc seed %s is disabled in state_assoc_config.json — skipping",
            url,
        )
        return []

    state = cfg.get("state", "")
    src_type = cfg.get("type", "unknown")
    logger.info("State: %s | source: %s", state, src_type)

    if src_type == "gotsport":
        event_ids = [int(e) for e in cfg.get("events", [])]
        multi_state = cfg.get("multi_state", False)
        return _scrape_gotsport(event_ids, league_name, state, multi_state=multi_state)

    if src_type == "google_maps":
        map_ids = cfg.get("map_ids", [])
        return _scrape_google_maps(map_ids, league_name, state)

    if src_type == "js_club_list":
        page_url = cfg.get("page_url", "")
        js_var = cfg.get("js_var", "clubs")
        return _scrape_js_club_list(page_url, js_var, league_name, state)

    if src_type == "html_club_list":
        page_url = cfg.get("page_url", "")
        skip_phrases = cfg.get("skip_phrases", [])
        return _scrape_html_club_list(page_url, skip_phrases, league_name, state)

    if src_type == "soccerwire":
        from extractors.soccerwire import scrape_soccerwire_state
        return scrape_soccerwire_state(state, league_name)

    if src_type == "html_club_page":
        page_url = cfg.get("page_url", "")
        handler = cfg.get("handler", "")
        if handler == "utah":
            from extractors.utah_clubs import scrape_utah_clubs
            return scrape_utah_clubs(league_name)
        elif handler == "north_dakota":
            from extractors.north_dakota_clubs import scrape_nd_clubs
            return scrape_nd_clubs(league_name)
        else:
            logger.warning("Unknown html_club_page handler '%s' for %s", handler, state)
            return []

    if src_type == "curated_seed":
        handler = cfg.get("handler", "")
        if handler == "south_dakota":
            from extractors.south_dakota_clubs import scrape_sd_clubs
            return scrape_sd_clubs(league_name)
        else:
            logger.warning("Unknown curated_seed handler '%s' for %s", handler, state)
            return []

    logger.info("No automated source for %s (%s) — skipping", state, url)
    return []


_STATE_PATTERN = "|".join(
    re.escape(url.replace("https://", "").replace("http://", ""))
    for url in _STATE_CONFIG
)


@register(_STATE_PATTERN)
def scrape_state_association(url: str, league_name: str) -> List[Dict]:
    """Dispatch to the appropriate sub-scraper based on state_assoc_config.json."""
    return _scrape_state(url, league_name)


# ---------------------------------------------------------------------------
# Replay-html parse_html dispatcher
# ---------------------------------------------------------------------------

#: Source-type → pure-function parser registry. The replay handler can use
#: this to route archived HTML when it knows the source-type, skipping the
#: URL-hostname heuristic in :func:`parse_html`. Each parser has the shared
#: signature ``(html, source_url=, league_name=, state=, **kwargs) -> list[dict]``
#: so the dispatcher can forward kwargs uniformly.
PARSERS: Dict[str, Callable[..., List[Dict]]] = {
    "gotsport": parse_gotsport_html,
    "html_club_list": parse_html_club_list_html,
    "soccerwire": parse_soccerwire_html,
}


def _detect_source_type(source_url: str) -> str:
    """
    Map ``source_url`` → one of the keys in :data:`PARSERS`.

    We inspect the hostname only — path + query don't disambiguate further
    for the three migrated source types. Returns ``""`` when no parser is
    known; callers should treat that as a skip.
    """
    if not source_url:
        return ""
    try:
        host = urlparse(source_url).hostname or ""
    except ValueError:
        return ""
    host = host.lower().lstrip("www.")

    if host.endswith("gotsport.com"):
        return "gotsport"
    if host.endswith("soccerwire.com"):
        return "soccerwire"

    # html_club_list currently serves two state-assoc pages: OYSA and PA
    # West. Both are plain HTML on the state-association domain itself.
    if host.endswith("oregonyouthsoccer.org") or host.endswith("pawest-soccer.org"):
        return "html_club_list"

    return ""


def _state_for_url(source_url: str) -> str:
    """
    Best-effort state lookup for a replay URL.

    Used by :func:`parse_html` to inject the ``state`` that the live path
    would have attached. For GotSport we can't reach the state from the URL
    alone (events are globally numbered), so callers may override via
    ``state=`` when they have richer context — otherwise the parsed
    records get ``state=""`` and downstream normaliser/linker fill it in.
    """
    if not source_url:
        return ""
    try:
        host = (urlparse(source_url).hostname or "").lower().lstrip("www.")
    except ValueError:
        return ""
    if not host:
        return ""

    # Walk the config's domain map built at import time.
    for domain, cfg in _CONFIG_BY_DOMAIN.items():
        if host == domain.lower().lstrip("www."):
            return cfg.get("state", "")
    return ""


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function dispatcher exposed to ``--source replay-html``.

    Inspects ``source_url`` to pick a sub-parser from :data:`PARSERS`,
    then returns whatever that parser returns. Known source-types:

    * ``gotsport`` — any ``*.gotsport.com`` URL. Routes to
      :func:`parse_gotsport_html`. State is not inferable from the URL
      alone so the records get ``state=""`` unless the caller happens
      to have additional context.
    * ``soccerwire`` — any ``*.soccerwire.com`` club page. Routes to
      :func:`parse_soccerwire_html`; no state filter is applied on replay
      (we have one page, we return the one record it parses into).
    * ``html_club_list`` — OYSA + PA West pages. Routes to
      :func:`parse_html_club_list_html` with a conservative default set
      of skip-phrases; the per-state override list lives in
      ``state_assoc_config.json`` and is only used on the live path.

    Returns ``[]`` — never raises — when the URL doesn't match a known
    source-type or the parser itself returns nothing. The replay handler
    logs a skip when this happens.
    """
    if not html:
        return []

    source_type = _detect_source_type(source_url)
    if not source_type:
        logger.debug(
            "[state_assoc replay] no parser for source_url=%s (hostname not in PARSERS map)",
            source_url,
        )
        return []

    parser = PARSERS[source_type]
    state = _state_for_url(source_url)

    try:
        return parser(
            html,
            source_url=source_url,
            league_name=league_name,
            state=state,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[state_assoc replay] parser %s raised on %s: %s",
            source_type, source_url, exc,
        )
        return []
