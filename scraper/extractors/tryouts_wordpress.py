"""
tryouts_wordpress.py — Scrape club-website tryout announcements.

Target: the big long tail of WordPress-hosted club sites (the vast
majority of youth-soccer club sites are WordPress). We probe a small
set of common paths (``/tryouts/``, ``/register/``, etc.) and extract
best-effort structured data from whatever the page contains.

Rules:
  - Permissive input. Two emission modes:
      1. DATED row — parseable date found → full row with ``tryout_date``
      2. REGISTRATION-ONLY row — no date found but platform registration
         link (LeagueApps, GotSport, TGS, SportsEngine) present on page
         → row with ``tryout_date=None`` and ``url=registration_link``
  - No Playwright — static HTML only. WordPress sites render server-side.
  - Backward-compatible — existing dated rows emit identically to before.

Output rows match the ``tryouts`` writer contract:

    {
        "club_name_raw": str,         # required
        "tryout_date":   datetime|None,  # may be None for registration-only
        "age_group":     str | None,  # "U12"
        "gender":        str | None,  # "M" | "F" | None
        "location":      str | None,  # becomes `location_name` at the writer
        "source_url":    str | None,  # the tryout page URL
        "url":           str | None,  # registration link (preferred over source_url)
        "notes":         str | None,  # JSON blob of platform IDs when captured
    }

COVERAGE & LIMITATIONS (April 2026)
-------------------------------------
The registration-link capture relies on clubs embedding public URLs to
third-party registration platforms. Known limitations:

  * LeagueApps API is auth-walled — we capture URLs but cannot fetch
    dates/age groups from LA directly. The row carries the URL for the
    user to click through.

  * SportsEngine widget API is auth-walled. Same tradeoff.

  * GotSport event IDs captured from /events/{id}/ URLs CAN be
    resolved later by an offline linker against the events table
    populated by gotsport_events_runner. The capture here just
    records the ID; correlation is a downstream pass.

  * Clubs using non-WordPress platforms entirely (pure SportsEngine,
    Webflow, Wix, Squarespace, custom CMSs) are NOT covered by this
    scraper. They need dedicated extractors if coverage matters.

  * Registration-only rows have no date → they unique-collapse to one
    row per (club, age, gender) via the
    ``tryouts_name_date_bracket_uq`` index (NULL dates coalesce to
    'epoch'). Re-runs overwrite cleanly; no unbounded growth.

  * Clubs that don't list tryouts at any ``/tryout*`` path are
    invisible — a generic "look at the homepage for links" fallback
    was intentionally NOT added because the false-positive rate
    swamps the signal.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}

# Common URL suffixes WordPress clubs put tryout info under.
# Ordered from most-specific to most-generic; the loop stops on first
# HTTP 200 that yields ANY output (dated or registration-only).
_TRYOUT_PATHS = (
    "/tryouts/",
    "/tryouts",
    "/tryout/",
    "/tryout",
    "/try-outs/",
    "/try-outs",
    "/competitive-registration/",
    "/academy-registration/",
    "/register/",
    "/registration/",
    "/join/",
    "/join",
)

# ---------------------------------------------------------------------------
# Registration platform detectors
# ---------------------------------------------------------------------------

# GotSport event URLs: system.gotsport.com/org_event/events/{event_id}/...
_GOTSPORT_EVENT_URL = re.compile(
    r"system\.gotsport\.com/(?:org_event/)?events/(\d+)\b",
    re.IGNORECASE,
)
# TGS event URLs: public.totalglobalsports.com/events/{event_id}
# or match the events/{id} path on any subdomain to be safe.
_TGS_EVENT_URL = re.compile(
    r"(?:public\.)?totalglobalsports\.com/events/(\d+)\b",
    re.IGNORECASE,
)
# LeagueApps: {club}.leagueapps.com/* or members.leagueapps.com/clubteams/{id}
# or accounts.leagueapps.com/login — all count as registration entry points.
_LEAGUEAPPS_URL = re.compile(
    r'https?://[a-z0-9.-]*leagueapps\.com[^\s"\'<>]*',
    re.IGNORECASE,
)

# LeagueApps marketing / documentation URLs to exclude from the
# captured registration links (noise).
_LEAGUEAPPS_NOISE_PATHS = (
    "/products/",
    "/pricing",
    "/blog",
    "/resources",
    "/partners",
    "/about",
    "/contact",
    "/demo",
    "/case-studies",
    "/integrations",
)
# SportsEngine: *.sportngin.com/* or sportsengine.com/*
_SPORTNGIN_URL = re.compile(
    r'https?://[a-z0-9.-]*(?:sportngin|sportsengine)\.com[^\s"\'<>]*',
    re.IGNORECASE,
)
# Generic platforms we don't dedicate handlers to but capture as fallback:
# Eventbrite, TeamSnap, Jotform, SignUpGenius.
_GENERIC_REG_URL = re.compile(
    r'https?://[a-z0-9.-]*(?:eventbrite\.com|teamsnap\.com|jotform\.com|signupgenius\.com)[^\s"\'<>]*',
    re.IGNORECASE,
)


def extract_registration_links(html: str) -> Dict[str, object]:
    """Pure function — scan HTML for third-party registration URLs.

    Returns a dict with platform-indexed ID lists + a best-guess
    primary URL. Empty lists / None when no match.

    Shape::

        {
            "gotsport_event_ids": ["45123", "50231"],
            "tgs_event_ids":      ["3979"],
            "leagueapps_urls":    ["https://solar.leagueapps.com/clubteams/3167131"],
            "sportngin_urls":     [...],
            "generic_urls":       [...],  # eventbrite, teamsnap, jotform, etc.
            "primary_url":        str | None,  # first captured URL across categories
        }
    """
    if not html:
        return {
            "gotsport_event_ids": [],
            "tgs_event_ids": [],
            "leagueapps_urls": [],
            "sportngin_urls": [],
            "generic_urls": [],
            "primary_url": None,
        }

    # Dedup while preserving first-seen order.
    def _uniq(items: Iterable[str]) -> List[str]:
        return list(dict.fromkeys(items))

    gs_ids = _uniq(_GOTSPORT_EVENT_URL.findall(html))
    tgs_ids = _uniq(_TGS_EVENT_URL.findall(html))
    la_urls_raw = _uniq(_LEAGUEAPPS_URL.findall(html))
    # Filter LeagueApps marketing / documentation URLs — these appear
    # in club site footers and aren't registration entry points.
    la_urls = [
        u for u in la_urls_raw
        if not any(noise in u.lower() for noise in _LEAGUEAPPS_NOISE_PATHS)
    ]
    se_urls = _uniq(_SPORTNGIN_URL.findall(html))
    generic_urls = _uniq(_GENERIC_REG_URL.findall(html))

    # Pick the first URL across platforms as the primary. Preference order:
    # GotSport > TGS > LeagueApps > SportsEngine > generic. We prefer
    # platforms we can correlate to internal data (GotSport/TGS events)
    # over auth-walled platforms (LA/SE) over generic embeds.
    primary_url: Optional[str] = None
    if gs_ids:
        primary_url = f"https://system.gotsport.com/org_event/events/{gs_ids[0]}"
    elif tgs_ids:
        primary_url = f"https://public.totalglobalsports.com/events/{tgs_ids[0]}"
    elif la_urls:
        primary_url = la_urls[0]
    elif se_urls:
        primary_url = se_urls[0]
    elif generic_urls:
        primary_url = generic_urls[0]

    return {
        "gotsport_event_ids": gs_ids,
        "tgs_event_ids": tgs_ids,
        "leagueapps_urls": la_urls,
        "sportngin_urls": se_urls,
        "generic_urls": generic_urls,
        "primary_url": primary_url,
    }

_MONTHS = (
    "january|february|march|april|may|june|july|august|september|"
    "october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec"
)

# "August 5, 2026" or "Aug 5, 2026". Also handles ranges: "Aug 5-7, 2026"
# (we capture the first day and the year).
_DATE_MONTH_DAY_YEAR = re.compile(
    rf"\b(?P<month>{_MONTHS})\.?\s+(?P<day>\d{{1,2}})(?:\s*[-–]\s*\d{{1,2}})?,?\s+(?P<year>\d{{4}})\b",
    re.IGNORECASE,
)
# "8/5/26" or "08/05/2026"
_DATE_NUMERIC = re.compile(
    r"\b(?P<m>\d{1,2})/(?P<d>\d{1,2})/(?P<y>\d{2,4})\b"
)

_MONTH_TO_INT = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

_AGE_RE = re.compile(r"\b[Uu]-?(?P<age>\d{1,2})\b")
_BIRTH_YEAR_AGE_RE = re.compile(r"\b(?P<year>20\d{2})\s+(?P<g>Boys|Girls)\b", re.IGNORECASE)
_GENDER_RE = re.compile(r"\b(?P<g>Boys|Girls|Coed|Co-ed)\b", re.IGNORECASE)


@dataclass
class WordPressClubSite:
    club_name_raw: str
    website: str


def parse_date(text: str) -> Optional[datetime]:
    """Parse the first recognizable date from a blob of text.

    Handles:
      - "August 5, 2026" / "Aug 5, 2026"
      - "August 5-7, 2026" (returns Aug 5)
      - "8/5/26" / "08/05/2026"

    Returns ``None`` if nothing parses.
    """
    if not text:
        return None
    m = _DATE_MONTH_DAY_YEAR.search(text)
    if m:
        month = _MONTH_TO_INT.get(m.group("month").lower())
        if month:
            try:
                return datetime(int(m.group("year")), month, int(m.group("day")))
            except ValueError:
                pass
    m = _DATE_NUMERIC.search(text)
    if m:
        year = int(m.group("y"))
        if year < 100:
            year += 2000
        try:
            return datetime(year, int(m.group("m")), int(m.group("d")))
        except ValueError:
            return None
    return None


def parse_age_group(text: str) -> Optional[str]:
    """Return ``"U<n>"`` if the text mentions one, else None.

    Also recognizes birth-year form (``"2015 Boys"`` → ``"U11"`` relative
    to Aug 2026). We stick to the explicit ``U<n>`` match in the common
    path; the birth-year form requires knowing the current seasonal age
    cutoff which we don't thread here.
    """
    if not text:
        return None
    m = _AGE_RE.search(text)
    if m:
        return f"U{int(m.group('age'))}"
    return None


def parse_gender(text: str) -> Optional[str]:
    """Free-text → ``"M" | "F" | None``. Co-ed becomes None."""
    if not text:
        return None
    m = _GENDER_RE.search(text)
    if not m:
        return None
    g = m.group("g").lower()
    if g in ("boys", "boy"):
        return "M"
    if g in ("girls", "girl"):
        return "F"
    return None


def parse_location(soup: BeautifulSoup) -> Optional[str]:
    """Best-effort location extractor. Prefers <address>, then anything
    near a "Location:" label, then None.
    """
    addr = soup.find("address")
    if addr:
        text = addr.get_text(" ", strip=True)
        if text:
            return text
    # Look for a label.
    for tag in soup.find_all(["p", "li", "div", "h3", "h4"]):
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        low = txt.lower()
        if low.startswith("location") or low.startswith("where:"):
            # Strip the leading label.
            cleaned = re.sub(r"^(location|where)\s*[:\-]?\s*", "", txt, flags=re.IGNORECASE)
            return cleaned.strip() or None
    return None


def parse_tryouts_page_html(
    html: str,
    *,
    club_name_raw: str,
    source_url: str,
) -> List[Dict]:
    """Extract zero-or-more tryout rows from a single page's HTML.

    Pure function — fixture-driven. Two-mode emission:

      * DATED row  — a parseable date was found in the text → emit a
                     full row with all parsed fields.
      * REGISTRATION-ONLY row — no date found BUT a third-party
                     registration platform link was found → emit a
                     single row with ``tryout_date=None`` carrying the
                     registration URL and a JSON ``notes`` blob of
                     captured platform IDs.
      * Neither    — log WARNING and return [].
    """
    # Extract registration links from the RAW HTML before we decompose
    # site chrome (some anchors live in header/footer CTAs).
    reg = extract_registration_links(html)

    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    text = soup.get_text("\n", strip=True)
    tryout_date = parse_date(text)
    age_group = parse_age_group(text)
    gender = parse_gender(text)
    location = parse_location(soup)

    # Build platform-IDs payload for notes JSON.
    has_any_link = bool(
        reg["gotsport_event_ids"] or reg["tgs_event_ids"]
        or reg["leagueapps_urls"] or reg["sportngin_urls"]
        or reg["generic_urls"]
    )
    notes_payload: Optional[str] = None
    if has_any_link:
        compact = {
            k: v for k, v in reg.items()
            if k != "primary_url" and v
        }
        if compact:
            notes_payload = json.dumps({"registration": compact}, sort_keys=True)

    # Mode 1: dated row (current behavior).
    if tryout_date is not None:
        return [{
            "club_name_raw": club_name_raw,
            "tryout_date": tryout_date,
            "age_group": age_group,
            "gender": gender,
            "location": location,
            "source_url": source_url,
            "url": reg["primary_url"] or source_url,
            "notes": notes_payload,
        }]

    # Mode 2: registration-only row (NEW behavior).
    if has_any_link:
        logger.info(
            "[tryouts-wordpress] registration-only row for %s @ %s "
            "(no date parsed, but platform link captured)",
            club_name_raw, source_url,
        )
        return [{
            "club_name_raw": club_name_raw,
            "tryout_date": None,
            "age_group": age_group,
            "gender": gender,
            "location": location,
            "source_url": source_url,
            "url": reg["primary_url"],
            "notes": notes_payload,
        }]

    # Neither a date nor a registration link — drop.
    logger.warning(
        "[tryouts-wordpress] no date or registration link for %s @ %s",
        club_name_raw, source_url,
    )
    return []


def _fetch(url: str, timeout: int = 20) -> Optional[str]:
    """HEAD+GET with retry. Returns None on non-200 / network failure."""
    def _do() -> Optional[str]:
        r = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None
        return r.text
    try:
        return retry_with_backoff(
            _do,
            max_retries=1,
            base_delay=1.5,
            retryable_exceptions=(requests.exceptions.RequestException,),
            label=f"tryouts-wordpress:{url}",
        )
    except Exception as exc:
        logger.info("[tryouts-wordpress] fetch failed %s: %s", url, exc)
        return None


def scrape_tryouts_wordpress(club_sites: Iterable[Dict]) -> List[Dict]:
    """Walk each club's website probing the known tryout paths.

    ``club_sites`` iterable items are dicts with keys ``club_name_raw``
    and ``website`` (absolute URL, trailing slash optional). Stops
    probing paths for a given site on the first HTTP 200 that yields
    a parseable date.
    """
    rows: List[Dict] = []
    for entry in club_sites:
        club_name = (entry.get("club_name_raw") or "").strip()
        website = (entry.get("website") or "").strip()
        if not club_name or not website:
            continue
        base = website.rstrip("/")
        found_for_site = False
        for path in _TRYOUT_PATHS:
            if found_for_site:
                break
            url = urljoin(base + "/", path.lstrip("/"))
            html = _fetch(url)
            if not html:
                continue
            page_rows = parse_tryouts_page_html(
                html,
                club_name_raw=club_name,
                source_url=url,
            )
            if page_rows:
                rows.extend(page_rows)
                found_for_site = True
        if not found_for_site:
            logger.info(
                "[tryouts-wordpress] no tryout page found for %s (%s)",
                club_name, website,
            )
    logger.info("[tryouts-wordpress] produced %d tryout rows", len(rows))
    return rows
