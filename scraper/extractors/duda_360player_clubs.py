"""
duda_360player_clubs.py — Probe Duda CMS + 360Player club sites.

Two distinct platforms, one extractor — they share the same HTTP fetch
loop, the same ``cms_detect``-based gating, and the same JSON-LD
extraction contract via ``extractors.jsonld_parser``.

PLATFORM 1 — Duda CMS (generic SaaS site builder)
-------------------------------------------------
Detection: ``cms_detect.detect_cms`` → ``"duda"`` (signatures:
``Server: Duda`` header, ``irp.cdn-website.com`` references in body, or
``irp-cdn.multiscreensite.com``).

Yield expectation: SPARSE. Duda sites are mostly marketing pages without
structured data. When clubs do publish JSON-LD, it tends to be:

    * ``Event`` blocks for tryouts → mapped to ``tryouts`` rows.
    * ``Person`` blocks for staff → mapped (in a future PR) to
      ``coach_discoveries``. NOT written by this scaffold; we only
      surface counts so a follow-up has signal.
    * Occasionally ``Organization`` / ``LocalBusiness`` for the club
      itself — ignored here (canonical_clubs already has these clubs).

PLATFORM 2 — 360Player (soccer-specific club management SaaS)
-------------------------------------------------------------
Detection: ``cms_detect.detect_cms`` → ``"360player"`` (signatures
added in this PR — see cms_detect.py changes).

Yield expectation: TBD. The interesting structured data lives behind
auth-walled widget XHR endpoints (``app.360player.com/api/...``). We
do NOT attempt to bypass auth — our brief explicitly forbids it.

What we DO try:
    * Same JSON-LD pull from the public marketing page.
    * A simple HEAD/GET probe of the public club directory at
      ``https://360player.com/clubs`` for seed-list discovery (see
      ``discover_360player_directory`` below). If that page is
      unauthenticated and renders a club list server-side, we surface
      it for cross-reference against ``canonical_clubs``. If it is
      auth-walled or empty, we log + skip.

OUTPUT CONTRACT
---------------
Both platforms emit two row collections at most:

    {
        "tryouts": [
            {
                "club_name_raw": "...",
                "tryout_date": datetime | None,
                "age_group": str | None,
                "gender": str | None,
                "location": str | None,
                "source_url": str,
                "url": str | None,           # registration link if extractable
                "notes": str | None,         # JSON {"jsonld_event": {...}} blob
            }, ...
        ],
        "coach_discoveries": [...],          # placeholder — not written this PR
    }

Rows match the ``tryouts`` writer contract; ``coach_discoveries`` are
collected for future PR wiring.

FAIL-SOFT
---------
Network errors on ANY site are caught and logged at INFO; the runner
continues to the next site. A single misbehaving club site MUST NEVER
stop the batch. This mirrors the ``tryouts_wordpress`` pattern.

NOTES
-----
This extractor does NOT bypass authentication. 360Player widget data is
auth-walled by design; if the public surface yields nothing for a given
club we record that and move on. A future PR can wire credentialed
access via Playwright if a partnership materializes.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from extractors.cms_detect import detect_cms
from extractors.jsonld_parser import extract_jsonld, find_by_type, extract_persons

logger = logging.getLogger(__name__)


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}

# Subpaths probed beyond the homepage. Both platforms commonly route
# tryout / staff content via these conventional slugs. Order is
# most-specific → least-specific so we stop early on the first hit.
_PROBE_PATHS = (
    "/",
    "/tryouts",
    "/tryouts/",
    "/register",
    "/register/",
    "/registration",
    "/registration/",
    "/about",
    "/staff",
    "/coaches",
)

# Public 360Player directory. Scraped opportunistically — if the URL
# 404s or the page renders client-side only, ``discover_360player_directory``
# returns an empty list and the runner falls back to canonical_clubs.
_360PLAYER_DIRECTORY_URL = "https://360player.com/clubs"


# --------------------------------------------------------------------- platform-aware fetch


@dataclass
class ProbeResult:
    """One probed site's raw outcome — pre-extraction."""

    club_name_raw: str
    website: str
    detected_platform: Optional[str] = None
    probed_urls: List[str] = field(default_factory=list)
    jsonld_blocks: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None


def _fetch(url: str, timeout: int = 15) -> Optional[requests.Response]:
    """One-shot GET with no retry. Returns Response on 200, None otherwise.

    Kept deliberately simple — Duda + 360Player marketing pages are CDN
    cached and don't need backoff. A failure here is logged at INFO and
    the caller moves on (fail-soft).
    """
    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
    except requests.exceptions.RequestException as exc:
        logger.info("[duda-360player] fetch failed %s: %s", url, exc)
        return None
    if r.status_code != 200:
        logger.info("[duda-360player] non-200 %s: HTTP %s", url, r.status_code)
        return None
    return r


def probe_site(club_name_raw: str, website: str) -> ProbeResult:
    """Detect platform on the homepage, then probe known subpaths.

    Returns a ``ProbeResult`` with the detected platform and any
    JSON-LD blocks accumulated across probed pages. Subpath probing
    short-circuits as soon as the platform is detected AND we've
    pulled JSON-LD from at least one page (we don't keep digging if
    the homepage already produced structured data).

    Returns an empty ``ProbeResult`` (with ``detected_platform=None``)
    when the homepage 404s or returns a non-Duda/non-360player page.
    """
    result = ProbeResult(club_name_raw=club_name_raw, website=website)
    base = website.rstrip("/")

    # Step 1: homepage detection.
    home_resp = _fetch(base + "/")
    if home_resp is None:
        result.error = "homepage unreachable"
        return result
    platform = detect_cms(home_resp)
    if platform not in ("duda", "360player"):
        # Not our target — leave detected_platform None so the runner
        # logs + skips. We do NOT log a warning here; that would spam
        # the log when the seed is mostly other platforms.
        return result
    result.detected_platform = platform
    result.probed_urls.append(base + "/")
    result.jsonld_blocks.extend(extract_jsonld(home_resp.text))

    # Step 2: probe the rest of the candidate paths if homepage yielded
    # nothing structured. Once we have ANY JSON-LD blocks we stop —
    # the homepage is conventionally where the canonical Org / Event
    # data sits, and the extra requests just add latency.
    if result.jsonld_blocks:
        return result

    for path in _PROBE_PATHS:
        if path == "/":
            continue  # already fetched above
        url = urljoin(base + "/", path.lstrip("/"))
        resp = _fetch(url)
        if resp is None:
            continue
        result.probed_urls.append(url)
        new_blocks = extract_jsonld(resp.text)
        if new_blocks:
            result.jsonld_blocks.extend(new_blocks)
            return result  # first hit wins

    return result


# --------------------------------------------------------------------- JSON-LD → row mapping


_AGE_RE = re.compile(r"\b[Uu]-?(?P<age>\d{1,2})\b")
_GENDER_RE = re.compile(r"\b(?P<g>Boys|Girls|Men|Women|Boy|Girl|Male|Female)\b", re.IGNORECASE)


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a schema.org Event ``startDate`` (ISO-8601) into datetime.

    schema.org permits date or date-time strings. We accept either:
      * ``2026-08-05`` → midnight on that day
      * ``2026-08-05T18:00:00-04:00`` → tz-aware datetime collapsed to naive
        local on the trailing-Z form, else strip the timezone

    Returns None on any parse failure (best-effort).
    """
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00 so fromisoformat accepts it on 3.10.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        # Strip tz to keep the column type stable across rows.
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except ValueError:
        # Try plain date.
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _location_string(event: Dict[str, Any]) -> Optional[str]:
    """Extract a flat location string from a schema.org Event location.

    schema.org Event.location may be a string, a Place dict (with name +
    address), or a list. We return a best-effort flat string; downstream
    the writer routes it into ``location_name``.
    """
    loc = event.get("location")
    if loc is None:
        return None
    if isinstance(loc, str):
        return loc.strip() or None
    if isinstance(loc, dict):
        name = (loc.get("name") or "").strip()
        addr = loc.get("address")
        if isinstance(addr, str):
            addr_str = addr.strip()
        elif isinstance(addr, dict):
            parts = [
                addr.get("streetAddress"),
                addr.get("addressLocality"),
                addr.get("addressRegion"),
                addr.get("postalCode"),
            ]
            addr_str = ", ".join(p for p in parts if p)
        else:
            addr_str = ""
        joined = ", ".join(p for p in (name, addr_str) if p)
        return joined or None
    if isinstance(loc, list):
        for item in loc:
            s = _location_string({"location": item})
            if s:
                return s
    return None


def _parse_age_gender_from_text(text: str) -> tuple[Optional[str], Optional[str]]:
    """Best-effort age / gender extraction from an Event name or description."""
    age: Optional[str] = None
    gender: Optional[str] = None
    if not text:
        return age, gender
    m = _AGE_RE.search(text)
    if m:
        age = f"U{int(m.group('age'))}"
    m2 = _GENDER_RE.search(text)
    if m2:
        g = m2.group("g").lower()
        if g in ("boys", "boy", "men", "male"):
            gender = "M"
        elif g in ("girls", "girl", "women", "female"):
            gender = "F"
    return age, gender


def event_block_to_tryout_row(
    event: Dict[str, Any],
    *,
    club_name_raw: str,
    source_url: str,
) -> Optional[Dict[str, Any]]:
    """Convert one schema.org ``Event`` block into a tryouts row.

    Returns None if the block lacks BOTH a parseable ``startDate`` and a
    ``url`` — there's nothing useful to write without one of those.
    """
    if not isinstance(event, dict):
        return None
    name = (event.get("name") or "").strip()
    description = (event.get("description") or "").strip()

    tryout_date = _parse_iso_datetime(event.get("startDate"))
    url = (event.get("url") or "").strip() or None
    location = _location_string(event)
    age, gender = _parse_age_gender_from_text(f"{name} {description}")

    # If neither a date nor a URL is present, skip — this is the same
    # contract as tryouts_wordpress (a registration-only row needs a URL).
    if tryout_date is None and not url:
        return None

    notes_payload = json.dumps(
        {"jsonld_event": {"name": name, "url": url}},
        sort_keys=True,
    )

    return {
        "club_name_raw": club_name_raw,
        "tryout_date": tryout_date,
        "age_group": age,
        "gender": gender,
        "location": location,
        "source_url": source_url,
        "url": url or source_url,
        "notes": notes_payload,
    }


def person_block_to_coach_dict(
    person: Dict[str, Any],
    *,
    club_name_raw: str,
    source_url: str,
) -> Optional[Dict[str, Any]]:
    """Surface a ``Person`` JSON-LD block in a coach-discovery shape.

    NOT written to ``coach_discoveries`` by this PR — the runner just
    counts these for follow-up wiring (matches the brief: "occasional
    Person → coach_discoveries"). Returned shape is intentionally
    aligned with what the youth_club_coaches extractor emits so a
    future writer can consume both.
    """
    if not isinstance(person, dict):
        return None
    name = (person.get("name") or "").strip()
    if not name:
        return None
    return {
        "club_name_raw": club_name_raw,
        "name": name,
        "title": (person.get("jobTitle") or "").strip() or None,
        "email": (person.get("email") or "").strip() or None,
        "phone": (person.get("telephone") or "").strip() or None,
        "source_url": source_url,
        "platform_family": "duda_or_360player",
    }


def extract_rows_from_probe(probe: ProbeResult) -> Dict[str, List[Dict[str, Any]]]:
    """Map a ``ProbeResult`` to {tryouts, coach_discoveries} row lists.

    Pure function — fixture-driven. Callers (tests + the runner) should
    rely on this rather than reimplementing the JSON-LD → row mapping.
    """
    out: Dict[str, List[Dict[str, Any]]] = {"tryouts": [], "coach_discoveries": []}
    if not probe.jsonld_blocks:
        return out
    src = probe.probed_urls[0] if probe.probed_urls else probe.website

    for event in find_by_type(probe.jsonld_blocks, "Event"):
        row = event_block_to_tryout_row(
            event,
            club_name_raw=probe.club_name_raw,
            source_url=src,
        )
        if row is not None:
            out["tryouts"].append(row)

    for person in extract_persons(probe.jsonld_blocks):
        coach = person_block_to_coach_dict(
            person,
            club_name_raw=probe.club_name_raw,
            source_url=src,
        )
        if coach is not None:
            out["coach_discoveries"].append(coach)

    return out


# --------------------------------------------------------------------- 360Player public directory


def discover_360player_directory(
    *,
    fetch=None,
    directory_url: str = _360PLAYER_DIRECTORY_URL,
) -> List[Dict[str, str]]:
    """Best-effort scrape of the 360Player public clubs directory.

    Returns a list of ``{"club_name_raw", "website"}`` entries. Empty
    list on any failure — including 404, non-200, or simply no club
    anchors in the rendered HTML.

    The ``fetch`` parameter is injected for tests; production calls
    use the module-level ``_fetch``. We expose this hook explicitly
    rather than monkey-patching ``requests`` so the test stays local.

    Heuristic for "this anchor is a club":
        * Anchor href starts with the directory URL OR points at a
          ``360player.com/<slug>`` path.
        * Anchor text is non-empty and longer than 1 character.

    If the page is JS-rendered (likely — Next.js SSG+CSR is the default
    on 360player.com), the directory walk returns 0 entries and the
    runner falls back to ``canonical_clubs.website`` URL pattern
    detection.
    """
    fetcher = fetch if fetch is not None else _fetch
    resp = fetcher(directory_url)
    if resp is None:
        logger.info(
            "[duda-360player] 360Player directory unreachable at %s; "
            "falling back to canonical_clubs", directory_url,
        )
        return []

    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as exc:  # pragma: no cover — bs4 rarely raises
        logger.info("[duda-360player] directory parse failed: %s", exc)
        return []

    base = urlparse(directory_url)
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(strip=True)
        if not text or len(text) < 2:
            continue
        # Resolve relative URLs.
        full = urljoin(directory_url, href)
        parsed = urlparse(full)
        # Must be on 360player.com and not the directory itself.
        if "360player.com" not in (parsed.netloc or "").lower():
            continue
        path = (parsed.path or "").rstrip("/")
        if not path or path == "/clubs":
            continue
        # Crude club-slug guard: one or two path segments under root.
        # ``/clubs/foo`` or ``/foo`` both count; ``/about/team`` skipped.
        segments = [s for s in path.split("/") if s]
        if not segments or segments[0] in {"about", "blog", "support", "login", "pricing", "contact"}:
            continue
        if full in seen:
            continue
        seen.add(full)
        out.append({"club_name_raw": text, "website": full})

    logger.info(
        "[duda-360player] 360Player directory yielded %d candidate club(s)",
        len(out),
    )
    return out


# --------------------------------------------------------------------- public batch entry


def scrape_duda_360player_clubs(
    club_sites: Iterable[Dict[str, str]],
) -> Dict[str, Any]:
    """Walk ``club_sites``, probe each, return aggregated rows + counts.

    ``club_sites`` items are dicts with keys ``club_name_raw`` and
    ``website`` (absolute URL). Sites that fail detection or yield no
    JSON-LD are silently skipped (per fail-soft); their counts are
    surfaced in the return value for the runner to log.

    Returns::

        {
            "tryouts": [...],                 # ready for tryouts_writer
            "coach_discoveries": [...],       # NOT written this PR
            "stats": {
                "sites_probed": int,
                "duda_sites": int,
                "_360player_sites": int,
                "other_or_unknown": int,
                "sites_with_jsonld": int,
            },
        }
    """
    stats = {
        "sites_probed": 0,
        "duda_sites": 0,
        "_360player_sites": 0,
        "other_or_unknown": 0,
        "sites_with_jsonld": 0,
    }
    tryouts: List[Dict[str, Any]] = []
    coaches: List[Dict[str, Any]] = []

    for entry in club_sites:
        club_name = (entry.get("club_name_raw") or "").strip()
        website = (entry.get("website") or "").strip()
        if not club_name or not website:
            continue
        stats["sites_probed"] += 1
        probe = probe_site(club_name, website)
        if probe.detected_platform == "duda":
            stats["duda_sites"] += 1
        elif probe.detected_platform == "360player":
            stats["_360player_sites"] += 1
        else:
            stats["other_or_unknown"] += 1
            continue

        if probe.jsonld_blocks:
            stats["sites_with_jsonld"] += 1

        rows = extract_rows_from_probe(probe)
        tryouts.extend(rows["tryouts"])
        coaches.extend(rows["coach_discoveries"])

    logger.info(
        "[duda-360player] probed=%d duda=%d 360player=%d other=%d jsonld=%d "
        "tryouts=%d persons=%d",
        stats["sites_probed"], stats["duda_sites"], stats["_360player_sites"],
        stats["other_or_unknown"], stats["sites_with_jsonld"],
        len(tryouts), len(coaches),
    )

    return {
        "tryouts": tryouts,
        "coach_discoveries": coaches,
        "stats": stats,
    }


__all__ = [
    "ProbeResult",
    "probe_site",
    "extract_rows_from_probe",
    "event_block_to_tryout_row",
    "person_block_to_coach_dict",
    "discover_360player_directory",
    "scrape_duda_360player_clubs",
]
