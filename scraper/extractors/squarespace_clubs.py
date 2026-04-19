"""
squarespace_clubs.py — Schema.org / JSON-LD harvester for Squarespace
hosted youth-soccer club sites.

This is the first downstream consumer of the shared infra in
``extractors.cms_detect`` and ``extractors.jsonld_parser`` (PR #55).

Strategy
--------
For each club whose ``canonical_clubs.website`` resolves to a Squarespace
site (per ``detect_cms``), we fetch the homepage plus a small fixed list
of probable subpaths (``/team``, ``/teams``, ``/coaches``, ``/staff``,
``/about``, ``/roster``, ``/players``). For every page we successfully
fetch, we parse all ``<script type="application/ld+json">`` blocks,
flatten ``@graph`` containers, and route the typed nodes into four sinks:

  1. ``SportsTeam.athlete[]`` → ``club_roster_snapshots`` rows (writer:
     ``ingest.roster_snapshot_writer.insert_roster_snapshots``).
  2. ``Person`` blocks (top-level OR nested under ``member`` /
     ``employee`` / ``coach`` on Organization-like blocks) → coach
     entries upserted into ``coach_discoveries`` (mirrors
     ``extractors.youth_club_coaches`` upsert pattern).
  3. ``Event`` blocks whose name/description match tryout-or-clinic
     keywords → ``tryouts`` rows (writer:
     ``ingest.tryouts_writer.insert_tryouts``). Non-tryout events are
     ignored — events ingestion lives in dedicated runners (GotSport,
     SincSports, TGS).
  4. ``SportsOrganization`` / ``Organization`` metadata (logo, social
     links) → ``canonical_clubs`` enrichment (writer:
     ``ingest.club_enrichment_writer.update_club_enrichment``).

Idempotency
-----------
All four writers used here are idempotent on re-run — they upsert with
WHERE-guarded DO UPDATE predicates that short-circuit when nothing has
changed. Re-running the Squarespace pipeline against the same site
without changes is a no-op.

Fail-soft
---------
Per ``CLAUDE.md``: every per-site failure (network, parse, malformed
JSON-LD) is logged and dropped — the runner continues to the next site.
The Python-level ``scrape_run_logger`` records the per-run summary even
when DATABASE_URL is unset, via the no-op singleton.

Linker contract
---------------
Roster snapshot + tryout rows are written with ``club_id = NULL`` and
``club_name_raw`` set to the canonical club name (we already know the
canonical row — we started from it!). The canonical-club linker
(``scraper/canonical_club_linker.py``) resolves the FK after the writer
finishes. The coach-discovery rows DO carry ``club_id`` directly because
``coach_discoveries.club_id`` is part of the natural key — Squarespace
discovery is cheap to attribute since the seed rows are
canonical-clubs-derived.

Schema notes
------------
``coach_discoveries.platform_family`` enum is restricted to
``'sportsengine' | 'leagueapps' | 'wordpress' | 'unknown'`` (see
``coach_discoveries_platform_family_enum`` check). Squarespace is NOT
enumerated; we tag rows ``'unknown'`` rather than churn the schema in
this PR. A follow-up PR can extend the enum if Squarespace coverage
becomes large enough to warrant attribution at query time.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import requests

from extractors.cms_detect import detect_cms
from extractors.jsonld_parser import (
    extract_athletes,
    extract_jsonld,
    extract_persons,
    find_by_type,
)
from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRAPER_KEY = "squarespace-clubs"

# Browser-y UA — Squarespace serves a different (less JSON-LD-rich)
# response to obvious bot UAs; the same spoof other extractors use.
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Subpaths to probe per club. Kept intentionally short — we are NOT
# crawling the site, just sampling pages where club info is most likely
# to be embedded as JSON-LD. Order is a soft preference; we fetch all
# that return 200 and aggregate JSON-LD across them.
DEFAULT_SUBPATHS: tuple[str, ...] = (
    "/",
    "/team",
    "/teams",
    "/coaches",
    "/staff",
    "/about",
    "/roster",
    "/players",
)

REQUEST_TIMEOUT = 15  # seconds
RETRY_ATTEMPTS = 1    # one retry per fetch — keep the runtime bounded

# Tryout / ID-clinic keyword set. Matched (case-insensitive) against the
# concatenation of an Event's ``name`` and ``description`` fields.
_TRYOUT_KEYWORDS_RE = re.compile(
    r"\b(try-?out|tryouts|id\s*clinic|id\s*camp|player\s*evaluation|"
    r"open\s*tryout|open\s*training|combine|player\s*assessment|"
    r"identification\s*clinic)\b",
    re.IGNORECASE,
)

# Confidence stamped on every discovery — Squarespace JSON-LD is
# self-published structured data, so the precision is high relative
# to HTML scraping. Value matches what ``youth_club_coaches`` uses for
# discovered (vs. canonical) staff pages.
DISCOVERY_CONFIDENCE = 0.85


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SquarespaceClubSite:
    """One canonical-clubs row to probe."""
    club_id: int
    club_name_canonical: str
    website: str
    state: Optional[str] = None


@dataclass
class SquarespaceHarvest:
    """Aggregate per-club extraction result.

    Each list maps directly onto a writer signature. The runner is
    responsible for handing each list to its corresponding writer; this
    extractor stays purely about parsing.
    """
    club_id: int
    club_name: str
    pages_fetched: int = 0
    is_squarespace: bool = False
    roster_rows: List[Dict[str, Any]] = field(default_factory=list)
    coach_rows: List[Dict[str, Any]] = field(default_factory=list)
    tryout_rows: List[Dict[str, Any]] = field(default_factory=list)
    enrichment_row: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
    })
    return s


def _fetch(
    session: requests.Session,
    url: str,
    *,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[requests.Response]:
    """Fetch ``url`` returning the ``Response`` (so detection can read
    headers + body). Returns ``None`` on any failure — caller logs +
    skips."""
    def _do() -> Optional[requests.Response]:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        if r.status_code != 200:
            return None
        ct = r.headers.get("content-type", "")
        if "html" not in ct and "text" not in ct:
            # Squarespace serves XML for /sitemap, JSON for some APIs;
            # we only want HTML pages with embedded JSON-LD.
            return None
        return r
    try:
        return retry_with_backoff(
            _do,
            max_retries=RETRY_ATTEMPTS,
            base_delay=1.5,
            retryable_exceptions=(requests.exceptions.RequestException,),
            label=f"squarespace:{url}",
        )
    except Exception as exc:
        logger.info("[squarespace-clubs] fetch failed %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Pure parsers — fixture-testable
# ---------------------------------------------------------------------------

def is_tryout_event(event: Dict[str, Any]) -> bool:
    """Return ``True`` if an Event block looks like a tryout/clinic.

    Combines ``name`` + ``description`` (whichever exist) and matches
    against the tryout-keyword set. Avoids false positives like a
    "Spring Tournament" or "Annual Picnic" that would otherwise spam
    the tryouts table.
    """
    parts: List[str] = []
    for key in ("name", "description"):
        v = event.get(key)
        if isinstance(v, str):
            parts.append(v)
    blob = " ".join(parts)
    if not blob:
        return False
    return bool(_TRYOUT_KEYWORDS_RE.search(blob))


def _parse_event_date(value: Any) -> Optional[datetime]:
    """Best-effort ISO-8601 → datetime. Returns None if unparseable."""
    if not isinstance(value, str) or not value:
        return None
    # schema.org Event.startDate is typically ISO-8601 (YYYY-MM-DD or
    # full timestamp). datetime.fromisoformat handles both in 3.11+.
    try:
        # Strip trailing Z (Python 3.10 fromisoformat doesn't accept it).
        v = value.rstrip("Z")
        return datetime.fromisoformat(v)
    except ValueError:
        # Last-ditch: just the YYYY-MM-DD prefix.
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return None


def _event_location_string(event: Dict[str, Any]) -> Optional[str]:
    """Squash schema.org ``Place`` into a flat free-text location string.

    Squarespace pages emit ``location`` as either a string OR a Place
    dict with ``name`` / ``address`` (sometimes nested PostalAddress).
    """
    loc = event.get("location")
    if isinstance(loc, str):
        return loc.strip() or None
    if isinstance(loc, dict):
        name = loc.get("name") if isinstance(loc.get("name"), str) else None
        addr = loc.get("address")
        if isinstance(addr, dict):
            # PostalAddress
            parts = [
                addr.get("streetAddress"),
                addr.get("addressLocality"),
                addr.get("addressRegion"),
            ]
            addr_str = ", ".join(p for p in parts if isinstance(p, str) and p)
        elif isinstance(addr, str):
            addr_str = addr
        else:
            addr_str = ""
        combined = ", ".join(p for p in (name, addr_str) if p)
        return combined or None
    return None


def _athlete_to_roster_row(
    athlete: Dict[str, Any],
    *,
    team: Dict[str, Any],
    club_name_raw: str,
    source_url: str,
    snapshot_date: datetime,
) -> Optional[Dict[str, Any]]:
    """Convert a JSON-LD athlete dict into a roster_snapshots row.

    Drops athletes without a name. Pulls jersey number from any of
    ``identifier`` / ``jerseyNumber`` / ``jersey``, position from
    ``jobTitle`` (schema.org convention).
    """
    name = athlete.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    jersey = (
        athlete.get("jerseyNumber")
        or athlete.get("jersey")
        or athlete.get("identifier")
    )
    if jersey is not None:
        jersey = str(jersey).strip() or None
    position = athlete.get("jobTitle")
    if position is not None:
        position = str(position).strip() or None

    # schema.org SportsTeam may carry an ``athleteOf`` / ``memberOf``
    # back-reference. We don't use it — we already know which club this
    # is from the seed.
    return {
        "club_name_raw": club_name_raw,
        "source_url": source_url,
        "snapshot_date": snapshot_date,
        "season": None,         # Squarespace JSON-LD rarely carries season
        "age_group": _team_age_group(team),
        "gender": _team_gender(team),
        "division": None,
        "player_name": name.strip(),
        "jersey_number": jersey,
        "position": position,
        "grad_year": None,
        "state": None,
        "event_id": None,
    }


def _team_age_group(team: Dict[str, Any]) -> Optional[str]:
    """Best-effort ``U<n>`` extraction from a SportsTeam name."""
    name = team.get("name")
    if not isinstance(name, str):
        return None
    m = re.search(r"\b[Uu]-?(\d{1,2})\b", name)
    if m:
        return f"U{int(m.group(1))}"
    return None


def _team_gender(team: Dict[str, Any]) -> Optional[str]:
    """Best-effort ``M`` / ``F`` from a SportsTeam name."""
    name = team.get("name")
    if not isinstance(name, str):
        return None
    low = name.lower()
    if re.search(r"\b(boys?|men|male)\b", low):
        return "M"
    if re.search(r"\b(girls?|women|female)\b", low):
        return "F"
    return None


def _person_to_coach_row(
    person: Dict[str, Any],
    *,
    club_id: int,
    source_url: str,
) -> Optional[Dict[str, Any]]:
    """Convert a JSON-LD Person dict to a coach_discoveries row.

    Drops persons without a name. Title comes from ``jobTitle``; email
    is unwrapped from ``mailto:`` if Squarespace embedded it that way.
    """
    name = person.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    title = person.get("jobTitle")
    if title is not None:
        title = str(title).strip() or None

    # schema.org Person.email is typically a bare email; sometimes a
    # ContactPoint dict. We accept either. Strip an optional ``mailto:``
    # prefix case-insensitively (real Squarespace pages have been
    # observed emitting both ``mailto:`` and ``MAILTO:``).
    email = person.get("email")
    if isinstance(email, dict):
        email = email.get("email")
    if isinstance(email, str):
        cleaned = email.strip()
        if cleaned.lower().startswith("mailto:"):
            cleaned = cleaned[len("mailto:"):]
        email = cleaned.split("?")[0].strip().lower() or None
    else:
        email = None

    phone = person.get("telephone")
    if isinstance(phone, str):
        phone = phone.strip() or None
    else:
        phone = None

    return {
        "club_id": club_id,
        "name": name.strip(),
        "title": title or "",
        "email": email,
        "phone": phone,
        "source_url": source_url,
        "confidence": DISCOVERY_CONFIDENCE,
        # Schema enum is restricted; squarespace is NOT enumerated yet
        # so we stamp 'unknown' to satisfy the CHECK constraint.
        "platform_family": "unknown",
    }


def _event_to_tryout_row(
    event: Dict[str, Any],
    *,
    club_name_raw: str,
    source_url: str,
) -> Dict[str, Any]:
    """Convert a tryout-keyword-matching Event into a tryouts row."""
    return {
        "club_name_raw": club_name_raw,
        "tryout_date": _parse_event_date(event.get("startDate")),
        "age_group": None,           # JSON-LD Events rarely carry age groups
        "gender": None,
        "location": _event_location_string(event),
        "source_url": source_url,
        "url": event.get("url") if isinstance(event.get("url"), str) else source_url,
        "notes": None,
        "source": "site_monitor",
        "status": "upcoming",
    }


def _organization_to_enrichment_row(
    org: Dict[str, Any],
    *,
    club_id: int,
) -> Optional[Dict[str, Any]]:
    """Convert an Organization / SportsOrganization block to an
    enrichment row. Returns None if no usable signal is present."""
    logo = org.get("logo")
    if isinstance(logo, dict):
        logo = logo.get("url") or logo.get("contentUrl")
    if not isinstance(logo, str):
        logo = None

    # schema.org carries social URLs in ``sameAs`` (a list or string).
    same_as = org.get("sameAs")
    same_urls: List[str] = []
    if isinstance(same_as, str):
        same_urls = [same_as]
    elif isinstance(same_as, list):
        same_urls = [u for u in same_as if isinstance(u, str)]

    instagram = next(
        (u for u in same_urls if "instagram.com" in u.lower()), None
    )
    facebook = next(
        (u for u in same_urls if "facebook.com" in u.lower()), None
    )
    twitter = next(
        (u for u in same_urls
         if "twitter.com" in u.lower() or "x.com/" in u.lower()),
        None,
    )

    if not any((logo, instagram, facebook, twitter)):
        return None

    return {
        "club_id": club_id,
        "logo_url": logo,
        "instagram": instagram,
        "facebook": facebook,
        "twitter": twitter,
        "staff_page_url": None,
        "website_status": "ok",
        "scrape_confidence": DISCOVERY_CONFIDENCE,
    }


# ---------------------------------------------------------------------------
# Main per-club harvest
# ---------------------------------------------------------------------------

def _normalize_base(website: str) -> Optional[str]:
    """Return the website's scheme://host base, or None if unparseable."""
    try:
        u = urlparse(website.strip())
        if not u.scheme or not u.netloc:
            return None
        return f"{u.scheme}://{u.netloc}"
    except Exception:
        return None


def harvest_squarespace_club(
    site: SquarespaceClubSite,
    *,
    session: Optional[requests.Session] = None,
    subpaths: Iterable[str] = DEFAULT_SUBPATHS,
    snapshot_date: Optional[datetime] = None,
) -> SquarespaceHarvest:
    """Detect, fetch, and aggregate JSON-LD across a single club site.

    Returns a ``SquarespaceHarvest`` with whatever was found. If the
    site isn't Squarespace, returns the harvest with ``is_squarespace=
    False`` and empty lists — caller skips writing.
    """
    own_session = session is None
    if own_session:
        session = _get_session()

    snap = snapshot_date or datetime.utcnow()
    harvest = SquarespaceHarvest(
        club_id=site.club_id, club_name=site.club_name_canonical
    )

    base = _normalize_base(site.website)
    if base is None:
        logger.info(
            "[squarespace-clubs] skipping unparseable website: %r",
            site.website,
        )
        return harvest

    try:
        # Stage 1: detection. Fetch homepage, run cms_detect.
        home = _fetch(session, base + "/")
        if home is None:
            logger.info(
                "[squarespace-clubs] homepage fetch failed for %s (%s)",
                site.club_name_canonical, base,
            )
            return harvest

        cms = detect_cms(home)
        if cms != "squarespace":
            logger.debug(
                "[squarespace-clubs] %s is not Squarespace (cms=%s)",
                base, cms,
            )
            # Fail-soft: not a Squarespace site. Caller will skip.
            return harvest

        harvest.is_squarespace = True
        harvest.pages_fetched = 1

        # Aggregate JSON-LD across homepage + each probed subpath.
        all_blocks: List[Dict[str, Any]] = []
        # Track which URL each block came from so we can attribute rows.
        page_blocks: List[tuple[str, List[Dict[str, Any]]]] = []

        home_blocks = extract_jsonld(home.text)
        if home_blocks:
            page_blocks.append((home.url or base + "/", home_blocks))
            all_blocks.extend(home_blocks)

        for path in subpaths:
            if path in ("/", ""):
                continue  # already fetched as homepage
            url = urljoin(base + "/", path.lstrip("/"))
            resp = _fetch(session, url)
            if resp is None:
                continue
            harvest.pages_fetched += 1
            blocks = extract_jsonld(resp.text)
            if blocks:
                page_blocks.append((resp.url or url, blocks))
                all_blocks.extend(blocks)

        if not all_blocks:
            logger.info(
                "[squarespace-clubs] no JSON-LD found across %d pages for %s",
                harvest.pages_fetched, site.club_name_canonical,
            )
            return harvest

        # ---- Sink 1: SportsTeam.athlete[] → roster snapshots ----
        for source_url, blocks in page_blocks:
            for team in find_by_type(blocks, "SportsTeam"):
                for athlete in extract_athletes([team]):
                    row = _athlete_to_roster_row(
                        athlete,
                        team=team,
                        club_name_raw=site.club_name_canonical,
                        source_url=source_url,
                        snapshot_date=snap,
                    )
                    if row is not None:
                        harvest.roster_rows.append(row)

        # ---- Sink 2: Person blocks → coach_discoveries ----
        seen_coach_keys: set = set()
        for source_url, blocks in page_blocks:
            for person in extract_persons(blocks):
                row = _person_to_coach_row(
                    person,
                    club_id=site.club_id,
                    source_url=source_url,
                )
                if row is None:
                    continue
                # Dedup by (name, title) within a single harvest — the
                # writer's natural-key constraint catches cross-run
                # duplicates but within-run duplicates would fight the
                # ON CONFLICT predicate redundantly.
                key = (row["name"].lower(), (row.get("title") or "").lower())
                if key in seen_coach_keys:
                    continue
                seen_coach_keys.add(key)
                harvest.coach_rows.append(row)

        # ---- Sink 3: Tryout-keyword Events → tryouts ----
        seen_tryout_keys: set = set()
        for source_url, blocks in page_blocks:
            for event in find_by_type(blocks, "Event"):
                if not is_tryout_event(event):
                    continue
                row = _event_to_tryout_row(
                    event,
                    club_name_raw=site.club_name_canonical,
                    source_url=source_url,
                )
                # Dedup within harvest by (date, name string).
                key = (
                    row.get("tryout_date"),
                    str(event.get("name") or "").lower(),
                )
                if key in seen_tryout_keys:
                    continue
                seen_tryout_keys.add(key)
                harvest.tryout_rows.append(row)

        # ---- Sink 4: SportsOrganization/Organization → enrichment ----
        for blocks in (b for _u, b in page_blocks):
            for org in find_by_type(blocks, "SportsOrganization") + find_by_type(
                blocks, "Organization"
            ):
                row = _organization_to_enrichment_row(
                    org, club_id=site.club_id
                )
                if row is not None:
                    # First org block with usable enrichment wins;
                    # don't overwrite with a second org node on a
                    # different page (those are usually less complete).
                    harvest.enrichment_row = row
                    break
            if harvest.enrichment_row is not None:
                break

        logger.info(
            "[squarespace-clubs] %s: pages=%d roster=%d coaches=%d "
            "tryouts=%d enrichment=%s",
            site.club_name_canonical,
            harvest.pages_fetched,
            len(harvest.roster_rows),
            len(harvest.coach_rows),
            len(harvest.tryout_rows),
            "yes" if harvest.enrichment_row else "no",
        )
        return harvest

    finally:
        if own_session and session is not None:
            try:
                session.close()
            except Exception:
                pass


def harvest_squarespace_clubs(
    sites: Iterable[SquarespaceClubSite],
    *,
    session: Optional[requests.Session] = None,
    snapshot_date: Optional[datetime] = None,
) -> List[SquarespaceHarvest]:
    """Convenience wrapper — call ``harvest_squarespace_club`` per site."""
    own_session = session is None
    if own_session:
        session = _get_session()
    out: List[SquarespaceHarvest] = []
    try:
        for site in sites:
            try:
                out.append(
                    harvest_squarespace_club(
                        site, session=session, snapshot_date=snapshot_date
                    )
                )
            except Exception as exc:
                # Fail-soft per CLAUDE.md — don't let one site abort the run.
                logger.warning(
                    "[squarespace-clubs] harvest crashed for %s: %s",
                    site.club_name_canonical, exc,
                )
    finally:
        if own_session and session is not None:
            try:
                session.close()
            except Exception:
                pass
    return out
