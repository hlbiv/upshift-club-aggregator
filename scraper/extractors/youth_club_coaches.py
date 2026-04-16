"""
Youth club coaching staff scraper — writes to ``coach_discoveries``.

Ported from the TypeScript scraper in the sibling player-platform repo
(``youth-club-staff-general-scraper.ts``). Key design decisions preserved:

- **Multi-strategy HTML extraction**: card-style divs, table rows with
  header detection, and generic card/div layouts (same 3 strategies as
  ``college_coaches.py``).
- **Title-based blocklist**: filters non-coach staff (trainers,
  equipment managers, video coordinators, groundskeepers, etc.).
- **Head coach detection**: title containing "head coach", "head soccer",
  or "director of soccer" sets ``is_head_coach = True``.
- **Count guard**: strategies with >15 results are treated as false
  positives and skipped.
- **Platform-aware URL paths**: SportsEngine sites get SE-specific
  paths tried first.
- **Rate limiting**: >= 1.5 s between HTTP requests.
- **Graceful degradation**: 404s, timeouts, and unparseable pages are
  logged and skipped, not fatal.

Targets ``canonical_clubs`` with a non-null ``website``, excluding
NCAA programs (URLs containing 'athletics.com' or clubs linked to
the ``colleges`` table).

CLI::

    python -m scraper.extractors.youth_club_coaches \\
        [--limit 5] [--state GA] [--platform-family sportsengine] \\
        [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Sibling package imports (scraper.*)
# ---------------------------------------------------------------------------

_SCRAPER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except ImportError:
    psycopg2 = None  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

REQUEST_TIMEOUT = 15  # seconds
RETRY_ATTEMPTS = 2
RETRY_DELAY = 1.0  # seconds between retries
RATE_LIMIT_DELAY = 1.5  # seconds between clubs

PARSE_COUNT_GUARD = 15  # strategies returning more are false positives

SCRAPER_KEY = "youth-club-coaches"

# ---------------------------------------------------------------------------
# Staff page URL paths — probed in order per club
# ---------------------------------------------------------------------------

# SportsEngine-specific paths (tried first when platform detected)
SPORTSENGINE_PATHS = [
    "/staff",
    "/coaches",
    "/about/staff",
    "/about/coaches",
    "/club-info/staff",
]

# Generic paths — work across most platforms
GENERIC_STAFF_PATHS = [
    "/staff",
    "/coaches",
    "/coaching-staff",
    "/about/coaching-staff",
    "/about-us",
    "/our-staff",
    "/about/staff",
    "/about/coaches",
    "/club-staff",
    "/team-staff",
    "/club/staff",
    "/leadership",
]

# ---------------------------------------------------------------------------
# Title-based blocklist — entries whose title matches any of these
# are not coaches and should be filtered out.
# ---------------------------------------------------------------------------

TITLE_BLOCKLIST_PATTERNS = [
    r"athletic\s+trainer",
    r"equipment\s+manager",
    r"strength",
    r"conditioning",
    r"academic",
    r"administrator",
    r"director\s+of\s+operations",
    r"video\s+coordinator",
    r"team\s+manager",
    r"sports?\s+information",
    r"communications",
    r"marketing",
    r"compliance",
    r"athletic\s+director",
    r"facilities",
    r"dietitian",
    r"nutritionist",
    r"psycholog",
    r"medical",
    r"physician",
    r"physical\s+therap",
    r"photographer",
    r"groundskeeper",
    r"field\s+manager",
    r"registrar",
    r"treasurer",
    r"secretary",
    r"bookkeeper",
    r"accountant",
    r"webmaster",
    r"social\s+media",
    r"graphic\s+design",
    r"intern\b",
]

_BLOCKLIST_RE = re.compile(
    "|".join(TITLE_BLOCKLIST_PATTERNS), re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Name validation — ported from TS looksLikeName()
# ---------------------------------------------------------------------------

_NAME_BLOCKLIST = {
    "about us", "contact us", "click here", "read more", "learn more",
    "meet the", "meet our", "our staff", "our team", "coaching staff",
    "support staff", "athletic staff", "staff directory",
    "head coach", "assistant coach", "associate head", "associate coach",
    "volunteer coach", "graduate assistant", "director of coaching",
    "director of operations", "athletic director", "technical director",
    "club director", "club president", "executive director",
    "men soccer", "women soccer", "mens soccer", "womens soccer",
    "soccer coaches", "coaching team", "soccer team",
    "social media", "quick links", "campus map", "office hours",
    "follow us", "connect with", "stay connected", "more information",
    "sign up", "log in", "new member", "member login",
}

_BLOCKLIST_TOKENS = {
    "soccer", "football", "basketball", "baseball", "softball", "volleyball",
    "lacrosse", "swimming", "tennis", "golf", "track", "wrestling",
    "coach", "coaching", "staff", "director", "athletic", "athletics",
    "university", "college", "school", "program", "department", "club",
    "email", "phone", "fax", "office", "contact", "bio",
    "schedule", "roster", "recruiting", "camps", "news", "media",
    "facebook", "twitter", "instagram", "youtube", "tiktok",
    "home", "about", "menu", "search", "login", "signup", "register",
    "calendar", "events", "tournament", "league", "division",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CoachEntry:
    name: str
    title: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    is_head_coach: bool = False


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


def fetch_with_retry(
    session: requests.Session,
    url: str,
    retries: int = RETRY_ATTEMPTS,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[str]:
    """Fetch a URL with retry + backoff. Returns HTML text or None."""
    for attempt in range(retries + 1):
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            if "html" not in ct and "text" not in ct:
                return None
            return resp.text
        except requests.RequestException:
            if attempt == retries:
                return None
            time.sleep(RETRY_DELAY * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# Name validation — ported from TS looksLikeName()
# ---------------------------------------------------------------------------

def looks_like_name(text: str) -> bool:
    """Return True if *text* looks like a person name (2-4 Title-case tokens,
    no digits, not all-caps, not a blocklisted phrase/token)."""
    t = text.strip()
    if len(t) < 4 or len(t) > 50:
        return False
    parts = t.split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    if not parts[0][0].isupper():
        return False
    if t == t.upper():
        return False
    if not all(p[0].isalpha() for p in parts):
        return False
    if re.search(r"\d", t):
        return False
    normalized = re.sub(r"[^a-z ]", "", t.lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    if normalized in _NAME_BLOCKLIST:
        return False
    lower_parts = [re.sub(r"[^a-z]", "", p.lower()) for p in parts]
    if any(p in _BLOCKLIST_TOKENS for p in lower_parts):
        return False
    return True


# ---------------------------------------------------------------------------
# Title filtering + head coach detection
# ---------------------------------------------------------------------------

def is_blocked_title(title: Optional[str]) -> bool:
    """Return True if the title matches the non-coach blocklist."""
    if not title:
        return False
    return bool(_BLOCKLIST_RE.search(title))


def detect_head_coach(title: Optional[str]) -> bool:
    """Return True if the title indicates a head coach."""
    if not title:
        return False
    lower = title.lower()
    return (
        "head coach" in lower
        or "head soccer" in lower
        or "director of soccer" in lower
    )


# ---------------------------------------------------------------------------
# Email extraction
# ---------------------------------------------------------------------------

def extract_email(el: Tag) -> Optional[str]:
    """Extract email from an element via mailto: link or text regex."""
    mailto = el.find("a", href=re.compile(r"^mailto:", re.IGNORECASE))
    if mailto:
        href = mailto.get("href", "")
        email = href.replace("mailto:", "").split("?")[0].strip().lower()
        if "@" in email:
            return email
    text = el.get_text()
    m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", text)
    return m.group(0).lower() if m else None


# ---------------------------------------------------------------------------
# Phone extraction
# ---------------------------------------------------------------------------

_PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}")


def extract_phone(el: Tag) -> Optional[str]:
    """Extract a US phone number from an element via tel: link or text regex."""
    tel = el.find("a", href=re.compile(r"^tel:", re.IGNORECASE))
    if tel:
        href = tel.get("href", "")
        digits = re.sub(r"[^\d]", "", href.replace("tel:", ""))
        if len(digits) >= 10:
            return f"({digits[-10:-7]}) {digits[-7:-4]}-{digits[-4:]}"
    text = el.get_text()
    m = _PHONE_RE.search(text)
    return m.group(0) if m else None


# ---------------------------------------------------------------------------
# Platform detection from URL
# ---------------------------------------------------------------------------

def detect_platform(url: str, html: str = "") -> str:
    """Detect the club website platform family from the URL and page HTML."""
    url_lower = url.lower()
    if "sportsengine.com" in url_lower or "sportngin" in url_lower:
        return "sportsengine"
    if "leagueapps.com" in url_lower:
        return "leagueapps"
    html_lower = html[:5000].lower() if html else ""
    if "wp-content" in html_lower or "wordpress" in html_lower:
        return "wordpress"
    return "unknown"


def get_staff_paths(platform: str) -> List[str]:
    """Return ordered staff page paths based on detected platform."""
    if platform == "sportsengine":
        return SPORTSENGINE_PATHS
    return GENERIC_STAFF_PATHS


# ---------------------------------------------------------------------------
# HTML parsing — three strategies, matching college_coaches.py
# ---------------------------------------------------------------------------

def parse_staff_html(html: str, club_name: str = "") -> List[CoachEntry]:
    """Extract coach entries from a youth club staff page.

    Three strategies are tried in order:

    1. **Card-style staff members** — ``.staff-member``, ``.coach-card``,
       and similar card selectors with semantic CSS classes.
    2. **Table rows** — ``<table>`` with ``<td>`` cells containing
       name + title data.
    3. **Generic card/div layout** — broad card selectors.

    Results are filtered through the title blocklist and name validator.
    Strategies returning more than PARSE_COUNT_GUARD results are treated
    as false positives and skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    coaches: List[CoachEntry] = []
    seen: set = set()

    # --- Strategy 1: Card-style staff members ---
    card_selectors = [
        ".staff-member",
        ".coach-card",
        ".staff-card",
        ".coach-item",
        ".staff-item",
        ".coaching-staff-item",
        ".c-coaching-staff__item",
        ".sidearm-staff-member",
        ".sidearm-staff-card",
        '[class*="coach-card"]',
        '[class*="staff-card"]',
        '[class*="staff-member"]',
        '[class*="coach-item"]',
    ]

    for sel in card_selectors:
        els = soup.select(sel)
        if not els:
            continue

        candidates: List[CoachEntry] = []
        candidate_seen: set = set()
        for el in els:
            # Find name element
            name_el = el.select_one(
                '[class*="name"], h2, h3, h4, strong, '
                '.coach-name, .staff-name, .card-title, '
                '.sidearm-staff-member-name a, .sidearm-staff-member-name'
            )
            name = name_el.get_text().strip() if name_el else ""
            if not name:
                a_el = el.select_one("a")
                name = a_el.get_text().strip() if a_el else ""
            if not looks_like_name(name):
                continue

            # Find title element
            title_el = el.select_one(
                '[class*="title"], [class*="position"], [class*="role"], '
                '.coach-title, .staff-title, .card-subtitle, '
                '.sidearm-staff-member-title, p'
            )
            title = title_el.get_text().strip() if title_el else None
            if title and title.lower() == name.lower():
                title = None
            if is_blocked_title(title):
                continue

            email = extract_email(el)
            phone = extract_phone(el)
            key = name.lower()
            if key not in candidate_seen:
                candidate_seen.add(key)
                candidates.append(CoachEntry(
                    name=name,
                    title=title or None,
                    email=email,
                    phone=phone,
                    is_head_coach=detect_head_coach(title),
                ))

        if len(candidates) > PARSE_COUNT_GUARD:
            logger.debug(
                "  [%s]: selector '%s' returned %d — false positive, skipping",
                club_name, sel, len(candidates),
            )
            continue

        if candidates:
            for c in candidates:
                if c.name.lower() not in seen:
                    seen.add(c.name.lower())
                    coaches.append(c)
            break

    if coaches:
        return coaches

    # --- Strategy 2: Table rows with count guard ---
    table_candidates: List[CoachEntry] = []
    table_seen: set = set()
    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            texts = [re.sub(r"\s+", " ", c.get_text()).strip() for c in cells]
            name = None
            for t in texts:
                if looks_like_name(t):
                    name = t
                    break
            if not name:
                continue
            # Title is the first non-name cell with reasonable length
            title = None
            for t in texts:
                if t != name and 3 < len(t) < 80:
                    title = t
                    break
            if is_blocked_title(title):
                continue
            email = extract_email(row)
            phone = extract_phone(row)
            key = name.lower()
            if key not in table_seen:
                table_seen.add(key)
                table_candidates.append(CoachEntry(
                    name=name,
                    title=title or None,
                    email=email,
                    phone=phone,
                    is_head_coach=detect_head_coach(title),
                ))

    if table_candidates:
        if len(table_candidates) > PARSE_COUNT_GUARD:
            logger.debug(
                "  [%s]: table returned %d rows — false positive, bailing",
                club_name, len(table_candidates),
            )
        else:
            for c in table_candidates:
                if c.name.lower() not in seen:
                    seen.add(c.name.lower())
                    coaches.append(c)

    if coaches:
        return coaches

    # --- Strategy 3: Generic card/div layout ---
    generic_selectors = [
        ".team-member",
        ".member-card",
        ".person-card",
        ".leadership-card",
        ".bio-card",
        '[class*="team-member"]',
        '[class*="person-card"]',
        '[class*="bio-card"]',
    ]
    for sel in generic_selectors:
        els = soup.select(sel)
        if not els:
            continue

        gen_candidates: List[CoachEntry] = []
        gen_seen: set = set()
        for el in els:
            name_el = el.select_one(
                '[class*="name"], .coach-name, .staff-name, h2, h3, h4, strong'
            )
            name = name_el.get_text().strip() if name_el else ""
            if not name:
                a_el = el.select_one("a")
                name = a_el.get_text().strip() if a_el else ""
            if not looks_like_name(name):
                continue

            title_el = el.select_one(
                '[class*="title"], [class*="position"], [class*="role"], '
                '.coach-title, .staff-title, p'
            )
            title = title_el.get_text().strip() if title_el else None
            if title and not title.strip():
                title = None
            if is_blocked_title(title):
                continue

            email = extract_email(el)
            phone = extract_phone(el)
            key = name.lower()
            if key not in gen_seen:
                gen_seen.add(key)
                gen_candidates.append(CoachEntry(
                    name=name,
                    title=title or None,
                    email=email,
                    phone=phone,
                    is_head_coach=detect_head_coach(title),
                ))

        if len(gen_candidates) > PARSE_COUNT_GUARD:
            logger.debug(
                "  [%s]: selector '%s' returned %d — false positive, skipping",
                club_name, sel, len(gen_candidates),
            )
            continue

        if gen_candidates:
            for c in gen_candidates:
                if c.name.lower() not in seen:
                    seen.add(c.name.lower())
                    coaches.append(c)
            break

    return coaches


# ---------------------------------------------------------------------------
# URL discovery — find the staff page for a club
# ---------------------------------------------------------------------------

def discover_staff_url(
    session: requests.Session,
    website: str,
    platform: str = "unknown",
) -> Optional[Tuple[str, float]]:
    """Try to find the coaching staff page URL for a club.

    Returns (url, confidence) or None. Confidence is 0.90 for
    platform-detected pages, 0.75 for generic.
    """
    base = website.rstrip("/")
    paths = get_staff_paths(platform)

    for path in paths:
        url = f"{base}{path}"
        html = fetch_with_retry(session, url)
        if html and len(html) > 500:
            # Platform-detected pages get higher confidence
            confidence = 0.90 if platform != "unknown" else 0.75
            return url, confidence
        time.sleep(0.3)

    return None


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _get_connection():
    """Get a psycopg2 connection from DATABASE_URL. Returns None if unavailable."""
    if psycopg2 is None:
        return None
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        conn = psycopg2.connect(url)
        return conn
    except Exception as exc:
        logger.warning("DB connect failed: %s", exc)
        return None


# Regex to exclude NCAA program URLs
_NCAA_URL_RE = re.compile(
    r"(athletics\.com|goheels\.com|goduke|gocards|goterps|gofrogs|"
    r"scarletknights|huskers\.com|hawkeyesports|ohiostatebuckeyes|"
    r"baylorbears|gostanford|calbears)",
    re.IGNORECASE,
)


def _fetch_clubs(
    conn,
    limit: Optional[int] = None,
    state: Optional[str] = None,
    platform_family: Optional[str] = None,
) -> List[Dict]:
    """Query canonical_clubs for youth clubs with websites.

    Excludes NCAA programs by filtering out athletics.com-pattern URLs
    and clubs that exist in the colleges table.
    """
    clauses = [
        "c.website IS NOT NULL",
        "c.website != ''",
    ]
    params: List = []

    if state:
        clauses.append("c.state = %s")
        params.append(state.upper())

    where = " AND ".join(clauses)

    query = f"""
        SELECT DISTINCT c.id, c.club_name_canonical, c.website, c.state,
               c.staff_page_url
        FROM canonical_clubs c
        WHERE {where}
        ORDER BY c.id
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        rows = [dict(row) for row in cur.fetchall()]

    # Post-filter: exclude NCAA-looking URLs
    filtered = []
    for row in rows:
        url = row.get("website", "")
        if _NCAA_URL_RE.search(url):
            continue
        # Platform filter (detected from URL)
        if platform_family:
            detected = detect_platform(url)
            if detected != platform_family:
                continue
        filtered.append(row)

    return filtered


def _upsert_discovery(
    cur,
    club_id: int,
    coach: CoachEntry,
    source_url: str,
    confidence: float,
    platform: str,
) -> str:
    """Insert or update a single coach_discoveries row. Returns 'inserted' or 'updated'."""
    cur.execute(
        """
        INSERT INTO coach_discoveries
            (club_id, name, title, email, phone, source_url,
             scraped_at, confidence, platform_family,
             first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s,
                NOW(), %s, %s,
                NOW(), NOW())
        ON CONFLICT ON CONSTRAINT coach_discoveries_club_name_title_uq
        DO UPDATE SET
            email           = COALESCE(EXCLUDED.email, coach_discoveries.email),
            phone           = COALESCE(EXCLUDED.phone, coach_discoveries.phone),
            source_url      = EXCLUDED.source_url,
            scraped_at      = NOW(),
            confidence      = EXCLUDED.confidence,
            platform_family = EXCLUDED.platform_family,
            last_seen_at    = NOW()
        RETURNING (xmax = 0) AS is_insert
        """,
        (
            club_id,
            coach.name,
            coach.title or "",
            coach.email,
            coach.phone,
            source_url,
            confidence,
            platform,
        ),
    )
    row = cur.fetchone()
    return "inserted" if row and row[0] else "updated"


def _update_last_scraped(cur, club_id: int) -> None:
    """Update the club's last_scraped_at timestamp."""
    cur.execute(
        "UPDATE canonical_clubs SET last_scraped_at = NOW() WHERE id = %s",
        (club_id,),
    )


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_youth_club_coaches(
    limit: Optional[int] = None,
    state: Optional[str] = None,
    platform_family: Optional[str] = None,
    dry_run: bool = False,
) -> Dict:
    """Scrape youth club coaching staff and write to coach_discoveries.

    Parameters
    ----------
    limit    : max number of clubs to process (for testing)
    state    : filter by state abbreviation (e.g. 'GA')
    platform_family : filter by platform ('sportsengine', 'wordpress', etc.)
    dry_run  : if True, parse pages but skip DB writes

    Returns
    -------
    dict with keys: scraped, rows_inserted, rows_updated, errors
    """
    logger.info(
        "Starting youth club coaches scrape: state=%s platform=%s limit=%s dry_run=%s",
        state, platform_family, limit, dry_run,
    )

    conn = _get_connection()
    if conn is None:
        if dry_run:
            logger.warning("No DB connection in dry-run mode; cannot fetch clubs list")
            return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 0}
        logger.error("DATABASE_URL not set or connection failed; aborting")
        return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 1}

    clubs = _fetch_clubs(conn, limit=limit, state=state, platform_family=platform_family)
    logger.info("Processing %d youth clubs", len(clubs))

    session = _get_session()
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    total_scraped = 0

    for i, club in enumerate(clubs):
        club_id = club["id"]
        club_name = club["club_name_canonical"]
        website = club["website"]
        tag = f"[{i + 1}/{len(clubs)}] {club_name}"

        try:
            platform = detect_platform(website)

            # If club has a known staff_page_url, try it first
            staff_page = club.get("staff_page_url")
            staff_url = None
            confidence = 0.75

            if staff_page:
                html = fetch_with_retry(session, staff_page)
                if html and len(html) > 500:
                    staff_url = staff_page
                    confidence = 0.90

            if not staff_url:
                result = discover_staff_url(session, website, platform)
                if result:
                    staff_url, confidence = result

            if not staff_url:
                logger.info("  SKIP %s - no staff URL found", tag)
                total_errors += 1
                continue

            html = fetch_with_retry(session, staff_url)
            if not html:
                logger.warning("  FAIL %s - fetch failed: %s", tag, staff_url)
                total_errors += 1
                continue

            # Re-detect platform from actual page
            platform = detect_platform(staff_url, html)

            coaches = parse_staff_html(html, club_name=club_name)
            if not coaches:
                logger.info("  SKIP %s - no coaches parsed from %s", tag, staff_url)
                total_errors += 1
                continue

            total_scraped += 1
            inserted = 0
            updated = 0

            if not dry_run:
                cur = conn.cursor()
                for c in coaches:
                    try:
                        result = _upsert_discovery(
                            cur, club_id, c, staff_url, confidence, platform,
                        )
                        if result == "inserted":
                            inserted += 1
                        else:
                            updated += 1
                    except Exception as exc:
                        logger.warning(
                            "  DB error for %s / %s: %s", club_name, c.name, exc,
                        )
                        conn.rollback()
                        continue
                conn.commit()
                _update_last_scraped(cur, club_id)
                conn.commit()
                cur.close()
            else:
                logger.info(
                    "  [dry-run] %s - would write %d coaches to club_id=%d",
                    tag, len(coaches), club_id,
                )
                for c in coaches:
                    logger.info(
                        "    %s | %s | %s | %s",
                        c.name, c.title or "(no title)",
                        c.email or "(no email)", c.phone or "(no phone)",
                    )

            total_inserted += inserted
            total_updated += updated

            logger.info(
                "  OK   %s - %d coaches (%d new, %d updated) from %s",
                tag, len(coaches), inserted, updated, staff_url,
            )

        except Exception as exc:
            logger.error("  ERROR %s - %s", tag, exc)
            total_errors += 1
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    if conn:
        conn.close()

    summary = {
        "scraped": total_scraped,
        "rows_inserted": total_inserted,
        "rows_updated": total_updated,
        "errors": total_errors,
    }
    logger.info("Youth club coaches scrape complete: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Scrape youth club coaching staff into coach_discoveries",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of clubs to process (for testing)",
    )
    parser.add_argument(
        "--state",
        default=None,
        help="Filter to a single state (e.g. GA, CA)",
    )
    parser.add_argument(
        "--platform-family",
        choices=["sportsengine", "leagueapps", "wordpress", "unknown"],
        default=None,
        help="Filter to a specific platform family",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse pages but skip DB writes",
    )
    args = parser.parse_args()

    result = scrape_youth_club_coaches(
        limit=args.limit,
        state=args.state,
        platform_family=args.platform_family,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(1 if result["errors"] > 0 and result["scraped"] == 0 else 0)


if __name__ == "__main__":
    main()
