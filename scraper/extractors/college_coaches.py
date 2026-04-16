"""
NCAA coaching staff scraper for D1/D2/D3 — writes to ``college_coaches``.

Ported from the TypeScript scrapers in the sibling player-platform repo
(``college-coaching-staff-scraper.ts``, ``ncaa-coach-scraper.ts``). Key
design decisions preserved:

- **Multi-strategy HTML extraction**: Sidearm staff cards, table rows
  with header detection, and generic card/div layouts.
- **Title-based blocklist**: filters non-coach staff (trainers,
  equipment managers, video coordinators, etc.).
- **Head coach detection**: title containing "Head Coach" sets
  ``is_head_coach = True``.
- **Count guard**: strategies with >15 results are treated as false
  positives and skipped.
- **Rate limiting**: >= 1 s between HTTP requests.
- **Graceful degradation**: 404s, timeouts, and unparseable pages are
  logged and skipped, not fatal.

CLI::

    python -m scraper.extractors.college_coaches \\
        [--division D1|D2|D3] [--gender mens|womens] \\
        [--limit 5] [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Sibling package imports (scraper.*)
# ---------------------------------------------------------------------------

_SCRAPER_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)

from scrape_run_logger import ScrapeRunLogger, FailureKind, classify_exception  # noqa: E402
from alerts import alert_scraper_failure  # noqa: E402

try:
    import psycopg2  # type: ignore
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
RATE_LIMIT_DELAY = 1.5  # seconds between schools

PARSE_COUNT_GUARD = 15  # strategies returning more are false positives

# Staff page URL paths to try, ordered by specificity.
# Gender-specific paths are tried first, then generic.
MENS_COACH_PATHS = [
    "/sports/mens-soccer/coaches",
    "/sports/msoc/coaches",
    "/sports/m-soccer/coaches",
    "/sports/mens-soccer/staff",
    "/sports/msoc/staff",
]
WOMENS_COACH_PATHS = [
    "/sports/womens-soccer/coaches",
    "/sports/wsoc/coaches",
    "/sports/w-soccer/coaches",
    "/sports/womens-soccer/staff",
    "/sports/wsoc/staff",
]
GENERIC_COACH_PATHS = [
    "/sports/soccer/coaches",
    "/sports/soccer/staff",
    "/coaches",
    "/staff",
    "/coaches.aspx",
    "/staff/coaching",
    "/coaching-staff",
    "/about/coaches",
    "/about/staff",
]

# Title-based blocklist — entries whose title matches any of these
# are not coaches and should be filtered out.
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
]

_BLOCKLIST_RE = re.compile(
    "|".join(TITLE_BLOCKLIST_PATTERNS), re.IGNORECASE
)

# Name validation — ported from TS looksLikeName()
_NAME_BLOCKLIST = {
    "about us", "contact us", "click here", "read more", "learn more",
    "meet the", "meet our", "our staff", "our team", "coaching staff",
    "support staff", "athletic staff", "staff directory",
    "head coach", "assistant coach", "associate head", "associate coach",
    "volunteer coach", "graduate assistant", "director of coaching",
    "director of operations", "athletic director", "technical director",
    "men soccer", "women soccer", "mens soccer", "womens soccer",
    "soccer coaches", "coaching team",
    "social media", "quick links", "campus map", "office hours",
    "follow us", "connect with", "stay connected", "more information",
}

_BLOCKLIST_TOKENS = {
    "soccer", "football", "basketball", "baseball", "softball", "volleyball",
    "lacrosse", "swimming", "tennis", "golf", "track", "wrestling",
    "coach", "coaching", "staff", "director", "athletic", "athletics",
    "university", "college", "school", "program", "department",
    "email", "phone", "fax", "office", "contact", "bio",
    "schedule", "roster", "recruiting", "camps", "news", "media",
    "facebook", "twitter", "instagram", "youtube", "tiktok",
    "home", "about", "menu", "search", "login", "signup",
}

SCRAPER_KEY_MAP = {
    "D1": "ncaa-d1-coaches",
    "D2": "ncaa-d2-coaches",
    "D3": "ncaa-d3-coaches",
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
    return "head coach" in lower or "head soccer" in lower or "director of soccer" in lower


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
    """Extract a US phone number from an element's text."""
    text = el.get_text()
    m = _PHONE_RE.search(text)
    return m.group(0) if m else None


# ---------------------------------------------------------------------------
# HTML parsing — three strategies, matching the TS scrapers
# ---------------------------------------------------------------------------

def parse_staff_html(html: str, school_name: str = "") -> List[CoachEntry]:
    """Extract coach entries from an NCAA staff/coaches page.

    Three strategies are tried in order:

    1. **Sidearm staff cards** — ``.sidearm-staff-member`` or similar
       card selectors with semantic CSS classes.
    2. **Table rows** — ``<table>`` with ``<td>`` cells containing
       name + title data.
    3. **Generic card/div layout** — broad card selectors
       (``.coach-card``, ``.staff-card``, etc.).

    Results are filtered through the title blocklist and name validator.
    Strategies returning more than PARSE_COUNT_GUARD results are treated
    as false positives and skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    coaches: List[CoachEntry] = []
    seen: set = set()

    def _add(name: str, title: Optional[str], email: Optional[str], phone: Optional[str]) -> bool:
        """Attempt to add a coach. Returns True if added."""
        if not looks_like_name(name):
            return False
        if is_blocked_title(title):
            return False
        key = name.lower()
        if key in seen:
            return False
        seen.add(key)
        coaches.append(CoachEntry(
            name=name,
            title=title or None,
            email=email,
            phone=phone,
            is_head_coach=detect_head_coach(title),
        ))
        return True

    # --- Strategy 1: Sidearm staff cards ---
    sidearm_selectors = [
        ".sidearm-staff-member",
        ".sidearm-staff-card",
        ".sidearm-coach",
        ".c-coaching-staff__item",
    ]
    for sel in sidearm_selectors:
        els = soup.select(sel)
        if not els:
            continue

        candidates: List[CoachEntry] = []
        candidate_seen: set = set()
        for el in els:
            name_el = el.select_one(
                ".sidearm-staff-member-name a, "
                ".sidearm-staff-member-name, "
                "h3 a, h4 a, h3, h4"
            )
            name = name_el.get_text().strip() if name_el else ""
            if not looks_like_name(name):
                continue

            title_el = el.select_one(
                ".sidearm-staff-member-title, "
                "[class*='title'], [class*='position'], [class*='role']"
            )
            title = title_el.get_text().strip() if title_el else None
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
            logger.debug("  [%s]: selector '%s' returned %d — false positive, skipping",
                         school_name, sel, len(candidates))
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
            logger.debug("  [%s]: table returned %d rows — false positive, bailing",
                         school_name, len(table_candidates))
        else:
            for c in table_candidates:
                if c.name.lower() not in seen:
                    seen.add(c.name.lower())
                    coaches.append(c)

    if coaches:
        return coaches

    # --- Strategy 3: Generic card/div layout ---
    card_selectors = [
        ".coach-card", ".staff-card", ".coach-item", ".staff-item",
        ".staff-member", ".coaching-staff-item",
        '[class*="coach-card"]', '[class*="staff-card"]',
        '[class*="staff-member"]',
    ]
    for sel in card_selectors:
        els = soup.select(sel)
        if not els:
            continue

        card_candidates: List[CoachEntry] = []
        card_seen: set = set()
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
            if key not in card_seen:
                card_seen.add(key)
                card_candidates.append(CoachEntry(
                    name=name,
                    title=title or None,
                    email=email,
                    phone=phone,
                    is_head_coach=detect_head_coach(title),
                ))

        if len(card_candidates) > PARSE_COUNT_GUARD:
            logger.debug("  [%s]: selector '%s' returned %d — false positive, skipping",
                         school_name, sel, len(card_candidates))
            continue

        if card_candidates:
            for c in card_candidates:
                if c.name.lower() not in seen:
                    seen.add(c.name.lower())
                    coaches.append(c)
            break

    return coaches


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def discover_staff_url(
    session: requests.Session,
    college: Dict,
    gender: str,
) -> Optional[str]:
    """Try to find the coaching staff page URL for a college.

    Tries, in order:
    1. ``soccer_program_url`` + ``/coaches`` suffix
    2. Gender-specific path segments appended to program URL base
    3. Generic coach paths appended to ``website``
    """
    program_url = college.get("soccer_program_url")
    website = college.get("website")
    gender_paths = WOMENS_COACH_PATHS if gender == "womens" else MENS_COACH_PATHS

    # Try program_url + /coaches
    if program_url:
        base = program_url.rstrip("/")
        base = re.sub(r"/(roster|schedule|coaches|staff)$", "", base, flags=re.IGNORECASE)
        url = f"{base}/coaches"
        html = fetch_with_retry(session, url)
        if html and len(html) > 500:
            return url

    # Try website + gender-specific paths
    if website:
        base = website.rstrip("/")
        for path in gender_paths:
            url = f"{base}{path}"
            html = fetch_with_retry(session, url)
            if html and len(html) > 500:
                return url
            time.sleep(0.3)

    # Try website + generic paths
    if website:
        base = website.rstrip("/")
        for path in GENERIC_COACH_PATHS:
            url = f"{base}{path}"
            html = fetch_with_retry(session, url)
            if html and len(html) > 500:
                return url
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


def _fetch_colleges(
    conn,
    division: Optional[str] = None,
    gender: Optional[str] = None,
) -> List[Dict]:
    """Query the colleges table. Returns list of dicts."""
    clauses = []
    params: List = []

    if division:
        clauses.append("division = %s")
        params.append(division)
    if gender:
        clauses.append("gender_program = %s")
        params.append(gender)

    where = ""
    if clauses:
        where = "WHERE " + " AND ".join(clauses)

    query = f"""
        SELECT id, name, slug, division, conference, state, city,
               website, soccer_program_url, gender_program,
               last_scraped_at
        FROM colleges
        {where}
        ORDER BY division, name
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _upsert_coach_row(
    cur,
    college_id: int,
    coach: CoachEntry,
    source_url: str,
    confidence: float,
) -> str:
    """Insert or update a single college_coaches row. Returns 'inserted' or 'updated'."""
    cur.execute(
        """
        INSERT INTO college_coaches
            (college_id, name, title, email, phone, is_head_coach,
             source, source_url, scraped_at, confidence,
             first_seen_at, last_seen_at)
        VALUES (%s, %s, %s, %s, %s, %s,
                %s, %s, NOW(), %s,
                NOW(), NOW())
        ON CONFLICT (college_id, name, title)
        DO UPDATE SET
            email        = EXCLUDED.email,
            phone        = EXCLUDED.phone,
            is_head_coach = EXCLUDED.is_head_coach,
            source       = EXCLUDED.source,
            source_url   = EXCLUDED.source_url,
            scraped_at   = NOW(),
            confidence   = EXCLUDED.confidence,
            last_seen_at = NOW()
        RETURNING (xmax = 0) AS is_insert
        """,
        (
            college_id,
            coach.name,
            coach.title,
            coach.email,
            coach.phone,
            coach.is_head_coach,
            "ncaa_staff_page",
            source_url,
            confidence,
        ),
    )
    row = cur.fetchone()
    return "inserted" if row and row[0] else "updated"


def _update_last_scraped(cur, college_id: int) -> None:
    cur.execute(
        "UPDATE colleges SET last_scraped_at = NOW() WHERE id = %s",
        (college_id,),
    )


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

def scrape_college_coaches(
    division: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict:
    """Scrape NCAA coaching staff and write to college_coaches.

    Parameters
    ----------
    division : 'D1', 'D2', 'D3', or None (all)
    gender   : 'mens', 'womens', or None (all)
    limit    : max number of colleges to process (for testing)
    dry_run  : if True, parse pages but skip DB writes

    Returns
    -------
    dict with keys: scraped, rows_inserted, rows_updated, errors
    """
    logger.info(
        "Starting NCAA coaches scrape: division=%s gender=%s limit=%s dry_run=%s",
        division, gender, limit, dry_run,
    )

    conn = _get_connection()
    if conn is None:
        if dry_run:
            logger.warning("No DB connection in dry-run mode; cannot fetch colleges list")
            return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 0}
        logger.error("DATABASE_URL not set or connection failed; aborting (use --dry-run for no-DB mode)")
        return {"scraped": 0, "rows_inserted": 0, "rows_updated": 0, "errors": 1}

    colleges = _fetch_colleges(conn, division=division, gender=gender)
    if limit:
        colleges = colleges[:limit]

    logger.info("Processing %d colleges", len(colleges))

    session = _get_session()
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    total_scraped = 0

    # Set up per-division run loggers
    divisions_seen: set = set()

    for i, college in enumerate(colleges):
        college_div = college["division"]
        college_gender = college["gender_program"]
        tag = f"[{i + 1}/{len(colleges)}] {college['name']} ({college_div} {college_gender})"

        scraper_key = SCRAPER_KEY_MAP.get(college_div, f"ncaa-{college_div.lower()}-coaches")
        if college_div not in divisions_seen:
            divisions_seen.add(college_div)
            run_logger = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NCAA {college_div}",
            )
            run_logger.start()

        try:
            staff_url = discover_staff_url(session, college, college_gender)
            if not staff_url:
                logger.info("  SKIP %s - no staff URL found", tag)
                total_errors += 1
                continue

            html = fetch_with_retry(session, staff_url)
            if not html:
                logger.warning("  FAIL %s - fetch failed: %s", tag, staff_url)
                total_errors += 1
                continue

            coaches = parse_staff_html(html, school_name=college["name"])
            if not coaches:
                logger.info("  SKIP %s - no coaches parsed from %s", tag, staff_url)
                total_errors += 1
                continue

            total_scraped += 1
            inserted = 0
            updated = 0

            # Confidence: Sidearm pages get 0.95, generic gets 0.80
            is_sidearm = "sidearm" in html.lower()[:5000]
            confidence = 0.95 if is_sidearm else 0.80

            if not dry_run:
                for c in coaches:
                    try:
                        result = _upsert_coach_row(
                            conn.cursor(), college["id"], c, staff_url, confidence,
                        )
                        if result == "inserted":
                            inserted += 1
                        else:
                            updated += 1
                    except Exception as exc:
                        logger.warning("  DB error for %s / %s: %s", college["name"], c.name, exc)
                        conn.rollback()
                        continue
                conn.commit()
                _update_last_scraped(conn.cursor(), college["id"])
                conn.commit()

            total_inserted += inserted
            total_updated += updated

            logger.info(
                "  OK   %s - %d coaches (%d new, %d updated) from %s",
                tag, len(coaches), inserted, updated, staff_url,
            )

        except Exception as exc:
            logger.error("  ERROR %s - %s", tag, exc)
            total_errors += 1
            kind = classify_exception(exc)
            alert_scraper_failure(
                scraper_key=scraper_key,
                failure_kind=kind.value,
                error_message=str(exc),
                league_name=f"NCAA {college_div}",
            )
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    # Finish run loggers
    for div in divisions_seen:
        key = SCRAPER_KEY_MAP.get(div, f"ncaa-{div.lower()}-coaches")
        run_logger = ScrapeRunLogger(scraper_key=key, league_name=f"NCAA {div}")
        run_logger.start()
        if total_errors > 0 and total_scraped == 0:
            run_logger.finish_failed(
                FailureKind.ZERO_RESULTS,
                error_message=f"{total_errors} colleges failed with no results",
            )
        else:
            run_logger.finish_ok(
                records_created=total_inserted,
                records_updated=total_updated,
                records_failed=total_errors,
            )

    if conn:
        conn.close()

    summary = {
        "scraped": total_scraped,
        "rows_inserted": total_inserted,
        "rows_updated": total_updated,
        "errors": total_errors,
    }
    logger.info("NCAA coaches scrape complete: %s", summary)
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
        description="Scrape NCAA D1/D2/D3 coaching staff into college_coaches",
    )
    parser.add_argument(
        "--division",
        choices=["D1", "D2", "D3"],
        default=None,
        help="Filter to a single division (default: all)",
    )
    parser.add_argument(
        "--gender",
        choices=["mens", "womens"],
        default=None,
        help="Filter to a single gender program (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of colleges to process (for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse pages but skip DB writes",
    )
    args = parser.parse_args()

    result = scrape_college_coaches(
        division=args.division,
        gender=args.gender,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(1 if result["errors"] > 0 and result["scraped"] == 0 else 0)


if __name__ == "__main__":
    main()
