"""
scrape_staff.py — Batch staff-page scraper for youth soccer club websites.

Reads clubs with a known website from the DB and attempts staff/coach page
discovery for each club. Discovered contacts are written to the
`coach_discoveries` table via upsert (idempotent).

Platform families detected
  sportsengine  — .sportsengine.com or SE-hosted pages
  leagueapps    — .leagueapps.com or LA-hosted pages
  wordpress     — generic WordPress sites with Team Members plugin or h3+p staff blocks
  unknown       — everything else (generic fallback parser)

Confidence scoring
  1.0  — staff found on /staff or /coaches path
  0.7  — staff found on /about/* path
  0.5  — homepage detection (fallback)

Usage
  python scrape_staff.py [options]

Options
  --limit N        Process at most N clubs (default: all)
  --tier N         Only process clubs affiliated with tier N (1–4)
  --dry-run        Parse and print results; do not write to DB
  --club-id ID     Process a single club by canonical_clubs.id
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup

# Shared coach-name guard — every record appended to ``records`` must
# pass ``looks_like_name`` before it reaches ``_upsert_discoveries``.
# Rejections are recorded on ``_NAME_REJECT_COUNTER`` so the run
# summary can print a per-reason breakdown (critical for spotting new
# pollution sources introduced by CMS updates).
_SCRAPER_ROOT = os.path.dirname(os.path.abspath(__file__))
if _SCRAPER_ROOT not in sys.path:
    sys.path.insert(0, _SCRAPER_ROOT)
from extractors._coach_name_guard import (  # noqa: E402
    RejectCounter,
    looks_like_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

_REQUEST_TIMEOUT = 15  # seconds per HTTP request

# Candidate URL path suffixes, probed in priority order.
# The priority order also drives the confidence score.
_STAFF_URL_CANDIDATES = [
    ("/staff", 1.0),
    ("/coaches", 1.0),
    ("/our-coaches", 1.0),
    ("/about/staff", 0.7),
    ("/club-staff", 0.7),
    ("/leadership", 0.7),
]

# Patterns that suggest a page is actually a staff directory
_STAFF_CONTENT_PATTERNS = re.compile(
    r"(head\s+coach|director|coaching\s+staff|technical\s+director|"
    r"staff\s+member|our\s+coaches|club\s+staff|meet\s+the\s+staff)",
    re.IGNORECASE,
)

# Basic email regex
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# Module-level counter — shared across every parser in this module so
# the end-of-run summary reports a single per-reason total.
_NAME_REJECT_COUNTER = RejectCounter()

# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

def _detect_platform(url: str, html: str) -> str:
    """Detect the club website platform family from the URL and page HTML."""
    url_lower = url.lower()
    if "sportsengine.com" in url_lower:
        return "sportsengine"
    if "leagueapps.com" in url_lower:
        return "leagueapps"

    soup = BeautifulSoup(html, "lxml")

    # SportsEngine: generator meta or se-specific body class
    meta_gen = soup.find("meta", {"name": re.compile(r"generator", re.I)})
    if meta_gen and meta_gen.get("content", ""):
        content = meta_gen["content"].lower()
        if "sportsengine" in content:
            return "sportsengine"

    se_classes = {"se-", "sports-engine", "sportsengine"}
    body = soup.find("body")
    if body:
        body_classes = " ".join(body.get("class", [])).lower()
        if any(c in body_classes for c in se_classes):
            return "sportsengine"

    # LeagueApps: look for leagueapps in script src or link href attributes
    for tag in soup.find_all("script"):
        src = tag.get("src", "")
        if src and "leagueapps" in src.lower():
            return "leagueapps"
    for tag in soup.find_all("link"):
        href = tag.get("href", "")
        if href and "leagueapps" in href.lower():
            return "leagueapps"

    # WordPress: generator or wp-content
    if meta_gen and "wordpress" in meta_gen.get("content", "").lower():
        return "wordpress"
    if soup.find(href=re.compile(r"/wp-content/", re.I)):
        return "wordpress"
    if soup.find(src=re.compile(r"/wp-content/", re.I)):
        return "wordpress"

    return "unknown"


# ---------------------------------------------------------------------------
# Staff URL discovery
# ---------------------------------------------------------------------------

def _fetch_html_static(url: str) -> Optional[str]:
    """Static HTTP fetch; returns HTML string or None if blocked/failed."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code == 200:
            return resp.text
        return None
    except requests.RequestException:
        return None


def _fetch_html_with_fallback(url: str) -> Optional[str]:
    """
    Try static fetch first; fall back to Playwright for blocked or
    JS-rendered pages (403, 429, or thin static response).
    """
    html = _fetch_html_static(url)
    if html and len(html) > 500:
        return html

    # Static fetch was empty, blocked, or returned a thin shell — try Playwright
    try:
        from extractors.playwright_helper import render_page
        logger.debug("  [Playwright] Rendering %s", url)
        pw_html = render_page(url)
        if pw_html:
            return pw_html
    except Exception as exc:
        logger.debug("  [Playwright] Failed for %s: %s", url, exc)

    return html  # return whatever we got (could be None)


def _find_staff_url(website: str) -> Tuple[Optional[str], float]:
    """
    Probe candidate staff URL patterns in priority order using both static
    requests and a Playwright fallback for JS-rendered or blocked pages.

    Returns (url, confidence) of the first URL that responds 200 and contains
    staff-like content. Falls back to the homepage with confidence 0.5 if no
    dedicated staff page is found.

    Confidence levels:
      1.0 — /staff or /coaches or /our-coaches path
      0.7 — /about/* or /club-staff or /leadership path
      0.5 — homepage fallback
    """
    base = website.rstrip("/")
    parsed = urlparse(base)
    if not parsed.scheme:
        base = "https://" + base

    for suffix, confidence in _STAFF_URL_CANDIDATES:
        url = base + suffix
        # Try static first; fall back to Playwright if blocked or thin
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT, allow_redirects=True)
            static_ok = resp.status_code == 200
            blocked = resp.status_code in (403, 406, 429)
            html = resp.text if static_ok else None
        except requests.RequestException as exc:
            logger.debug("  %s → error: %s", url, exc)
            static_ok = False
            blocked = False
            html = None

        # Use Playwright if blocked or if static HTML is too thin for parsing
        if blocked or (static_ok and html and len(html) < 500):
            try:
                from extractors.playwright_helper import render_page
                logger.info("  [Playwright] Rendering %s (blocked=%s)", url, blocked)
                pw_html = render_page(url)
                if pw_html:
                    html = pw_html
                    static_ok = True
            except Exception as exc:
                logger.debug("  [Playwright] Failed for %s: %s", url, exc)

        if not static_ok or not html:
            logger.debug("  %s → not reachable", url)
            continue

        if _STAFF_CONTENT_PATTERNS.search(html):
            logger.info("  Found staff URL: %s (confidence=%.1f)", url, confidence)
            return url, confidence
        logger.debug("  %s → 200 but no staff content", url)

    # Fallback: try the homepage itself at confidence 0.5
    homepage_html = _fetch_html_with_fallback(base)
    if homepage_html and _STAFF_CONTENT_PATTERNS.search(homepage_html):
        logger.info("  Homepage has staff content: %s (confidence=0.5)", base)
        return base, 0.5

    return None, 0.0


# ---------------------------------------------------------------------------
# Parsers per platform family
# ---------------------------------------------------------------------------

def _extract_emails_from_html(soup: BeautifulSoup) -> List[str]:
    """Collect all email addresses visible in the HTML (mailto: links or plain text)."""
    emails = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if addr:
                emails.append(addr)
    # Also scan visible text for bare addresses
    text_emails = _EMAIL_RE.findall(soup.get_text())
    emails.extend(text_emails)
    return list(dict.fromkeys(emails))  # dedupe preserving order


def _parse_sportsengine(soup: BeautifulSoup) -> List[Dict]:
    """Parse SportsEngine staff pages (staff-card / .staff-member elements)."""
    records = []

    def _extract_card(card) -> Optional[Dict]:
        name_el = (
            card.find(class_=re.compile(r"staff[-_]?name|person[-_]?name", re.I))
            or card.find(["h3", "h4", "strong"])
        )
        if not name_el:
            return None
        name = name_el.get_text(strip=True)
        if not name:
            return None
        # Shared guard — filters out nav-menu, CTA, date, and
        # all-caps-banner strings the card selector sometimes grabs.
        if not looks_like_name(name, _NAME_REJECT_COUNTER):
            return None

        title_el = (
            card.find(class_=re.compile(r"staff[-_]?title|person[-_]?title|role|position", re.I))
            or card.find("p")
        )
        title = title_el.get_text(strip=True) if title_el else None
        if title == name:
            title = None

        email = None
        mailto = card.find("a", href=re.compile(r"^mailto:", re.I))
        if mailto:
            email = mailto["href"][7:].split("?")[0].strip() or None
        else:
            text_emails = _EMAIL_RE.findall(card.get_text())
            if text_emails:
                email = text_emails[0]

        return {"name": name, "title": title, "email": email}

    # Try explicit card selectors
    for sel in [
        "staff-card",
        ".staff-card",
        ".staff-member",
        ".se-staff-card",
        "[class*='staff-card']",
        "[class*='staff-member']",
    ]:
        cards = soup.select(sel)
        for card in cards:
            rec = _extract_card(card)
            if rec:
                records.append(rec)
        if records:
            break

    return records


def _parse_leagueapps(soup: BeautifulSoup) -> List[Dict]:
    """Parse LeagueApps staff pages."""
    records = []

    bio_containers = soup.select(
        ".la-staff, .staff-bio, .coach-bio, [class*='staff'], [class*='coach-bio']"
    )

    for container in bio_containers:
        name_el = container.find(["h2", "h3", "h4", "strong"])
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name:
            continue
        if not looks_like_name(name, _NAME_REJECT_COUNTER):
            continue

        title = None
        for el in container.find_all(["p", "span"]):
            text = el.get_text(strip=True)
            if text and text != name and len(text) < 120:
                # Skip email-looking text
                if not _EMAIL_RE.match(text):
                    title = text
                    break

        email = None
        mailto = container.find("a", href=re.compile(r"^mailto:", re.I))
        if mailto:
            email = mailto["href"][7:].split("?")[0].strip() or None
        else:
            text_emails = _EMAIL_RE.findall(container.get_text())
            if text_emails:
                email = text_emails[0]

        records.append({"name": name, "title": title, "email": email})

    return records


def _parse_wordpress(soup: BeautifulSoup) -> List[Dict]:
    """
    Parse WordPress-based club sites:
    - Team Members plugin blocks (select by CSS class)
    - Genesis child theme staff sections
    - Generic h3 + p fallback within staff section containers
    """
    records = []

    def _card_name(card) -> Optional[str]:
        """Extract name from a Team Members-style card using proper BS4 selectors."""
        # CSS class-based lookup via select_one (not find() with class strings)
        name_el = (
            card.select_one(".team-member-name")
            or card.select_one(".member-name")
            or card.find(["h3", "h4", "strong"])
        )
        if name_el:
            return name_el.get_text(strip=True) or None
        return None

    def _card_title(card, name: str) -> Optional[str]:
        """Extract title/role from a card."""
        title_el = (
            card.select_one(".team-member-role")
            or card.select_one(".team-member-title")
            or card.select_one(".member-role")
            or card.select_one(".role")
            or card.find("p")
        )
        if title_el:
            t = title_el.get_text(strip=True)
            return t if t and t != name else None
        return None

    def _card_email(card) -> Optional[str]:
        mailto = card.find("a", href=re.compile(r"^mailto:", re.I))
        if mailto:
            return mailto["href"][7:].split("?")[0].strip() or None
        text_emails = _EMAIL_RE.findall(card.get_text())
        return text_emails[0] if text_emails else None

    # Team Members plugin (Themeisle, generic .team-member, .team-member-card)
    tm_cards = soup.select(
        ".team-member, .wp-block-themeisle-blocks-team-member, .team-member-card"
    )
    for card in tm_cards:
        name = _card_name(card)
        if not name:
            continue
        if not looks_like_name(name, _NAME_REJECT_COUNTER):
            continue
        title = _card_title(card, name)
        email = _card_email(card)
        records.append({"name": name, "title": title, "email": email})

    if records:
        return records

    # Generic fallback: look for staff section containers then h3/h4 + p pairs
    staff_sections = soup.select(
        "#staff, #coaches, #our-staff, #our-coaches, "
        ".staff-section, .coaches-section, .staff-list, .coaches-list, "
        "[id*='staff'], [id*='coach'], [class*='staff-section'], [class*='coach-section']"
    )

    sections_to_search = staff_sections if staff_sections else [soup]

    for section in sections_to_search:
        headings = section.find_all(["h3", "h4"])
        for heading in headings:
            name = heading.get_text(strip=True)
            if not name or len(name) > 80:
                continue
            # Heuristic: real person names rarely contain these
            if any(kw in name.lower() for kw in ["http", "©", "menu", "navigation"]):
                continue
            # Shared guard — WordPress h3 fallback was a major
            # pollution source pre-guard ("Newsletter Sign-Up",
            # "Related Articles", section titles, ...).
            if not looks_like_name(name, _NAME_REJECT_COUNTER):
                continue

            # Look for title in the very next sibling or within the same container
            title = None
            email = None
            sib = heading.find_next_sibling()
            if sib and sib.name in ("p", "span", "div"):
                sib_text = sib.get_text(strip=True)
                if sib_text and sib_text != name and len(sib_text) < 120:
                    if _EMAIL_RE.match(sib_text):
                        email = sib_text
                    else:
                        title = sib_text

            if name:
                records.append({"name": name, "title": title, "email": email})

    return records


def _parse_generic(soup: BeautifulSoup) -> List[Dict]:
    """Generic fallback parser: heuristic h3/h4+p scanning for any site."""
    records = []
    headings = soup.find_all(["h3", "h4"])
    for heading in headings:
        name = heading.get_text(strip=True)
        if not name or len(name) > 80:
            continue
        if any(kw in name.lower() for kw in ["http", "©", "menu", "navigation", "copyright"]):
            continue
        # Shared guard — the generic fallback is the noisiest path in
        # this module and must not leak section headers, CTA strings,
        # or date fragments into coach_discoveries.
        if not looks_like_name(name, _NAME_REJECT_COUNTER):
            continue

        title = None
        email = None
        sib = heading.find_next_sibling()
        if sib and sib.name in ("p", "span", "div"):
            sib_text = sib.get_text(strip=True)
            if sib_text and sib_text != name and len(sib_text) < 120:
                if _EMAIL_RE.match(sib_text):
                    email = sib_text
                else:
                    title = sib_text

        if name:
            records.append({"name": name, "title": title, "email": email})

    return records


def _parse_staff_page(html: str, platform: str, staff_url: str) -> List[Dict]:
    """
    Route HTML to the correct platform parser.
    Enriches each record with any mailto: emails found on the page.
    Falls back to generic parser if platform-specific one returns nothing.
    """
    soup = BeautifulSoup(html, "lxml")

    # Collect all emails on the page for cross-referencing
    page_emails = _extract_emails_from_html(soup)

    if platform == "sportsengine":
        records = _parse_sportsengine(soup)
    elif platform == "leagueapps":
        records = _parse_leagueapps(soup)
    elif platform == "wordpress":
        records = _parse_wordpress(soup)
    else:
        records = _parse_generic(soup)

    # If platform parser returned nothing, try generic fallback
    if not records and platform not in ("unknown",):
        records = _parse_generic(soup)

    # Fill missing emails from page-level email list when only one email
    # appears on the page and a record has no email
    if len(page_emails) == 1:
        for rec in records:
            if not rec.get("email"):
                rec["email"] = page_emails[0]

    # Normalize title: always a string (never None) for null-safe idempotency
    for rec in records:
        if rec.get("title") is None:
            rec["title"] = ""

    return records


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_db_conn():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL environment variable is not set")
    return psycopg2.connect(db_url)


def _load_clubs(
    conn,
    limit: Optional[int] = None,
    tier: Optional[int] = None,
    club_id: Optional[int] = None,
) -> List[Dict]:
    """Load canonical_clubs that have a website, with optional filters."""
    params = []
    where_clauses = ["c.website IS NOT NULL", "c.website != ''"]

    if club_id is not None:
        where_clauses.append("c.id = %s")
        params.append(club_id)
    elif tier is not None:
        where_clauses.append(
            "EXISTS (SELECT 1 FROM club_affiliations ca WHERE ca.club_id = c.id AND ca.platform_tier = %s)"
        )
        params.append(str(tier))

    where_sql = " AND ".join(where_clauses)
    query = f"""
        SELECT DISTINCT c.id, c.club_name_canonical, c.website
        FROM canonical_clubs c
        WHERE {where_sql}
        ORDER BY c.id
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def _upsert_discoveries(conn, club_id: int, records: List[Dict], source_url: str, confidence: float, platform: str) -> int:
    """
    Upsert coach_discoveries records for a club.
    Conflict key: (club_id, name, title).
    Returns the number of rows affected.
    """
    if not records:
        return 0

    upsert_sql = """
        INSERT INTO coach_discoveries
            (club_id, name, title, email, source_url, scraped_at, confidence, platform_family)
        VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT ON CONSTRAINT coach_discoveries_club_name_title_uq
        DO UPDATE SET
            email        = EXCLUDED.email,
            source_url   = EXCLUDED.source_url,
            scraped_at   = EXCLUDED.scraped_at,
            confidence   = EXCLUDED.confidence,
            platform_family = EXCLUDED.platform_family
    """

    rows = [
        (
            club_id,
            rec["name"][:255],
            (rec.get("title") or "")[:255],
            (rec.get("email") or None),
            source_url,
            confidence,
            platform,
        )
        for rec in records
        if rec.get("name")
    ]

    if not rows:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, upsert_sql, rows)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_club(club: Dict, dry_run: bool = False) -> Optional[Dict]:
    """
    Scrape a single club's staff page.

    Returns a result dict with keys: club_id, club_name, staff_count,
    platform, source_url, confidence, records — or None on failure.
    """
    club_id = club["id"]
    name = club["club_name_canonical"]
    website = club["website"]

    logger.info("[%d] %s (%s)", club_id, name, website)

    staff_url, confidence = _find_staff_url(website)
    if not staff_url:
        logger.info("  No staff URL found for %s", name)
        return None

    html = _fetch_html_with_fallback(staff_url)
    if not html:
        logger.warning("  Could not fetch %s", staff_url)
        return None

    platform = _detect_platform(staff_url, html)
    logger.info("  Platform: %s", platform)

    records = _parse_staff_page(html, platform, staff_url)
    logger.info("  Found %d staff record(s)", len(records))

    for rec in records:
        logger.debug("    %s | %s | %s", rec.get("name"), rec.get("title"), rec.get("email"))

    return {
        "club_id": club_id,
        "club_name": name,
        "staff_count": len(records),
        "platform": platform,
        "source_url": staff_url,
        "confidence": confidence,
        "records": records,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Scrape staff/coach pages for youth soccer club websites"
    )
    parser.add_argument("--limit", type=int, default=None, help="Max number of clubs to process")
    parser.add_argument("--tier", type=int, default=None, help="Filter clubs by competitive tier (1–4)")
    parser.add_argument("--dry-run", action="store_true", help="Parse and print; do not write to DB")
    parser.add_argument("--club-id", type=int, default=None, help="Process a single club by ID")
    args = parser.parse_args()

    conn = _get_db_conn()

    try:
        clubs = _load_clubs(conn, limit=args.limit, tier=args.tier, club_id=args.club_id)
        logger.info("Processing %d club(s)...", len(clubs))

        if not clubs:
            logger.warning("No clubs with websites found matching the given filters.")
            sys.exit(0)

        summary = []
        platform_counts: Dict[str, int] = defaultdict(int)
        total_staff = 0

        for club in clubs:
            result = scrape_club(club, dry_run=args.dry_run)
            if result is None:
                continue

            platform_counts[result["platform"]] += 1
            total_staff += result["staff_count"]

            if not args.dry_run and result["records"]:
                written = _upsert_discoveries(
                    conn,
                    result["club_id"],
                    result["records"],
                    result["source_url"],
                    result["confidence"],
                    result["platform"],
                )
                logger.info(
                    "  [%d] Wrote %d record(s) for %s",
                    result["club_id"],
                    written,
                    result["club_name"],
                )
            elif args.dry_run:
                logger.info("  [dry-run] Would write %d record(s)", result["staff_count"])

            summary.append(result)
            time.sleep(0.5)  # polite delay between clubs

        # --- Summary report ---
        logger.info("")
        logger.info("=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info("Clubs processed : %d", len(clubs))
        logger.info("Clubs with staff: %d", len(summary))
        logger.info("Total staff found: %d", total_staff)
        logger.info("")
        logger.info("Platform family breakdown:")
        for platform, count in sorted(platform_counts.items(), key=lambda x: -x[1]):
            logger.info("  %-16s %d clubs", platform, count)
        logger.info("")
        logger.info("Per-club staff counts:")
        for res in sorted(summary, key=lambda r: r["club_name"]):
            logger.info(
                "  [%4d] %-40s %2d staff  %-14s %s",
                res["club_id"],
                res["club_name"][:40],
                res["staff_count"],
                res["platform"],
                res["source_url"],
            )

        # --- Name-guard rejection breakdown ---
        reject_summary = _NAME_REJECT_COUNTER.summary()
        if reject_summary:
            logger.info("")
            logger.info(
                "Coach-name guard rejections (%d total):",
                _NAME_REJECT_COUNTER.total(),
            )
            for reason, count in sorted(
                reject_summary.items(), key=lambda x: -x[1]
            ):
                logger.info("  %-22s %d", reason, count)
        else:
            logger.info("")
            logger.info("Coach-name guard rejections: 0 (nothing filtered)")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
