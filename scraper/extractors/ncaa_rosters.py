"""
NCAA roster scraper for D1/D2/D3 — writes to ``college_roster_history``.

Ported from the TypeScript scrapers in the sibling player-platform repo
(``ncaa-d2-roster-scraper.ts``, ``ncaa-d3-roster-scraper.ts``,
``college-roster-scraper.ts``). Key design decisions preserved:

- **Header-aware table parsing**: column positions are detected from
  ``<th>`` text, never hardcoded offsets.
- **Multi-strategy HTML extraction**: Sidearm roster elements, generic
  ``<table>`` with header detection, and card/div layouts.
- **Year/class normalization**: Fr, So, Jr, Sr, Gr, RS-Fr, R-So, 5th,
  etc. all map to the ``year`` enum values expected by the schema.
- **Rate limiting**: >= 1 s between HTTP requests.
- **Graceful degradation**: 404s, timeouts, and unparseable pages are
  logged and skipped, not fatal.

CLI::

    python -m scraper.extractors.ncaa_rosters \\
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
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Sibling package imports (scraper.*)
# ---------------------------------------------------------------------------

# Ensure the parent ``scraper/`` package is importable when invoked as
# ``python -m scraper.extractors.ncaa_rosters``.
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

# Year/class normalization — matches the TS ACADEMIC_YEAR_MAP exactly
YEAR_MAP: Dict[str, str] = {
    "fr": "freshman",
    "fr.": "freshman",
    "freshman": "freshman",
    "r-fr": "freshman",
    "rs-fr": "freshman",
    "rs fr": "freshman",
    "r-fr.": "freshman",
    "so": "sophomore",
    "so.": "sophomore",
    "sophomore": "sophomore",
    "r-so": "sophomore",
    "rs-so": "sophomore",
    "rs so": "sophomore",
    "r-so.": "sophomore",
    "jr": "junior",
    "jr.": "junior",
    "junior": "junior",
    "r-jr": "junior",
    "rs-jr": "junior",
    "rs jr": "junior",
    "r-jr.": "junior",
    "sr": "senior",
    "sr.": "senior",
    "senior": "senior",
    "r-sr": "senior",
    "rs-sr": "senior",
    "rs sr": "senior",
    "r-sr.": "senior",
    "gr": "grad",
    "gr.": "grad",
    "grad": "grad",
    "graduate": "grad",
    "5th": "grad",
    "5th yr": "grad",
    "5th year": "grad",
}

# Soccer-specific path segments tried when discovering roster URL
MENS_PATHS = ["mens-soccer", "msoc", "m-soccer", "soccer"]
WOMENS_PATHS = ["womens-soccer", "wsoc", "w-soccer", "soccer"]

SCRAPER_KEY_MAP = {
    "D1": "ncaa-d1-rosters",
    "D2": "ncaa-d2-rosters",
    "D3": "ncaa-d3-rosters",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RosterPlayer:
    player_name: str
    position: Optional[str] = None
    year: Optional[str] = None
    hometown: Optional[str] = None
    prev_club: Optional[str] = None
    jersey_number: Optional[str] = None


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
# Year normalization
# ---------------------------------------------------------------------------

def normalize_year(raw: Optional[str]) -> Optional[str]:
    """Map free-text year/class to the schema enum value.

    Handles: Fr, So, Jr, Sr, Gr, RS-Fr, R-So, 5th, etc.
    Returns one of: freshman, sophomore, junior, senior, grad, or None.
    """
    if not raw:
        return None
    key = raw.strip().lower().replace(".", "")
    # Direct lookup
    if key in YEAR_MAP:
        return YEAR_MAP[key]
    # Try stripping leading "rs " or "r-" prefix for redshirt variants
    for prefix in ("rs-", "rs ", "r-"):
        if key.startswith(prefix):
            base = key[len(prefix):]
            if base in YEAR_MAP:
                return YEAR_MAP[base]
    return None


# ---------------------------------------------------------------------------
# Academic year (season string)
# ---------------------------------------------------------------------------

def current_academic_year() -> str:
    """Return season string like '2025-26'."""
    now = datetime.now(timezone.utc)
    y = now.year
    m = now.month
    if m >= 8:
        return f"{y}-{str(y + 1)[-2:]}"
    else:
        return f"{y - 1}-{str(y)[-2:]}"


# ---------------------------------------------------------------------------
# Header-aware column index — ported from college-roster-scraper.ts
# ---------------------------------------------------------------------------

@dataclass
class ColumnIndex:
    jersey_number: Optional[int] = None
    player_name: Optional[int] = None
    position: Optional[int] = None
    class_year: Optional[int] = None
    height: Optional[int] = None
    hometown: Optional[int] = None
    high_school: Optional[int] = None


def build_column_index(headers: List[str]) -> ColumnIndex:
    """Detect column semantics from header text. Never hardcodes positions."""
    idx = ColumnIndex()
    for i, raw in enumerate(headers):
        raw_stripped = raw.strip()
        h = re.sub(r"[^a-z0-9 ]", " ", raw_stripped.lower()).strip()
        h = re.sub(r"\s+", " ", h)

        # Bare "#" becomes empty after cleanup — detect it from raw
        if idx.jersey_number is None and raw_stripped == "#":
            idx.jersey_number = i
            continue

        if not h:
            continue

        if idx.jersey_number is None and re.match(r"^(no|num|number|jersey)\b", h):
            idx.jersey_number = i
        elif idx.player_name is None and re.search(r"\b(name|player|full name)\b", h):
            idx.player_name = i
        elif idx.position is None and re.search(r"\b(pos|position)\b", h):
            idx.position = i
        elif idx.class_year is None and re.search(r"\b(yr|year|class|cl|academic)\b", h):
            idx.class_year = i
        elif idx.height is None and re.search(r"\b(ht|height)\b", h):
            idx.height = i
        elif idx.hometown is None and re.search(r"\b(hometown|home town|from)\b", h):
            idx.hometown = i
        elif idx.high_school is None and re.search(
            r"\b(high school|hs|previous|prev|club|school|last school)\b", h
        ):
            idx.high_school = i

    return idx


def _cell_text(td: Tag) -> str:
    return re.sub(r"\s+", " ", td.get_text()).strip()


# ---------------------------------------------------------------------------
# HTML parsing — three strategies, matching the TS scrapers
# ---------------------------------------------------------------------------

def parse_roster_html(html: str) -> List[RosterPlayer]:
    """Extract player rows from an NCAA roster page.

    Three strategies are tried in order:

    1. **Sidearm roster elements** — ``li.sidearm-roster-player`` or
       ``div.sidearm-roster-player`` with semantic CSS classes for each field.
    2. **Header-aware table** — any ``<table>`` whose ``<th>`` row contains
       a "Name" column. Column positions are detected from headers.
    3. **Card/div layout** — ``.s-person-card``, ``.roster-card``,
       ``.s-person`` containers with nested class selectors.
    """
    soup = BeautifulSoup(html, "html.parser")
    players: List[RosterPlayer] = []

    # --- Strategy 1: Sidearm roster elements ---
    sidearm_els = soup.select("li.sidearm-roster-player, div.sidearm-roster-player")
    for el in sidearm_els:
        name_el = el.select_one("h3 a, h4 a, .sidearm-roster-player-name a")
        name = name_el.get_text().strip() if name_el else ""
        if not name or len(name) < 2:
            continue

        jersey_el = el.select_one(".sidearm-roster-player-jersey-number")
        jersey = jersey_el.get_text().strip() if jersey_el else None

        pos_el = (
            el.select_one(".sidearm-roster-player-position-long-short.hide-on-small-down")
            or el.select_one(".sidearm-roster-player-position span.text-bold")
        )
        position = pos_el.get_text().strip() if pos_el else None

        year_el = el.select_one(".sidearm-roster-player-academic-year")
        year_raw = year_el.get_text().strip() if year_el else None

        hometown_el = el.select_one(".sidearm-roster-player-hometown")
        hometown = hometown_el.get_text().rstrip(".").strip() if hometown_el else None

        prev_el = el.select_one(
            ".sidearm-roster-player-highschool, .sidearm-roster-player-previous-school"
        )
        prev_club = prev_el.get_text().strip() if prev_el else None

        players.append(RosterPlayer(
            player_name=name,
            position=position or None,
            year=normalize_year(year_raw),
            hometown=hometown or None,
            prev_club=prev_club or None,
            jersey_number=jersey or None,
        ))

    if players:
        return players

    # --- Strategy 2: Header-aware <table> parsing ---
    for table in soup.find_all("table"):
        # Find header row
        headers: List[str] = []
        thead = table.find("thead")
        if thead:
            first_tr = thead.find("tr")
            if first_tr:
                headers = [th.get_text().strip() for th in first_tr.find_all("th")]
        if not headers:
            first_tr = table.find("tr")
            if first_tr:
                ths = first_tr.find_all("th")
                if ths:
                    headers = [th.get_text().strip() for th in ths]

        if not headers:
            continue

        idx = build_column_index(headers)
        if idx.player_name is None:
            continue

        # Parse body rows
        tbody = table.find("tbody")
        body_rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

        for tr in body_rows:
            cells = [_cell_text(td) for td in tr.find_all("td")]
            if len(cells) < 2:
                continue

            def _get(col_idx: Optional[int]) -> Optional[str]:
                if col_idx is None or col_idx >= len(cells):
                    return None
                v = cells[col_idx].strip()
                return v if v else None

            name = _get(idx.player_name) or ""
            # Strip leading jersey number that some sites embed in the name cell
            name = re.sub(r"^#?\d+\s*", "", name).strip()
            if not name or len(name) < 2:
                continue

            jersey_raw = _get(idx.jersey_number)
            jersey = jersey_raw.lstrip("#").strip() if jersey_raw else None

            players.append(RosterPlayer(
                player_name=name,
                position=_get(idx.position),
                year=normalize_year(_get(idx.class_year)),
                hometown=_get(idx.hometown),
                prev_club=_get(idx.high_school),
                jersey_number=jersey or None,
            ))

        if players:
            return players

    # --- Strategy 3: Card/div layout ---
    card_selectors = [
        ".s-person-card",
        ".roster-card",
        ".s-person",
        "tr.s-table-body__row",
    ]
    for sel in card_selectors:
        for el in soup.select(sel):
            if sel == "tr.s-table-body__row":
                # Presto-style table rows without proper <th> headers
                cells = [_cell_text(td) for td in el.find_all("td")]
                if len(cells) < 2:
                    continue
                has_jersey = bool(re.match(r"^\d+$", cells[0]))
                offset = 1 if has_jersey else 0
                n = cells[offset] if offset < len(cells) else ""
                if not n or len(n) < 2:
                    continue
                players.append(RosterPlayer(
                    player_name=n,
                    position=cells[offset + 1] if offset + 1 < len(cells) else None,
                    year=normalize_year(cells[offset + 2] if offset + 2 < len(cells) else None),
                    hometown=(
                        (cells[offset + 4] if offset + 4 < len(cells) else "")
                        or (cells[offset + 3] if offset + 3 < len(cells) else "")
                    ).split("/")[0].strip() or None,
                    jersey_number=cells[0] if has_jersey else None,
                ))
            else:
                name_el = el.select_one(
                    ".s-person-card__name, .roster-card__name, h3, h4, .name"
                )
                n = name_el.get_text().strip() if name_el else ""
                if not n or len(n) < 2:
                    continue
                num_el = el.select_one(".s-person-card__number, .number")
                pos_el = el.select_one(".s-person-card__position, .position")
                yr_el = el.select_one(".s-person-card__year, .year")
                ht_el = el.select_one(".s-person-card__hometown, .hometown")
                players.append(RosterPlayer(
                    player_name=n,
                    position=pos_el.get_text().strip() if pos_el else None,
                    year=normalize_year(yr_el.get_text().strip() if yr_el else None),
                    hometown=ht_el.get_text().strip() if ht_el else None,
                    jersey_number=num_el.get_text().strip() if num_el else None,
                ))

        if players:
            return players

    return players


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def discover_roster_url(
    session: requests.Session,
    college: Dict,
    gender: str,
) -> Optional[str]:
    """Try to find the roster page URL for a college.

    Tries, in order:
    1. ``soccer_program_url`` from the DB (if it looks like a roster page)
    2. ``soccer_program_url`` as a base + ``/roster`` suffix
    3. Candidate path segments appended to the program URL base
    4. ``website`` field + sport path candidates
    """
    program_url = college.get("soccer_program_url")
    website = college.get("website")
    paths = WOMENS_PATHS if gender == "womens" else MENS_PATHS

    # If program_url already ends in /roster, try it directly
    if program_url and "/roster" in program_url.lower():
        html = fetch_with_retry(session, program_url)
        if html and len(html) > 500:
            return program_url

    # Try program_url as a base
    if program_url:
        base = program_url.rstrip("/")
        # Strip trailing /roster or /schedule if present
        base = re.sub(r"/(roster|schedule)$", "", base, flags=re.IGNORECASE)
        url = f"{base}/roster"
        html = fetch_with_retry(session, url)
        if html and len(html) > 500:
            return url

    # Try website + sport path candidates
    if website:
        base = website.rstrip("/")
        for path in paths:
            url = f"{base}/sports/{path}/roster"
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


def _upsert_roster_row(
    cur,
    college_id: int,
    player: RosterPlayer,
    academic_year: str,
) -> str:
    """Insert or update a single roster row. Returns 'inserted' or 'updated'."""
    cur.execute(
        """
        INSERT INTO college_roster_history
            (college_id, player_name, position, year, academic_year,
             hometown, prev_club, jersey_number, scraped_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (college_id, player_name, academic_year)
        DO UPDATE SET
            position       = EXCLUDED.position,
            year           = EXCLUDED.year,
            hometown       = EXCLUDED.hometown,
            prev_club      = EXCLUDED.prev_club,
            jersey_number  = EXCLUDED.jersey_number,
            scraped_at     = NOW()
        RETURNING (xmax = 0) AS is_insert
        """,
        (
            college_id,
            player.player_name,
            player.position,
            player.year,
            academic_year,
            player.hometown,
            player.prev_club,
            player.jersey_number,
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

def scrape_college_rosters(
    division: Optional[str] = None,
    gender: Optional[str] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> Dict:
    """Scrape NCAA rosters and write to college_roster_history.

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
    academic_year = current_academic_year()
    logger.info(
        "Starting NCAA roster scrape: division=%s gender=%s limit=%s dry_run=%s academic_year=%s",
        division, gender, limit, dry_run, academic_year,
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

        # Start run logger on first encounter of this division
        scraper_key = SCRAPER_KEY_MAP.get(college_div, f"ncaa-{college_div.lower()}-rosters")
        if college_div not in divisions_seen:
            divisions_seen.add(college_div)
            run_logger = ScrapeRunLogger(
                scraper_key=scraper_key,
                league_name=f"NCAA {college_div}",
            )
            run_logger.start()

        try:
            roster_url = discover_roster_url(session, college, college_gender)
            if not roster_url:
                logger.info("  SKIP %s - no roster URL found", tag)
                total_errors += 1
                continue

            html = fetch_with_retry(session, roster_url)
            if not html:
                logger.warning("  FAIL %s - fetch failed: %s", tag, roster_url)
                total_errors += 1
                continue

            players = parse_roster_html(html)
            if not players:
                logger.info("  SKIP %s - no players parsed from %s", tag, roster_url)
                total_errors += 1
                continue

            total_scraped += 1
            inserted = 0
            updated = 0

            if not dry_run:
                for p in players:
                    try:
                        result = _upsert_roster_row(conn.cursor(), college["id"], p, academic_year)
                        if result == "inserted":
                            inserted += 1
                        else:
                            updated += 1
                    except Exception as exc:
                        logger.warning("  DB error for %s / %s: %s", college["name"], p.player_name, exc)
                        conn.rollback()
                        continue
                conn.commit()
                _update_last_scraped(conn.cursor(), college["id"])
                conn.commit()

            total_inserted += inserted
            total_updated += updated

            logger.info(
                "  OK   %s - %d players (%d new, %d updated) from %s",
                tag, len(players), inserted, updated, roster_url,
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
        key = SCRAPER_KEY_MAP.get(div, f"ncaa-{div.lower()}-rosters")
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
    logger.info("NCAA roster scrape complete: %s", summary)
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
        description="Scrape NCAA D1/D2/D3 soccer rosters into college_roster_history",
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

    result = scrape_college_rosters(
        division=args.division,
        gender=args.gender,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    print(f"\nSummary: {result}")
    sys.exit(1 if result["errors"] > 0 and result["scraped"] == 0 else 0)


if __name__ == "__main__":
    main()
