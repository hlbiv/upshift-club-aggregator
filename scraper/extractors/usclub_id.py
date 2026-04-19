"""
US Club Soccer iD program extractor.

Two sources, two surface areas:

  * **Option A — SoccerWire articles (public).** SoccerWire publishes
    write-ups for every iD National Pool, regional iD event, and Training
    Center selection cycle. We discover them via the WordPress REST API
    (``/wp-json/wp/v2/posts``) and filter by title/slug keywords. Public,
    no creds, no ToS exposure. ~70-85% precision; lower recall.

  * **Option B — usclubsoccer.org members area (login-gated).** The
    canonical roster lists live behind a login. Capturing them requires
    Playwright + secrets (``USCLUB_USERNAME`` / ``USCLUB_PASSWORD`` in
    Replit) and a ToS review. Stubbed in this module so the wiring is
    obvious; flip on once credentials are provisioned.

Two-phase pipeline:

  1. **Discovery** — ``scrape_soccerwire_id_articles`` walks the WP REST
     API and returns metadata for posts whose title/slug match any
     iD-program keyword. Cheap (one HTTP call per page of 50 posts) and
     idempotent — safe to re-run.

  2. **Body parsing** — ``parse_article_body`` fetches one article's
     full HTML and extracts player rows from the embedded roster table.
     Returns a list of dicts in the shape that
     ``ingest.id_selection_writer.insert_player_id_selections`` expects.

The body parser is template-fragile by design: SoccerWire's editorial
team uses several layouts (a real ``<table>`` with NAME/POSITION/
HOMETOWN/CLUB columns, embedded roster images for selection events,
and freeform prose for announcements). The parser handles only the
table layout. Articles whose roster lives in an ``<img>`` or in
freeform prose are skipped with a warning rather than crashing the run
— the writer will still pick up roster-bearing articles published
later, and the discovery step will keep their URLs in the
``scrape_run_logs`` for backfill.

Pattern after ``scraper/extractors/soccerwire.py:_fetch_all_slugs`` for
the WP REST API call shape.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, Iterable, List, Optional

import requests

try:
    from bs4 import BeautifulSoup, Tag  # type: ignore
except ImportError:  # pragma: no cover — tests stub the parser
    BeautifulSoup = None  # type: ignore
    Tag = None  # type: ignore

logger = logging.getLogger(__name__)


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_API_POSTS_URL = "https://www.soccerwire.com/wp-json/wp/v2/posts"
_PAGE_SIZE = 50

# Title / slug substrings that flag a post as iD-related. Lowercased
# at compare time. Over-inclusive on purpose — manual review of the
# discovery output sets precision before we commit to body parsing.
_ID_KEYWORDS = (
    "id national pool",
    "us club id",
    "u.s. club id",
    "training center",
    "id pool",
    "id program",
)

# Header strings we recognise in roster tables. Match is
# case-insensitive substring. The "name" header is required (without it
# we can't anchor any row); "club" is strongly preferred. "Position"
# and "hometown" are bonuses that we capture into `position` / `state`
# when present.
_HEADER_NAME_TOKENS = ("name",)
_HEADER_CLUB_TOKENS = ("club",)
_HEADER_POSITION_TOKENS = ("position", "pos")
_HEADER_HOMETOWN_TOKENS = ("hometown", "city/state", "city", "state")

# Pool-tier inference. Order matters — checked top to bottom; first
# match wins. Lowercased haystack from title.
#
# `training-center` is checked BEFORE `national` deliberately: events
# like "id2 Boys National Training Camp" are training-camp events (the
# camp where players compete *for* a National Selection spot), not
# national-pool selections themselves. Sorting national first would
# mis-tier those rows.
_POOL_TIER_RULES = (
    ("training-center", ("training camp", "training center", "regional camp",
                         "regional training")),
    ("national",        ("national pool", "national selection", "national team")),
    ("regional",        ("regional pool", "regional selection", "regional event",
                         "selection event", "id selection")),
)

# Birth-year inference patterns ordered most-specific first. Anchored
# on year tokens so we don't accidentally pick up the article date.
_BIRTH_YEAR_PATTERNS = (
    re.compile(r"born in\s+((?:19|20)\d{2})", re.IGNORECASE),
    re.compile(r"((?:19|20)\d{2})\s+age group", re.IGNORECASE),
    re.compile(r"((?:19|20)\d{2})\s+(?:boys|girls|standouts|cohort|class)\b",
               re.IGNORECASE),
    re.compile(r"\bid2\s+((?:19|20)\d{2})\b", re.IGNORECASE),
    # 'U-15' / 'U15' -> infer birth year as (selection_year - age). This
    # only fires from helper code below where selection_year is known.
)
_U_AGE_PATTERN = re.compile(r"\bU-?(\d{1,2})\b")


def _post_matches_id_filter(post: Dict[str, Any]) -> bool:
    """Return True if the post's title or slug looks iD-related."""
    title = (post.get("title", {}) or {}).get("rendered") or ""
    slug = post.get("slug") or ""
    haystack = f"{title} {slug}".lower()
    return any(kw in haystack for kw in _ID_KEYWORDS)


def _strip_html(text: str) -> str:
    """Best-effort HTML tag strip for excerpts. Cheap and dependency-free."""
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", cleaned).strip()


def scrape_soccerwire_id_articles(
    *,
    max_pages: int = 4,
    keywords: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Discover SoccerWire posts about the US Club iD program.

    INITIAL VERSION (this PR): returns parsed metadata only — title,
    url, date, slug, excerpt — and prints discovered titles. Body
    parsing into player rows is a follow-up PR (see module docstring).

    Parameters
    ----------
    max_pages
        Cap on WP REST API pagination. Default 4 = 200 most recent posts.
    keywords
        Optional override for the iD filter substrings. Defaults to
        :data:`_ID_KEYWORDS`.

    Returns
    -------
    List of dicts with keys ``id``, ``title``, ``slug``, ``url``,
    ``date``, ``excerpt``. Empty list on network failure (fail-soft —
    never crash the runner).
    """
    filter_terms = tuple((kw or "").lower() for kw in (keywords or _ID_KEYWORDS))

    discovered: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        url = f"{_API_POSTS_URL}?per_page={_PAGE_SIZE}&page={page}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
            if r.status_code == 400:
                # WP returns 400 once you page past the last page.
                break
            r.raise_for_status()
            data = r.json()
        except requests.RequestException as exc:
            logger.warning(
                "[usclub-id] SoccerWire WP REST page %d failed: %s", page, exc,
            )
            break
        if not data:
            break

        for post in data:
            title = (post.get("title", {}) or {}).get("rendered") or ""
            slug = post.get("slug") or ""
            haystack = f"{title} {slug}".lower()
            if not any(kw in haystack for kw in filter_terms):
                continue
            excerpt_html = (post.get("excerpt", {}) or {}).get("rendered") or ""
            discovered.append({
                "id": post.get("id"),
                "title": _strip_html(title),
                "slug": slug,
                "url": post.get("link"),
                "date": post.get("date"),
                "excerpt": _strip_html(excerpt_html),
            })

        total_pages = int(r.headers.get("X-WP-TotalPages", 1))
        if page >= total_pages:
            break

    logger.info(
        "[usclub-id] Discovered %d candidate iD posts on SoccerWire",
        len(discovered),
    )
    for post in discovered:
        logger.info("[usclub-id]   %s -> %s", post.get("date"), post.get("title"))

    return discovered


# ---------------------------------------------------------------------------
# Body parsing — fetch one article and extract player rows
# ---------------------------------------------------------------------------


def fetch_article_html(url: str, *, timeout: float = 20.0) -> Optional[str]:
    """Fetch one SoccerWire article. Returns HTML body, or None on
    network failure (fail-soft — never raise into the runner)."""
    try:
        r = requests.get(url, headers=_HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.text
    except requests.RequestException as exc:
        logger.warning("[usclub-id] article fetch failed for %s: %s", url, exc)
        return None


def _norm_header(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip().lower()


def _classify_header_row(cells: List[str]) -> Optional[Dict[str, int]]:
    """Map a candidate header row's cells onto column indices.

    Returns ``{"name": idx, "club": idx, "position": idx, "hometown": idx}``
    with a key per column we recognise. Keys missing → that column
    doesn't exist in this table.

    Returns ``None`` if the row doesn't even contain a NAME column —
    the caller should treat that as "this isn't a roster table" and
    move on rather than mis-emit garbage.
    """
    out: Dict[str, int] = {}
    for i, cell in enumerate(cells):
        h = _norm_header(cell)
        if not h:
            continue
        if "name" not in out and any(t in h for t in _HEADER_NAME_TOKENS):
            out["name"] = i
        elif "club" not in out and any(t in h for t in _HEADER_CLUB_TOKENS):
            out["club"] = i
        elif "position" not in out and any(t in h for t in _HEADER_POSITION_TOKENS):
            out["position"] = i
        elif "hometown" not in out and any(t in h for t in _HEADER_HOMETOWN_TOKENS):
            out["hometown"] = i
    if "name" not in out:
        return None
    return out


def _row_text_cells(row: "Tag") -> List[str]:
    """Extract trimmed text content from each <td>/<th> in a row."""
    cells = row.find_all(["td", "th"], recursive=False)
    return [c.get_text(separator=" ", strip=True) for c in cells]


def _parse_hometown(value: str) -> Optional[str]:
    """Pull the state code out of "City, ST" / "City, State". Returns
    the upper-case state token if it looks like a 2-letter code, the
    full state name if not, or None if the value is empty / unparseable.

    Best-effort: SoccerWire is inconsistent (some rows are "City, MD",
    some "City State", some omit the comma). We fall through to the
    last whitespace-separated token if it's a 2-letter alpha code,
    which catches the missing-comma cases without false positives on
    multi-word states like "New York"."""
    v = (value or "").strip()
    if not v:
        return None
    if "," in v:
        tail = v.rsplit(",", 1)[-1].strip()
        if tail:
            return tail
    parts = v.rsplit(None, 1)
    if len(parts) == 2 and len(parts[1]) == 2 and parts[1].isalpha():
        return parts[1].upper()
    return None


def _infer_pool_tier(title: str, slug: str) -> Optional[str]:
    """Map article title/slug to a pool-tier enum string.

    Returns one of ``"national"`` | ``"regional"`` | ``"training-center"``,
    or ``None`` if the article doesn't fit any tier (caller can drop
    those — they're typically program-overview posts, not rosters).
    """
    haystack = f"{title} {slug}".lower()
    for tier, markers in _POOL_TIER_RULES:
        if any(m in haystack for m in markers):
            return tier
    return None


def _infer_gender(title: str, slug: str) -> Optional[str]:
    """Return ``'M'`` for boys/men or ``'F'`` for girls/women.

    Mixed-roster articles (e.g. "Boys and Girls rosters revealed")
    return ``None`` — those need to be split per-section by the caller
    or skipped. We keep this conservative because mis-stamping gender
    poisons the natural-key uniqueness check downstream.
    """
    h = f" {title} {slug} ".lower()
    has_boys = bool(re.search(r"\b(boys|men's|mens|male)\b", h))
    has_girls = bool(re.search(r"\b(girls|women's|womens|female)\b", h))
    if has_boys and not has_girls:
        return "M"
    if has_girls and not has_boys:
        return "F"
    return None


def _infer_birth_year(
    body_text: str,
    *,
    title: str = "",
    selection_year: Optional[int] = None,
) -> Optional[int]:
    """Scan article body + title for a birth-year hint.

    Search order: explicit "born in YYYY" / "YYYY age group" / "YYYY
    boys" / "id2 YYYY" patterns first; only fall back to "U-XX" age
    inference if a selection_year is known and no explicit pattern
    matched. ``U-XX`` arithmetic is approximate (off by one for
    early-year birthdays) so we prefer explicit hints whenever
    possible.

    Returns the four-digit birth year, or ``None`` if nothing matched.
    """
    haystack = f"{title or ''}\n{body_text or ''}"
    for pattern in _BIRTH_YEAR_PATTERNS:
        m = pattern.search(haystack)
        if m:
            try:
                year = int(m.group(1))
                if 1990 <= year <= 2030:
                    return year
            except (TypeError, ValueError):
                pass
    if selection_year is not None:
        m = _U_AGE_PATTERN.search(haystack)
        if m:
            try:
                age = int(m.group(1))
                if 8 <= age <= 23:
                    return selection_year - age
            except (TypeError, ValueError):
                pass
    return None


def _infer_selection_year(article_date: Optional[str]) -> Optional[int]:
    """Return the four-digit year from an ISO-8601 article date.

    SoccerWire's WP REST returns ``"2024-02-08T16:46:00"``; we accept
    anything starting with four digits. Returns None on garbage.
    """
    if not article_date:
        return None
    m = re.match(r"((?:19|20)\d{2})", article_date)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _select_article_body(soup: "BeautifulSoup") -> Optional["Tag"]:
    """Return the article body container, or fall back to the whole soup.

    SoccerWire wraps the article body in ``<div class="single__content">``.
    If the wrapper class drifts we still want to find tables, so the
    fallback returns the whole document — false positives are
    constrained by ``_classify_header_row`` requiring a NAME column.
    """
    body = soup.find("div", class_="single__content")
    if body is not None:
        return body
    return soup


def _build_player_row(
    *,
    name: str,
    club: Optional[str],
    position: Optional[str],
    hometown: Optional[str],
    selection_year: int,
    birth_year: Optional[int],
    gender: str,
    pool_tier: str,
    region: Optional[str],
    source_url: str,
    announced_at: Optional[str],
) -> Dict[str, Any]:
    """Shape one player into the ingest dict expected by
    ``ingest.id_selection_writer.insert_player_id_selections``."""
    state = _parse_hometown(hometown or "")
    return {
        "player_name": name,
        "selection_year": selection_year,
        "birth_year": birth_year,
        "gender": gender,
        "pool_tier": pool_tier,
        "region": region,
        "club_name_raw": club or None,
        "state": state,
        "position": (position or None) if position else None,
        "source_url": source_url,
        "source": "soccerwire",
        "announced_at": announced_at,
    }


def parse_article_body(
    html: str,
    *,
    article: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Parse one fetched article into a list of player_id_selection rows.

    Returns ``[]`` and logs a warning when the article doesn't carry a
    parseable roster (template variation, image-only roster,
    announcement post). Never raises — robustness over completeness.

    Inputs:
        html     — raw HTML from :func:`fetch_article_html`
        article  — discovery dict (keys: ``url``, ``title``, ``slug``,
                   ``date``)
    """
    if BeautifulSoup is None:
        logger.warning(
            "[usclub-id] bs4 unavailable; cannot parse %s", article.get("url"),
        )
        return []
    if not html:
        return []

    title = (article.get("title") or "").strip()
    slug = (article.get("slug") or "").strip()
    url = article.get("url") or ""
    date = article.get("date") or None

    selection_year = _infer_selection_year(date)
    pool_tier = _infer_pool_tier(title, slug)
    gender = _infer_gender(title, slug)

    if selection_year is None:
        logger.warning(
            "[usclub-id] %s: missing/unparseable date '%s'; skipping",
            url, date,
        )
        return []
    if pool_tier is None:
        logger.warning(
            "[usclub-id] %s: title doesn't match any pool tier; skipping", url,
        )
        return []
    if gender is None:
        logger.warning(
            "[usclub-id] %s: ambiguous or mixed gender; skipping (operators "
            "can split mixed rosters per-section in a follow-up if needed)",
            url,
        )
        return []

    soup = BeautifulSoup(html, "html.parser")
    body = _select_article_body(soup)
    if body is None:
        logger.warning("[usclub-id] %s: no article body found", url)
        return []

    body_text = body.get_text(separator=" ", strip=True)
    birth_year = _infer_birth_year(
        body_text, title=title, selection_year=selection_year,
    )
    if birth_year is None:
        logger.info(
            "[usclub-id] %s: no birth_year hint found; rows will collapse "
            "same-name players (per schema docstring)", url,
        )

    rows: List[Dict[str, Any]] = []

    tables = body.find_all("table") if hasattr(body, "find_all") else []
    if not tables:
        logger.warning(
            "[usclub-id] %s: no <table> elements (likely image-only roster "
            "or announcement post); skipping", url,
        )
        return []

    for table in tables:
        try:
            table_rows = _parse_one_table(
                table,
                url=url,
                selection_year=selection_year,
                birth_year=birth_year,
                gender=gender,
                pool_tier=pool_tier,
                announced_at=date,
            )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "[usclub-id] %s: table parse crashed (%s); skipping table",
                url, exc,
            )
            continue
        rows.extend(table_rows)

    if not rows:
        logger.warning(
            "[usclub-id] %s: tables present but no roster rows extracted "
            "(headers mismatched expected NAME / CLUB / etc.)", url,
        )
    else:
        logger.info(
            "[usclub-id] %s: extracted %d player rows", url, len(rows),
        )
    return rows


def _parse_one_table(
    table: "Tag",
    *,
    url: str,
    selection_year: int,
    birth_year: Optional[int],
    gender: str,
    pool_tier: str,
    announced_at: Optional[str],
) -> List[Dict[str, Any]]:
    """Parse one <table>. Skips silently if the header row doesn't
    look like a roster — articles can have unrelated tables (related
    posts, editorial widgets) that we never want to mis-interpret."""
    all_rows = table.find_all("tr")
    if not all_rows:
        return []

    # The first row with at least 2 cells is the candidate header.
    header_row = None
    for r in all_rows:
        cells = _row_text_cells(r)
        if len(cells) >= 2:
            header_row = (r, cells)
            break
    if header_row is None:
        return []

    columns = _classify_header_row(header_row[1])
    if columns is None:
        return []

    name_idx = columns["name"]
    club_idx = columns.get("club")
    position_idx = columns.get("position")
    hometown_idx = columns.get("hometown")

    out: List[Dict[str, Any]] = []
    seen_header = False
    for r in all_rows:
        if not seen_header:
            if r is header_row[0]:
                seen_header = True
            continue
        cells = _row_text_cells(r)
        if not cells:
            continue
        # Defensive: skip rows that don't have enough columns to reach
        # the name index. Some articles end the roster with a
        # blank-or-footer row.
        if name_idx >= len(cells):
            continue
        name = (cells[name_idx] or "").strip()
        if not name:
            continue
        # Skip a repeated header row (rare but seen on multi-page
        # tables that re-emit the header for legibility).
        if _norm_header(name) in _HEADER_NAME_TOKENS:
            continue
        club = cells[club_idx].strip() if club_idx is not None and club_idx < len(cells) else None
        position = (
            cells[position_idx].strip()
            if position_idx is not None and position_idx < len(cells)
            else None
        )
        hometown = (
            cells[hometown_idx].strip()
            if hometown_idx is not None and hometown_idx < len(cells)
            else None
        )
        out.append(_build_player_row(
            name=name,
            club=club,
            position=position,
            hometown=hometown,
            selection_year=selection_year,
            birth_year=birth_year,
            gender=gender,
            pool_tier=pool_tier,
            region=None,  # National-Selection articles don't carry region
            source_url=url,
            announced_at=announced_at,
        ))
    return out


def scrape_soccerwire_id_articles_full(
    *,
    max_pages: int = 4,
    keywords: Optional[List[str]] = None,
    article_limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """End-to-end: discover candidate posts AND extract player rows.

    Returns a list of dicts ready for
    ``ingest.id_selection_writer.insert_player_id_selections``. Articles
    that can't be parsed (image-only rosters, announcement posts,
    template drift) are skipped with a warning rather than crashing the
    run.

    Parameters
    ----------
    max_pages
        WP REST API page cap. Default 4 = 200 most recent posts.
    keywords
        Optional override for the iD-keyword filter.
    article_limit
        Optional cap on how many discovered articles we body-parse.
        Useful for ``--limit`` runs that want to validate end-to-end
        on a small slice.
    """
    discovered = scrape_soccerwire_id_articles(
        max_pages=max_pages, keywords=keywords,
    )
    if article_limit is not None:
        discovered = discovered[:article_limit]
    rows: List[Dict[str, Any]] = []
    for article in discovered:
        url = article.get("url") or ""
        if not url:
            continue
        html = fetch_article_html(url)
        if not html:
            continue
        rows.extend(parse_article_body(html, article=article))
    logger.info(
        "[usclub-id] full pipeline: %d articles → %d player rows",
        len(discovered), len(rows),
    )
    return rows


def scrape_usclubsoccer_members(creds: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """Option B — login-gated US Club Soccer members area.

    Stubbed pending credential provisioning + ToS review. Implementation
    plan: Playwright session, login via ``USCLUB_USERNAME`` /
    ``USCLUB_PASSWORD``, then walk the iD pool roster pages.
    """
    raise NotImplementedError(
        "US Club iD members area requires login. Set USCLUB_USERNAME and "
        "USCLUB_PASSWORD env vars and implement Playwright login flow."
    )


__all__ = [
    "scrape_soccerwire_id_articles",
    "scrape_soccerwire_id_articles_full",
    "fetch_article_html",
    "parse_article_body",
    "scrape_usclubsoccer_members",
]
