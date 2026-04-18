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

Scope of THIS scaffold PR:
    ``scrape_soccerwire_id_articles`` discovers the candidate posts and
    returns parsed *metadata* only (title, url, date, excerpt). The
    follow-up PR will add per-post-template body parsing into player
    rows and feed them through ``ingest.id_selection_writer``. The body
    parser is the riskiest, most fragile piece — yearly template churn —
    so we intentionally ship discovery first and iterate.

Pattern after ``scraper/extractors/soccerwire.py:_fetch_all_slugs`` for
the WP REST API call shape.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Optional

import requests

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
    "scrape_usclubsoccer_members",
]
