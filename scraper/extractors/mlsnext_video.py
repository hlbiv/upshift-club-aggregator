"""
mlsnext_video.py — MLS NEXT video-library HTML extractor.

MLS NEXT has no dedicated YouTube channel; its video library lives at
``https://www.mlssoccer.com/mlsnext/video/`` as a server-rendered
Brightcove-backed list. Each video card looks like::

    <a class="fm-card-wrap -customentity -brightcovevideo"
       href="/mlsnext/video/<slug>"
       title="<human title>"
       data-id="<brightcove-uuid>"
       ...>
      <article class="fm-card ...">
        ... <img data-src="<thumbnail-url>" ...>
        ... <span class='mls-o-video-card__duration-lock'>M:SS</span>
      </article>
    </a>

Path chosen: **A** — pure HTML parse. The page is server-rendered
(no __NEXT_DATA__, no inline JSON, no iframe-to-YouTube). The
Brightcove UUID (``data-id``) is a stable per-video identifier, so
we store with ``source_platform='mls_com'``, ``external_id=<uuid>``.

Scope:
  - ``fetch_page_html(url)`` — zero-auth GET. Returns HTML text or None.
  - ``parse_video_list(html, base_url)`` — pure function, BeautifulSoup.
    Returns list of dicts with the subset of video_sources columns.
  - ``fetch_mlsnext_videos(url)`` — GET + parse convenience wrapper.

``published_at`` is NULL — the listing page does not expose it (we'd
have to follow each detail page to read it, which is not worth the
extra ~12 requests per run). The detail-page enrichment can land in
a follow-up PR.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

_DEFAULT_LIST_URL = "https://www.mlssoccer.com/mlsnext/video/"
_MLS_BASE = "https://www.mlssoccer.com"

# Brightcove UUIDs are RFC-4122 canonical. Validate shape to avoid
# picking up unrelated data-id values that might appear on the page.
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
)

_DURATION_RE = re.compile(r"^\s*(\d+):(\d{2})\s*$")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_page_html(url: str = _DEFAULT_LIST_URL) -> Optional[str]:
    """GET the MLS NEXT video listing page. Returns HTML text or None.

    Soft-fails (logs a warning + returns None) on any HTTP/network
    error. Caller is responsible for telemetry.
    """
    try:
        from utils.http import get as http_get  # type: ignore
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("[mlsnext-video] utils.http unavailable: %s", exc)
        return None

    try:
        resp = http_get(
            url,
            timeout=20,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; "
                    "+https://upshift.club)"
                ),
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.8",
            },
        )
    except Exception as exc:
        logger.warning("[mlsnext-video] GET failed for %s: %s", url, exc)
        return None

    if getattr(resp, "status_code", 0) != 200:
        logger.warning(
            "[mlsnext-video] %s → HTTP %s",
            url, getattr(resp, "status_code", "?"),
        )
        return None

    return resp.text or ""


def parse_video_list(
    html: str,
    base_url: str = _MLS_BASE,
) -> List[Dict[str, Any]]:
    """Parse the MLS NEXT video-library HTML into video-source rows.

    Pure function — no I/O. Unparseable cards are skipped (logged).
    Dedupes by ``external_id`` to protect against the page rendering
    the same video in multiple slots (e.g. featured + grid).

    Each output dict has:
      - ``external_id``: Brightcove UUID from ``data-id``
      - ``source_url``: absolute mlssoccer.com URL
      - ``title``: card title (from ``title`` / ``aria-label``)
      - ``thumbnail_url``: best-available image URL (may be None)
      - ``duration_seconds``: parsed from ``M:SS`` badge (may be None)
      - ``published_at``: None (not on the listing page)
      - ``metadata``: ``{"detail_path": "/mlsnext/video/<slug>",
                         "origin": "mlssoccer.com/mlsnext/video"}``
    """
    if not html:
        return []

    try:
        from bs4 import BeautifulSoup  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.warning("[mlsnext-video] BeautifulSoup unavailable: %s", exc)
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("a.fm-card-wrap.-brightcovevideo")

    rows: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for card in cards:
        data_id = (card.get("data-id") or "").strip().lower()
        if not data_id or not _UUID_RE.match(data_id):
            logger.debug(
                "[mlsnext-video] skipping card with invalid data-id=%r",
                data_id,
            )
            continue
        if data_id in seen_ids:
            continue
        seen_ids.add(data_id)

        href = (card.get("href") or "").strip()
        if not href:
            continue
        source_url = urljoin(base_url, href)

        # Title preference: title attr → aria-label → inner <h2>.
        title = (card.get("title") or card.get("aria-label") or "").strip()
        if not title:
            h2 = card.select_one("h2")
            if h2 is not None:
                title = h2.get_text(strip=True)

        # Thumbnail preference: <img data-src> (lazy-loaded) → <img src>.
        # The inline src on this page is a 1x1 gif placeholder, so prefer
        # data-src. Walking <source> would give us responsive variants —
        # we just want the canonical URL.
        thumb_url: Optional[str] = None
        img = card.select_one("img")
        if img is not None:
            data_src = (img.get("data-src") or "").strip()
            if data_src and not data_src.startswith("data:"):
                thumb_url = data_src
            else:
                src = (img.get("src") or "").strip()
                if src and not src.startswith("data:"):
                    thumb_url = src

        duration_seconds = _parse_duration(card)

        rows.append({
            "external_id": data_id,
            "source_url": source_url,
            "title": title or None,
            "thumbnail_url": thumb_url,
            "duration_seconds": duration_seconds,
            "published_at": None,
            "metadata": {
                "detail_path": href,
                "origin": "mlssoccer.com/mlsnext/video",
            },
        })

    return rows


def fetch_mlsnext_videos(
    url: str = _DEFAULT_LIST_URL,
) -> List[Dict[str, Any]]:
    """GET + parse the MLS NEXT video listing page. Returns [] on failure."""
    html = fetch_page_html(url)
    if not html:
        return []
    return parse_video_list(html, base_url=url)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _parse_duration(card: Any) -> Optional[int]:
    """Extract ``M:SS`` duration from the card's duration-lock span.

    Returns None if the span is missing or doesn't match ``M:SS``.
    """
    span = card.select_one(".mls-o-video-card__duration-lock")
    if span is None:
        return None
    text = (span.get_text(strip=True) or "").strip()
    m = _DURATION_RE.match(text)
    if not m:
        return None
    minutes = int(m.group(1))
    seconds = int(m.group(2))
    return minutes * 60 + seconds
