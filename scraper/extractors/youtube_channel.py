"""
youtube_channel.py — Generic YouTube channel RSS extractor.

Every YouTube channel exposes a zero-auth Atom feed at::

    https://www.youtube.com/feeds/videos.xml?channel_id=<UC...>

The feed returns the last ~15 videos with ``videoId``, ``title``,
``published``, ``media:thumbnail``, ``media:description``. That's
enough to seed ``video_sources`` rows for Pipeline 1a. Duration is
NOT in the feed — fetching it requires YouTube Data API v3 and is
deferred to a follow-up PR.

Scope:
  - ``resolve_channel_id(handle_or_id)`` — accepts ``UC...`` directly,
    or resolves ``@handle`` via the channel HTML.
  - ``parse_feed(xml)`` — pure function. Input: Atom XML bytes/str.
    Output: list of dicts matching ``video_sources`` columns (subset).
  - ``fetch_channel_videos(channel_id)`` — RSS GET + ``parse_feed``.

All functions are exception-tolerant: on parse/HTTP failure, empty
list is returned and a warning is logged. Callers are responsible
for threading telemetry into ``scrape_run_logs``.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

_RSS_BASE = "https://www.youtube.com/feeds/videos.xml"
_CHANNEL_HTML_BASE = "https://www.youtube.com/"

_UC_RE = re.compile(r"^UC[A-Za-z0-9_-]{22}$")
_CHANNEL_META_RE = re.compile(
    r'<meta\s+itemprop="(?:identifier|channelId)"\s+content="(UC[A-Za-z0-9_-]{22})"'
)
_CHANNEL_URL_RE = re.compile(
    r'"channelId":"(UC[A-Za-z0-9_-]{22})"'
)

# Atom feed namespaces used by YouTube's RSS endpoint.
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "yt": "http://www.youtube.com/xml/schemas/2015",
    "media": "http://search.yahoo.com/mrss/",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_channel_id(handle_or_id: str) -> Optional[str]:
    """Return the ``UC...`` channel id for either a channel id or an
    ``@handle`` / legacy username.

    If ``handle_or_id`` already matches the ``UC`` format it is returned
    unchanged. Otherwise we GET the channel's HTML page and extract the
    ``channelId`` meta tag.

    Returns None on HTTP/parse failure. Caller handles the None.
    """
    if not handle_or_id:
        return None
    s = handle_or_id.strip()
    if _UC_RE.match(s):
        return s

    # Normalize input to a URL fragment. Accept either "@TheECNL" or
    # "TheECNL" or even a full "/c/foo" URL — strip leading slash.
    fragment = s.lstrip("/")
    if not fragment.startswith("@") and "/" not in fragment:
        # Treat a bare token as a handle.
        fragment = f"@{fragment}"

    url = f"{_CHANNEL_HTML_BASE}{fragment}"
    try:
        # Lazy import — keep `parse_feed` pure / importable without HTTP deps.
        from utils.http import get as http_get  # type: ignore
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("[youtube-channel] utils.http unavailable: %s", exc)
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
                "Accept-Language": "en-US,en;q=0.8",
            },
        )
    except Exception as exc:
        logger.warning("[youtube-channel] resolve GET failed for %s: %s", url, exc)
        return None

    if getattr(resp, "status_code", 0) != 200:
        logger.warning(
            "[youtube-channel] resolve %s → HTTP %s",
            url, getattr(resp, "status_code", "?"),
        )
        return None

    html = resp.text or ""
    # Prefer the explicit meta tag; fall back to inline JSON blob.
    m = _CHANNEL_META_RE.search(html) or _CHANNEL_URL_RE.search(html)
    if not m:
        logger.warning(
            "[youtube-channel] could not extract channelId from %s", url,
        )
        return None
    return m.group(1)


def parse_feed(xml: str) -> List[Dict[str, Any]]:
    """Parse a YouTube Atom feed XML string into video-source rows.

    Pure function — no I/O, no logging side-effects beyond warnings.

    Each output dict has:
      - ``external_id``: the YouTube videoId
      - ``source_url``: canonical watch URL
      - ``title``: entry title
      - ``published_at``: ISO-8601 string as-published (tz-aware)
      - ``thumbnail_url``: best-available thumbnail URL (may be None)
      - ``metadata``: ``{"description": "..."}`` — other platform-
        specific fields land here to avoid schema migrations.
    """
    if not xml:
        return []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        logger.warning("[youtube-channel] feed XML parse error: %s", exc)
        return []

    rows: List[Dict[str, Any]] = []
    for entry in root.findall("atom:entry", _NS):
        video_id_node = entry.find("yt:videoId", _NS)
        if video_id_node is None or not (video_id_node.text or "").strip():
            continue
        video_id = video_id_node.text.strip()

        title_node = entry.find("atom:title", _NS)
        title = (title_node.text or "").strip() if title_node is not None else None

        published_node = entry.find("atom:published", _NS)
        published_at = (
            (published_node.text or "").strip() if published_node is not None else None
        )

        link_node = entry.find("atom:link", _NS)
        source_url = None
        if link_node is not None:
            source_url = link_node.attrib.get("href")
        if not source_url:
            source_url = f"https://www.youtube.com/watch?v={video_id}"

        thumbnail_url: Optional[str] = None
        media_group = entry.find("media:group", _NS)
        if media_group is not None:
            thumb = media_group.find("media:thumbnail", _NS)
            if thumb is not None:
                thumbnail_url = thumb.attrib.get("url")
            description_node = media_group.find("media:description", _NS)
            description = (
                (description_node.text or "").strip()
                if description_node is not None
                else None
            )
        else:
            description = None

        rows.append({
            "external_id": video_id,
            "source_url": source_url,
            "title": title,
            "published_at": published_at,
            "thumbnail_url": thumbnail_url,
            "metadata": {"description": description} if description else {},
        })

    return rows


def fetch_channel_videos(channel_id: str) -> List[Dict[str, Any]]:
    """GET the channel's RSS feed and parse it. Returns [] on failure.

    Caller is responsible for logging telemetry — this function only
    logs warnings on soft failures.
    """
    if not channel_id:
        return []
    url = f"{_RSS_BASE}?channel_id={channel_id}"
    try:
        from utils.http import get as http_get  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.warning("[youtube-channel] utils.http unavailable: %s", exc)
        return []

    try:
        resp = http_get(
            url,
            timeout=20,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; "
                    "+https://upshift.club)"
                ),
                "Accept": "application/atom+xml,application/xml;q=0.9",
            },
        )
    except Exception as exc:
        logger.warning("[youtube-channel] RSS GET failed for %s: %s", url, exc)
        return []

    if getattr(resp, "status_code", 0) != 200:
        logger.warning(
            "[youtube-channel] RSS %s → HTTP %s",
            url, getattr(resp, "status_code", "?"),
        )
        return []

    return parse_feed(resp.text or "")
