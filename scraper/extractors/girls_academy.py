"""
Custom extractor for Girls Academy and GA Aspire.

Both pages share the same structure:
  <article>
    <h3>Conference Name</h3>
    <ul>
      <li>Club Name (City, State)</li>
      ...
    </ul>
  </article>

City and state are extracted from the trailing parenthetical.

The HTML-parsing logic lives in ``parse_html(html, source_url, league_name)``
at module level so the replay handler (see ``run.py::_handle_replay_html``)
can dispatch to it against archived HTML without re-fetching.
"""

from __future__ import annotations

import re
import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}

# Matches "(City, ST)" or "(City, State Name)" at end of li text
_LOCATION_RE = re.compile(r"\(([^,)]+),\s*([^)]+)\)\s*$")


def _parse_location(text: str) -> tuple[str, str, str]:
    """Return (club_name, city, state) parsed from 'Club Name (City, ST)'."""
    m = _LOCATION_RE.search(text)
    if m:
        club = text[: m.start()].strip()
        city = m.group(1).strip()
        state = m.group(2).strip()
    else:
        club = text.strip()
        city = ""
        state = ""
    return club, city, state


def parse_html(html: str, source_url: str = "", league_name: str = "") -> List[Dict]:
    """Pure parser for Girls Academy / GA Aspire members pages.

    Takes raw HTML and returns the same list-of-dicts shape that the
    registered scrape entry points produce. Used by both the live
    scrapers (which fetch via ``requests.get`` then call this) and the
    ``--source replay-html`` handler (which re-parses archived HTML).

    The ``source_url`` and ``league_name`` arguments accept empty strings
    so the replay handler can call this with whatever metadata it has
    stored against the archive row — they are passed through into each
    record.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    article = soup.find("article")
    if not article:
        logger.warning("GA: no <article> found on %s", source_url or "<no-url>")
        return []

    records: List[Dict] = []
    current_conf = ""

    for el in article.find_all(["h3", "h4", "li"]):
        if el.name in ("h3", "h4"):
            current_conf = el.get_text(strip=True)
            continue
        # Skip Divi social-follow icons ("Follow" links to FB/Twitter/IG
        # bleed through on the members page as fake <li> rows).
        classes = el.get("class") or []
        if any("et_pb_social" in c for c in classes):
            continue
        # Capture any direct website link in the <li> before stripping tags
        a_tag = el.find("a", href=True)
        website = ""
        if a_tag:
            href = a_tag["href"].strip()
            if href.startswith("http"):
                website = href
        text = el.get_text(strip=True)
        if not text or len(text) < 3:
            continue
        club_name, city, state = _parse_location(text)
        if not club_name:
            continue
        records.append({
            "club_name": club_name,
            "league_name": league_name,
            "city": city,
            "state": state,
            "source_url": source_url,
            "conference": current_conf,
            "website": website,
        })

    return records


@register(r"girlsacademyleague\.com/(members|aspire-membership)")
def scrape_girls_academy(url: str, league_name: str) -> List[Dict]:
    logger.info("[GA custom] Scraping %s", url)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("GA fetch failed: %s", exc)
        return []

    records = parse_html(r.text, source_url=url, league_name=league_name)
    logger.info("[GA custom] Found %d clubs on %s", len(records), url)
    return records
