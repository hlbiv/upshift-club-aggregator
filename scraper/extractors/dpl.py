"""
Custom extractor for DPL (Development Player League).

DPL runs all its events through GotSport. Each event page on dpleague.org
embeds a GotSport org_event URL containing the event ID.

Strategy:
  1. Fetch every known event page on dpleague.org via the WordPress Pages API.
  2. Parse the raw HTML for embedded GotSport event IDs (pattern: events/NNNNN).
  3. Deduplicate event IDs; skip the shared navigation stub (41948 appears on
     every page as part of the site-wide header widget and covers the current
     DPL season — it IS a real event and is included).
  4. Call scrape_gotsport_event() for each event ID to get club names.
  5. Return the union of all clubs, deduplicated.

No Playwright required — all data is available in plain HTML.

GotSport event IDs confirmed across DPL pages (as of April 2026):
  41948  — DPL Fall 2025 (main season, site-wide header)
  47256  — DPL Eastern Regional U15-U19 2026 (Tampa)
  47257  — DPL Summit 2026
  43116  — DPL Finals 2025
  37201  — DPL Summit 2025
  47254  — DPL Western Regional U15-U19 2025 (Phoenix)
  47252  — DPL Western Regional U13/U14 2025 (Provo)
  37207  — DPL Eastern Regional U13/U14 2025 (Greensboro)
  37200  — DPL Eastern Regional U15-U19 2025 (Tampa25)
  37202  — DPL Western Regional 2024 (Tucson)
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict, Set

import requests

from extractors.registry import register
from extractors.gotsport import parse_gotsport_event_html, scrape_gotsport_event

logger = logging.getLogger(__name__)


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function parser for one DPL-hosted GotSport event-clubs page.

    DPL orchestrates many GotSport events (see ``_KNOWN_EVENT_IDS``); the
    live ``scrape_dpl`` entry point loops over them and dedups. For replay
    (``--source replay-html``) we receive a single pre-fetched page at a
    time, so ``parse_html`` delegates straight to
    ``parse_gotsport_event_html`` and lets cross-page dedup / club-name
    filtering stay in the orchestrator.
    """
    return parse_gotsport_event_html(html, source_url, league_name=league_name)

# Strings that indicate a GotSport row is a league/org label rather than a club
_NOT_A_CLUB: Set[str] = {
    "the dpl",
    "dpl",
    "development player league",
    "gotsport",
    "schedule",
}

_NOT_A_CLUB_PREFIXES = ("zz-",)


def _is_valid_club(club_name: str) -> bool:
    """Return False if the club name looks like an org/league label, not a real club."""
    low = club_name.lower().strip()
    if low in _NOT_A_CLUB:
        return False
    if any(low.startswith(p) for p in _NOT_A_CLUB_PREFIXES):
        return False
    return True

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}
_BASE = "https://dpleague.org"

_EVENT_KEYWORDS = ("regional", "finals", "summit", "eastern", "western", "bracket",
                   "standings", "tucson", "provo", "phoenix", "greensboro", "tampa",
                   "richmond", "college-scouts")

_KNOWN_EVENT_IDS: List[int] = [
    41948,  # DPL Fall 2025 (site-wide header / main season event)
    47256,  # DPL Eastern Regional U15-U19 2026 (Tampa)
    47257,  # DPL Summit 2026
    43116,  # DPL Finals 2025
    37201,  # DPL Summit 2025
    47254,  # DPL Western Regional U15-U19 2025 (Phoenix)
    47252,  # DPL Western Regional U13/U14 2025 (Provo)
    37207,  # DPL Eastern Regional U13/U14 2025 (Greensboro)
    37200,  # DPL Eastern Regional U15-U19 2025 (Tampa25)
    37202,  # DPL Western Regional 2024 (Tucson)
]

_GOTSPORT_ID_RE = re.compile(r"events/(\d{4,6})")


def _discover_event_ids_from_wp() -> Set[int]:
    """
    Scrape dpleague.org event pages via the WordPress API and extract
    any GotSport event IDs embedded in the page HTML.
    """
    event_ids: Set[int] = set()
    try:
        r = requests.get(
            f"{_BASE}/wp-json/wp/v2/pages?per_page=100",
            headers=_HEADERS, timeout=20,
        )
        if r.status_code != 200:
            return event_ids
        pages = r.json()
    except Exception as exc:
        logger.warning("[DPL] WP API error: %s", exc)
        return event_ids

    for page in pages:
        slug = page.get("slug", "").lower()
        title = page.get("title", {}).get("rendered", "").lower()
        link = page.get("link", "")
        if not any(k in slug or k in title for k in _EVENT_KEYWORDS):
            continue
        # Fetch the actual page HTML to extract embedded GotSport IDs
        try:
            resp = requests.get(link, headers=_HEADERS, timeout=20)
            for m in _GOTSPORT_ID_RE.finditer(resp.text):
                event_ids.add(int(m.group(1)))
        except Exception as exc:
            logger.debug("[DPL] Could not fetch %s: %s", link, exc)

    logger.info("[DPL] Discovered %d event IDs from WP pages", len(event_ids))
    return event_ids


@register(r"dpleague\.org")
def scrape_dpl(url: str, league_name: str) -> List[Dict]:
    logger.info("[DPL custom] Starting scrape via GotSport")

    # Seed with known IDs then augment with dynamic discovery
    all_event_ids: Set[int] = set(_KNOWN_EVENT_IDS)
    try:
        discovered = _discover_event_ids_from_wp()
        all_event_ids |= discovered
    except Exception as exc:
        logger.warning("[DPL] Dynamic discovery failed: %s", exc)

    logger.info("[DPL] Total unique GotSport event IDs to scrape: %d", len(all_event_ids))

    seen_clubs: Set[str] = set()
    all_records: List[Dict] = []

    for event_id in sorted(all_event_ids):
        try:
            records = scrape_gotsport_event(event_id, league_name, state="")
        except Exception as exc:
            logger.warning("[DPL] GotSport event %s failed: %s", event_id, exc)
            continue

        new = [r for r in records
               if r["club_name"] not in seen_clubs and _is_valid_club(r["club_name"])]
        seen_clubs.update(r["club_name"] for r in new)
        all_records.extend(new)
        logger.info("[DPL] Event %s → %d clubs (%d new)", event_id, len(records), len(new))

    logger.info("[DPL custom] Total unique clubs: %d", len(all_records))
    return all_records
