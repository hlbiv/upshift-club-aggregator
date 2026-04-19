"""
Custom extractor for USL Academy League.

Data source: Modular11 standings API
  The USL Academy League standings are hosted on Modular11 (modular11.com),
  which is embedded as an iframe in the official USL Academy site
  (usl-academy.com/league-standings). The Modular11 API returns standings HTML
  that includes team/club names organised by division. We parse both the
  men's (UID_gender=1) and women's (UID_gender=2) divisions to build a
  deduplicated club list.

API endpoint (discovered via network traffic inspection):
  https://www.modular11.com/public_schedule/league/get_teams
    ?tournament_type=league
    &UID_age=43          (U20 — the only age group in the league)
    &UID_gender=<1|2>    (1 = Men, 2 = Women)
    &UID_event=22        (season ID — verified current as of 2025-26)
    &list_type=29        (standings view)

Club name format: `<p data-title="Club Name">` — division headings contain
  "Male" or "Female" and are skipped; all other <p data-title> values are club
  names.

SEASONAL MAINTENANCE — UID_event rollover:
  The `_CURRENT_EVENT_ID` constant below is the Modular11 season ID for the
  current competition year. Modular11 increments this ID each season. When the
  scraper starts returning 0 clubs (or the count drops well below the expected
  ~95+ men / ~10+ women), bump _CURRENT_EVENT_ID by 1 and re-run.
  Verification: open https://www.usl-academy.com/league-standings in a browser,
  inspect the network request to get_teams, and read the UID_event parameter.

  Last verified: 22 (2025-26 season, April 2026)
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
    "Referer": "https://www.usl-academy.com/",
}

# Season ID in Modular11's system — increment each season if stale.
# See SEASONAL MAINTENANCE note in module docstring.
_CURRENT_EVENT_ID = 22

_MODULAR11_BASE = (
    "https://www.modular11.com/public_schedule/league/get_teams"
    f"?tournament_type=league&UID_age=43&UID_event={_CURRENT_EVENT_ID}&list_type=29"
)

_GENDERS = {1: "Men", 2: "Women"}

# Headings that indicate a division label rather than a club name
_DIVISION_RE = re.compile(r"\b(Male|Female|Men|Women)\b", re.IGNORECASE)

# Minimum clubs expected per gender — used to detect stale/wrong event IDs
_MIN_CLUBS_MEN = 30
_MIN_CLUBS_WOMEN = 5


def _parse_live_html(html: str) -> List[str]:
    """
    Pure parser: extract unique club names from a Modular11 standings
    HTML snippet. Division-heading `data-title` values (containing
    "Male"/"Female"/"Men"/"Women") are skipped.

    No HTTP, no logging side-effects beyond caller context — the caller
    tracks which gender this HTML came from.
    """
    if not html or len(html) < 500:
        return []

    soup = BeautifulSoup(html, "lxml")
    clubs: List[str] = []
    for p in soup.find_all("p", attrs={"data-title": True}):
        title = p.get("data-title", "").strip()
        if not title or _DIVISION_RE.search(title):
            continue
        clubs.append(title)
    return clubs


def parse_html(
    html: str,
    source_url: str | None = None,
    league_name: str | None = None,
) -> List[Dict]:
    """
    Pure-function entry point for ``--source replay-html``.

    Given a single archived Modular11 standings HTML response, returns
    canonical club records (one per unique club name found in the
    snippet). The live scrape at :func:`scrape_usl_academy` fetches
    both gender divisions and deduplicates across them — replay here
    operates on exactly one HTML body at a time, so the caller replays
    each archived URL (men's + women's) independently and the dedup
    happens at the aggregation layer.
    """
    names = _parse_live_html(html)
    if not names:
        return []
    effective_league = league_name or "USL Academy League"
    effective_url = source_url or "https://www.usl-academy.com/league-standings"
    return [
        {
            "club_name": name,
            "league_name": effective_league,
            "city": "",
            "state": "",
            "source_url": effective_url,
        }
        for name in sorted(set(names))
    ]


def _fetch_clubs_by_gender(uid_gender: int) -> List[str]:
    """
    Fetch the standings HTML for one gender and extract unique club names.

    Logs a warning if the result falls below the expected minimum, which may
    indicate that _CURRENT_EVENT_ID needs to be bumped for the new season.

    Returns a list of club name strings.
    """
    url = f"{_MODULAR11_BASE}&UID_gender={uid_gender}"
    gender_label = _GENDERS.get(uid_gender, str(uid_gender))
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as exc:
        logger.error("[USL Academy] Failed to fetch %s divisions: %s", gender_label, exc)
        return []

    if len(r.text) < 500:
        logger.warning("[USL Academy] Suspiciously short response for gender=%d", uid_gender)
        return []

    clubs = _parse_live_html(r.text)

    logger.info("[USL Academy] Gender=%s → %d clubs", gender_label, len(clubs))

    # Warn if the result looks suspiciously low — possible event ID rollover
    min_expected = _MIN_CLUBS_MEN if uid_gender == 1 else _MIN_CLUBS_WOMEN
    if 0 < len(clubs) < min_expected:
        logger.warning(
            "[USL Academy] Only %d %s clubs found (expected >=%d). "
            "Consider bumping _CURRENT_EVENT_ID (currently %d) if this is a new season.",
            len(clubs), gender_label, min_expected, _CURRENT_EVENT_ID,
        )
    elif len(clubs) == 0:
        logger.warning(
            "[USL Academy] Zero %s clubs returned. "
            "_CURRENT_EVENT_ID=%d may be stale — check the network traffic on "
            "https://www.usl-academy.com/league-standings and update the constant.",
            gender_label, _CURRENT_EVENT_ID,
        )

    return clubs


@register(r"usl-academy\.com/academy-league")
def scrape_usl_academy(url: str, league_name: str) -> List[Dict]:
    """
    Extract USL Academy League clubs from the Modular11 standings API.

    Fetches both men's and women's divisions, deduplicates, and returns
    a list of club records with league_name set. City/state are not
    available from the Modular11 source.
    """
    logger.info("[USL Academy] Fetching clubs from Modular11 API (event_id=%d)", _CURRENT_EVENT_ID)

    all_clubs: set[str] = set()
    for uid_gender in _GENDERS:
        clubs = _fetch_clubs_by_gender(uid_gender)
        all_clubs.update(clubs)

    if not all_clubs:
        logger.warning("[USL Academy] No clubs found via Modular11 API")
        return []

    records: List[Dict] = [
        {
            "club_name": club,
            "league_name": league_name,
            "city": "",
            "state": "",
            "source_url": url,
        }
        for club in sorted(all_clubs)
    ]

    logger.info("[USL Academy] Total unique clubs: %d", len(records))
    return records
