"""
Custom extractor for ECNL (Elite Club National League).

Data source: AthleteOne standings API (api.athleteone.com), which is the backend
powering the TGS/Sidearm standings widget embedded in theecnl.com.

The API exposes per-conference standings pages.  Each conference_id maps to one
specific age-group + league-tier combination (ECNL / ECNL RL / Pre-ECNL) for the
Pacific Northwest / Mountain West region.  Clubs from other regional conferences
(Florida, Texas, Midwest, etc.) are accessible via the same API under different
conference_ids; a secondary Playwright path captures any clubs not found via
the direct API sweep.

Known conference IDs (current 2025-26 season, org_id=12):
  41  G2010 ECNL        42  B2010 ECNL
  47  G2011 Pre-ECNL    48  B2011 Pre-ECNL
  49  G2011 ECNL        50  B2011 ECNL
  55  G2012 Pre-ECNL    56  B2012 Pre-ECNL
  60  G2012 ECNL        61  B2012 ECNL
  62  G2012 ECNL RL     63  B2012 ECNL RL
  66  G2013 Pre-ECNL    67  B2013 Pre-ECNL
  69  G2013 ECNL        70  B2013 ECNL
  71  G2013 ECNL RL     72  B2013 ECNL RL
  75  G2015 Pre-ECNL    76  B2015 Pre-ECNL
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)",
    "Referer": "https://theecnl.com/",
}

_BASE = "https://api.athleteone.com/api/Script/get-conference-standings"
_ORG_ID = 12

# All currently-known conference IDs for the active ECNL season
_CONF_IDS = [41, 42, 47, 48, 49, 50, 55, 56, 60, 61, 62, 63, 66, 67, 69, 70, 71, 72, 75, 76]

# Strip the " ECNL B13Qualification:..." suffix from team-name cells
_CLUB_RE = re.compile(r"^(.+?)\s+(?:Pre-)?ECNL(?:\s+RL)?\s+[BG]\d+", re.IGNORECASE)

# Sidearm CSS selectors to try on Playwright-rendered fallback pages
_SIDEARM_SELECTORS = [
    ".s-directory-list-item",
    ".s-directory-item",
    "[class*='directory'] li",
    ".s-main-section li",
    "#sidearm-page-content li",
    "main li",
    "article li",
]

_MIN = 4
_MAX = 80


def _fetch_conf(conf_id: int, event_id: int = 0) -> List[str]:
    """Fetch one conference's standings and return club names."""
    url = f"{_BASE}/0/{_ORG_ID}/{conf_id}/{event_id}/0"
    try:
        r = requests.get(url, headers=_HEADERS, timeout=10)
        if r.status_code != 200 or len(r.text) < 100:
            return []
        soup = BeautifulSoup(r.text, "lxml")
        clubs: List[str] = []
        for td in soup.find_all("td"):
            text = td.get_text(strip=True)
            m = _CLUB_RE.match(text)
            if m:
                club = m.group(1).strip()
                if _MIN < len(club) <= _MAX:
                    clubs.append(club)
        return clubs
    except Exception as exc:
        logger.debug("AthleteOne conf %d failed: %s", conf_id, exc)
        return []


def _scrape_via_api(league_name: str, url: str) -> List[Dict]:
    """Scrape all known ECNL conferences via AthleteOne API."""
    all_clubs: set = set()

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(_fetch_conf, cid): cid for cid in _CONF_IDS}
        for f in as_completed(futs):
            for club in f.result():
                all_clubs.add(club)

    logger.info("[ECNL API] Collected %d unique clubs across %d conferences",
                len(all_clubs), len(_CONF_IDS))

    return [
        {"club_name": c, "league_name": league_name, "city": "", "state": "", "source_url": url}
        for c in all_clubs
    ]


def _scrape_via_playwright(url: str, league_name: str) -> List[Dict]:
    """Fallback: use Playwright on the ECNL standings page and try Sidearm selectors."""
    from extractors.playwright_helper import render_page
    html = render_page(url, wait_until="networkidle", timeout_ms=35_000)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    for tag in soup.find_all(["nav", "footer", "header", "script", "style"]):
        tag.decompose()
    seen: set = set()
    records: List[Dict] = []
    for sel in _SIDEARM_SELECTORS:
        items = soup.select(sel)
        if not items:
            continue
        for item in items:
            text = item.get_text(strip=True)
            if _MIN < len(text) <= _MAX and text not in seen:
                seen.add(text)
                records.append({"club_name": text, "league_name": league_name,
                                "city": "", "state": "", "source_url": url})
        if records:
            return records
    return records


@register(r"theecnl\.com/sports/directory")
def scrape_ecnl(url: str, league_name: str) -> List[Dict]:
    logger.info("[ECNL custom] Starting (AthleteOne API path)")
    records = _scrape_via_api(league_name, url)
    if not records:
        logger.info("[ECNL custom] API returned nothing, trying Playwright fallback")
        records = _scrape_via_playwright(url, league_name)
    logger.info("[ECNL custom] Total: %d clubs", len(records))
    return records


@register(r"theecnl\.com/sports/ecnl-regional-league")
def scrape_ecnl_rl(url: str, league_name: str) -> List[Dict]:
    """ECNL Regional League — uses the same AthleteOne API (RL conferences included)."""
    logger.info("[ECNL RL custom] Starting (AthleteOne API path)")
    # RL-specific conf_ids (subsets of _CONF_IDS that contain RL data)
    rl_conf_ids = [62, 63, 71, 72]
    seen: set = set()
    records: List[Dict] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(_fetch_conf, cid): cid for cid in rl_conf_ids}
        for f in as_completed(futs):
            for club in f.result():
                if club not in seen:
                    seen.add(club)
                    records.append({"club_name": club, "league_name": league_name,
                                    "city": "", "state": "", "source_url": url})
    logger.info("[ECNL RL custom] Total: %d clubs", len(records))
    return records
