"""
Custom extractor for ECNL (Elite Club National League) and ECNL Regional League.

Data source: AthleteOne standings API (api.athleteone.com), which backs the TGS
widget embedded in theecnl.com.

Discovery mechanism (2025-26 season):
  The API endpoint /get-conference-standings/0/{org_id}/{org_season_id}/0/0
  returns an HTML page that includes a full <select id="event-select"> dropdown
  listing EVERY conference for that org_season (with their event_ids). We parse
  that dropdown to get all event_ids, then fetch each conference's standings to
  collect team names.

Org season IDs (org_id=12, current season):
  69 → Girls ECNL    (10 conferences: Mid-Atlantic, Midwest, New England, ...)
  70 → Boys ECNL     (16 conferences: Far West, Florida, Heartland, ...)
  71 → Girls RL      (24 conferences: Carolinas, Florida, Frontier, ...)
  72 → Boys RL       (26 conferences: Carolinas, Chicago Metro, Far West, ...)

Team name format in standings: "Oregon Premier ECNL B13Qualification:..."
Club name extraction: strip " ECNL [BG]YY..." suffix.

Replay coverage
---------------
``parse_html(html, source_url, league_name)`` dispatches a pre-fetched HTML
page back through the same parsers used by the live path. It inspects the
``source_url`` to decide whether the page is a dropdown (discovery) snapshot
or a per-conference standings page:

* URL shape ``/get-conference-standings/0/<org_id>/<org_season_id>/0/0``
  → dropdown. Returns ``[]`` — discovery metadata is not a consumable row
  type for the clubs pipeline, but we still exercise the parser so
  snapshot-replay doesn't error.
* URL shape ``/get-conference-standings/<event_id>/<org_id>/<org_season_id>/0/0``
  with ``event_id > 0`` → per-conference standings. Returns the per-club
  records the live path writes.
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://theecnl.com/",
    "Accept": "*/*",
}

_BASE = "https://api.athleteone.com/api/Script/get-conference-standings"
_ORG_ID = 12

# Strip " ECNL B13Qualification:..." or " ECNL RL G12..." or " - PRE ECNL B13..." suffix.
# Handles all observed formats:
#   "Oregon Premier ECNL B13"
#   "Bloomingdale Lightning FC Pre-ECNL B13"
#   "Chicago Magic  - PRE ECNL B13"   (space-dash-space before PRE)
#   "Fort Lauderdale United FC ECNL RL G13"
_CLUB_RE = re.compile(
    r"^(.+?)\s+(?:-\s+)?(?:Pre[-\s]+)?ECNL(?:\s+RL)?\s+[BG]\d+",
    re.IGNORECASE,
)

_MIN = 3
_MAX = 80

# URL dispatch regexes. The canonical URL shape is
#   /get-conference-standings/<event_id>/<org_id>/<org_season_id>/0/0
# where event_id == 0 signals the dropdown (discovery) page and any other
# integer is a per-conference standings page.
_URL_DROPDOWN_RE = re.compile(
    r"/get-conference-standings/0/(\d+)/(\d+)/0/0",
    re.IGNORECASE,
)
_URL_STANDINGS_RE = re.compile(
    r"/get-conference-standings/(\d+)/(\d+)/(\d+)/0/0",
    re.IGNORECASE,
)


def _api_url(org_season_id: int | str, event_id: int | str = 0) -> str:
    # Correct order: /{event_id}/{org_id}/{org_season_id}/0/0
    # event_id=0 returns default conference + full dropdown listing all conference event_ids
    return f"{_BASE}/{event_id}/{_ORG_ID}/{org_season_id}/0/0"


# ---------------------------------------------------------------------------
# Pure-function parsers (no HTTP)
# ---------------------------------------------------------------------------

def parse_event_select_dropdown_html(html: str) -> List[Tuple[str, str]]:
    """
    Parse a ``<select id="event-select">`` dropdown snapshot.

    Returns a list of ``(event_id, conference_name)`` tuples. Options with
    ``value`` missing or equal to ``"0"`` (the default "choose a conference"
    placeholder) are skipped.

    Empty / unrecognised HTML returns an empty list — never raises.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    event_sel = soup.find("select", id="event-select")
    if not event_sel:
        return []

    events: List[Tuple[str, str]] = []
    for opt in event_sel.find_all("option"):
        val = opt.get("value", "").strip()
        txt = opt.get_text(strip=True)
        if val and val != "0":
            events.append((val, txt))
    return events


def parse_conference_standings_html(
    html: str,
    *,
    league_name: str = "",
    source_url: str = "",
    org_season_id: str = "",
    event_id: str = "",
    conf_name: str = "",
) -> List[Dict]:
    """
    Parse one conference's AthleteOne standings HTML.

    Returns a list of per-team records (one row per team in the standings
    table). Each record includes the scraped club name plus the full
    standings stats and AthleteOne IDs.

    The ``league_name``/``source_url``/``org_season_id``/``event_id``/
    ``conf_name`` kwargs are copied onto each record so the replay path
    can reproduce the exact same dict shape the live fetch emits. When
    the caller omits them (e.g. replay with only a URL) we fall back to
    sensible defaults.

    Empty / unrecognised HTML returns an empty list — never raises.
    """
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    team_records: List[Dict] = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue

        raw_name = tds[1].get_text(separator=" ", strip=True)
        m = _CLUB_RE.match(raw_name)
        if not m:
            continue

        club_name = m.group(1).strip()
        if not (_MIN < len(club_name) <= _MAX):
            continue

        club, age_group, gender_letter, qualification = _parse_team_name(raw_name)
        gender = _GENDER_MAP.get(gender_letter, "")

        # Extract AthleteOne IDs from the individual-team-item span
        span = tds[1].find("span", class_="individual-team-item")
        club_id = span.get("data-club-id", "") if span else ""
        team_id = span.get("data-team-id", "") if span else ""
        # data-event-id on the span equals the conference event_id we already have

        def _td(i: int) -> str:
            return tds[i].get_text(strip=True) if i < len(tds) else ""

        team_records.append({
            "club_name":     club or club_name,
            "team_name_raw": raw_name,
            "age_group":     age_group,
            "gender":        gender,
            "conference":    conf_name,
            "org_season_id": str(org_season_id),
            "event_id":      str(event_id),
            "club_id":       club_id,
            "team_id":       team_id,
            "qualification": qualification,
            "rank":  _td(0),
            "gp":    _td(2),
            "w":     _td(3),
            "l":     _td(4),
            "d":     _td(5),
            "gf":    _td(6),
            "ga":    _td(7),
            "gd":    _td(8),
            "ppg":   _td(9),
            "pts":   _td(10),
            "source_url":  source_url,
            "league_name": league_name,
        })

    return team_records


def parse_html(
    html: str,
    source_url: str = "",
    league_name: str = "",
) -> List[Dict]:
    """
    Pure-function dispatcher exposed to ``--source replay-html``.

    Inspects ``source_url`` to decide which sub-parser to call:

    * Dropdown page (``event_id=0``) → returns ``[]``. The dropdown is
      discovery metadata (a listing of per-conference event_ids). It
      doesn't produce club rows, so there's nothing for the clubs
      pipeline to write. We still invoke the parser so any malformed
      snapshot surfaces as a log warning instead of silently succeeding.
    * Per-conference standings (``event_id > 0``) → returns the list of
      per-team records produced by :func:`parse_conference_standings_html`,
      post-processed the same way the live path does (``club_name``
      collapsed + ``league_name`` stamped).
    * URL with no recognisable ECNL shape → we best-effort treat the HTML
      as a per-conference standings page. This covers any future archive
      rows whose ``source_url`` wasn't captured cleanly; the parser is
      resilient to missing tables and will return ``[]`` in that case.

    Returns a list of club-ish records with keys ``club_name``,
    ``league_name``, ``city``, ``state``, ``source_url`` (matching the
    contract used by the rest of the scraper pipeline) for the standings
    path, or an empty list for the dropdown path.
    """
    url = source_url or ""

    if _URL_DROPDOWN_RE.search(url):
        # Discovery-only page. Parse it so malformed fixtures fail loudly,
        # but the clubs pipeline gets nothing to write.
        events = parse_event_select_dropdown_html(html)
        logger.debug(
            "[ECNL replay] dropdown snapshot url=%s → %d conferences",
            url, len(events),
        )
        return []

    m = _URL_STANDINGS_RE.search(url)
    if m:
        event_id, _org_id, org_season_id = m.group(1), m.group(2), m.group(3)
    else:
        event_id, org_season_id = "", ""

    team_records = parse_conference_standings_html(
        html,
        league_name=league_name,
        source_url=url,
        org_season_id=org_season_id,
        event_id=event_id,
        conf_name="",
    )

    # Collapse to the club-level shape the rest of the pipeline expects.
    seen: set[str] = set()
    club_records: List[Dict] = []
    for rec in team_records:
        key = rec["club_name"].strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        club_records.append({
            "club_name":   rec["club_name"],
            "league_name": league_name,
            "city":        "",
            "state":       "",
            "source_url":  url,
        })
    return club_records


# ---------------------------------------------------------------------------
# Fetch wrappers (live path)
# ---------------------------------------------------------------------------

def _get_conference_event_ids(org_season_id: int | str) -> List[Tuple[str, str]]:
    """
    Call the API with event_id=0 to get the full list of conference event IDs.
    Returns list of (event_id, conference_name) tuples.
    """
    url = _api_url(org_season_id, 0)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=15)
        if r.status_code != 200 or len(r.text) < 100:
            logger.warning("Conference list fetch failed for org_season=%s: status=%d",
                           org_season_id, r.status_code)
            return []
    except Exception as exc:
        logger.error("Conference list fetch exception (org_season=%s): %s", org_season_id, exc)
        return []

    events = parse_event_select_dropdown_html(r.text)
    if not events:
        logger.warning("No event-select found in response for org_season=%s", org_season_id)
        return []

    logger.info("org_season=%s → %d conferences discovered", org_season_id, len(events))
    return events


_TEAM_AGE_RE = re.compile(r"[BG](\d{2})\b")
_GENDER_MAP = {"B": "Male", "G": "Female"}

# Columns present in the standings table (no header row rendered)
# Order: rank, team_name, gp, w, l, d, gf, ga, gd, ppg, pts
_STANDING_COLS = ["rank", "team_name_raw", "gp", "w", "l", "d", "gf", "ga", "gd", "ppg", "pts"]


def _parse_team_name(raw: str) -> Tuple[str, str, str, str]:
    """
    Parse a raw cell like 'Florida Kraze ECNL B13 Qualification: Champions League 28'.

    Returns (club_name, age_group, gender_letter, qualification).
    """
    qual_split = re.split(r"\s*Qualification:\s*", raw, maxsplit=1)
    base = qual_split[0].strip()
    qualification = qual_split[1].strip() if len(qual_split) > 1 else ""

    m = _CLUB_RE.match(base)
    if not m:
        return "", "", "", qualification

    club_name = m.group(1).strip()
    suffix = base[len(club_name):].strip()
    age_m = _TEAM_AGE_RE.search(suffix)
    age_group = age_m.group(1) if age_m else ""
    gender_letter = "B" if re.search(r"\bB\d{2}\b", suffix) else ("G" if re.search(r"\bG\d{2}\b", suffix) else "")

    return club_name, age_group, gender_letter, qualification


def _fetch_clubs_for_event(
    org_season_id: int | str,
    event_id: str,
    conf_name: str = "",
) -> Tuple[List[str], List[Dict]]:
    """
    Fetch one conference's standings.

    Returns:
        (club_names, team_records)
        club_names  — unique base club names (for the existing club-level pipeline)
        team_records — one record per team row with full standings data
    """
    url = _api_url(org_season_id, event_id)
    try:
        r = requests.get(url, headers=_HEADERS, timeout=12)
        if r.status_code != 200 or len(r.text) < 100:
            return [], []
    except Exception as exc:
        logger.debug("Fetch failed (org_season=%s event=%s): %s", org_season_id, event_id, exc)
        return [], []

    team_records = parse_conference_standings_html(
        r.text,
        league_name="",  # filled in by _scrape_org_seasons
        source_url=url,
        org_season_id=str(org_season_id),
        event_id=str(event_id),
        conf_name=conf_name,
    )

    clubs: set[str] = set()
    for rec in team_records:
        if rec["club_name"]:
            clubs.add(rec["club_name"])

    return list(clubs), team_records


def _scrape_org_seasons(
    org_season_ids: List[int],
    league_name: str,
    source_url: str,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Scrape one or more org_seasons, collecting all clubs and team standings
    across every conference. Uses dynamic conference discovery and concurrent fetches.

    Returns:
        (club_records, team_records)
    """
    all_events: List[Tuple[str, str, str]] = []
    for org_season_id in org_season_ids:
        for event_id, conf_name in _get_conference_event_ids(org_season_id):
            all_events.append((str(org_season_id), event_id, conf_name))

    if not all_events:
        logger.error("No conferences discovered for org_seasons=%s", org_season_ids)
        return [], []

    logger.info("[ECNL API] Fetching %d conferences across %d org_seasons",
                len(all_events), len(org_season_ids))

    all_clubs: set[str] = set()
    all_team_records: List[Dict] = []

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {
            ex.submit(_fetch_clubs_for_event, org_season_id, event_id, conf_name):
                (org_season_id, event_id, conf_name)
            for org_season_id, event_id, conf_name in all_events
        }
        for f in as_completed(futs):
            org_season_id, event_id, conf_name = futs[f]
            clubs, team_records = f.result()
            logger.debug("  %s (event=%s) → %d clubs, %d teams",
                         conf_name, event_id, len(clubs), len(team_records))
            all_clubs.update(clubs)
            for rec in team_records:
                rec["league_name"] = league_name
            all_team_records.extend(team_records)

    logger.info("[ECNL API] Total unique clubs: %d  |  total team rows: %d",
                len(all_clubs), len(all_team_records))

    club_records = [
        {
            "club_name": c,
            "league_name": league_name,
            "city": "",
            "state": "",
            "source_url": source_url,
        }
        for c in sorted(all_clubs)
    ]
    return club_records, all_team_records


# ---------------------------------------------------------------------------
# Registered extractors
# ---------------------------------------------------------------------------

def _scrape_and_save_teams(org_season_ids: List[int], league_name: str, url: str) -> List[Dict]:
    """
    Shared wrapper: scrapes clubs + team standings, saves teams CSV as a side
    effect (no extra HTTP requests needed), returns club records for the main
    pipeline.
    """
    from storage import save_teams_csv  # imported here to avoid circular issues at module load

    club_records, team_records = _scrape_org_seasons(org_season_ids, league_name, url)

    if team_records:
        save_teams_csv(team_records, league_name)

    return club_records


@register(r"theecnl\.com/sports/directory")
def scrape_ecnl(url: str, league_name: str) -> List[Dict]:
    """ECNL — gender-aware dispatch.

    league_name contains "girls"  → Girls ECNL only  (org_season_id=69, 10 conferences)
    league_name contains "boys"   → Boys  ECNL only  (org_season_id=70, 16 conferences)
    otherwise (combined row)      → both genders      (org_season_ids=70, 69)
    """
    name_lower = league_name.lower()
    if "girls" in name_lower and "boys" not in name_lower:
        org_seasons = [69]
        logger.info("[ECNL custom] Girls ECNL only (org_season=69)")
    elif "boys" in name_lower and "girls" not in name_lower:
        org_seasons = [70]
        logger.info("[ECNL custom] Boys ECNL only (org_season=70)")
    else:
        org_seasons = [70, 69]
        logger.info("[ECNL custom] Boys + Girls ECNL (org_seasons=70,69)")
    return _scrape_and_save_teams(org_seasons, league_name, url)


@register(r"theecnl\.com/sports/ecnl-regional-league")
def scrape_ecnl_rl(url: str, league_name: str) -> List[Dict]:
    """ECNL Regional League — Boys RL (72) or Girls RL (71)."""
    logger.info("[ECNL RL custom] Scraping ECNL RL via AthleteOne API")
    if "boys" in url.lower():
        org_seasons = [72]
    elif "girls" in url.lower():
        org_seasons = [71]
    else:
        org_seasons = [72, 71]
    return _scrape_and_save_teams(org_seasons, league_name, url)


@register(r"theecnl\.com/sports/2024/11/12/Directory")
def scrape_pre_ecnl(url: str, league_name: str) -> List[Dict]:
    """
    Pre-ECNL and Pre-ECNL North — development league one tier below ECNL.

    Org season IDs (org_id=12):
      67 = Pre-ECNL Boys     (11 conferences, older age groups)
      66 = Pre-ECNL Girls    ( 9 conferences, older age groups)
      76 = Pre-ECNL North Boys  (16 conferences, younger B2015)
      75 = Pre-ECNL North Girls (13 conferences, younger G2015)

    Both "Pre-ECNL Boys/Girls" and "Pre-ECNL North Boys/Girls" share the
    same directory URL, so we dispatch based on league_name.

    Team name formats observed:
      "Bloomingdale Lightning FC Pre-ECNL B13"
      "Chicago Magic  - PRE ECNL B13"
    Both are handled by the updated _CLUB_RE.
    """
    name_lower = league_name.lower()
    logger.info("[Pre-ECNL custom] Scraping %s via AthleteOne API", league_name)

    if "north" in name_lower and "boys" in name_lower:
        org_seasons = [76]
    elif "north" in name_lower and "girls" in name_lower:
        org_seasons = [75]
    elif "boys" in name_lower:
        org_seasons = [67]
    elif "girls" in name_lower:
        org_seasons = [66]
    else:
        # Scrape all four Pre-ECNL org_seasons
        org_seasons = [67, 66, 76, 75]

    return _scrape_and_save_teams(org_seasons, league_name, url)
