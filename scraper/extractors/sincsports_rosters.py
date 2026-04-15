"""
SincSports team rosters extractor.

For a given SincSports tournament id (``tid``), walks the team list page
(``TTTeamList.aspx``) to discover ``(teamid, team_metadata)`` pairs, then
fetches each ``TTRoster.aspx?tid=<tid>&teamid=<teamid>`` page and parses
the roster table.

Output rows match the ``club_roster_snapshots`` contract consumed by
``scraper/ingest/roster_snapshot_writer.py``:

    {
        "club_name_raw": str,         # required
        "source_url":    str,         # TTRoster URL
        "snapshot_date": datetime,    # now() at scrape time
        "season":        str,         # "2025-26" / "2026-27"
        "age_group":     str | None,  # "U12"
        "gender":        str | None,  # "M" | "F"
        "division":      str | None,  # "Gold 7v7"
        "player_name":   str,         # required
        "jersey_number": str | None,
        "position":      None,        # SincSports roster pages don't expose this
        "event_id":      None,        # resolved downstream
    }

STRATEGY
--------
**April 2026 update — SincSports does NOT expose public roster URLs.**
The ``TTTeamList.aspx`` page renders team rows as plain text cells with
no ``teamid=``-bearing anchors (verified against 14 real tournament
pages: GULFC, etc). ``TTRoster.aspx?teamid=<N>`` 302-redirects to
``/pageNotFound.aspx`` even when invoked directly. Rosters are gated
behind team-admin auth and ``__doPostBack`` calls on the Schedules
page — there is no public HTML surface we can scrape.

PR #15's original design (fetch TTTeamList → follow per-team
``TTRoster.aspx`` anchors → parse roster table) was built against a
fabricated fixture (``teamlist_ROSTR.html`` with invented
``TTRoster.aspx?teamid=NNN`` links). The real-world HTML has no such
anchors, so ``parse_team_descriptors`` always returns ``[]`` and the
scraper logs ``0 teams discovered`` for every seed tid.

The extractor now:
  * keeps ``parse_team_descriptors`` + ``parse_roster_html`` as pure
    functions (fixture-driven tests still pass; both functions are
    sound when given a page with the expected shape — e.g. a future
    SincSports redesign, or an alternate roster source that reuses
    this module).
  * at runtime (``scrape_sincsports_rosters``) detects the "no teamid
    anchors on real TTTeamList" case and emits a single loud warning
    per run instead of continuing to thrash the network. Also counts
    total ``teamid=`` regex hits in the raw HTML so future SincSports
    template changes that re-expose the links are caught automatically.
  * returns ``[]`` gracefully so the runner's ZERO_RESULTS path records
    the partial run correctly.

If/when SincSports exposes public rosters via a different endpoint, the
pure parse functions can be reused against that endpoint with only
``scrape_sincsports_rosters`` needing updated fetch logic. Writer +
runner + dispatcher are unchanged.

Roster pages are plain HTML, one table whose header row contains at
least one of ``{name, player}`` and optionally ``{jersey, number, #}``.
Empty rosters (SincSports teams sometimes have not posted players yet)
yield an empty list for that team; we keep going.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from extractors.sincsports_events import (
    _SKIP_NAMES,
    _parse_division,
    normalize_gender,
)
from utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"
}
_BASE_URL = "https://soccer.sincsports.com"
_TEAM_LIST_PATH = "/TTTeamList.aspx"
_ROSTER_PATH = "/TTRoster.aspx"

_TEAMID_RE = re.compile(r"teamid=(\d+)", re.IGNORECASE)

# Roster table header tokens. SincSports varies column names across
# tournaments — match on any that looks like a player name / jersey.
_NAME_HEADERS = {"name", "player", "player name", "first name"}
_JERSEY_HEADERS = {"jersey", "jersey number", "number", "#", "no", "no."}


def current_season_tag(now: Optional[datetime] = None) -> str:
    """Return the current youth-soccer season tag, e.g. ``2025-26``.

    US youth soccer seasons roll over in August — months August (8)
    through December belong to the current-year-to-next-year season;
    January through July belong to the prior-year-to-current season.

    Keeps the same convention as the platform's TS ``getCurrentSeason()``
    helper (see CLAUDE.md → Scraper insert utilities). We re-implement
    here because the scraper has no Python season helper of its own yet.
    """
    n = now or datetime.utcnow()
    if n.month >= 8:
        return f"{n.year}-{(n.year + 1) % 100:02d}"
    return f"{n.year - 1}-{n.year % 100:02d}"


@dataclass
class TeamDescriptor:
    """One team within a SincSports tournament, discovered from the
    team-list page. ``teamid`` is the numeric SincSports team id used
    to build the TTRoster URL.
    """
    tid: str
    teamid: str
    team_name_raw: str
    club_name: str
    state: Optional[str]
    age_group: Optional[str]
    gender: Optional[str]
    division_code: Optional[str]
    birth_year: Optional[int]


def _is_roster_team_table(table: Tag) -> bool:
    first_row = table.find("tr")
    if not first_row:
        return False
    cells = [c.get_text(strip=True).lower() for c in first_row.find_all(["td", "th"])]
    return bool({"team", "club", "state"} & set(cells)) and "team" in cells


def parse_team_descriptors(html: str, tid: str) -> List[TeamDescriptor]:
    """Extract ``TeamDescriptor`` rows from a TTTeamList HTML document.

    Pure function — no HTTP, no DB. Fixture-driven.
    """
    soup = BeautifulSoup(html, "lxml")
    descriptors: List[TeamDescriptor] = []
    seen_keys: set = set()
    claimed_tables: set = set()

    for h2 in soup.find_all("h2"):
        header_text = h2.get_text(" ", strip=True)
        if not header_text:
            continue
        age_group, gender, division_code, birth_year = _parse_division(header_text)
        if age_group is None and division_code is None:
            continue

        # Find next team-shaped table (mirrors sincsports_events.py).
        nxt = h2.find_next("table")
        while nxt is not None and (id(nxt) in claimed_tables or not _is_roster_team_table(nxt)):
            nxt = nxt.find_next("table")
        if nxt is None:
            continue
        claimed_tables.add(id(nxt))

        header_cells = [c.get_text(strip=True).lower() for c in nxt.find("tr").find_all(["td", "th"])]
        try:
            team_idx = header_cells.index("team")
            club_idx = header_cells.index("club")
            state_idx = header_cells.index("state")
        except ValueError:
            continue

        for row in nxt.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < max(team_idx, club_idx, state_idx) + 1:
                continue

            team_cell = cells[team_idx]
            team_name = team_cell.get_text(" ", strip=True)
            club_name = cells[club_idx].get_text(" ", strip=True)
            state_raw = cells[state_idx].get_text(strip=True)

            if not team_name or team_name.lower() in _SKIP_NAMES:
                continue
            if not club_name or club_name.lower() in _SKIP_NAMES:
                club_name = team_name

            # Find the teamid from any <a href> inside the team cell.
            teamid: Optional[str] = None
            for a in team_cell.find_all("a", href=True):
                m = _TEAMID_RE.search(a["href"])
                if m:
                    teamid = m.group(1)
                    break
            if teamid is None:
                # No linked team → we can't fetch a roster. Skip.
                continue

            state = state_raw.upper() if (len(state_raw) == 2 and state_raw.isalpha()) else None
            key = (teamid, team_name.strip().lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)

            descriptors.append(TeamDescriptor(
                tid=tid,
                teamid=teamid,
                team_name_raw=team_name.strip(),
                club_name=club_name.strip(),
                state=state,
                age_group=age_group,
                gender=gender,
                division_code=division_code,
                birth_year=birth_year,
            ))

    return descriptors


def _resolve_header_indexes(header_cells: List[str]) -> Tuple[Optional[int], Optional[int]]:
    name_idx: Optional[int] = None
    jersey_idx: Optional[int] = None
    for i, h in enumerate(header_cells):
        if name_idx is None and h in _NAME_HEADERS:
            name_idx = i
        if jersey_idx is None and h in _JERSEY_HEADERS:
            jersey_idx = i
    # Fallback: SincSports sometimes uses "Player Name" in a merged cell.
    if name_idx is None:
        for i, h in enumerate(header_cells):
            if "name" in h:
                name_idx = i
                break
    return name_idx, jersey_idx


def parse_roster_html(html: str) -> List[Tuple[str, Optional[str]]]:
    """Extract ``(player_name, jersey_number)`` tuples from a TTRoster page.

    Pure function — no HTTP. Empty rosters return ``[]``.
    """
    soup = BeautifulSoup(html, "lxml")
    results: List[Tuple[str, Optional[str]]] = []
    seen_names: set = set()

    for table in soup.find_all("table"):
        header_row = table.find("tr")
        if not header_row:
            continue
        header_cells = [c.get_text(strip=True).lower() for c in header_row.find_all(["td", "th"])]
        name_idx, jersey_idx = _resolve_header_indexes(header_cells)
        if name_idx is None:
            continue

        for row in table.find_all("tr")[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) <= name_idx:
                continue
            player_name = cells[name_idx].get_text(" ", strip=True)
            if not player_name or player_name.lower() in _SKIP_NAMES:
                continue
            # Heuristic: a "player name" cell shouldn't be a number —
            # guard against parsing the wrong column on quirky pages.
            if player_name.isdigit():
                continue
            jersey: Optional[str] = None
            if jersey_idx is not None and jersey_idx < len(cells):
                j = cells[jersey_idx].get_text(strip=True)
                jersey = j or None

            if player_name in seen_names:
                continue
            seen_names.add(player_name)
            results.append((player_name, jersey))

        if results:
            # First table with a recognizable roster wins.
            break

    return results


def _fetch(url: str, timeout: int = 25) -> str:
    def _do() -> str:
        r = requests.get(url, headers=_HEADERS, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text
    return retry_with_backoff(
        _do,
        max_retries=2,
        base_delay=2.0,
        retryable_exceptions=(requests.exceptions.RequestException,),
        label=f"sincsports:{url}",
    )


def scrape_sincsports_rosters(
    tid: str,
    *,
    max_teams: Optional[int] = None,
    snapshot_date: Optional[datetime] = None,
    season: Optional[str] = None,
) -> List[Dict]:
    """Scrape every team's roster for a SincSports tournament.

    Returns a list of ``club_roster_snapshots`` row dicts ready for
    ``insert_roster_snapshots``. Emits one row per player per team.
    Network errors on a single team are logged and skipped — the
    overall scrape continues.
    """
    tid = tid.strip()
    if not tid:
        return []
    snap = snapshot_date or datetime.utcnow()
    season_tag = season or current_season_tag(snap)

    team_list_url = f"{_BASE_URL}{_TEAM_LIST_PATH}?tid={tid}"
    logger.info("[sincsports-rosters] fetching team list tid=%s", tid)
    try:
        html = _fetch(team_list_url)
    except Exception as exc:
        logger.error("[sincsports-rosters] team list fetch failed tid=%s: %s", tid, exc)
        return []

    # SincSports' public TTTeamList page does not render TTRoster.aspx
    # anchors — rosters are gated behind team-admin auth + __doPostBack.
    # Detect that shape explicitly (zero teamid= hits anywhere in the
    # raw HTML) and fail loudly *once* rather than logging "0 teams"
    # per seed on every run. If SincSports ever starts exposing teamid
    # links again, this guard auto-disables and parse_team_descriptors
    # takes over.
    teamid_hits = len(_TEAMID_RE.findall(html))
    if teamid_hits == 0:
        logger.warning(
            "[sincsports-rosters] tid=%s — TTTeamList exposes no teamid "
            "anchors; SincSports rosters are not publicly scrapable via "
            "TTRoster.aspx. Skipping. (HTML length=%d; grep teamid=0)",
            tid, len(html),
        )
        return []

    descriptors = parse_team_descriptors(html, tid)
    if max_teams is not None:
        descriptors = descriptors[:max_teams]
    if not descriptors:
        logger.warning(
            "[sincsports-rosters] tid=%s — TTTeamList had %d teamid hits "
            "but parse_team_descriptors yielded 0 teams (page structure "
            "may have changed — inspect fixture + parser)",
            tid, teamid_hits,
        )
        return []

    rows: List[Dict] = []
    for td in descriptors:
        url = f"{_BASE_URL}{_ROSTER_PATH}?tid={td.tid}&teamid={td.teamid}"
        try:
            roster_html = _fetch(url)
        except Exception as exc:
            logger.warning(
                "[sincsports-rosters] roster fetch failed tid=%s teamid=%s: %s",
                td.tid, td.teamid, exc,
            )
            continue
        players = parse_roster_html(roster_html)
        if not players:
            logger.info(
                "[sincsports-rosters] empty roster tid=%s teamid=%s team=%s",
                td.tid, td.teamid, td.team_name_raw,
            )
            continue
        for player_name, jersey in players:
            rows.append({
                "club_name_raw": td.club_name,
                "source_url": url,
                "snapshot_date": snap,
                "season": season_tag,
                "age_group": td.age_group,
                "gender": td.gender,
                "division": td.division_code,
                "player_name": player_name,
                "jersey_number": jersey,
                "position": None,
                "event_id": None,
            })

    logger.info(
        "[sincsports-rosters] tid=%s → %d teams, %d player rows",
        tid, len(descriptors), len(rows),
    )
    return rows
