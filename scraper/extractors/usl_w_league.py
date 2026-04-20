"""
Custom extractor for USL W League — women's pre-professional (amateur) tier.

USL W League is distinct from USL Super League (pro). Its public site runs
on SportNgin SiteBuilder behind Cloudflare. Phase 0 reconnaissance (issue
#156) determined this is a hard-go: the team directory is fully
server-rendered HTML, so no Playwright is needed.

Two complementary data sources, used whichever is cleaner per field:

Source 1 (primary) — ``/league-teams`` on uslwleague.com
  ~132 KB of server-rendered HTML grouped by geographic division (e.g.
  "NORTHEAST DIVISION", "SOCAL DIVISION"). Each team is a
  ``<span class="teamname">…</span>`` nested inside a
  ``<a href="https://www.uslwleague.com/<slug>">`` anchor. Divisions are
  interleaved via ``<h2 class="division">…</h2>`` headings — the parser
  simply reads every ``<span class="teamname">`` and treats each as a
  club name. 95 teams expected (verified 2026-04-20).

Source 2 (cross-check) — Modular11 iframe, tenant ``w-league``
  The schedule/standings pages at ``uslwleague.com/league-schedule``
  embed a Modular11 iframe at
  ``https://www.modular11.com/league-schedule/w-league``. That page
  contains a ``<select name="team">`` with every team as an ``<option>``
  (value = Modular11 team id, text = team display name). This is useful
  for graceful degradation: if SportNgin reorganises ``/league-teams``
  and the primary source returns 0 teams, the extractor falls back to
  the Modular11 dropdown.

The Modular11 configuration is also accessible server-side — the inline
``scheduleConfig`` JS object sets ``tournament: 25`` for USL W, which is
the Modular11 UID_event (tournament id) for the current competition.
Unlike ``usl_academy.py`` we do NOT call Modular11's ``get_teams`` JSON
endpoint: for USL W that endpoint returns "There are no teams" regardless
of gender/age parameters (the USL W standings shape is group-scoped
rather than gender-scoped). The team dropdown on the iframe page is the
stable cross-check.

SEASONAL MAINTENANCE — UID_event rollover:
  ``_MODULAR11_TOURNAMENT_ID`` below is the Modular11 tournament id for
  the current season. Modular11 typically bumps this each season. When
  the Modular11 cross-check starts returning 0 teams (or far fewer than
  expected), bump ``_MODULAR11_TOURNAMENT_ID`` by 1-3 and re-run.
  Verification: fetch ``https://www.modular11.com/league-schedule/w-league``
  and regex-grep for ``tournament:\\s*\\d+`` in the inline JS block —
  the number there is the current value.

  Last verified: 25 (2025-26 season, April 2026 — 95 teams via primary
  source, 95 via Modular11 dropdown).
"""

from __future__ import annotations

import logging
import re
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

from extractors.registry import register

logger = logging.getLogger(__name__)

# Real Chrome UA — SportNgin behind Cloudflare is picky about bare
# clients. Matches the UA used by ``scripts/src/probe-hudl-fan.ts``.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
}

# Current Modular11 tournament id — used only for the cross-check fallback.
# See SEASONAL MAINTENANCE note in module docstring.
_MODULAR11_TOURNAMENT_ID = 25

_LEAGUE_TEAMS_URL = "https://www.uslwleague.com/league-teams"
_MODULAR11_SCHEDULE_URL = "https://www.modular11.com/league-schedule/w-league"

# Expected team count. The published USL W directory has ~95-96 clubs
# as of the 2025-26 season; if we see fewer than 80 it's a strong signal
# that either the /league-teams layout changed or the Cloudflare policy
# tightened.
_MIN_CLUBS = 80

# Division-heading filter (guards against a malformed page where the
# <h2 class="division"> label ends up parsed as a teamname span).
_DIVISION_RE = re.compile(r"\bDIVISION\b", re.IGNORECASE)

# Modular11 inline JS: `tournament: 25` inside the scheduleConfig object.
_TOURNAMENT_RE = re.compile(r"tournament\s*:\s*(\d+)")


def _parse_league_teams_html(html: str) -> List[str]:
    """
    Pure parser for the ``/league-teams`` HTML shape.

    Reads every ``<span class="teamname">`` and returns the stripped text.
    Division-heading labels (filtered by ``_DIVISION_RE``) are excluded
    defensively even though they normally live in ``<h2>`` rather than
    ``<span class="teamname">``.
    """
    if not html or len(html) < 500:
        return []

    soup = BeautifulSoup(html, "lxml")
    names: List[str] = []
    for span in soup.find_all("span", class_="teamname"):
        text = (span.get_text() or "").strip()
        if not text:
            continue
        if _DIVISION_RE.search(text):
            continue
        names.append(text)
    return names


def _parse_modular11_html(html: str) -> List[str]:
    """
    Pure parser for the Modular11 ``league-schedule/w-league`` iframe HTML.

    Reads every ``<option>`` under ``<select name="team">`` with a
    non-zero value and returns the display name. The sentinel
    "Nothing Selected" option (value="0") is excluded.
    """
    if not html or len(html) < 500:
        return []

    soup = BeautifulSoup(html, "lxml")
    sel = soup.find("select", attrs={"name": "team"})
    if sel is None:
        return []

    names: List[str] = []
    for opt in sel.find_all("option"):
        val = (opt.get("value") or "").strip()
        if not val or val == "0":
            continue
        text = (opt.get_text() or "").strip()
        if text and text.lower() != "nothing selected":
            names.append(text)
    return names


def _extract_modular11_tournament_id(html: str) -> int | None:
    """Return the UID_event/tournament id from an inline scheduleConfig JS block."""
    if not html:
        return None
    m = _TOURNAMENT_RE.search(html)
    if m is None:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def parse_html(
    html: str,
    source_url: str | None = None,
    league_name: str | None = None,
) -> List[Dict]:
    """
    Pure-function entry point for ``--source replay-html``.

    Heuristic: the ``/league-teams`` HTML always contains at least one
    ``<span class="teamname">``; the Modular11 iframe HTML always contains
    ``<select name="team">``. Try them in order and return whichever
    yields names first.
    """
    names = _parse_league_teams_html(html)
    if not names:
        names = _parse_modular11_html(html)
    if not names:
        return []

    effective_league = league_name or "USL W League"
    effective_url = source_url or _LEAGUE_TEAMS_URL
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


def _fetch(url: str, label: str) -> str:
    """
    Fetch a URL with the real-Chrome UA headers. Returns an empty string
    on any HTTP error, so callers can gracefully degrade to the
    alternate source rather than raising.
    """
    try:
        r = requests.get(url, headers=_HEADERS, timeout=30)
    except requests.RequestException as exc:
        logger.error("[USL W League] %s fetch failed: %s", label, exc)
        return ""

    if r.status_code == 403:
        logger.warning(
            "[USL W League] %s returned 403 — Cloudflare may be rate-limiting "
            "this egress. Consider retrying from a different IP or via the "
            "Replit-production egress. Continuing without crashing.",
            label,
        )
        return ""
    if r.status_code >= 400:
        logger.warning(
            "[USL W League] %s returned HTTP %d — skipping this source.",
            label, r.status_code,
        )
        return ""

    if len(r.text) < 500:
        logger.warning(
            "[USL W League] %s returned suspiciously short response "
            "(%d bytes) — treating as empty.",
            label, len(r.text),
        )
        return ""

    return r.text


def _fetch_primary() -> List[str]:
    """Fetch ``/league-teams`` and parse teamname spans."""
    html = _fetch(_LEAGUE_TEAMS_URL, "league-teams (primary)")
    if not html:
        return []
    names = _parse_league_teams_html(html)
    logger.info("[USL W League] Primary /league-teams source → %d teams", len(names))
    return names


def _fetch_modular11_crosscheck() -> List[str]:
    """
    Fetch the Modular11 iframe HTML and parse the team dropdown.

    Also logs a warning if the discovered tournament id drifts from
    ``_MODULAR11_TOURNAMENT_ID`` — that's the SEASONAL MAINTENANCE
    signal to bump the constant.
    """
    html = _fetch(_MODULAR11_SCHEDULE_URL, "Modular11 cross-check")
    if not html:
        return []

    tid = _extract_modular11_tournament_id(html)
    if tid is not None and tid != _MODULAR11_TOURNAMENT_ID:
        logger.warning(
            "[USL W League] Modular11 tournament id drifted: page says %d, "
            "code has %d. Consider bumping _MODULAR11_TOURNAMENT_ID. "
            "(See SEASONAL MAINTENANCE note in extractor docstring.)",
            tid, _MODULAR11_TOURNAMENT_ID,
        )

    names = _parse_modular11_html(html)
    logger.info(
        "[USL W League] Modular11 cross-check source → %d teams (tournament id=%s)",
        len(names), tid if tid is not None else "unknown",
    )
    return names


@register(r"uslwleague\.com")
def scrape_usl_w_league(url: str, league_name: str) -> List[Dict]:
    """
    Extract USL W League clubs.

    Primary source: ``/league-teams`` server-rendered HTML.
    Cross-check: Modular11 iframe team dropdown (tenant=w-league).

    Returns canonical club records. City/state are not available from
    either source — the canonical-club-linker resolves those via
    ``club_aliases`` + ``canonical_clubs``. Both sources are cheap
    (~132 KB and ~50 KB respectively), so we fetch both and union the
    results — the cross-check catches teams that slip out of the
    primary page's division rendering.
    """
    logger.info(
        "[USL W League] Scraping %s (tournament id=%d, min expected=%d)",
        url, _MODULAR11_TOURNAMENT_ID, _MIN_CLUBS,
    )

    all_clubs: set[str] = set()
    all_clubs.update(_fetch_primary())
    all_clubs.update(_fetch_modular11_crosscheck())

    if not all_clubs:
        logger.warning(
            "[USL W League] Both sources returned 0 teams. Possible causes: "
            "(1) Cloudflare 403 from this egress; (2) SportNgin changed the "
            "/league-teams layout; (3) Modular11 tournament id %d is stale. "
            "Re-run from Replit-production egress before filing as a hard block.",
            _MODULAR11_TOURNAMENT_ID,
        )
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

    if len(records) < _MIN_CLUBS:
        logger.warning(
            "[USL W League] Only %d unique clubs (expected >= %d). "
            "Consider bumping _MODULAR11_TOURNAMENT_ID or checking "
            "/league-teams for layout changes.",
            len(records), _MIN_CLUBS,
        )
    else:
        logger.info("[USL W League] Total unique clubs: %d", len(records))

    return records
