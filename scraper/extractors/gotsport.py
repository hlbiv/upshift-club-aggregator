"""
Shared helper for scraping GotSport club roster pages.

GotSport is used by several leagues (SOCAL, MSPSP, etc.) to manage
league events. Each event has a clubs tab at:
  https://system.gotsport.com/org_event/events/{event_id}/clubs

The page renders plain HTML (no JS required). Rows starting with "ZZ-"
are internal admin/SRA placeholder entries and are filtered out.

Team-level data (opt-in via scrape_gotsport_teams):
  Each club has a detail page at /clubs/{club_id} listing every team
  registered for the event (name, gender, age group, division, bracket)
  plus a contact directory (registrar email/phone, director).
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time
from typing import List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import MAX_RETRIES, RETRY_BASE_DELAY_SECONDS
from utils.retry import retry_with_backoff, TransientError

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0; +https://upshift.club)"}
_BASE = "https://system.gotsport.com"

_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        code = exc.response.status_code if exc.response is not None else 0
        return code in _RETRYABLE_STATUS_CODES
    return False


def _get_with_retry(url: str, timeout: int = 20) -> requests.Response:
    """Fetch *url* with retry/backoff on transient errors."""
    def _fetch() -> requests.Response:
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if _is_retryable(exc):
                raise TransientError(str(exc)) from exc
            raise

    return retry_with_backoff(
        _fetch,
        max_retries=MAX_RETRIES,
        base_delay=RETRY_BASE_DELAY_SECONDS,
        label=f"gotsport:{url}",
    )


def _event_clubs_url(event_id: int | str) -> str:
    return f"{_BASE}/org_event/events/{event_id}/clubs"


def _fetch_gotsport_event(url: str) -> str:
    """
    Fetch the raw HTML of a GotSport event clubs page.

    Pure HTTP — no parsing. Returns the response body as text. Uses
    ``_get_with_retry`` so transient 5xx / connection errors are retried.
    Callers that need replay/fixture testing can skip this helper and
    feed HTML directly into ``parse_gotsport_event_html``.
    """
    r = _get_with_retry(url)
    return r.text


def _parse_clubs_with_ids_from_html(html: str) -> List[Tuple[str, str]]:
    """
    Parse the (club_name, club_id) pairs out of a GotSport clubs-page body.

    Filters ZZ- placeholder rows. Pure function — no HTTP, no logging.
    """
    soup = BeautifulSoup(html, "lxml")
    clubs: List[Tuple[str, str]] = []

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        raw = tds[0].get_text(strip=True)
        club_name = raw.replace("Schedule", "").strip()
        if not club_name or len(club_name) < 2 or club_name.startswith("ZZ-"):
            continue

        link = row.find("a", href=re.compile(r"/clubs/\d+$"))
        club_id = ""
        if link:
            m = re.search(r"/clubs/(\d+)$", link["href"])
            if m:
                club_id = m.group(1)

        clubs.append((club_name, club_id))

    return clubs


def parse_gotsport_event_html(
    html: str,
    url: str,
    league_name: str = "",
    state: str = "",
    multi_state: bool = False,
) -> List[Dict]:
    """
    Turn a GotSport event-clubs HTML page into the list of club dicts that
    ``scrape_gotsport_event`` returns.

    Pure function — no HTTP. Callers feeding fixture HTML into this for
    replay coverage should pass the canonical ``url`` of the source page so
    every record's ``source_url`` matches live output.

    Args:
        html:        Raw HTML body of ``/org_event/events/{id}/clubs``.
        url:         Canonical source URL, stamped onto each record's
                     ``source_url``.
        league_name: League name to tag on each record.
        state:       State name/code to inject when the event is single-state.
                     Ignored when ``multi_state=True``.
        multi_state: When True, the event spans multiple states. Club state
                     is left empty and each record is marked
                     ``_state_derived=True`` so downstream enrichment
                     (geocoding / manual mapping) can fill it in.

    Returns:
        List of club dicts ready for the normalizer.
    """
    clubs = _parse_clubs_with_ids_from_html(html)
    if not clubs:
        return []

    effective_state = "" if multi_state else state

    records: List[Dict] = []
    for club_name, _club_id in clubs:
        rec = {
            "club_name": club_name,
            "league_name": league_name,
            "city": "",
            "state": effective_state,
            "source_url": url,
        }
        if multi_state:
            rec["_state_derived"] = True
        records.append(rec)

    return records


def _get_clubs_with_ids(event_id: int | str) -> List[Tuple[str, str]]:
    """
    Fetch the clubs list page and return (club_name, club_id) pairs.
    Filters ZZ- placeholder rows.

    Thin compatibility wrapper around ``_fetch_gotsport_event`` +
    ``_parse_clubs_with_ids_from_html`` so the existing retry/logging
    contract (swallow transient fetch errors and return []) is preserved
    for ``scrape_gotsport_teams``.
    """
    url = _event_clubs_url(event_id)
    try:
        html = _fetch_gotsport_event(url)
    except (TransientError, requests.RequestException) as exc:
        logger.error("GotSport clubs fetch failed (event_id=%s): %s", event_id, exc)
        return []

    return _parse_clubs_with_ids_from_html(html)


def _fetch_club_detail(event_id: int | str, club_name: str, club_id: str) -> Dict:
    """
    Fetch one club's detail page and extract teams + contacts.

    Raises requests.RequestException or TransientError on fetch failure so that
    callers can distinguish a real failure from an empty-result page.

    Returns {"teams": [...], "contacts": [...], "url": str} on success.
    """
    url = f"{_BASE}/org_event/events/{event_id}/clubs/{club_id}"
    result: Dict = {"teams": [], "contacts": [], "url": url}

    r = _get_with_retry(url, timeout=15)

    soup = BeautifulSoup(r.text, "lxml")

    for widget in soup.find_all("div", class_="widget"):
        header_div = widget.find("div", class_="widget-header")
        if not header_div:
            continue
        h4 = header_div.find("h4")
        if not h4:
            continue
        section_title = h4.get_text(strip=True).lower()

        body_div = widget.find("div", class_="widget-body")
        table = body_div.find("table") if body_div else widget.find("table")
        if not table:
            continue

        if "teams" in section_title:
            for row in table.find_all("tr"):
                tds = row.find_all("td")
                if not tds:
                    continue
                team = {
                    "team_name": tds[0].get_text(strip=True) if len(tds) > 0 else "",
                    "gender":    tds[1].get_text(strip=True) if len(tds) > 1 else "",
                    "division":  tds[3].get_text(strip=True) if len(tds) > 3 else "",
                    "bracket":   tds[4].get_text(strip=True) if len(tds) > 4 else "",
                }
                age_m = re.search(r"\b([BG])(\d{2})\b", team["team_name"])
                team["age_group"] = f"{age_m.group(1)}{age_m.group(2)}" if age_m else ""
                if team["team_name"]:
                    result["teams"].append(team)

        elif "contact" in section_title:
            for row in table.find_all("tr"):
                tds = row.find_all("td")
                if not tds:
                    continue
                contact = {
                    "role":  tds[0].get_text(strip=True) if len(tds) > 0 else "",
                    "name":  tds[1].get_text(strip=True) if len(tds) > 1 else "",
                    "email": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                    "phone": tds[3].get_text(strip=True) if len(tds) > 3 else "",
                }
                if contact["name"] and contact["role"]:
                    result["contacts"].append(contact)

    return result


def scrape_gotsport_event(
    event_id: int | str,
    league_name: str,
    state: str = "",
    multi_state: bool = False,
) -> List[Dict]:
    """
    Fetch all clubs from a GotSport event clubs page.

    Thin orchestrator around ``_fetch_gotsport_event`` (HTTP) and
    ``parse_gotsport_event_html`` (pure parse). Public signature is
    preserved for the 13+ extractors that call this function — splitting
    fetch + parse enables fixture-based replay coverage of parsing logic
    without touching caller code.

    Args:
        event_id:    The numeric event ID from the GotSport URL.
        league_name: League name to tag on each record.
        state:       State name/code to inject when the event is single-state.
                     Ignored when multi_state=True.
        multi_state: When True, the event spans clubs from multiple states.
                     Club state is left empty so downstream enrichment (e.g.
                     geocoding or manual mapping) can derive it from the club's
                     own data rather than inheriting the parent event's region.

    Returns:
        List of club dicts ready for normalizer.
    """
    url = _event_clubs_url(event_id)
    logger.info("[GotSport] Fetching event %s: %s", event_id, url)

    try:
        html = _fetch_gotsport_event(url)
    except (TransientError, requests.RequestException) as exc:
        logger.error("GotSport clubs fetch failed (event_id=%s): %s", event_id, exc)
        return []

    if multi_state and state:
        logger.info(
            "[GotSport] event %s is multi-state; ignoring hardcoded state '%s' — "
            "club state will be derived from club data",
            event_id, state,
        )

    records = parse_gotsport_event_html(
        html,
        url,
        league_name=league_name,
        state=state,
        multi_state=multi_state,
    )

    logger.info("[GotSport] event %s → %d clubs", event_id, len(records))
    return records


def _collect_detail_results(
    event_id: int | str,
    league_name: str,
    clubs_with_ids: List[Tuple[str, str]],
    max_workers: int,
) -> Tuple[List[Dict], List[Dict], List[Tuple[str, str]]]:
    """
    Run one pass of parallel club-detail fetches.

    Returns (teams, contacts, failed_clubs) where failed_clubs are those
    whose futures raised an exception (fetch or parse failure).
    """
    all_teams: List[Dict] = []
    all_contacts: List[Dict] = []
    failed: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {
            ex.submit(_fetch_club_detail, event_id, club_name, club_id): (club_name, club_id)
            for club_name, club_id in clubs_with_ids
        }
        for fut in as_completed(futs):
            club_name, club_id = futs[fut]
            try:
                detail = fut.result()
            except Exception as exc:
                logger.debug(
                    "[GotSport] Detail fetch failed for %s (id=%s): %s",
                    club_name, club_id, exc,
                )
                failed.append((club_name, club_id))
                continue

            src = detail["url"]
            for team in detail["teams"]:
                all_teams.append({
                    "club_name":   club_name,
                    "team_name":   team["team_name"],
                    "gender":      team["gender"],
                    "age_group":   team["age_group"],
                    "division":    team["division"],
                    "bracket":     team["bracket"],
                    "league_name": league_name,
                    "event_id":    str(event_id),
                    "source_url":  src,
                })
            for contact in detail["contacts"]:
                all_contacts.append({
                    "club_name":   club_name,
                    "role":        contact["role"],
                    "name":        contact["name"],
                    "email":       contact["email"],
                    "phone":       contact["phone"],
                    "league_name": league_name,
                    "event_id":    str(event_id),
                    "source_url":  src,
                })

    return all_teams, all_contacts, failed


def scrape_gotsport_teams(
    event_id: int | str,
    league_name: str,
    state: str = "",
    max_workers: int = 20,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Scrape team-level and contact data from every club's detail page.

    Failed workers are retried up to MAX_RETRIES times (with a short
    delay between rounds) before being recorded as permanently failed.

    Returns:
        (teams, contacts) — two lists of dicts ready to be written as CSVs.

    teams columns:
        club_name, team_name, gender, age_group, division, bracket,
        league_name, event_id, source_url

    contacts columns:
        club_name, role, name, email, phone, league_name, event_id, source_url
    """
    logger.info("[GotSport] Scraping team details for event %s (%s)", event_id, league_name)

    clubs = _get_clubs_with_ids(event_id)
    if not clubs:
        logger.warning("[GotSport] No clubs found for event %s", event_id)
        return [], []

    clubs_with_ids = [(cn, cid) for cn, cid in clubs if cid]

    all_teams: List[Dict] = []
    all_contacts: List[Dict] = []
    remaining = clubs_with_ids

    for attempt in range(MAX_RETRIES + 1):
        if not remaining:
            break

        if attempt > 0:
            delay = min(RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)), 60.0)
            logger.warning(
                "[GotSport] event %s — retry attempt %d/%d for %d failed club(s), waiting %.1fs",
                event_id, attempt, MAX_RETRIES, len(remaining), delay,
            )
            time.sleep(delay)

        teams, contacts, failed = _collect_detail_results(
            event_id, league_name, remaining, max_workers
        )
        all_teams.extend(teams)
        all_contacts.extend(contacts)
        remaining = failed

    if remaining:
        logger.error(
            "[GotSport] event %s — %d club(s) permanently failed after %d attempt(s): %s",
            event_id,
            len(remaining),
            MAX_RETRIES + 1,
            ", ".join(cn for cn, _ in remaining),
        )

    logger.info(
        "[GotSport] event %s → %d teams, %d contacts (from %d clubs)",
        event_id, len(all_teams), len(all_contacts), len(clubs),
    )
    return all_teams, all_contacts
