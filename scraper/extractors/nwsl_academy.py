"""
Custom extractor for NWSL Academy.

Data source strategy:
  The official nwslsoccer.com/nwsl-academy page is a Next.js app that returns
  404 for static requests and exposes no public club-directory API. NWSL Academy
  clubs are the development academies affiliated with each NWSL first-team club
  (launched 2022, growing each season). Because no machine-readable public
  directory exists we use a curated seed list derived from public NWSL press
  releases, club websites, and the league's official communications.

  The list is intentionally conservative: only clubs that have been publicly
  confirmed as NWSL Academy programmes are included. City/state data is sourced
  from each club's known home city.

Update cadence:
  When new NWSL clubs or academy affiliates are announced, add them to
  NWSL_ACADEMY_CLUBS below and re-run. The source_url points to the league page.
"""

from __future__ import annotations

import logging
from typing import List, Dict

from extractors.registry import register

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Curated NWSL Academy club list (as of 2025-26 season)
# Each entry: (club_name, city, state)
# Sources: NWSL press releases, individual club websites, news coverage
# ---------------------------------------------------------------------------
NWSL_ACADEMY_CLUBS: List[tuple[str, str, str]] = [
    # Western Conference
    ("Angel City FC Academy", "Los Angeles", "CA"),
    ("Bay FC Academy", "San Jose", "CA"),
    ("San Diego Wave FC Academy", "San Diego", "CA"),
    ("Seattle Reign Academy", "Seattle", "WA"),
    ("Utah Royals Academy", "Sandy", "UT"),
    # Central
    ("Houston Dash Academy", "Houston", "TX"),
    ("Kansas City Current Academy", "Kansas City", "MO"),
    # Eastern Conference
    ("Boston Legacy Academy", "Boston", "MA"),
    ("Chicago Red Stars Academy", "Chicago", "IL"),
    ("NJ/NY Gotham FC Academy", "Harrison", "NJ"),
    ("North Carolina Courage Academy", "Cary", "NC"),
    ("Orlando Pride Academy", "Orlando", "FL"),
    ("Portland Thorns Academy", "Portland", "OR"),
    ("Racing Louisville FC Academy", "Louisville", "KY"),
    ("Washington Spirit Academy", "Germantown", "MD"),
    # 2025 expansion affiliate
    ("Denver Aurora FC Academy", "Aurora", "CO"),
]

_SOURCE_URL = "https://www.nwslsoccer.com/nwsl-academy"


def _parse_live_html(
    html: str,
    league_name: str,
    source_url: str,
) -> List[Dict]:
    """
    Build NWSL Academy club records.

    The official nwslsoccer.com/nwsl-academy page returns a Next.js
    shell (often a 404 for server-side fetches) and exposes no public
    club-directory API, so this extractor does not actually parse HTML
    content. The ``html`` argument is accepted so the function shares
    the ``parse_html`` signature expected by ``--source replay-html``
    and so archived HTML stays associated with the produced rows; the
    records themselves are emitted from the curated ``NWSL_ACADEMY_CLUBS``
    seed list (maintained manually from NWSL press releases).

    Returning the seed even when the archived HTML is empty is correct
    for this source: replay is supposed to reproduce the scheduled
    scrape's output, and the scheduled scrape ignores the live page too.
    """
    # html is intentionally unused — see docstring.
    del html
    return [
        {
            "club_name": club_name,
            "league_name": league_name,
            "city": city,
            "state": state,
            "source_url": source_url,
        }
        for club_name, city, state in NWSL_ACADEMY_CLUBS
    ]


def parse_html(
    html: str,
    source_url: str = _SOURCE_URL,
    league_name: str | None = None,
) -> List[Dict]:
    """
    Pure-function entry point for ``--source replay-html``.

    Because NWSL Academy has no machine-readable public directory (see
    module docstring), this returns the curated seed list regardless of
    the archived HTML content. Callers that want to diff the archive
    against future schema changes should inspect the HTML separately —
    what this function guarantees is parity with the scheduled scrape.
    """
    return _parse_live_html(
        html,
        league_name or "NWSL Academy",
        source_url,
    )


@register(r"nwslsoccer\.com/nwsl-academy")
def scrape_nwsl_academy(url: str, league_name: str) -> List[Dict]:
    """
    Return curated NWSL Academy club records.

    This extractor does not make HTTP requests because the NWSL Academy page
    returns 404 and no public club-directory API exists. The seed data is
    maintained manually in NWSL_ACADEMY_CLUBS.
    """
    logger.info("[NWSL Academy] Using curated seed list (%d clubs)", len(NWSL_ACADEMY_CLUBS))
    records = _parse_live_html(html="", league_name=league_name, source_url=_SOURCE_URL)
    logger.info("[NWSL Academy] Returning %d records", len(records))
    return records
