"""
Custom extractors for additional US Club NPL member sub-leagues discovered from
the US Club Soccer NPL Member Leagues page (usclubsoccer.org/npl-member-leagues/).

Each sub-league uses a GotSport event page as its authoritative club directory.

Leagues covered:
  GLA NPL Division     – Great Lakes Alliance (OH, PA, WV)   event 43157
  NISL NPL Division    – Northern Illinois Soccer League      event 49634
  FCL NPL Division     – Florida Club League NPL              event 44970
  Red River NPL        – Texas/Oklahoma/AR region              event 45381
  Minnesota NPL        – Minnesota                             event 47013
  South Atlantic PLN   – TN, AL, GA, SC, NC region             event 51028
  MDL NPL Division     – Midwest Developmental League          event 43156
  Pacific NW NPL       – WA/OR Pacific Northwest               events 50025 + 48496 + 49835
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams

logger = logging.getLogger(__name__)


def _multi_event_scrape(events: list, league_name: str, state: str = "") -> List[Dict]:
    """Helper: scrape multiple GotSport events and deduplicate."""
    seen: set = set()
    records: List[Dict] = []
    for event_id, label in events:
        if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
            from storage import save_teams_csv, save_contacts_csv
            lbl = f"{league_name} – {label}"
            teams, contacts = scrape_gotsport_teams(event_id, lbl, state=state)
            save_teams_csv(teams, lbl)
            save_contacts_csv(contacts, lbl)
        for rec in scrape_gotsport_event(event_id, league_name, state=state):
            key = rec["club_name"].lower().strip()
            if key not in seen:
                seen.add(key)
                records.append(rec)
    return records


@register(r"glasoccer\.com")
def scrape_gla(url: str, league_name: str) -> List[Dict]:
    logger.info("[GLA NPL custom] GotSport event 43157")
    events = [(43157, "GLA NPL current"), (43156, "GLA MDL NPL")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[GLA NPL custom] %d clubs", len(records))
    return records


@register(r"northernillinoissoccerleague\.com")
def scrape_nisl(url: str, league_name: str) -> List[Dict]:
    logger.info("[NISL NPL custom] GotSport event 49634")
    events = [(49634, "NISL NPL current")]
    records = _multi_event_scrape(events, league_name, state="IL")
    logger.info("[NISL NPL custom] %d clubs", len(records))
    return records


@register(r"floridaclubleague\.com")
def scrape_fcl(url: str, league_name: str) -> List[Dict]:
    logger.info("[FCL NPL custom] GotSport event 44970")
    events = [(44970, "FCL NPL current")]
    records = _multi_event_scrape(events, league_name, state="FL")
    logger.info("[FCL NPL custom] %d clubs", len(records))
    return records


@register(r"u90c\.com.*red-river|redrivernpl\.com")
def scrape_red_river(url: str, league_name: str) -> List[Dict]:
    logger.info("[Red River NPL custom] GotSport event 45381")
    events = [(45381, "Red River NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Red River NPL custom] %d clubs", len(records))
    return records


@register(r"tcslsoccer\.com.*minnesota|minnesotanpl\.com")
def scrape_minnesota_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Minnesota NPL custom] GotSport event 47013")
    events = [(47013, "Minnesota NPL current")]
    records = _multi_event_scrape(events, league_name, state="MN")
    logger.info("[Minnesota NPL custom] %d clubs", len(records))
    return records


@register(r"southatlanticpremierleague\.com")
def scrape_sapl(url: str, league_name: str) -> List[Dict]:
    logger.info("[South Atlantic PL custom] GotSport event 51028")
    events = [(51028, "South Atlantic PL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[South Atlantic PL custom] %d clubs", len(records))
    return records


@register(r"pnwnpl\.com|pacificnorthwestnpl\.com")
def scrape_pnw_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[PNW NPL custom] GotSport events 50025 + 48496 + 49835")
    events = [
        (50025, "PNW NPL 2025-26"),
        (48496, "PNW NPL 2024-25"),
        (49835, "PNW NPL prior"),
    ]
    records = _multi_event_scrape(events, league_name)
    logger.info("[PNW NPL custom] %d clubs", len(records))
    return records
