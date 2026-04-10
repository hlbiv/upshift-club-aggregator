"""
Custom extractors for additional US Club NPL member sub-leagues discovered from
the US Club Soccer NPL Member Leagues page (usclubsoccer.org/npl-member-leagues/).

Each sub-league uses a GotSport event page as its authoritative club directory.

Leagues covered (original):
  GLA NPL Division     – Great Lakes Alliance (OH, PA, WV)   event 43157
  NISL NPL Division    – Northern Illinois Soccer League      event 49634
  FCL NPL Division     – Florida Club League NPL              event 44970
  Red River NPL        – Texas/Oklahoma/AR region              event 45381
  Minnesota NPL        – Minnesota                             event 47013
  South Atlantic PLN   – TN, AL, GA, SC, NC region             event 51028
  MDL NPL Division     – Midwest Developmental League          event 43156
  Pacific NW NPL       – WA/OR Pacific Northwest               events 50025 + 48496 + 49835

Leagues covered (added Task #21):
  Ohio Valley NPL      – OH/KY/WV/IN region                   event 47989
  Desert NPL           – AZ/NV region                          event 49002
  NYCSL NPL            – NY/NJ/CT tri-state                    event 47325
  Southeast NPL        – AL/GA/FL/TN region                    event 48550
  Great Lakes NPL      – MI/IN/OH/WI region                    event 46810
  Keystone Premier     – PA/NJ/NY/MD region                    event 50140
  Gulf Coast NPL       – LA/MS/AL/FL panhandle                  event 48820

State/regional leagues (added Task #21 — GotSport-backed):
  Washington Premier   – WA state                              event 45320
  Florida Premier      – FL statewide                          event 46900
  Empire Soccer        – NY metro / upstate                    event 45817
  Mid-Atlantic SA      – VA/MD/DC metro                        event 47655
"""

from __future__ import annotations

import logging
import os
from typing import List, Dict

from rapidfuzz import fuzz

from config import FUZZY_THRESHOLD
from extractors.registry import register
from extractors.gotsport import scrape_gotsport_event, scrape_gotsport_teams
from normalizer import _canonical

logger = logging.getLogger(__name__)


def _multi_event_scrape(events: list, league_name: str, state: str = "") -> List[Dict]:
    """Helper: scrape multiple GotSport events and deduplicate.

    Uses canonical normalization + fuzzy token_sort_ratio (>= FUZZY_THRESHOLD)
    so near-duplicate club names across events (e.g. "FC Seattle" vs "FC Seattle SC")
    are collapsed at extraction time rather than propagated to the caller.
    """
    seen_canonicals: List[str] = []
    records: List[Dict] = []
    for event_id, label in events:
        if os.environ.get("UPSHIFT_SCRAPE_TEAMS"):
            from storage import save_teams_csv, save_contacts_csv
            lbl = f"{league_name} – {label}"
            teams, contacts = scrape_gotsport_teams(event_id, lbl, state=state)
            save_teams_csv(teams, lbl)
            save_contacts_csv(contacts, lbl)
        for rec in scrape_gotsport_event(event_id, league_name, state=state):
            canonical = _canonical(rec["club_name"])
            if not any(
                fuzz.token_sort_ratio(canonical, seen) >= FUZZY_THRESHOLD
                for seen in seen_canonicals
            ):
                seen_canonicals.append(canonical)
                records.append(rec)
    return records


# ---------------------------------------------------------------------------
# Original NPL sub-league extractors
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# New NPL sub-league extractors (Task #21)
# ---------------------------------------------------------------------------

@register(r"ovnpl\.com")
def scrape_ohio_valley_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Ohio Valley NPL custom] GotSport event 47989")
    events = [(47989, "Ohio Valley NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Ohio Valley NPL custom] %d clubs", len(records))
    return records


@register(r"desertnpl\.com")
def scrape_desert_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Desert NPL custom] GotSport event 49002")
    events = [(49002, "Desert NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Desert NPL custom] %d clubs", len(records))
    return records


@register(r"nycsl\.net")
def scrape_nycsl(url: str, league_name: str) -> List[Dict]:
    logger.info("[NYCSL NPL custom] GotSport event 47325")
    events = [(47325, "NYCSL NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[NYCSL NPL custom] %d clubs", len(records))
    return records


@register(r"southeastnpl\.com")
def scrape_southeast_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Southeast NPL custom] GotSport event 48550")
    events = [(48550, "Southeast NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Southeast NPL custom] %d clubs", len(records))
    return records


@register(r"greatlakesnpl\.com")
def scrape_great_lakes_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Great Lakes NPL custom] GotSport event 46810")
    events = [(46810, "Great Lakes NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Great Lakes NPL custom] %d clubs", len(records))
    return records


@register(r"keystonepremierleague\.com")
def scrape_keystone_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Keystone Premier League custom] GotSport event 50140")
    events = [(50140, "Keystone Premier current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Keystone Premier League custom] %d clubs", len(records))
    return records


@register(r"gulfcoastnpl\.com")
def scrape_gulf_coast_npl(url: str, league_name: str) -> List[Dict]:
    logger.info("[Gulf Coast NPL custom] GotSport event 48820")
    events = [(48820, "Gulf Coast NPL current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Gulf Coast NPL custom] %d clubs", len(records))
    return records


# ---------------------------------------------------------------------------
# New state/regional league extractors (Task #21 — GotSport-backed)
# ---------------------------------------------------------------------------

@register(r"wapremier\.com")
def scrape_wa_premier(url: str, league_name: str) -> List[Dict]:
    logger.info("[WA Premier FC custom] GotSport event 45320")
    events = [(45320, "WA Premier current")]
    records = _multi_event_scrape(events, league_name, state="Washington")
    logger.info("[WA Premier FC custom] %d clubs", len(records))
    return records


@register(r"fpl\.soccer")
def scrape_florida_premier(url: str, league_name: str) -> List[Dict]:
    logger.info("[Florida Premier League custom] GotSport event 46900")
    events = [(46900, "Florida Premier current")]
    records = _multi_event_scrape(events, league_name, state="Florida")
    logger.info("[Florida Premier League custom] %d clubs", len(records))
    return records


@register(r"empiresoccer\.com")
def scrape_empire_soccer(url: str, league_name: str) -> List[Dict]:
    logger.info("[Empire Soccer League custom] GotSport event 45817")
    events = [(45817, "Empire Soccer current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Empire Soccer League custom] %d clubs", len(records))
    return records


@register(r"masasoccer\.org")
def scrape_masa(url: str, league_name: str) -> List[Dict]:
    logger.info("[Mid-Atlantic SA custom] GotSport event 47655")
    events = [(47655, "MASA current")]
    records = _multi_event_scrape(events, league_name)
    logger.info("[Mid-Atlantic SA custom] %d clubs", len(records))
    return records
