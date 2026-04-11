"""
Utah Youth Soccer Association club extractor.

Source: https://www.utahyouthsoccer.net/youth-members/
Structure: 6 geographic regions, each with an HTML table listing clubs.
  - <h3> heading per region ("REGION 1", "REGION 2", etc.)
  - <table> with columns: Logo | Name | Location | Website | Programs
  - 71 clubs total as of April 2026.

This is a static HTML page — no JavaScript rendering needed.
"""

from __future__ import annotations

import logging
from typing import List, Dict

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; UpshiftClubBot/1.0)"}
_SOURCE_URL = "https://www.utahyouthsoccer.net/youth-members/"


def scrape_utah_clubs(league_name: str) -> List[Dict]:
    """Scrape the UYSA youth-members page for all member clubs."""
    try:
        r = requests.get(_SOURCE_URL, headers=_HEADERS, timeout=20)
        if r.status_code != 200:
            logger.warning("[Utah] youth-members page returned %s", r.status_code)
            return []
    except Exception as exc:
        logger.warning("[Utah] Failed to fetch youth-members page: %s", exc)
        return []

    soup = BeautifulSoup(r.text, "lxml")
    records: List[Dict] = []

    # Find all tables on the page — each region has one
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Column layout: Logo | Name | Location | Website | Programs
            # Name is in cell index 1, Location in 2, Website link in 3
            name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            location = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            website = ""
            if len(cells) > 3:
                link = cells[3].find("a", href=True)
                if link:
                    website = link["href"].strip()

            if not name or len(name) < 2:
                continue

            # Skip header rows
            lower = name.lower()
            if lower in ("name", "club name", "club", "logo"):
                continue

            # Extract city from location (format: "City, UT" or just "City")
            city = ""
            if location:
                city = location.split(",")[0].strip()

            record = {
                "club_name": name,
                "league_name": league_name,
                "city": city,
                "state": "Utah",
                "source_url": _SOURCE_URL,
            }
            if website:
                record["website"] = website

            records.append(record)

    logger.info("[Utah] Scraped %d clubs from %s", len(records), _SOURCE_URL)
    return records
