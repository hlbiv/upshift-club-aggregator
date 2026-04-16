"""
Seed the colleges table with NCAA D1/D2/D3 + NAIA soccer programs.

Data sources:
  - D1: Hardcoded from upshift-player-platform ncaa.ts (SEC, ACC, Big Ten,
    Big East, Big 12, Mountain West, WCC, AAC, A-10 conferences).
    ~117 programs covering the major conferences.
  - D2: Hardcoded from upshift-player-platform ncaa-d2-roster-scraper.ts
    KNOWN_D2_URLS map. ~100 schools with known athletic site URLs.
  - D3: Hardcoded from upshift-player-platform ncaa-d3-roster-scraper.ts
    KNOWN_D3_URLS map. ~100 schools with known athletic site URLs.
  - NAIA: Not included yet — the player-platform fetches NAIA schools from
    Wikipedia at runtime. A future scraper can populate these.

Gaps:
  - D1 list covers major conferences only; smaller conferences (Horizon,
    Summit, ASUN, Sun Belt, etc.) are missing. The ncaa-d1-scraper.ts fetches
    the full ~210 men's + ~340 women's programs from Wikipedia at runtime.
    A follow-up scraper task can backfill the rest.
  - D2/D3 lists are the schools with known athletic URLs, not the full
    division membership. ~200-300 schools per division are missing.
  - NAIA (~235 schools) is entirely absent.

Usage:
    python -m scraper.seeds.seed_colleges              # real run
    python -m scraper.seeds.seed_colleges --dry-run    # preview only
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import logging
from typing import List, Dict, Optional, Tuple

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_colleges")

# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------

def slugify(name: str, division: str, gender_program: str) -> str:
    """Generate a URL-friendly slug from school name + division + gender."""
    base = name.lower()
    base = re.sub(r"[''`]", "", base)
    base = re.sub(r"[^a-z0-9]+", "-", base)
    base = base.strip("-")
    gender_suffix = "m" if gender_program == "mens" else "w" if gender_program == "womens" else "b"
    return f"{base}-{division.lower()}-{gender_suffix}"


# ---------------------------------------------------------------------------
# D1 seed data — sourced from upshift-player-platform/artifacts/scraper/src/ncaa.ts
# Major conferences only. Each entry creates both mens + womens rows unless
# genders is restricted.
# ---------------------------------------------------------------------------

_D1_SCHOOLS: List[Dict] = [
    # SEC (women only — SEC does not sponsor men's soccer)
    {"name": "University of Alabama", "city": "Tuscaloosa", "state": "AL", "conference": "SEC", "website": "https://rolltide.com", "genders": ["womens"]},
    {"name": "University of Arkansas", "city": "Fayetteville", "state": "AR", "conference": "SEC", "website": "https://arkansasrazorbacks.com", "genders": ["womens"]},
    {"name": "Auburn University", "city": "Auburn", "state": "AL", "conference": "SEC", "website": "https://auburntigers.com", "genders": ["womens"]},
    {"name": "University of Florida", "city": "Gainesville", "state": "FL", "conference": "SEC", "website": "https://floridagators.com", "genders": ["womens"]},
    {"name": "University of Georgia", "city": "Athens", "state": "GA", "conference": "SEC", "website": "https://georgiadogs.com", "genders": ["womens"]},
    {"name": "University of Kentucky", "city": "Lexington", "state": "KY", "conference": "SEC", "website": "https://ukathletics.com", "genders": ["womens"]},
    {"name": "Louisiana State University", "city": "Baton Rouge", "state": "LA", "conference": "SEC", "website": "https://lsusports.net", "genders": ["womens"]},
    {"name": "Mississippi State University", "city": "Starkville", "state": "MS", "conference": "SEC", "website": "https://hailstate.com", "genders": ["womens"]},
    {"name": "University of Missouri", "city": "Columbia", "state": "MO", "conference": "SEC", "website": "https://mutigers.com", "genders": ["womens"]},
    {"name": "University of Mississippi", "city": "Oxford", "state": "MS", "conference": "SEC", "website": "https://olemisssports.com", "genders": ["womens"]},
    {"name": "University of Oklahoma", "city": "Norman", "state": "OK", "conference": "SEC", "website": "https://soonersports.com", "genders": ["womens"]},
    {"name": "University of South Carolina", "city": "Columbia", "state": "SC", "conference": "SEC", "website": "https://gamecocksonline.com", "genders": ["womens"]},
    {"name": "University of Tennessee", "city": "Knoxville", "state": "TN", "conference": "SEC", "website": "https://utsports.com", "genders": ["womens"]},
    {"name": "University of Texas", "city": "Austin", "state": "TX", "conference": "SEC", "website": "https://texassports.com", "genders": ["womens"]},
    {"name": "Texas A&M University", "city": "College Station", "state": "TX", "conference": "SEC", "website": "https://12thman.com", "genders": ["womens"]},
    {"name": "Vanderbilt University", "city": "Nashville", "state": "TN", "conference": "SEC", "website": "https://vucommodores.com", "genders": ["womens"]},
    # ACC (both genders, except Florida State = women only)
    {"name": "Boston College", "city": "Chestnut Hill", "state": "MA", "conference": "ACC", "website": "https://bceagles.com", "genders": ["mens", "womens"]},
    {"name": "Clemson University", "city": "Clemson", "state": "SC", "conference": "ACC", "website": "https://clemsontigers.com", "genders": ["mens", "womens"]},
    {"name": "Duke University", "city": "Durham", "state": "NC", "conference": "ACC", "website": "https://goduke.com", "genders": ["mens", "womens"]},
    {"name": "Florida State University", "city": "Tallahassee", "state": "FL", "conference": "ACC", "website": "https://seminoles.com", "genders": ["womens"]},
    {"name": "Georgia Institute of Technology", "city": "Atlanta", "state": "GA", "conference": "ACC", "website": "https://ramblinwreck.com", "genders": ["mens", "womens"]},
    {"name": "University of Louisville", "city": "Louisville", "state": "KY", "conference": "ACC", "website": "https://gocards.com", "genders": ["mens", "womens"]},
    {"name": "University of Miami", "city": "Coral Gables", "state": "FL", "conference": "ACC", "website": "https://miamihurricanes.com", "genders": ["mens", "womens"]},
    {"name": "NC State University", "city": "Raleigh", "state": "NC", "conference": "ACC", "website": "https://gopack.com", "genders": ["mens", "womens"]},
    {"name": "University of North Carolina", "city": "Chapel Hill", "state": "NC", "conference": "ACC", "website": "https://goheels.com", "genders": ["mens", "womens"]},
    {"name": "University of Notre Dame", "city": "South Bend", "state": "IN", "conference": "ACC", "website": "https://fightingirish.com", "genders": ["mens", "womens"]},
    {"name": "University of Pittsburgh", "city": "Pittsburgh", "state": "PA", "conference": "ACC", "website": "https://pittsburghpanthers.com", "genders": ["mens", "womens"]},
    {"name": "Stanford University", "city": "Stanford", "state": "CA", "conference": "ACC", "website": "https://gostanford.com", "genders": ["mens", "womens"]},
    {"name": "Syracuse University", "city": "Syracuse", "state": "NY", "conference": "ACC", "website": "https://cuse.com", "genders": ["mens", "womens"]},
    {"name": "University of Virginia", "city": "Charlottesville", "state": "VA", "conference": "ACC", "website": "https://virginiasports.com", "genders": ["mens", "womens"]},
    {"name": "Virginia Tech", "city": "Blacksburg", "state": "VA", "conference": "ACC", "website": "https://hokiesports.com", "genders": ["mens", "womens"]},
    {"name": "Wake Forest University", "city": "Winston-Salem", "state": "NC", "conference": "ACC", "website": "https://godeacs.com", "genders": ["mens", "womens"]},
    {"name": "University of California", "city": "Berkeley", "state": "CA", "conference": "ACC", "website": "https://calbears.com", "genders": ["mens", "womens"]},
    {"name": "SMU", "city": "Dallas", "state": "TX", "conference": "ACC", "website": "https://smumustangs.com", "genders": ["mens", "womens"]},
    # Big Ten (both genders)
    {"name": "University of Illinois", "city": "Champaign", "state": "IL", "conference": "Big Ten", "website": "https://fightingillini.com", "genders": ["mens", "womens"]},
    {"name": "Indiana University", "city": "Bloomington", "state": "IN", "conference": "Big Ten", "website": "https://iuhoosiers.com", "genders": ["mens", "womens"]},
    {"name": "University of Iowa", "city": "Iowa City", "state": "IA", "conference": "Big Ten", "website": "https://hawkeyesports.com", "genders": ["mens", "womens"]},
    {"name": "University of Maryland", "city": "College Park", "state": "MD", "conference": "Big Ten", "website": "https://umterps.com", "genders": ["mens", "womens"]},
    {"name": "University of Michigan", "city": "Ann Arbor", "state": "MI", "conference": "Big Ten", "website": "https://mgoblue.com", "genders": ["mens", "womens"]},
    {"name": "Michigan State University", "city": "East Lansing", "state": "MI", "conference": "Big Ten", "website": "https://msuspartans.com", "genders": ["mens", "womens"]},
    {"name": "University of Minnesota", "city": "Minneapolis", "state": "MN", "conference": "Big Ten", "website": "https://gophersports.com", "genders": ["mens", "womens"]},
    {"name": "University of Nebraska", "city": "Lincoln", "state": "NE", "conference": "Big Ten", "website": "https://huskers.com", "genders": ["mens", "womens"]},
    {"name": "Northwestern University", "city": "Evanston", "state": "IL", "conference": "Big Ten", "website": "https://nusports.com", "genders": ["mens", "womens"]},
    {"name": "Ohio State University", "city": "Columbus", "state": "OH", "conference": "Big Ten", "website": "https://ohiostatebuckeyes.com", "genders": ["mens", "womens"]},
    {"name": "Penn State University", "city": "University Park", "state": "PA", "conference": "Big Ten", "website": "https://gopsusports.com", "genders": ["mens", "womens"]},
    {"name": "Purdue University", "city": "West Lafayette", "state": "IN", "conference": "Big Ten", "website": "https://purduesports.com", "genders": ["mens", "womens"]},
    {"name": "Rutgers University", "city": "Piscataway", "state": "NJ", "conference": "Big Ten", "website": "https://scarletknights.com", "genders": ["mens", "womens"]},
    {"name": "University of Wisconsin", "city": "Madison", "state": "WI", "conference": "Big Ten", "website": "https://uwbadgers.com", "genders": ["mens", "womens"]},
    {"name": "University of Oregon", "city": "Eugene", "state": "OR", "conference": "Big Ten", "website": "https://goducks.com", "genders": ["mens", "womens"]},
    {"name": "University of California, Los Angeles", "city": "Los Angeles", "state": "CA", "conference": "Big Ten", "website": "https://uclabruins.com", "genders": ["mens", "womens"]},
    {"name": "University of Southern California", "city": "Los Angeles", "state": "CA", "conference": "Big Ten", "website": "https://usctrojans.com", "genders": ["mens", "womens"]},
    {"name": "University of Washington", "city": "Seattle", "state": "WA", "conference": "Big Ten", "website": "https://gohuskies.com", "genders": ["mens", "womens"]},
    # Big East (both genders)
    {"name": "Butler University", "city": "Indianapolis", "state": "IN", "conference": "Big East", "website": "https://butlersports.com", "genders": ["mens", "womens"]},
    {"name": "Creighton University", "city": "Omaha", "state": "NE", "conference": "Big East", "website": "https://gocreighton.com", "genders": ["mens", "womens"]},
    {"name": "DePaul University", "city": "Chicago", "state": "IL", "conference": "Big East", "website": "https://depaulbluedemons.com", "genders": ["mens", "womens"]},
    {"name": "Georgetown University", "city": "Washington", "state": "DC", "conference": "Big East", "website": "https://guhoyas.com", "genders": ["mens", "womens"]},
    {"name": "Marquette University", "city": "Milwaukee", "state": "WI", "conference": "Big East", "website": "https://gomarquette.com", "genders": ["mens", "womens"]},
    {"name": "Providence College", "city": "Providence", "state": "RI", "conference": "Big East", "website": "https://friars.com", "genders": ["mens", "womens"]},
    {"name": "Seton Hall University", "city": "South Orange", "state": "NJ", "conference": "Big East", "website": "https://shupirates.com", "genders": ["mens", "womens"]},
    {"name": "St. John's University", "city": "Queens", "state": "NY", "conference": "Big East", "website": "https://redstormsports.com", "genders": ["mens", "womens"]},
    {"name": "University of Connecticut", "city": "Storrs", "state": "CT", "conference": "Big East", "website": "https://uconnhuskies.com", "genders": ["mens", "womens"]},
    {"name": "Villanova University", "city": "Villanova", "state": "PA", "conference": "Big East", "website": "https://villanova.com", "genders": ["mens", "womens"]},
    {"name": "Xavier University", "city": "Cincinnati", "state": "OH", "conference": "Big East", "website": "https://goxavier.com", "genders": ["mens", "womens"]},
    # Big 12 (women only, except West Virginia = both)
    {"name": "University of Arizona", "city": "Tucson", "state": "AZ", "conference": "Big 12", "website": "https://arizonawildcats.com", "genders": ["womens"]},
    {"name": "Arizona State University", "city": "Tempe", "state": "AZ", "conference": "Big 12", "website": "https://thesundevils.com", "genders": ["womens"]},
    {"name": "Baylor University", "city": "Waco", "state": "TX", "conference": "Big 12", "website": "https://baylorbears.com", "genders": ["womens"]},
    {"name": "Brigham Young University", "city": "Provo", "state": "UT", "conference": "Big 12", "website": "https://byucougars.com", "genders": ["womens"]},
    {"name": "University of Cincinnati", "city": "Cincinnati", "state": "OH", "conference": "Big 12", "website": "https://gobearcats.com", "genders": ["womens"]},
    {"name": "University of Colorado", "city": "Boulder", "state": "CO", "conference": "Big 12", "website": "https://cubuffs.com", "genders": ["womens"]},
    {"name": "University of Houston", "city": "Houston", "state": "TX", "conference": "Big 12", "website": "https://uhcougars.com", "genders": ["womens"]},
    {"name": "Iowa State University", "city": "Ames", "state": "IA", "conference": "Big 12", "website": "https://cyclones.com", "genders": ["womens"]},
    {"name": "University of Kansas", "city": "Lawrence", "state": "KS", "conference": "Big 12", "website": "https://kuathletics.com", "genders": ["womens"]},
    {"name": "Kansas State University", "city": "Manhattan", "state": "KS", "conference": "Big 12", "website": "https://kstatesports.com", "genders": ["womens"]},
    {"name": "Oklahoma State University", "city": "Stillwater", "state": "OK", "conference": "Big 12", "website": "https://okstate.com", "genders": ["womens"]},
    {"name": "Texas Christian University", "city": "Fort Worth", "state": "TX", "conference": "Big 12", "website": "https://gofrogs.com", "genders": ["womens"]},
    {"name": "Texas Tech University", "city": "Lubbock", "state": "TX", "conference": "Big 12", "website": "https://texastech.com", "genders": ["womens"]},
    {"name": "University of Central Florida", "city": "Orlando", "state": "FL", "conference": "Big 12", "website": "https://ucfknights.com", "genders": ["womens"]},
    {"name": "University of Utah", "city": "Salt Lake City", "state": "UT", "conference": "Big 12", "website": "https://utahutes.com", "genders": ["womens"]},
    {"name": "West Virginia University", "city": "Morgantown", "state": "WV", "conference": "Big 12", "website": "https://wvusports.com", "genders": ["mens", "womens"]},
    # Mountain West (women only)
    {"name": "Boise State University", "city": "Boise", "state": "ID", "conference": "Mountain West", "website": "https://broncosports.com", "genders": ["womens"]},
    {"name": "Colorado State University", "city": "Fort Collins", "state": "CO", "conference": "Mountain West", "website": "https://csurams.com", "genders": ["womens"]},
    {"name": "Fresno State University", "city": "Fresno", "state": "CA", "conference": "Mountain West", "website": "https://gobulldogs.com", "genders": ["womens"]},
    {"name": "University of Nevada", "city": "Reno", "state": "NV", "conference": "Mountain West", "website": "https://nevadawolfpack.com", "genders": ["womens"]},
    {"name": "University of New Mexico", "city": "Albuquerque", "state": "NM", "conference": "Mountain West", "website": "https://golobos.com", "genders": ["womens"]},
    {"name": "San Diego State University", "city": "San Diego", "state": "CA", "conference": "Mountain West", "website": "https://goaztecs.com", "genders": ["womens"]},
    {"name": "San Jose State University", "city": "San Jose", "state": "CA", "conference": "Mountain West", "website": "https://sjsuspartans.com", "genders": ["womens"]},
    {"name": "UNLV", "city": "Las Vegas", "state": "NV", "conference": "Mountain West", "website": "https://unlvrebels.com", "genders": ["womens"]},
    {"name": "Utah State University", "city": "Logan", "state": "UT", "conference": "Mountain West", "website": "https://utahstateaggies.com", "genders": ["womens"]},
    {"name": "University of Wyoming", "city": "Laramie", "state": "WY", "conference": "Mountain West", "website": "https://gowyo.com", "genders": ["womens"]},
    # WCC (both genders)
    {"name": "Gonzaga University", "city": "Spokane", "state": "WA", "conference": "WCC", "website": "https://gozags.com", "genders": ["mens", "womens"]},
    {"name": "Loyola Marymount University", "city": "Los Angeles", "state": "CA", "conference": "WCC", "website": "https://lmulions.com", "genders": ["mens", "womens"]},
    {"name": "Pepperdine University", "city": "Malibu", "state": "CA", "conference": "WCC", "website": "https://pepperdinewaves.com", "genders": ["mens", "womens"]},
    {"name": "University of Portland", "city": "Portland", "state": "OR", "conference": "WCC", "website": "https://portlandpilots.com", "genders": ["mens", "womens"]},
    {"name": "University of San Diego", "city": "San Diego", "state": "CA", "conference": "WCC", "website": "https://toreros.com", "genders": ["mens", "womens"]},
    {"name": "University of San Francisco", "city": "San Francisco", "state": "CA", "conference": "WCC", "website": "https://usfcadets.com", "genders": ["mens", "womens"]},
    {"name": "Santa Clara University", "city": "Santa Clara", "state": "CA", "conference": "WCC", "website": "https://santaclarabroncos.com", "genders": ["mens", "womens"]},
    {"name": "Saint Mary's College of California", "city": "Moraga", "state": "CA", "conference": "WCC", "website": "https://smcgaels.com", "genders": ["mens", "womens"]},
    {"name": "Pacific University", "city": "Stockton", "state": "CA", "conference": "WCC", "website": "https://pacific.edu", "genders": ["mens", "womens"]},
    # AAC (women only)
    {"name": "University of Alabama at Birmingham", "city": "Birmingham", "state": "AL", "conference": "AAC", "website": "https://uabsports.com", "genders": ["womens"]},
    {"name": "University of Charlotte", "city": "Charlotte", "state": "NC", "conference": "AAC", "website": "https://charlotte49ers.com", "genders": ["womens"]},
    {"name": "East Carolina University", "city": "Greenville", "state": "NC", "conference": "AAC", "website": "https://ecupirates.com", "genders": ["womens"]},
    {"name": "Florida Atlantic University", "city": "Boca Raton", "state": "FL", "conference": "AAC", "website": "https://fausports.com", "genders": ["womens"]},
    {"name": "University of Memphis", "city": "Memphis", "state": "TN", "conference": "AAC", "website": "https://gotigersgo.com", "genders": ["womens"]},
    {"name": "Rice University", "city": "Houston", "state": "TX", "conference": "AAC", "website": "https://riceowls.com", "genders": ["womens"]},
    {"name": "University of South Florida", "city": "Tampa", "state": "FL", "conference": "AAC", "website": "https://gousfbulls.com", "genders": ["womens"]},
    {"name": "Temple University", "city": "Philadelphia", "state": "PA", "conference": "AAC", "website": "https://owlsports.com", "genders": ["womens"]},
    {"name": "Tulane University", "city": "New Orleans", "state": "LA", "conference": "AAC", "website": "https://tulanegreenwave.com", "genders": ["womens"]},
    {"name": "University of Tulsa", "city": "Tulsa", "state": "OK", "conference": "AAC", "website": "https://tulsahurricane.com", "genders": ["womens"]},
    {"name": "UTSA", "city": "San Antonio", "state": "TX", "conference": "AAC", "website": "https://utsaathletics.com", "genders": ["womens"]},
    {"name": "Wichita State University", "city": "Wichita", "state": "KS", "conference": "AAC", "website": "https://goshockers.com", "genders": ["womens"]},
    # A-10 (mixed — some both, some women only)
    {"name": "University of Dayton", "city": "Dayton", "state": "OH", "conference": "A-10", "website": "https://daytonflyers.com", "genders": ["womens"]},
    {"name": "Fordham University", "city": "Bronx", "state": "NY", "conference": "A-10", "website": "https://fordhamsports.com", "genders": ["mens", "womens"]},
    {"name": "George Mason University", "city": "Fairfax", "state": "VA", "conference": "A-10", "website": "https://gomason.com", "genders": ["mens", "womens"]},
    {"name": "George Washington University", "city": "Washington", "state": "DC", "conference": "A-10", "website": "https://gwsports.com", "genders": ["mens", "womens"]},
    {"name": "La Salle University", "city": "Philadelphia", "state": "PA", "conference": "A-10", "website": "https://goexplorers.com", "genders": ["womens"]},
    {"name": "Loyola University Chicago", "city": "Chicago", "state": "IL", "conference": "A-10", "website": "https://loyolaramblers.com", "genders": ["mens", "womens"]},
    {"name": "University of Massachusetts", "city": "Amherst", "state": "MA", "conference": "A-10", "website": "https://umassmintutes.com", "genders": ["womens"]},
    {"name": "University of Rhode Island", "city": "Kingston", "state": "RI", "conference": "A-10", "website": "https://gorhody.com", "genders": ["womens"]},
    {"name": "University of Richmond", "city": "Richmond", "state": "VA", "conference": "A-10", "website": "https://richmondspiders.com", "genders": ["mens", "womens"]},
    {"name": "Saint Joseph's University", "city": "Philadelphia", "state": "PA", "conference": "A-10", "website": "https://sjuhawks.com", "genders": ["womens"]},
    {"name": "Saint Louis University", "city": "St. Louis", "state": "MO", "conference": "A-10", "website": "https://slubillikens.com", "genders": ["mens", "womens"]},
    {"name": "Virginia Commonwealth University", "city": "Richmond", "state": "VA", "conference": "A-10", "website": "https://vcuathletics.com", "genders": ["mens", "womens"]},
]


# ---------------------------------------------------------------------------
# D2 seed data — sourced from KNOWN_D2_URLS in ncaa-d2-roster-scraper.ts
# These schools have verified athletic site URLs. Both genders assumed.
# State is not available from the URL map; left as None for now (can be
# backfilled from the college coaching staff scraper later).
# ---------------------------------------------------------------------------

_D2_SCHOOLS: List[Dict] = [
    {"name": "American International College", "website": "https://aicyellowjackets.com"},
    {"name": "Angelo State University", "website": "https://angelosports.com"},
    {"name": "Assumption University", "website": "https://assumptiongreyhounds.com"},
    {"name": "Augustana University", "website": "https://goaugie.com"},
    {"name": "Bemidji State University", "website": "https://bsubeavers.com"},
    {"name": "Bentley University", "website": "https://bentleyfalcons.com"},
    {"name": "Caldwell University", "website": "https://caldwellathletics.com"},
    {"name": "California Baptist University", "website": "https://cbulancers.com"},
    {"name": "Colorado Mesa University", "website": "https://cmumavericks.com"},
    {"name": "Colorado School of Mines", "website": "https://minesathletics.com"},
    {"name": "Columbus State University", "website": "https://csucougars.com"},
    {"name": "Drury University", "website": "https://drurypanthers.com"},
    {"name": "East Stroudsburg University", "website": "https://esuwarriors.com"},
    {"name": "Flagler College", "website": "https://flaglerathletics.com"},
    {"name": "Florida Southern College", "website": "https://fscmocs.com"},
    {"name": "Franklin Pierce University", "website": "https://fpuravens.com"},
    {"name": "Grand Valley State University", "website": "https://gvsulakers.com"},
    {"name": "Harding University", "website": "https://hardingsports.com"},
    {"name": "Indiana University of Pennsylvania", "website": "https://iupathletics.com"},
    {"name": "Lander University", "website": "https://landerbearcats.com"},
    {"name": "Le Moyne College", "website": "https://lemoynedolphins.com"},
    {"name": "Lenoir-Rhyne University", "website": "https://lrbears.com"},
    {"name": "Lewis University", "website": "https://lewisflyers.com"},
    {"name": "Lincoln Memorial University", "website": "https://lmurailsplitters.com"},
    {"name": "Lindenwood University", "website": "https://lindenwoodlions.com"},
    {"name": "Mercyhurst University", "website": "https://mercyhurstlakers.com"},
    {"name": "Michigan Tech", "website": "https://michigantechhuskies.com"},
    {"name": "Millersville University", "website": "https://millersvilleathletics.com"},
    {"name": "University of Minnesota Duluth", "website": "https://umdbulldogs.com"},
    {"name": "Missouri S&T", "website": "https://minerathletics.com"},
    {"name": "Molloy University", "website": "https://molloylions.com"},
    {"name": "University of New Haven", "website": "https://newhavenchargers.com"},
    {"name": "Newberry College", "website": "https://newberrywolves.com"},
    {"name": "Northern Michigan University", "website": "https://nmuwildcats.com"},
    {"name": "Nova Southeastern University", "website": "https://nsusharks.com"},
    {"name": "Ouachita Baptist University", "website": "https://obutigers.com"},
    {"name": "Pace University", "website": "https://paceuathletics.com"},
    {"name": "Palm Beach Atlantic University", "website": "https://pbasailfish.com"},
    {"name": "Saint Anselm College", "website": "https://saintanselmhawks.com"},
    {"name": "Saint Leo University", "website": "https://saintleolions.com"},
    {"name": "Seattle Pacific University", "website": "https://spufalcons.com"},
    {"name": "Shippensburg University", "website": "https://shipraiders.com"},
    {"name": "Southern Connecticut State University", "website": "https://southernctowls.com"},
    {"name": "University of Southern Indiana", "website": "https://gousieagles.com"},
    {"name": "Southern New Hampshire University", "website": "https://snhupenmen.com"},
    {"name": "Stonehill College", "website": "https://stonehillskyhawks.com"},
    {"name": "Tiffin University", "website": "https://gotiffindragons.com"},
    {"name": "Truman State University", "website": "https://trumanbulldogs.com"},
    {"name": "Valdosta State University", "website": "https://vstateblazers.com"},
    {"name": "University of West Florida", "website": "https://goargos.com"},
    {"name": "West Texas A&M University", "website": "https://gobuffsgo.com"},
    {"name": "Western Washington University", "website": "https://wwuvikings.com"},
    {"name": "Wingate University", "website": "https://wingatebulldogs.com"},
    {"name": "Young Harris College", "website": "https://yhcathletics.com"},
    {"name": "Bellarmine University", "website": "https://bellarmineknights.com"},
    {"name": "Maryville University of St. Louis", "website": "https://maryvilleathletics.com"},
    {"name": "McKendree University", "website": "https://mckendreeathletics.com"},
    {"name": "Quincy University", "website": "https://quincyhawks.com"},
    {"name": "Southwest Baptist University", "website": "https://sbubears.com"},
    {"name": "University of Illinois Springfield", "website": "https://uisathletics.com"},
    {"name": "University of Indianapolis", "website": "https://uindy.edu/athletics"},
    {"name": "William Jewell College", "website": "https://williamjewellathletics.com"},
    {"name": "Alderson Broaddus University", "website": "https://abbattlers.com"},
    {"name": "Charleston (WV)", "website": "https://ucathletics.com"},
    {"name": "Concord University", "website": "https://concordmountainlions.com"},
    {"name": "Davis & Elkins College", "website": "https://deathletics.com"},
    {"name": "Fairmont State University", "website": "https://fairmontstateathletics.com"},
    {"name": "Glenville State University", "website": "https://gsupioneerathletics.com"},
    {"name": "Notre Dame College", "website": "https://ndcfalcons.com"},
    {"name": "University of Charleston", "website": "https://ucgoeagles.com"},
    {"name": "West Liberty University", "website": "https://wluhilltopers.com"},
    {"name": "West Virginia Wesleyan College", "website": "https://wvwcathletics.com"},
    {"name": "Wheeling University", "website": "https://wheelingcardinals.com"},
    {"name": "Barry University", "website": "https://barryathletics.com"},
    {"name": "Eckerd College", "website": "https://eckerdathletics.com"},
    {"name": "Florida Institute of Technology", "website": "https://fitpanthers.com"},
    {"name": "Lynn University", "website": "https://lynnfightingknights.com"},
    {"name": "Rollins College", "website": "https://rollinssports.com"},
    {"name": "Tampa", "website": "https://ut-spartans.com"},
    {"name": "Webber International University", "website": "https://webberathletics.com"},
    {"name": "Eastern New Mexico University", "website": "https://enmuathletics.com"},
    {"name": "Midwestern State University", "website": "https://msumustangs.com"},
    {"name": "Texas A&M International University", "website": "https://tamiudust.com"},
    {"name": "University of Texas of the Permian Basin", "website": "https://utpbathletics.com"},
    {"name": "Texas A&M University-Commerce", "website": "https://golionstamuc.com"},
    {"name": "Texas A&M University-Kingsville", "website": "https://javelinas.tamuk.edu"},
    {"name": "University of Arkansas-Fort Smith", "website": "https://uafsathletics.com"},
    {"name": "Chestnut Hill College", "website": "https://chcgriffins.com"},
    {"name": "Dominican University of California", "website": "https://dominicancougars.com"},
    {"name": "Georgian Court University", "website": "https://gculions.com"},
    {"name": "Holy Family University", "website": "https://hfuathletics.com"},
    {"name": "Post University", "website": "https://postathletics.com"},
    {"name": "University of the District of Columbia", "website": "https://udcfirebirds.com"},
    {"name": "Cedarville University", "website": "https://yellowjackets.cedarville.edu"},
    {"name": "Findlay", "website": "https://gooilers.com"},
    {"name": "Hillsdale College", "website": "https://hillsdalechargers.com"},
    {"name": "Lake Erie College", "website": "https://lakeeriestorm.com"},
    {"name": "Malone University", "website": "https://malonepioneer.com"},
    {"name": "Ohio Dominican University", "website": "https://odpanthers.com"},
    {"name": "Walsh University", "website": "https://walshathletics.com"},
    {"name": "Adelphi University", "website": "https://adelphis.com"},
    {"name": "Bridgeport", "website": "https://ubknights.com"},
    {"name": "Dominican College", "website": "https://dominicanchargers.com"},
    {"name": "Merrimack College", "website": "https://merrimackathletics.com"},
    {"name": "Saint Michael's College", "website": "https://gosmcathletics.com"},
    {"name": "Bloomsburg University", "website": "https://bloomsburgathletics.com"},
    {"name": "California University of Pennsylvania", "website": "https://calpuathletics.com"},
    {"name": "Clarion University", "website": "https://clariongoleneagles.com"},
    {"name": "Kutztown University", "website": "https://kutztownathletics.com"},
    {"name": "Lock Haven University", "website": "https://athletics.lockhaven.edu"},
    {"name": "Mansfield University", "website": "https://mansfieldathletics.com"},
    {"name": "Slippery Rock University", "website": "https://sruathletics.com"},
    {"name": "West Chester University", "website": "https://wcuathletics.com"},
    {"name": "Anderson University (SC)", "website": "https://andersonathletics.com"},
    {"name": "Barton College", "website": "https://bartonbulldogs.com"},
    {"name": "Carson-Newman University", "website": "https://cneagles.com"},
    {"name": "Catawba College", "website": "https://catawbaathletics.com"},
    {"name": "Coker University", "website": "https://cokerpride.com"},
    {"name": "King University", "website": "https://kingathletics.com"},
    {"name": "Mars Hill University", "website": "https://mhuathletics.com"},
    {"name": "Tusculum University", "website": "https://tusculumathletics.com"},
    {"name": "University of Virginia's College at Wise", "website": "https://uvawise.edu/athletics"},
    {"name": "Christian Brothers University", "website": "https://cbuathletics.com"},
    {"name": "Delta State University", "website": "https://deltastatestatesman.com"},
    {"name": "Mississippi College", "website": "https://mcchoctaws.com"},
    {"name": "Shorter University", "website": "https://shorterathletics.com"},
    {"name": "University of Alabama in Huntsville", "website": "https://uahchargers.com"},
    {"name": "University of North Alabama", "website": "https://roarlions.com"},
]


# ---------------------------------------------------------------------------
# D3 seed data — sourced from KNOWN_D3_URLS in ncaa-d3-roster-scraper.ts
# Both genders assumed. No state/city/conference available from the URL map.
# ---------------------------------------------------------------------------

_D3_SCHOOLS: List[Dict] = [
    {"name": "St. John Fisher University", "website": "https://sjfathletics.com"},
    {"name": "Nazareth University", "website": "https://nazathletics.com"},
    {"name": "SUNY Fredonia", "website": "https://fredoniabluedevils.com"},
    {"name": "SUNY Geneseo", "website": "https://geneseoknights.com"},
    {"name": "SUNY Oneonta", "website": "https://oneontaathletics.com"},
    {"name": "Pomona-Pitzer Colleges", "website": "https://sagehens.com"},
    {"name": "Brooklyn College", "website": "https://brooklyncollegeathletics.com"},
    {"name": "CCNY", "website": "https://ccnyathletics.com"},
    {"name": "York College (CUNY)", "website": "https://yorkathletics.com"},
    {"name": "Lehman College", "website": "https://lehmanathletics.com"},
    {"name": "John Jay College", "website": "https://johnjayathletics.com"},
    {"name": "MIT", "website": "https://mitathletics.com"},
    {"name": "University of Chicago", "website": "https://athletics.uchicago.edu"},
    {"name": "Emory University", "website": "https://emoryathletics.com"},
    {"name": "Carleton College", "website": "https://athletics.carleton.edu"},
    {"name": "St. Olaf College", "website": "https://athletics.stolaf.edu"},
    {"name": "Macalester College", "website": "https://athletics.macalester.edu"},
    {"name": "Hamilton College", "website": "https://athletics.hamilton.edu"},
    {"name": "Rensselaer Polytechnic Institute", "website": "https://rpiathletics.com"},
    {"name": "Trinity University (TX)", "website": "https://trinitytigers.com"},
    {"name": "Bowdoin College", "website": "https://athletics.bowdoin.edu"},
    {"name": "Keene State College", "website": "https://keeneowls.com"},
    {"name": "Oberlin College", "website": "https://goyeo.com"},
    {"name": "Otterbein University", "website": "https://otterbeincardinals.com"},
    {"name": "Kenyon College", "website": "https://athletics.kenyon.edu"},
    {"name": "Amherst College", "website": "https://athletics.amherst.edu"},
    {"name": "Union College", "website": "https://unionathletics.com"},
    {"name": "Guilford College", "website": "https://guilfordquakers.com"},
    {"name": "Randolph College", "website": "https://randolphwildcats.com"},
    {"name": "SUNY Cortland", "website": "https://cortlandreddragons.com"},
    {"name": "Middlebury College", "website": "https://athletics.middlebury.edu"},
    {"name": "Trinity College (CT)", "website": "https://bantamsports.com"},
    {"name": "Wesleyan University (CT)", "website": "https://athletics.wesleyan.edu"},
    {"name": "Brandeis University", "website": "https://brandeisjudges.com"},
    {"name": "DePauw University", "website": "https://depauwtigers.com"},
    {"name": "University of Scranton", "website": "https://athletics.scranton.edu"},
    {"name": "Mount Holyoke College", "website": "https://athletics.mtholyoke.edu"},
    {"name": "Elmira College", "website": "https://athletics.elmira.edu"},
    {"name": "Marymount University", "website": "https://marymountsaints.com"},
    {"name": "Johns Hopkins University", "website": "https://hopkinssports.com"},
    {"name": "Alma College", "website": "https://almascots.com"},
    {"name": "Adrian College", "website": "https://adrianbulldogs.com"},
    {"name": "Hope College", "website": "https://athletics.hope.edu"},
    {"name": "Olivet College", "website": "https://olivetcomets.com"},
    {"name": "Anderson University", "website": "https://athletics.anderson.edu"},
    {"name": "Hanover College", "website": "https://athletics.hanover.edu"},
    {"name": "Rose-Hulman Institute", "website": "https://athletics.rose-hulman.edu"},
    {"name": "Monmouth College", "website": "https://monmouthscots.com"},
    {"name": "North Park University", "website": "https://athletics.northpark.edu"},
    {"name": "Augustana College (IL)", "website": "https://athletics.augustana.edu"},
    {"name": "Carthage College", "website": "https://athletics.carthage.edu"},
    {"name": "Millikin University", "website": "https://athletics.millikin.edu"},
    {"name": "North Central College", "website": "https://northcentralcardinals.com"},
    {"name": "Wheaton College (IL)", "website": "https://athletics.wheaton.edu"},
    {"name": "Hendrix College", "website": "https://hendrixwarriors.com"},
    {"name": "Rhodes College", "website": "https://rhodeslynx.com"},
    {"name": "Centre College", "website": "https://centrecolonels.com"},
    {"name": "Sewanee: University of the South", "website": "https://sewaneetigers.com"},
    {"name": "Hampden-Sydney College", "website": "https://hscathletics.com"},
    {"name": "Randolph-Macon College", "website": "https://rmcathletics.com"},
    {"name": "Rutgers-Newark", "website": "https://rutgersnewarkathletics.com"},
    {"name": "Schreiner University", "website": "https://schreinermountaineers.com"},
    {"name": "Southwestern University", "website": "https://southwesternpirates.com"},
    {"name": "Rivier University", "website": "https://rivierathletics.com"},
    {"name": "Gordon College", "website": "https://athletics.gordon.edu"},
    {"name": "Occidental College", "website": "https://oxyathletics.com"},
    {"name": "Claremont-Mudd-Scripps", "website": "https://cmsathletics.org"},
    {"name": "Chapman University", "website": "https://chapmanathletics.com"},
    {"name": "Ramapo College", "website": "https://ramapoathletics.com"},
    {"name": "NJCU", "website": "https://knightathletics.com"},
    {"name": "Kean University", "website": "https://keanathletics.com"},
    {"name": "Montclair State University", "website": "https://montclairathletics.com"},
    {"name": "William Paterson University", "website": "https://wpupioneers.com"},
    {"name": "Mount Saint Mary College", "website": "https://msmcknights.com"},
    {"name": "Buffalo State University", "website": "https://buffalostateathletics.com"},
    {"name": "Roanoke College", "website": "https://roanokemaroons.com"},
    {"name": "Maryville University", "website": "https://maryvillesaints.com"},
    {"name": "Principia College", "website": "https://principiaathletics.com"},
    {"name": "Westminster College", "website": "https://athletics.westminster.edu"},
    {"name": "Grove City College", "website": "https://athletics.gcc.edu"},
    {"name": "SUNY Potsdam", "website": "https://potsdambears.com"},
    {"name": "SUNY Morrisville", "website": "https://morrisvillemustangs.com"},
    {"name": "SUNY Oswego", "website": "https://oswegolakers.com"},
    {"name": "Luther College", "website": "https://luthernorse.com"},
    {"name": "Central College", "website": "https://athletics.central.edu"},
    {"name": "Bates College", "website": "https://athletics.bates.edu"},
    {"name": "Colby College", "website": "https://athletics.colby.edu"},
    {"name": "Connecticut College", "website": "https://camelbacks.com"},
    {"name": "Tufts University", "website": "https://gotuftsjumbos.com"},
    {"name": "Williams College", "website": "https://ephsports.williams.edu"},
    {"name": "Dickinson College", "website": "https://athletics.dickinson.edu"},
    {"name": "Franklin & Marshall College", "website": "https://fandmathletics.com"},
    {"name": "Gettysburg College", "website": "https://gettysburgathletics.com"},
    {"name": "Haverford College", "website": "https://athletics.haverford.edu"},
    {"name": "Muhlenberg College", "website": "https://muhlenbergmules.com"},
    {"name": "Swarthmore College", "website": "https://swarthmoreathletics.com"},
    {"name": "Ursinus College", "website": "https://ursinus.edu/athletics"},
    {"name": "Washington College", "website": "https://washcollathletics.com"},
    {"name": "Berry College", "website": "https://athletics.berry.edu"},
    {"name": "Millsaps College", "website": "https://millsapsmajors.com"},
    {"name": "George Fox University", "website": "https://athletics.georgefox.edu"},
    {"name": "Lewis & Clark College", "website": "https://gopioneers.athletics.lclark.edu"},
    {"name": "Linfield University", "website": "https://linfieldsports.com"},
    {"name": "Pacific Lutheran University", "website": "https://plulutes.com"},
    {"name": "Pacific University", "website": "https://pacificboxers.com"},
    {"name": "University of Puget Sound", "website": "https://pugetsoundathletics.com"},
    {"name": "Whitman College", "website": "https://whitmanathletics.com"},
    {"name": "Whitworth University", "website": "https://whitworthpirates.com"},
    {"name": "Willamette University", "website": "https://willametteathletics.com"},
    {"name": "Beloit College", "website": "https://buccaneers.beloit.edu"},
    {"name": "Cornell College", "website": "https://athletics.cornellcollege.edu"},
    {"name": "Grinnell College", "website": "https://athletics.grinnell.edu"},
    {"name": "Illinois College", "website": "https://illinoiscollegeathletics.com"},
    {"name": "Knox College", "website": "https://athletics.knox.edu"},
    {"name": "Lake Forest College", "website": "https://foresters.com"},
    {"name": "Lawrence University", "website": "https://lawrencevikings.com"},
    {"name": "Ripon College", "website": "https://riponathletics.com"},
    {"name": "Benedictine University", "website": "https://bueagles.com"},
    {"name": "Concordia University Chicago", "website": "https://cuchicagoathletics.com"},
    {"name": "Edgewood College", "website": "https://edgewoodathletics.com"},
    {"name": "Lakeland University", "website": "https://lakelanda.org"},
    {"name": "Marian University (WI)", "website": "https://marianathletics.com"},
    {"name": "Rockford University", "website": "https://rockfordregents.com"},
    {"name": "University of Wisconsin-Eau Claire", "website": "https://gobleugold.com"},
    {"name": "University of Wisconsin-Oshkosh", "website": "https://uwotitans.com"},
    {"name": "University of Wisconsin-Stevens Point", "website": "https://uwsppointers.com"},
    {"name": "Allegheny College", "website": "https://alleghenygators.com"},
    {"name": "Bethany College (WV)", "website": "https://bethanybisons.com"},
    {"name": "Case Western Reserve University", "website": "https://athletics.case.edu"},
    {"name": "Geneva College", "website": "https://athletics.geneva.edu"},
    {"name": "Thiel College", "website": "https://thielwings.com"},
    {"name": "Thomas More University", "website": "https://thomasmoreathletics.com"},
    {"name": "Waynesburg University", "website": "https://waynesburgyellowjackets.com"},
    {"name": "Westminster College (PA)", "website": "https://westminstertitans.com"},
    {"name": "Alfred University", "website": "https://athletics.alfred.edu"},
    {"name": "Hartwick College", "website": "https://hartwickhawks.com"},
    {"name": "Houghton University", "website": "https://houghtonathletics.com"},
    {"name": "Ithaca College", "website": "https://athletics.ithaca.edu"},
    {"name": "Stevens Institute of Technology", "website": "https://stevensathletics.com"},
    {"name": "Utica University", "website": "https://uticapioneer.com"},
    {"name": "Bard College", "website": "https://athletics.bard.edu"},
    {"name": "Clarkson University", "website": "https://clarksonathletics.com"},
    {"name": "Hobart and William Smith Colleges", "website": "https://hwsathletics.com"},
    {"name": "Skidmore College", "website": "https://skidmoreathletics.com"},
    {"name": "St. Lawrence University", "website": "https://saintsathletics.com"},
    {"name": "Vassar College", "website": "https://athletics.vassar.edu"},
    {"name": "College of Mount Saint Vincent", "website": "https://cmsvathletics.com"},
    {"name": "Farmingdale State College", "website": "https://farmingdaleathletics.com"},
    {"name": "Medgar Evers College", "website": "https://mecathletics.com"},
    {"name": "Old Westbury", "website": "https://oldwestburypanthers.com"},
    {"name": "Purchase College", "website": "https://purchaseathletics.com"},
    {"name": "Sage Colleges", "website": "https://sagegators.com"},
    {"name": "SUNY Maritime College", "website": "https://sunymaritime.edu/athletics"},
    {"name": "Yeshiva University", "website": "https://yumaccabees.com"},
]


# ---------------------------------------------------------------------------
# Flatten schools into individual rows (one per name+division+gender_program)
# ---------------------------------------------------------------------------

def _build_rows() -> List[Dict]:
    """Expand the seed lists into flat insert-ready rows."""
    rows: List[Dict] = []
    seen: set = set()

    # D1 schools have full metadata
    for school in _D1_SCHOOLS:
        for gender in school["genders"]:
            slug = slugify(school["name"], "D1", gender)
            key = (school["name"], "D1", gender)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "name": school["name"],
                "slug": slug,
                "division": "D1",
                "conference": school.get("conference"),
                "state": school.get("state"),
                "city": school.get("city"),
                "website": school.get("website"),
                "gender_program": gender,
                "scholarship_available": True,  # D1 offers athletic scholarships
            })

    # D2 schools — both genders, partial metadata
    for school in _D2_SCHOOLS:
        for gender in ["mens", "womens"]:
            slug = slugify(school["name"], "D2", gender)
            key = (school["name"], "D2", gender)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "name": school["name"],
                "slug": slug,
                "division": "D2",
                "conference": None,
                "state": None,
                "city": None,
                "website": school.get("website"),
                "gender_program": gender,
                "scholarship_available": True,  # D2 offers athletic scholarships
            })

    # D3 schools — both genders, partial metadata
    for school in _D3_SCHOOLS:
        for gender in ["mens", "womens"]:
            slug = slugify(school["name"], "D3", gender)
            key = (school["name"], "D3", gender)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "name": school["name"],
                "slug": slug,
                "division": "D3",
                "conference": None,
                "state": None,
                "city": None,
                "website": school.get("website"),
                "gender_program": gender,
                "scholarship_available": False,  # D3 does not offer athletic scholarships
            })

    return rows


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

def _get_connection():
    url = os.environ.get("DATABASE_URL")
    if not url:
        logger.error("DATABASE_URL environment variable is not set")
        sys.exit(1)
    return psycopg2.connect(url)


def seed(dry_run: bool = False) -> None:
    rows = _build_rows()
    logger.info("Prepared %d rows to seed (%d D1, %d D2, %d D3)",
                len(rows),
                sum(1 for r in rows if r["division"] == "D1"),
                sum(1 for r in rows if r["division"] == "D2"),
                sum(1 for r in rows if r["division"] == "D3"))

    if dry_run:
        by_div: Dict[str, int] = {}
        for r in rows:
            by_div[r["division"]] = by_div.get(r["division"], 0) + 1
        logger.info("[DRY RUN] Would insert/update:")
        for div in sorted(by_div):
            logger.info("  %s: %d rows", div, by_div[div])
        logger.info("Sample rows:")
        for r in rows[:5]:
            logger.info("  %s | %s | %s | %s | %s",
                        r["name"], r["division"], r["gender_program"],
                        r["conference"] or "—", r["state"] or "—")
        return

    conn = _get_connection()
    cur = conn.cursor()

    inserted = 0
    updated = 0
    errored = 0

    upsert_sql = """
        INSERT INTO colleges (
            name, slug, division, conference, state, city,
            website, gender_program, scholarship_available
        ) VALUES (
            %(name)s, %(slug)s, %(division)s, %(conference)s, %(state)s, %(city)s,
            %(website)s, %(gender_program)s, %(scholarship_available)s
        )
        ON CONFLICT (name, division, gender_program) DO UPDATE SET
            slug = EXCLUDED.slug,
            conference = COALESCE(EXCLUDED.conference, colleges.conference),
            state = COALESCE(EXCLUDED.state, colleges.state),
            city = COALESCE(EXCLUDED.city, colleges.city),
            website = COALESCE(EXCLUDED.website, colleges.website),
            scholarship_available = COALESCE(EXCLUDED.scholarship_available, colleges.scholarship_available)
        RETURNING (xmax = 0) AS was_inserted
    """

    for row in rows:
        try:
            cur.execute(upsert_sql, row)
            result = cur.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            logger.warning("Failed to upsert %s (%s %s): %s",
                           row["name"], row["division"], row["gender_program"], e)
            conn.rollback()
            errored += 1
            continue

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Seed complete: %d inserted, %d updated, %d errored (of %d total)",
                inserted, updated, errored, len(rows))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Seed the colleges table with NCAA D1/D2/D3 soccer programs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be inserted without writing to the database",
    )
    args = parser.parse_args()
    seed(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
