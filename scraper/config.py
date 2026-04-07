"""
Configuration for the Upshift Club Aggregator scraper.

Add league entries to LEAGUES. Each entry defines:
  - name:        Display name for the league
  - url:         The directory page to scrape
  - js_required: True if the page needs JavaScript to render
  - state:       Default state (can be overridden per club if parsed)
  - city_field:  CSS selector hint for city extraction (optional)

Scraped output goes to:
  output/leagues/<league_slug>.csv   — per-league CSV
  output/master.csv                  — deduplicated master dataset
"""

LEAGUES = [
    {
        "name": "US Club Soccer",
        "url": "https://www.usclubsoccer.org/find-a-club/",
        "js_required": True,
        "state": "",
    },
    {
        "name": "AYSO",
        "url": "https://www.ayso.org/find-a-program/",
        "js_required": True,
        "state": "",
    },
    {
        "name": "USYS National League",
        "url": "https://www.usyouthsoccer.org/members/member-organizations/",
        "js_required": False,
        "state": "",
    },
    # Add more leagues here following the same pattern
]

OUTPUT_DIR = "output"
LEAGUES_DIR = "output/leagues"
MASTER_CSV = "output/master.csv"

FUZZY_THRESHOLD = 88          # RapidFuzz similarity score (0-100) to consider clubs identical
PLAYWRIGHT_TIMEOUT = 30_000   # ms to wait for JS pages to settle
PLAYWRIGHT_WAIT_FOR = "networkidle"  # Playwright wait_until strategy
