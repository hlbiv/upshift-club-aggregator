# Upshift Club Aggregator — League Scraper

Extracts youth soccer club data from 83 US league directories, normalizes club names, deduplicates with fuzzy matching, and outputs structured CSVs ready for loading into the canonical club graph.

---

## Quick Start

```bash
cd scraper

# Install dependencies (first time only)
pip install -r requirements.txt
python3 -m playwright install chromium

# Print full league inventory
python3 run.py --list

# Dry-run to verify before writing files
python3 run.py --tier 1 --dry-run

# Scrape Tier 1 national elite leagues
python3 run.py --tier 1

# Scrape all high-priority leagues
python3 run.py --priority high

# Scrape everything
python3 run.py
```

---

## CLI Reference

```
python3 run.py [options]

Filters (can be combined):
  --tier N          Tier 1=national elite, 2=high performance, 3=regional, 4=state hubs
  --priority        high | medium | low
  --gender          boys | girls | boys_and_girls
  --scope           national | national_regional | regional | state
  --league NAME     Partial name match (case-insensitive); overrides other filters

Modes:
  --dry-run         Run scrapers but do not write any files
  --list            Print league inventory table and exit
```

---

## Output

```
output/
├── master.csv                  # Deduplicated master dataset across all leagues
└── leagues/
    ├── mls-next.csv
    ├── ecnl.csv
    ├── girls-academy.csv
    └── ...                     # One file per scraped league
```

### CSV Schema

| Column | Description |
|---|---|
| `club_name` | Raw name as scraped from the source |
| `canonical_name` | Normalized name (FC/SC/United/Club stripped, title-cased) |
| `league_name` | League this record came from |
| `city` | City if available from source |
| `state` | State/region (injected from seed for state-association entries) |
| `source_url` | URL that was scraped |

---

## League Coverage

83 leagues loaded from `data/leagues_master.csv` where `has_public_clubs = True`.

### Tier 1 — National Elite (5)

| League | Gender | URL |
|---|---|---|
| MLS NEXT | Boys | mlsnextsoccer.com/clubs |
| ECNL | Boys + Girls | theecnl.com/sports/directory |
| Girls Academy | Girls | girlsacademyleague.com/members |
| NWSL Academy | Girls | nwslsoccer.com/nwsl-academy |
| USL Academy League | Boys + Girls | usl-academy.com/academy-league |

### Tier 2 — High Performance (7)

ECNL Regional League (Boys + Girls), GA Aspire, DPL, NPL, USYS National League, US Club Soccer iD

### Tier 3 — Regional Power Leagues (17)

EDP Soccer, SCCL, CCL, SOCAL, NorCal Premier, Arizona Soccer Club League, MSPSP, Super Y League, Heartland Soccer, Sunshine State, NorCal NPL + 6 NPL member leagues (Frontier, Central States, Mid-Atlantic, Texas, Mountain West, New England Impact)

### Tier 4 — USYS State Association Hubs (54)

All 54 official US Youth Soccer member associations. These are scraped with the static (non-JS) scraper since most use simple CMS sites. State/region name is injected from `data/usys_state_associations_seed.csv`.

---

## How It Works

```
run.py
  └─ loads leagues from data/leagues_master.csv (filtered by your flags)
      └─ for each league:
          ├─ custom extractor? → extractors/registry.py (URL-pattern lookup)
          │    ├─ girlsacademyleague.com  → extractors/girls_academy.py
          │    ├─ norcalpremier.com       → extractors/norcal.py
          │    ├─ theecnl.com/sports/dir* → extractors/ecnl.py
          │    ├─ theecnl.com/sports/ecnl-regional-league* → extractors/ecnl.py
          │    ├─ dpleague.org            → extractors/dpl.py
          │    └─ edpsoccer.com           → extractors/edp.py
          ├─ js_required=True  → scraper_js.py (Playwright headless Chromium)
          └─ js_required=False → scraper_static.py (requests + BeautifulSoup)
              └─ normalizer.py
                  ├─ normalize()    — strip FC/SC/United/Club, title-case → canonical_name
                  └─ deduplicate() — RapidFuzz token_sort_ratio, threshold=88
                      └─ storage.py
                          ├─ save_league_csv()    → output/leagues/<slug>.csv
                          └─ append_to_master()  → output/master.csv
```

**JS detection** is inferred from `source_type` in the CSV. `state_association_hub` and `news` entries use static scraping; everything else uses Playwright.

**Extraction priority**: custom extractor (if registered) → tables → lists → anchor links. Falls back gracefully if a page layout changes.

---

## Custom Extractors

High-value leagues have site-specific extractors registered in `extractors/registry.py`.  Each extractor maps one or more URL patterns to a Python function that returns a clean `List[Dict]`.

| League | Extractor | Data Source | Clubs (live) |
|---|---|---|---|
| Girls Academy (members) | `girls_academy.py` | `<article> <li>` HTML (WordPress) | 126 |
| GA Aspire (aspire-membership) | `girls_academy.py` | Same structure | 100 |
| NorCal Premier Soccer | `norcal.py` | `/clubs/` table (WordPress) | 286 |
| ECNL (Boys + Girls) | `ecnl.py` | AthleteOne API — all 26 national conferences | 200 |
| ECNL RL Boys | `ecnl.py` | AthleteOne API — all 26 national conferences | 286 |
| ECNL RL Girls | `ecnl.py` | AthleteOne API — all 24 national conferences | 281 |
| SOCAL Soccer League | `socal.py` | GotSport event 43086 clubs page | 172 |
| MSPSP (Michigan) | `mspsp.py` | GotSport event 50611 clubs page | 88 |
| DPL | `dpl.py` | WordPress pages + Playwright on bracket pages | 0 |
| EDP Soccer | `edp.py` | Static + link crawl (Wix fallback) | 0 |

### Adding a Custom Extractor

1. Create `extractors/<league>.py`
2. Define a function and decorate with `@register(r"<url-pattern>")`
3. Import the module in `extractors/registry.py` at the bottom

```python
# extractors/my_league.py
from extractors.registry import register

@register(r"myleague\.org/clubs")
def scrape_my_league(url: str, league_name: str) -> list[dict]:
    # ...return list of {"club_name": ..., "league_name": ..., "city": ..., "state": ..., "source_url": ...}
```

### ECNL AthleteOne API — Full National Coverage

The AthleteOne API (`api.athleteone.com/api/Script/get-conference-standings`) backs the TGS standings widget on theecnl.com.

**Correct URL format (discovered 2026-04-09):**
```
/{event_id}/{org_id}/{org_season_id}/0/0
```

**Org season IDs (org_id = 12):**
| org_season_id | League | Conferences |
|---|---|---|
| 70 | Boys ECNL | 16 (Far West, Florida, Heartland, Mid-America, …) |
| 69 | Girls ECNL | 10 (Mid-Atlantic, Midwest, New England, …) |
| 72 | Boys RL | 26 (Carolinas, Chicago Metro, Far West, …) |
| 71 | Girls RL | 24 (Carolinas, Florida, Frontier, …) |

**Discovery:** Calling `event_id=0` returns a full `<select id="event-select">` dropdown listing every conference and its event_id. The extractor auto-discovers these on each run — no hardcoded conference IDs needed.

**Team name format:** `"Oregon Premier ECNL B13Qualification:Champions League..."`  
**Club extraction regex:** `^(.+?)\s+(?:Pre-)?ECNL(?:\s+RL)?\s+[BG]\d+`

---

## Data Files

| File | Purpose |
|---|---|
| `data/leagues_master.csv` | Primary league inventory — edit this to add/update leagues |
| `data/league_sources_seed.csv` | Official source URLs registry for top-level platforms |
| `data/usys_state_associations_seed.csv` | 54 USYS state associations with region metadata |
| `data/canonical_schema.sql` | Postgres schema for `canonical_clubs`, `club_affiliations`, `club_aliases` |

### Adding a League

Add a row to `data/leagues_master.csv`:

```csv
My League,NPL,US Club Soccer,3,NPL Member League,boys_and_girls,regional,True,medium,homepage,https://myleague.com/clubs,Short note
```

Required: `league_name`, `has_public_clubs=True`, `official_url`, `tier_numeric`, `scrape_priority`.

---

## Deduplication

Club names are deduplicated in two passes:

1. **Within each league** — after scraping a single source
2. **Across all leagues** — before writing `master.csv`

Two clubs are considered the same if their `canonical_name` values score ≥ 88 on RapidFuzz `token_sort_ratio`. Adjust `FUZZY_THRESHOLD` in `config.py` to tune sensitivity.

Examples of what gets merged:
- `Portland FC United` + `Portland United FC` → `Portland`
- `Top Hat SC` + `TopHat Soccer Club` + `Tophat` → one entry

---

## Priority Order (per seed pack README)

1. MLS NEXT
2. ECNL / ECNL RL
3. Girls Academy / GA Aspire
4. DPL
5. NPL (+ member leagues)
6. USYS National League
7. USYS State Association hubs
8. Regional power leagues (EDP, SCCL, CCL, SOCAL, NorCal, etc.)

Run tiers in order to build the dataset from highest-confidence sources first.

---

## 2026 Note

US Club Soccer and US Youth Soccer announced a unified top competition for 2026–27 that will merge NPL and National League. Both source families remain in the database until the new structure and club lists stabilize.
