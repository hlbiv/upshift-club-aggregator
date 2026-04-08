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

**Extraction priority**: tables → lists → anchor links. Falls back gracefully if a page layout changes.

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
