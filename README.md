# Upshift Club Aggregator

A scraper toolkit, PostgreSQL graph database, and REST API for US youth soccer club data. Covers 127 league directories across all tiers of the US youth soccer pyramid — from MLS NEXT down to 54 state associations — normalizing and deduplicating clubs into a single canonical dataset.

---

## What It Does

1. **Scrapes** club rosters from 115 publicly accessible league directories (7 Tier-1 national, 13 Tier-2 high performance, 41 Tier-3 regional, 54 Tier-4 state associations)
2. **Normalizes** club names (stripping suffixes, fixing casing) and **deduplicates** across sources with fuzzy matching (RapidFuzz, threshold 88)
3. **Enriches** clubs with website URLs — first from directory pages, then via Brave Search API for remaining clubs
4. **Discovers coaches** by scraping staff pages (SportsEngine, LeagueApps, WordPress)
5. **Seeds** a PostgreSQL graph database with canonical clubs, aliases, affiliations, events, and coaches
6. **Exposes** a typed REST API on port 8080 with search, analytics, and graph traversal endpoints

---

## Quick Start

### Python Scraper

```bash
cd scraper
pip install -r requirements.txt
python3 -m playwright install chromium

# Scrape all 115 leagues
python3 run.py

# Scrape by tier / filter
python3 run.py --tier 1              # Tier 1 national elite (7 leagues)
python3 run.py --scope state         # All 54 USYS state associations
python3 run.py --league "ECNL"       # Single league by name
python3 run.py --dry-run             # Preview without writing
python3 run.py --list                # Print league inventory
```

### Website & Staff Enrichment

```bash
cd scraper
python3 enrich_clubs.py              # Extract websites from directory pages
python3 enrich_websites.py --limit 200  # Brave Search for remaining clubs
python3 scrape_staff.py --limit 100  # Scrape staff pages for coaches
```

### Database + API

```bash
# Push schema and seed DB from master.csv
pnpm --filter @workspace/db run push
npx tsx lib/db/src/seed.ts

# Start API (port 8080)
pnpm --filter @workspace/api-server run dev
```

---

## League Coverage

127 entries total, 115 scrapeable (`has_public_clubs=True`).

| Tier | Count | Notable Leagues |
|---|---|---|
| 1 — National Elite | 7 | MLS NEXT, ECNL Girls, ECNL Boys, Girls Academy, NWSL Academy, USL Academy, Elite 64 |
| 2 — High Performance | 13 | ECNL RL (Boys + Girls), GA Aspire, DPL, US Club NPL, USYS National League, Pre-ECNL |
| 3 — Regional Power | 41 | EDP, NorCal Premier, SOCAL, Super Y, SincSports tournaments (14), NPL regional leagues |
| 4 — State Hubs | 54 | All 54 USYS member state/regional associations |

### Source Types

| Type | Count | How It Works |
|---|---|---|
| `state_association_hub` | 54 | GotSport event rosters or Google My Maps KML |
| `homepage` | 39 | BeautifulSoup HTML scraping of club directory pages |
| `sincsports` | 14 | Static HTML from `soccer.sincsports.com/TTTeamList.aspx?tid=` |
| `program` | 6 | Program/division-specific directory pages |
| `league_page` | 4 | League-owned team/club listing pages |
| `athleteone_api` | 4 | ECNL's AthleteOne JSON API |
| `directory` | 2 | Club directory index pages |
| `no_source` | 1 | No public club listing available |

---

## Data Pipeline

```
leagues_master.csv
      │
      ▼
  run.py ──► extractors/ ──► scraper_static.py / scraper_js.py
      │              (per-site custom extractors)
      │
      ▼
normalizer.py  ←  RapidFuzz (threshold=88)
      │
      ▼
storage.py ──► output/master.csv
                output/leagues/<slug>.csv
      │
      ▼
enrich_clubs.py   (website URLs from directory pages)
enrich_websites.py (Brave Search API for remaining clubs)
scrape_staff.py   (coach discovery via staff pages)
      │
      ▼
lib/db/src/seed.ts ──► PostgreSQL
```

---

## Database Schema

PostgreSQL, managed with Drizzle ORM. Push schema: `pnpm --filter @workspace/db run push`.

| Table | Purpose |
|---|---|
| `canonical_clubs` | Master club records with website and status fields |
| `club_aliases` | All scraped name variants per canonical club |
| `club_affiliations` | League/source associations (unique on `club_id + source_name`) |
| `club_events` | Event/bracket participation: age group, gender, division, dates |
| `club_coaches` | Coach records from league directories |
| `coach_discoveries` | Staff-page-discovered coaches with platform family and confidence score |
| `leagues_master` | League directory inventory |
| `league_sources` | Official scrape source registry |

---

## REST API

Base URL: `/api` — port 8080. All list endpoints are paginated (`?page=1&page_size=20`, max 100).

### Club Endpoints

| Endpoint | Description |
|---|---|
| `GET /clubs` | Paginated list; filter by `state`, `tier`, `gender_program` |
| `GET /clubs/search` | Advanced search: `name`, `state`, `league`, `has_website` |
| `GET /clubs/:id` | Single club with affiliations and aliases |
| `GET /clubs/:id/related` | Related clubs sharing affiliations |
| `GET /clubs/:id/staff` | Discovered coaches from staff pages |

### Search & Discovery

| Endpoint | Description |
|---|---|
| `GET /search` | Fuzzy club name search |
| `GET /events/search` | Filter events by `club_id`, `league`, `age_group`, `gender`, `season`, `start_date_from/to` |
| `GET /coaches/search` | Filter coaches by `club_id`, `name`, `title`, `min_confidence` |

### Leagues

| Endpoint | Description |
|---|---|
| `GET /leagues` | All leagues in master directory |
| `GET /leagues/:id/clubs` | All clubs for a specific league |

### Analytics

| Endpoint | Description |
|---|---|
| `GET /analytics/duplicates` | Near-duplicate club clusters (normalized name + state); includes source labels |
| `GET /analytics/coverage` | Per-state and per-league club counts; flags states below `min_clubs` threshold |
| `GET /analytics/overlap` | Clubs appearing in 2+ leagues; useful for detecting wrong-source associations |

### Example Queries

```bash
# Clubs in California with a known website
curl "http://localhost:8080/api/clubs/search?state=CA&has_website=true&page_size=10"

# Events in 2024-2025 season
curl "http://localhost:8080/api/events/search?season=2024-2025&page_size=20"

# High-confidence coaches for a club
curl "http://localhost:8080/api/coaches/search?club_id=155&min_confidence=0.8"

# Coverage report (states below 5 clubs)
curl "http://localhost:8080/api/analytics/coverage?min_clubs=5"

# Near-duplicate clubs in Texas
curl "http://localhost:8080/api/analytics/duplicates?state=TX"

# Clubs in multiple leagues (potential wrong associations)
curl "http://localhost:8080/api/analytics/overlap?min_leagues=5&page_size=20"
```

---

## Extractors

Custom extractors live in `scraper/extractors/` and are matched by URL pattern via `registry.py`.

| Extractor | Leagues | Technique |
|---|---|---|
| `ecnl.py` | ECNL Boys + Girls, ECNL RL B+G | AthleteOne JSON API; auto-discovers all conference event IDs |
| `girls_academy.py` | Girls Academy, GA Aspire | `<article><li>` HTML structure |
| `mls_next.py` | MLS NEXT | Pattern A (table) + Pattern B (card grid); extracts website links |
| `norcal.py` | NorCal Premier Soccer | `/clubs/` table |
| `gotsport.py` | SOCAL, MSPSP, state assocs, NPL regions | GotSport `org_event/events/{id}/clubs` roster pages |
| `sincsports.py` | 14 SincSports tournaments | `TTTeamList.aspx?tid=` static HTML; single response, no pagination |
| `state_assoc.py` | All 54 USYS state associations | GotSport events or Google Maps KML per state |
| `npl_extra.py` | SE NPL, Empire Soccer, Mid-Atlantic, NY Club Soccer | GotSport event IDs via `_multi_event_scrape` helper |
| `edp.py` | EDP Soccer | Wix static crawl |
| `dpl.py` | DPL | WordPress pages |

---

## Reliability

- **Retry logic**: All HTTP requests use `utils/retry.py` — exponential backoff (2s base, cap 60s), 3 retries. `ConnectionError`, `Timeout`, 5xx → `TransientError`. Playwright navigation errors retried similarly.
- **Failure reporting**: `run.py` tracks each league scrape result. End-of-run summary shows counts by `FailureKind`: `timeout`, `network`, `parse_error`, `zero_results`, `unknown`.
- **Multi-state events**: GotSport events spanning multiple states (MN, WV) set `multi_state=true` in config; clubs keep blank state rather than inheriting wrong parent state.
- **Deduplication**: RapidFuzz token-sort ratio at threshold 88 merges near-identical club names across sources into a single canonical record.

---

## Tech Stack

### Python Scraper

- Python 3.11+
- `requests`, `beautifulsoup4`, `lxml`, `html5lib` — static scraping
- `playwright` — JS-rendered pages (headless Chromium)
- `pandas`, `rapidfuzz` — normalization and fuzzy deduplication
- `psycopg2-binary` — direct PostgreSQL writes from staff scraper

### TypeScript / Node

- **Monorepo**: pnpm workspaces
- **Runtime**: Node.js 24
- **API**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod v4 + drizzle-zod
- **API spec**: OpenAPI 3.1 → Orval codegen (Zod validators + TS types)
- **Build**: esbuild (single ESM bundle)

### Key Commands

```bash
pnpm run typecheck                               # typecheck all packages
pnpm --filter @workspace/api-spec run codegen    # regenerate Zod types from OpenAPI
pnpm --filter @workspace/db run push             # sync schema to DB
pnpm --filter @workspace/api-server run dev      # start API (port 8080)
```

---

## Project Structure

```
.
├── scraper/                   # Python data pipeline
│   ├── extractors/            # Per-site scrapers + GotSport/SincSports helpers
│   ├── utils/                 # retry_with_backoff utility
│   ├── data/                  # League inventory CSVs + state config JSON
│   └── output/                # Generated CSVs (gitignored)
├── lib/
│   ├── db/                    # Drizzle schema, seed script, DB client
│   ├── api-spec/              # OpenAPI 3.1 YAML
│   └── api-zod/               # Generated Zod validators + TypeScript types
└── artifacts/
    └── api-server/            # Express API server
        └── src/
            ├── routes/        # clubs, events, coaches, leagues, analytics, search
            └── lib/           # pagination, analytics normalization helpers
```
