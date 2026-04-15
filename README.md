# Upshift Data

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

# Backfill coaches master from coach_discoveries (idempotent)
pnpm --filter @workspace/scripts run backfill-coaches -- --dry-run
pnpm --filter @workspace/scripts run backfill-coaches

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

PostgreSQL, managed with Drizzle ORM. 26 tables after the April 2026 Path A expansion. Push schema: `pnpm --filter @workspace/db run push`.

### Core club graph

| Table | Purpose |
|---|---|
| `canonical_clubs` | Master club records; website, status, socials, last-scraped timestamps |
| `club_aliases` | All scraped name variants per canonical club |
| `club_affiliations` | League/source associations (unique on `club_id + source_name`) |
| `leagues_master` | League directory inventory |
| `league_sources` | Official scrape source registry |

### Coaches (Path A)

| Table | Purpose |
|---|---|
| `coaches` | Master coach records; `person_hash` dedup; `manually_merged` guard for operator curation |
| `coach_discoveries` | Primary coach read model; FK to `coaches` via `coach_id`; platform family + confidence |
| `coach_career_history` | Role+tenure records per coach across clubs |
| `coach_movement_events` | Hire/leave/promotion events |
| `coach_scrape_snapshots` | Point-in-time snapshot per scrape run |
| `coach_effectiveness` | Aggregated outcome metrics |

### Competition + rosters (Path A)

| Table | Purpose |
|---|---|
| `events` / `event_teams` | Tournaments, leagues, showcases and participating teams |
| `matches` / `club_results` | Individual games + aggregated per-club results |
| `roster_diffs` / `tryouts` | Roster change log + tryout announcements |
| `club_roster_snapshots` / `club_site_changes` | Point-in-time roster diffing + website change detection |

### Colleges (Path A)

| Table | Purpose |
|---|---|
| `colleges` / `college_coaches` / `college_roster_history` | NCAA/NAIA/NJCAA dataset |

### Scrape telemetry (Path A)

| Table | Purpose |
|---|---|
| `scrape_run_logs` | Per-run telemetry with `failure_kind` enum; written by `scraper/scrape_run_logger.py` |
| `scrape_health` | Rolling health rollups |

### Dropped legacy tables (April 2026)

`club_coaches` was dropped after the backfill verified zero residual rows and `/api/coaches/search` was rewired to `coach_discoveries`. `club_events` was dropped after `/api/events/search` was rewired to `events` + `event_teams`.

See `docs/path-a-data-model.md` for the full domain-by-domain spec and `CLAUDE.md` for session context.

---

## REST API

Base URL: `/api` — port 8080. All list endpoints are paginated (`?page=1&page_size=20`, max 100).

### Authentication

Every request under `/api/*` except `/api/healthz` requires a machine-to-machine API key (when enforcement is turned on — see bootstrap below). Pass it in either header:

```
X-API-Key: <key>
```
```
Authorization: Bearer <key>
```

Requests without a valid key return `401 { "error": "unauthorized" }`. The response body is intentionally the same for missing, unknown, and revoked keys — detailed reason is logged server-side only. There are no user sessions — this is a pure M2M API.

#### Bootstrap sequence (first deploy)

Enforcement is gated by the `API_KEY_AUTH_ENABLED` env var. A fresh deploy with the flag unset accepts all `/api/*` traffic so you can bring the table up and mint a key before flipping it on.

1. Pull, install, and push the schema (creates the `api_keys` table):
   ```bash
   pnpm install
   pnpm --filter @workspace/db run push
   ```
2. Create the first key (plaintext prints once — copy immediately into the caller's env):
   ```bash
   pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-player-platform prod"
   ```
3. Set `API_KEY_AUTH_ENABLED=true` in Replit Secrets.
4. Restart the API server. The boot log will print `[api-key-auth] enabled`; from here every `/api/*` call requires the header.

The plaintext key is printed ONCE. Only the sha256 hash is stored in the database — a lost key cannot be recovered, only revoked and replaced.

#### Rotating a key

1. Create a new key with `create-api-key` (different `--name` suffix or timestamp).
2. Update the caller's env var and redeploy.
3. Confirm the new key works by tailing logs for 401s.
4. Revoke the old key:
   ```bash
   pnpm --filter @workspace/scripts run revoke-api-key -- --prefix <8-char-prefix>
   ```

#### Calling from `upshift-player-platform`

```ts
const res = await fetch(`${process.env.UPSHIFT_DATA_API_URL}/api/clubs`, {
  headers: { "X-API-Key": process.env.UPSHIFT_DATA_API_KEY! },
});
```

`/api/healthz` remains open for Replit liveness probes.

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
