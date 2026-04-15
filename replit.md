# Workspace

## Overview

pnpm workspace monorepo with TypeScript and a standalone Python scraper toolkit. Builds **Upshift Data** — a data pipeline and REST API for US youth soccer clubs across 127 league directories, deduplicating into a PostgreSQL graph database and serving data via a typed REST API at port 8080.

---

## Python Scraper — Upshift Data

Located in `scraper/`. Extracts youth soccer club data from league directories, normalizes club names, deduplicates with fuzzy matching (FUZZY_THRESHOLD=88), and outputs structured CSVs. Also provides club website enrichment (directory extraction + Brave Search API) and a staff page scraper for coach discovery.

### Directory Layout

```
scraper/
├── config.py              # League inventory loader; MAX_RETRIES=3, RETRY_BASE_DELAY=2s
├── scraper_static.py      # BeautifulSoup scraper for plain HTML pages (with retry)
├── scraper_js.py          # Playwright (headless Chromium) for JS-rendered pages
├── normalizer.py          # Club name normalization + RapidFuzz deduplication (threshold=88)
├── storage.py             # Per-league CSV and master CSV writer; COLUMNS includes website, source_type
├── run.py                 # CLI entry point; FailureKind enum + structured failure reporting
├── enrich_clubs.py        # Extracts website URLs from scraped league directory pages
├── enrich_websites.py     # Brave Search API enrichment for clubs missing a website
├── scrape_staff.py        # Staff page scraper (SportsEngine, LeagueApps, WordPress, generic)
├── requirements.txt       # Python package list
├── utils/
│   └── retry.py          # retry_with_backoff() + TransientError sentinel; exp. backoff cap 60s
├── extractors/            # Per-site custom extractors (URL-pattern matched before generic scrapers)
│   ├── registry.py            # URL-pattern → extractor function mapping via @register decorator
│   ├── playwright_helper.py   # Shared Playwright render helper (with retry on nav errors)
│   ├── girls_academy.py       # Girls Academy + GA Aspire (<article><li> structure)
│   ├── norcal.py              # NorCal Premier Soccer (/clubs/ table)
│   ├── ecnl.py                # ECNL + ECNL RL (AthleteOne API + Playwright fallback)
│   ├── dpl.py                 # DPL (WordPress pages)
│   ├── edp.py                 # EDP Soccer (Wix static crawl)
│   ├── mls_next.py            # MLS NEXT (patterns A+B, website extraction)
│   ├── gotsport.py            # GotSport event roster scraper (shared helper, with retry)
│   ├── sincsports.py          # SincSports TTTeamList.aspx extractor (static HTML)
│   ├── soccerwire.py          # SoccerWire WP REST API + individual club page extractor
│   ├── state_assoc.py         # All 54 USYS tier-4 state associations (GotSport + Maps KML + SoccerWire)
│   ├── npl_extra.py           # NPL regional leagues + additional GotSport-backed directories
│   ├── socal.py               # SOCAL Soccer League via GotSport
│   ├── mspsp.py               # Michigan State Premier Soccer Program via GotSport
│   ├── ne_impact.py           # New England Impact / Impact Soccer via GotSport
│   ├── supery.py              # Super Y League
│   ├── usl_academy.py         # USL Academy League
│   ├── nwsl_academy.py        # NWSL Academy
│   ├── elite64.py             # Elite 64
│   ├── heartland.py           # Heartland Soccer Association
│   ├── frontier.py            # Frontier Soccer League
│   ├── az_soccer.py           # Arizona Soccer Club League
│   ├── central_states.py      # Central States Soccer League
│   ├── mountain_west.py       # Mountain West Soccer League
│   ├── mapl.py                # Mid-Atlantic Premier League
│   ├── tcsl.py                # Texas Club Soccer League
│   └── sssl.py                # Sunshine State Soccer League
├── data/
│   ├── leagues_master.csv              # 127-row league inventory (source of truth)
│   ├── league_sources_seed.csv         # Official scrape source registry
│   ├── usys_state_associations_seed.csv # All 54 USYS member associations
│   ├── state_assoc_config.json         # Maps state URL → {type, events/map_ids, multi_state}; soccerwire type for HI/LA/MA/MS/NE/RI/SC/WI
│   ├── soccerwire_slugs_cache.json     # Cached SoccerWire WP REST API slug list (1,067 clubs; auto-refreshed)
│   └── canonical_schema.sql            # Postgres schema for canonical club graph
└── output/
    ├── master.csv                       # Deduplicated master dataset
    ├── website_coverage.txt             # Per-extractor website extraction summary
    ├── website_enrichment_progress.json # Brave Search enrichment checkpoint
    └── leagues/<league-slug>.csv        # One CSV per scraped league
```

### League Coverage (127 entries, 115 scrapeable)

| Tier | Scrapeable | Examples |
|---|---|---|
| 1 — National Elite | 7 | MLS NEXT, ECNL, Girls Academy, NWSL Academy, USL Academy, Elite 64, ECNL Boys |
| 2 — High Performance | 13 | ECNL RL (B+G), GA Aspire, DPL, NPL, USYS NL, US Club iD, Pre-ECNL |
| 3 — Regional Power | 41 | EDP, NorCal, SOCAL, SincSports events (14), 6 NPL regions, Super Y, state leagues |
| 4 — USYS State Hubs | 54 | All 54 state/regional youth soccer associations |

Source types: `state_association_hub` (54), `homepage` (39), `sincsports` (14), `program` (6), `league_page` (4), `athleteone_api` (4), `directory` (2), plus `no_source` (1).

### CSV Output Schema

```
club_name, canonical_name, league_name, city, state, source_url, website, source_type
```

### Key CLI Commands

```bash
cd scraper

# Scraping
python3 run.py                          # scrape all 115 scrapeable leagues
python3 run.py --tier 1                 # Tier 1 national elite only (7 leagues)
python3 run.py --priority high          # high-priority leagues
python3 run.py --gender girls           # girls programs only
python3 run.py --scope state            # all 54 USYS state associations
python3 run.py --league "ECNL"          # single league by name (partial match)
python3 run.py --dry-run                # run without writing files
python3 run.py --list                   # print full league inventory and exit

# Website enrichment
python3 enrich_clubs.py                 # extract websites from scraped directory pages
python3 enrich_websites.py --limit 100  # Brave Search API for clubs missing websites
python3 enrich_websites.py --dry-run    # preview only (no API calls, no writes)

# Coach/staff discovery
python3 scrape_staff.py --limit 50      # scrape staff pages for clubs with websites
python3 scrape_staff.py --tier 1        # staff for Tier 1 clubs only
python3 scrape_staff.py --dry-run       # dry run (no DB writes)

# Database seeding
cd ..
pnpm --filter @workspace/db run push    # push schema changes
npx tsx lib/db/src/seed.ts              # seed PostgreSQL from master.csv
```

### Custom Extractor Notes

**ECNL AthleteOne API**: URL format `/{event_id}/{org_id}/{org_season_id}/0/0`. Calling with event_id=0 returns default conference data plus a `<select id="event-select">` listing all conference event_ids. org_season maps: 70=Boys ECNL, 69=Girls ECNL, 72=Boys RL, 71=Girls RL.

**GotSport pattern**: Rosters at `system.gotsport.com/org_event/events/{event_id}/clubs`. Shared helper in `extractors/gotsport.py`. Filter ZZ- rows (admin placeholders). Retry logic wraps all HTTP calls.

**SincSports pattern**: Static HTML at `soccer.sincsports.com/TTTeamList.aspx?tid=TID`. Division selector is client-side only — all teams in a single HTTP response. Filter "NO CLUB" placeholder rows.

**State association strategy**: 54 USYS tier-4 sites configured. GotSport events (34 states), Google My Maps KML (6 states), JS club list (1 state — NC), HTML club list (2 states — OR, PA-West), SoccerWire WP REST API (8 states — HI, LA, MA, MS, NE, RI, SC, WI), no-source (3 states — ND, SD, UT). `state_assoc_config.json` has `multi_state: true` for MN, WV where parent event crosses state lines — records tagged `_state_derived=True` to prevent state overwrite.

**SoccerWire strategy** (Task #22): For 8 previously zero-coverage states, `state_assoc_config.json` entries use `type: soccerwire`. `state_assoc.py` delegates to `extractors/soccerwire.py` which: (1) fetches all 1,067 club slugs from the WP REST API (`/wp-json/wp/v2/clubs?per_page=100`), cached to `data/soccerwire_slugs_cache.json`; (2) filters slugs by state-specific keywords (city names, team name fragments); (3) fetches individual club pages in parallel to extract Location + Membership. Verified counts: HI=6, LA=3, MA=3, MS=3, NE=5, RI=1, SC=2, WI=5 (total 28 new clubs). ND and SD have zero SoccerWire clubs. UT has 6 SoccerWire clubs but 2+ overlap ECNL GotSport coverage — held pending dedup.

**AYSO finding** (Task #22): AYSO section websites (aysos1.org–aysos9.org) all DNS-fail. aysou.org redirects to wiki. Blue Sombrero (registration) is private. No AYSO-specific GotSport events found in scan range 43000–51100. AYSO clubs that appear in SoccerWire are tagged with "U.S. Youth Soccer" membership.

**US Club Soccer finding** (Task #22): No public member club registry. `usclubsoccer.org/members/` requires GotSport login. US Club Soccer–affiliated clubs appear in SoccerWire `Memberships` field (e.g. South Carolina United FC, Omaha United SC). NPL member clubs are covered by individual NPL member-league extractors.

**Retry strategy**: All HTTP calls wrapped in `retry_with_backoff(fn, max_retries=3, base_delay=2s)`. ConnectionError, Timeout, 5xx → `TransientError`. Playwright navigation errors (ERR_NAME_NOT_RESOLVED, net::ERR_*, timeouts) also retried.

**Failure reporting**: `run.py` tracks failures with `FailureKind` enum (timeout, network, parse_error, zero_results, unknown). End-of-run summary shows leagues succeeded/failed + per-type breakdown.

### Python Dependencies

```
playwright, beautifulsoup4, requests, pandas, rapidfuzz, lxml, html5lib, psycopg2-binary
```

Install: `pip install -r scraper/requirements.txt && python3 -m playwright install chromium`

---

## PostgreSQL Graph Database (Path A, April 2026)

26 tables across 8 schema files in `lib/db/src/schema/`. Push with `pnpm --filter @workspace/db run push`.

### Core club graph (`schema/index.ts`)

| Table | Description |
|---|---|
| `canonical_clubs` | Deduplicated master records. Path A additions: `logo_url`, `founded_year`, `twitter`, `instagram`, `facebook`, `staff_page_url`, `website_last_checked_at`, `last_scraped_at`, `scrape_confidence` |
| `club_aliases` | All raw scraped name variants pointing to a canonical club |
| `club_affiliations` | League/source associations per club (unique on `club_id + source_name`) |
| `coach_discoveries` | **Primary coach read model.** Path A additions: `coach_id` FK → `coaches.id`, `person_hash`, `phone`, `first_seen_at`, `last_seen_at` |
| `leagues_master` / `league_sources` | League directory inventory + source registry |

`club_coaches` and `club_events` were dropped April 2026 after their API routes were rewired to the Path A model (`coach_discoveries` and `events` + `event_teams`, respectively).

### Coaches (`schema/coaches.ts`)

`coaches` (master, person-hash deduped, `manually_merged` guard), `coach_career_history`, `coach_movement_events`, `coach_scrape_snapshots`, `coach_effectiveness`.

### Competition (`schema/events.ts`, `schema/matches.ts`)

`events`, `event_teams`, `matches`, `club_results`.

### Rosters / discovery (`schema/rosters-and-tryouts.ts`, `schema/clubs-extended.ts`)

`roster_diffs`, `tryouts`, `club_roster_snapshots`, `club_site_changes`.

### Colleges (`schema/colleges.ts`)

`colleges`, `college_coaches`, `college_roster_history`.

### Scrape telemetry (`schema/scrape-health.ts`)

`scrape_run_logs` (per-run log; `failure_kind` ∈ `timeout | network | parse_error | zero_results | unknown`; `records_touched` is a STORED generated column), `scrape_health` (rolling rollups).

### Contract invariants

- **FailureKind enum** is synchronized across 3 places: Postgres CHECK on `scrape_run_logs`, `scraper/run.py:FailureKind`, `scraper/scrape_run_logger.py:FailureKind`. A parity pytest enforces this.
- **Partial unique index with COALESCE**: `matches_natural_key_uq` uses `COALESCE(match_date, 'epoch'::timestamp)` etc. because Postgres treats NULLs as distinct under plain `unique()`. Same pattern in `tryouts_club_date_bracket_uq`.
- **Drizzle 0.45 `generatedAlwaysAs`** is single-arg only. Do not pass `{mode: 'stored'}`.
- **ESM forward-ref FK**: `coach_discoveries.coach_id` references `coaches.id`. Drizzle resolves FK callbacks lazily, so the `coaches.ts ↔ index.ts` ESM cycle is safe.

### Path A runbook (post-schema-push)

```bash
pnpm install                                                          # refresh workspace symlinks
pnpm --filter @workspace/db run push                                  # apply schema
pnpm --filter @workspace/scripts run backfill-coaches -- --dry-run    # preview
pnpm --filter @workspace/scripts run backfill-coaches                 # commit
psql "$DATABASE_URL" -c "SELECT count(*) FROM coach_discoveries WHERE coach_id IS NULL;"  # should return 0
```

Verified on Replit April 2026: 2,647 discoveries scanned → 2,603 coaches inserted, 44-row `person_hash` collision rate, residual `coach_id IS NULL` = 0.

### Post-scrape runbook — canonical-club linker

`event_teams` and `matches` scrapers deliberately write NULL `canonical_club_id` / `home_club_id` / `away_club_id`. After every scrape, run the linker to populate those FKs:

```bash
cd scraper
python3 run.py --source link-canonical-clubs --dry-run   # preview
python3 run.py --source link-canonical-clubs             # commit

# smoke check
psql "$DATABASE_URL" -c "SELECT count(*) FROM event_teams WHERE canonical_club_id IS NULL;"
psql "$DATABASE_URL" -c "SELECT count(*) FROM matches WHERE home_club_id IS NULL OR away_club_id IS NULL;"
```

Idempotent — only touches rows where the FK is currently NULL. Fuzzy hits write new `club_aliases` rows so re-runs short-circuit at the exact-alias pass. `/api/events/search?club_id=N` and the `matches` → `club_results` rollup both require at least one linker pass after each scrape to function.

See `docs/path-a-data-model.md` for the full spec + changelog; `CLAUDE.md` for session context.

---

## REST API — Express on Port 8080

All routes under `/api`. Source in `artifacts/api-server/src/routes/`.

### Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/api/healthz` | Health check |
| GET | `/api/clubs` | Paginated club list; filter by state, tier, gender_program |
| GET | `/api/clubs/search` | Advanced search: name, state, league (affiliation), has_website |
| GET | `/api/clubs/:id` | Single club with affiliations and aliases |
| GET | `/api/clubs/:id/related` | Related clubs by shared affiliations |
| GET | `/api/clubs/:id/staff` | Coach discoveries from staff scraper |
| GET | `/api/events/search` | Event search: club_id, league, age_group, gender, season, source, date range |
| GET | `/api/coaches/search` | Coach search: club_id, name, title, min_confidence |
| GET | `/api/leagues` | All leagues in master directory |
| GET | `/api/leagues/:id/clubs` | All clubs for a league |
| GET | `/api/search` | Fuzzy club name search |
| GET | `/api/analytics/duplicates` | Near-duplicate clusters by normalized name + state, with source labels |
| GET | `/api/analytics/coverage` | Per-state and per-league club counts; threshold flagging |
| GET | `/api/analytics/overlap` | Clubs with 2+ league affiliations |

All search/analytics endpoints are paginated: `?page=1&page_size=20` (max 100).

### API Shared Libraries

- `lib/pagination.ts` — `parsePagination()` + `buildWhere()` filter helper used by all search routes
- `lib/analytics.ts` — `normalizeClubName()` (JS) + `PG_NORMALIZE_EXPR` (Postgres regex) for dedup analytics

---

## TypeScript / Node Stack

### Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM (`drizzle-orm/node-postgres`)
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API spec**: OpenAPI 3.1 (`lib/api-spec/openapi.yaml`)
- **API codegen**: Orval (Zod schemas + types from OpenAPI spec)
- **Build**: esbuild (single ESM bundle)

### Packages

| Package | Purpose |
|---|---|
| `@workspace/db` | Drizzle schema, seed script, DB client |
| `@workspace/api-spec` | OpenAPI YAML + Orval codegen config |
| `@workspace/api-zod` | Generated Zod validators and TypeScript types |
| `@workspace/api-server` | Express API server (port 8080) |
| `@workspace/mockup-sandbox` | Vite component preview server (port 8081) |

### Key Commands

```bash
pnpm run typecheck                               # full typecheck across all packages
pnpm run build                                   # typecheck + build all packages
pnpm --filter @workspace/api-spec run codegen    # regenerate Zod schemas from OpenAPI
pnpm --filter @workspace/db run push             # push DB schema changes (dev only)
pnpm --filter @workspace/api-server run dev      # run API server locally (port 8080)
```

### Adding a New API Endpoint

1. Add path + response schema to `lib/api-spec/openapi.yaml`
2. Run `pnpm --filter @workspace/api-spec run codegen` to regenerate Zod types
3. Add route handler in `artifacts/api-server/src/routes/`
4. Register in `artifacts/api-server/src/routes/index.ts`

### Adding a New League

1. Add a row to `scraper/data/leagues_master.csv` with `has_public_clubs=True`
2. If a custom extractor is needed, add it to `scraper/extractors/` and register in `registry.py`
3. For GotSport events, find the event ID and use the shared `gotsport.py` helper
4. For SincSports tournaments, find the `tid=` parameter and use `source_type=sincsports`
