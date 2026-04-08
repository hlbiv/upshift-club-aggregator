# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Includes a standalone Python scraper for the Upshift Club Aggregator project.

---

## Python Scraper — Upshift Club Aggregator

Located in `scraper/`. Extracts youth soccer club data from 83 league directories across all tiers of US youth soccer, normalizes club names, deduplicates with fuzzy matching, and outputs structured CSVs.

### Directory Layout

```
scraper/
├── config.py             # Loads leagues dynamically from seed CSVs; scraping settings
├── scraper_static.py     # BeautifulSoup scraper for plain HTML pages
├── scraper_js.py         # Playwright (headless Chromium) for JS-rendered pages
├── normalizer.py         # Club name normalization + RapidFuzz deduplication
├── storage.py            # Per-league CSV and master CSV writer
├── run.py                # CLI entry point
├── requirements.txt      # Python package list
├── data/
│   ├── leagues_master.csv              # 89-row league inventory (source of truth)
│   ├── league_sources_seed.csv         # Official scrape source registry
│   ├── usys_state_associations_seed.csv # All 54 USYS member associations
│   └── canonical_schema.sql            # Postgres schema for canonical club graph
└── output/
    ├── master.csv                       # Deduplicated master dataset
    └── leagues/<league-slug>.csv        # One CSV per scraped league
```

### League Coverage (83 total, all with has_public_clubs=True)

| Tier | Count | Examples |
|---|---|---|
| 1 — National Elite | 5 | MLS NEXT, ECNL, Girls Academy, NWSL Academy, USL Academy |
| 2 — High Performance | 7 | ECNL RL, GA Aspire, DPL, NPL, USYS National League, US Club iD |
| 3 — Regional Power | 17 | EDP, SCCL, CCL, NorCal, SOCAL, 6 NPL member leagues, Super Y, more |
| 4 — USYS State Hubs | 54 | All 54 state/regional youth soccer associations |

69 are high-priority, 14 medium-priority.

### CSV Output Schema

```
club_name, canonical_name, league_name, city, state, source_url
```

### Key CLI Commands

```bash
cd scraper

python3 run.py                          # scrape all 83 leagues
python3 run.py --tier 1                 # Tier 1 national elite only (5 leagues)
python3 run.py --priority high          # 69 high-priority leagues
python3 run.py --gender girls           # girls programs only
python3 run.py --scope state            # all 54 USYS state associations
python3 run.py --league "ECNL"         # single league by name (partial match)
python3 run.py --dry-run               # run without writing files
python3 run.py --list                  # print full league inventory and exit
```

### Adding a League

Add a row to `scraper/data/leagues_master.csv` with `has_public_clubs=True`. The config auto-loads on next run. Key fields:

| Field | Values |
|---|---|
| `tier_numeric` | 1–4 |
| `scrape_priority` | high / medium / low |
| `source_type` | `homepage`, `club_directory`, `state_association_hub`, etc. |
| `js_required` | inferred from `source_type`; static = `state_association_hub` / `news` |

### Python Dependencies

```
playwright, beautifulsoup4, requests, pandas, rapidfuzz, lxml, html5lib
```

Install: `pip install -r scraper/requirements.txt && python3 -m playwright install chromium`

---

## TypeScript / Node Stack

### Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

### Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server locally
