# Workspace

## Overview

pnpm workspace monorepo using TypeScript. Each package manages its own dependencies.

## Python Scraper — Upshift Club Aggregator

Located in `scraper/`. A standalone Python toolkit for scraping soccer club data from league directories.

### Modules

| File | Purpose |
|---|---|
| `config.py` | League URLs, output paths, fuzzy threshold settings |
| `scraper_static.py` | BeautifulSoup scraper for plain HTML pages |
| `scraper_js.py` | Playwright (headless Chromium) scraper for JS-rendered pages |
| `normalizer.py` | Club name normalization + RapidFuzz deduplication |
| `storage.py` | Per-league CSV and master CSV writer |
| `run.py` | Main CLI entry point |

### Usage

```bash
cd scraper

# Scrape all leagues
python3 run.py

# Scrape one league
python3 run.py --league "AYSO"

# Preview without writing files
python3 run.py --dry-run
```

### Output

- `scraper/output/leagues/<league-slug>.csv` — one file per league
- `scraper/output/master.csv` — deduplicated master dataset

### CSV Schema

`club_name, canonical_name, league_name, city, state, source_url`

### Adding a League

Edit `scraper/config.py` and add an entry to the `LEAGUES` list:

```python
{
    "name": "My League",
    "url": "https://example.com/clubs",
    "js_required": False,   # True for JS-rendered pages
    "state": "TX",          # Default state if page doesn't include it
}
```

## Stack

- **Monorepo tool**: pnpm workspaces
- **Node.js version**: 24
- **Package manager**: pnpm
- **TypeScript version**: 5.9
- **API framework**: Express 5
- **Database**: PostgreSQL + Drizzle ORM
- **Validation**: Zod (`zod/v4`), `drizzle-zod`
- **API codegen**: Orval (from OpenAPI spec)
- **Build**: esbuild (CJS bundle)

## Key Commands

- `pnpm run typecheck` — full typecheck across all packages
- `pnpm run build` — typecheck + build all packages
- `pnpm --filter @workspace/api-spec run codegen` — regenerate API hooks and Zod schemas from OpenAPI spec
- `pnpm --filter @workspace/db run push` — push DB schema changes (dev only)
- `pnpm --filter @workspace/api-server run dev` — run API server locally

See the `pnpm-workspace` skill for workspace structure, TypeScript setup, and package details.
