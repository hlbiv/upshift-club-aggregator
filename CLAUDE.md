# CLAUDE.md — Upshift Data

Persistent context for Claude Code. Read at the start of every session.

---

## What This Repo Is

Upshift Data is the reference-data backend for Upshift soccer club/coach intelligence:

- **Python scraper toolkit** (`scraper/`) — 150+ extractors across 127 league directories
- **PostgreSQL graph** (`lib/db/`) — canonical clubs, coaches master, events, matches, rosters, tryouts, scrape health
- **REST API** (`artifacts/api-server/`) — Express 5 on port 8080, consumed by `upshift-player-platform`

**Relationship to sibling repo:** `upshift-player-platform` owns user/claim/payment data. This repo owns everything scraped (orgs, coaches, events, rosters, results). Player platform calls this API — never the DB directly.

---

## Workflow

- Local: `pnpm typecheck`, `pnpm test`, `pnpm install` are fine on macOS.
- Never run DB operations (`pnpm --filter @workspace/db run push`, seed, migrations) locally — always on Replit.
- Never run scrapers locally — always on Replit.
- `gh` CLI is authenticated as `hlbiv`. Use it directly for PRs.

### Commit & Push Ritual

Every "commit and push" is composite:

1. `git fetch origin master && git merge origin/master --no-edit`
2. Commit
3. Push
4. `gh pr create` (or update existing PR)

Pushing without a PR is incomplete — Replit pulls from master, so only merged PRs are visible.

### Stacked PR Warning

Do **not** base one Claude PR on another Claude PR branch. When the base PR merges to master, the stacked PR's "merge" target disappears and the stacked commits quietly fail to reach master. See PR #6 for the recovery pattern. Always base new PRs on `master`.

---

## Monorepo Layout

```
.
├── scraper/                     # Python 3.11+ pipeline
│   ├── run.py                   # Entry point; FailureKind enum + scrape_run_logs wiring
│   ├── scrape_run_logger.py     # Module-level psycopg2 singleton, lazy-connect, no-ops if DB unreachable
│   ├── scraper_static.py        # BeautifulSoup
│   ├── scraper_js.py            # Playwright
│   ├── normalizer.py            # RapidFuzz dedup (threshold 88)
│   ├── extractors/              # Per-site custom extractors (URL-pattern matched)
│   └── tests/                   # Pytest suite
├── lib/
│   ├── db/                      # Drizzle schema, seed script, DB client
│   │   └── src/schema/
│   │       ├── index.ts                # legacy tables + canonical_clubs extensions + coach_discoveries extensions
│   │       ├── coaches.ts              # coaches master + career/movement/snapshot/effectiveness
│   │       ├── colleges.ts             # colleges + college_coaches + college_roster_history
│   │       ├── events.ts               # events + event_teams
│   │       ├── matches.ts              # matches + club_results
│   │       ├── rosters-and-tryouts.ts  # roster_diffs + tryouts
│   │       ├── clubs-extended.ts       # club_roster_snapshots + club_site_changes
│   │       └── scrape-health.ts        # scrape_run_logs + scrape_health
│   ├── api-spec/                # OpenAPI 3.1 YAML
│   └── api-zod/                 # Orval-generated Zod validators + TS types
├── artifacts/
│   └── api-server/src/routes/   # Express routers (clubs, events, coaches, leagues, analytics, search)
└── scripts/
    └── src/backfill-coaches-master.ts  # Idempotent coaches master backfill
```

---

## Database Model (Path A)

26 tables. Schema pushed with `pnpm --filter @workspace/db run push` on Replit.

### Legacy / core (in `schema/index.ts`)

| Table | Purpose |
|---|---|
| `leagues_master` | League directory inventory (127 rows) |
| `league_sources` | Official scrape source registry |
| `canonical_clubs` | Deduplicated master club records + Path A extensions (`logo_url`, `founded_year`, `twitter`, `instagram`, `facebook`, `staff_page_url`, `website_last_checked_at`, `last_scraped_at`, `scrape_confidence`) |
| `club_aliases` | Raw scraped name variants → canonical |
| `club_affiliations` | League associations per club (unique `club_id + source_name`) |
| `coach_discoveries` | Primary coach read model + Path A extensions (`coach_id` FK → `coaches.id`, `person_hash`, `phone`, `first_seen_at`, `last_seen_at`) |
| `club_events` | **Deferred drop** — `/api/events/search` still reads from it; drops in the PR that rewires the route to `events` + `event_teams` |

### Path A new tables (18 total)

| File | Tables | Purpose |
|---|---|---|
| `coaches.ts` | `coaches`, `coach_career_history`, `coach_movement_events`, `coach_scrape_snapshots`, `coach_effectiveness` | Coaches master; `person_hash` dedup; career + movement tracking |
| `colleges.ts` | `colleges`, `college_coaches`, `college_roster_history` | NCAA/NAIA/NJCAA dataset |
| `events.ts` | `events`, `event_teams` | Tournaments, leagues, showcases |
| `matches.ts` | `matches`, `club_results` | Game records + aggregated results |
| `rosters-and-tryouts.ts` | `roster_diffs`, `tryouts` | Roster change log + tryout announcements |
| `clubs-extended.ts` | `club_roster_snapshots`, `club_site_changes` | Point-in-time roster diffing + website change detection |
| `scrape-health.ts` | `scrape_run_logs`, `scrape_health` | Per-run telemetry + rolling health rollups |

### Key rules

- `coach_discoveries.coach_id` is the authoritative pointer. All coach reads in the API go through `coach_discoveries`.
- `coaches.person_hash` = `sha256(normalize(name) + '|' + lower(email))` if email exists, else `sha256(normalize(name) + '|no-email|' + club_id)`. Dedup collisions collapse multiple discoveries into one master row.
- `coaches.manually_merged = true` — operator-curated rows; backfill NEVER overwrites these.
- `matches_natural_key_uq` is a partial unique index with `COALESCE(...)` on `match_date`/`age_group`/`gender` because Postgres NULL-distinct breaks plain `unique()`. Don't "fix" this.
- `scrape_run_logs.records_touched` is a STORED generated column. Drizzle 0.45.1's `generatedAlwaysAs` is single-arg only — no `{mode: 'stored'}`. Postgres doesn't support VIRTUAL pre-17 anyway.
- `scrape_run_logs.failure_kind` enum (`timeout | network | parse_error | zero_results | unknown`) is synchronized across three places: the Postgres CHECK constraint, Python `run.py:FailureKind`, and Python `scrape_run_logger.py:FailureKind`. Changes require updating all three. The parity test `test_run_py_failure_kind_matches_logger_and_db_enum` enforces this.

---

## Python Scraper

### Key invariants

- All HTTP calls wrapped in `utils/retry.py::retry_with_backoff(fn, max_retries=3, base_delay=2s)`. Retries on `ConnectionError`, `Timeout`, 5xx, Playwright nav errors.
- `FailureKind` classification in `run.py` decides how a scrape failure is logged to `scrape_run_logs`. Markers are compared against `.lower()`-ed strings — all marker literals must be lowercase.
- `scrape_run_logger._conn()` is a module-level lazy singleton. If the DB is unreachable, `_CONN_FAILED` latches and every subsequent call no-ops. Scraping continues regardless. This means PR merges that add logger calls can safely ship before the `db push` runs on Replit.
- `scrape_run_logger.close_connection()` is called from `run.py`'s `finally` block to release the connection on exit.

### Common commands (on Replit)

```bash
cd scraper

python3 run.py                   # all 115 scrapeable leagues
python3 run.py --tier 1          # Tier 1 national elite (7 leagues)
python3 run.py --scope state     # 54 USYS state associations
python3 run.py --league "ECNL"   # name filter
python3 run.py --dry-run         # no writes
python3 run.py --list            # inventory dump
```

---

## TypeScript / Node

- pnpm workspaces, Node 24, TS 5.9
- Drizzle 0.45.1, Postgres, `drizzle-orm/node-postgres`
- Express 5, Zod v4, drizzle-zod
- OpenAPI 3.1 → Orval → Zod types
- `@workspace/db`, `@workspace/api-spec`, `@workspace/api-zod`, `@workspace/api-server`, `@workspace/scripts`

### Key commands

```bash
pnpm typecheck                                         # all packages
pnpm --filter @workspace/db run test:schema            # Drizzle schema smoke test
pnpm --filter @workspace/db run push                   # Replit only — apply schema
pnpm --filter @workspace/scripts run backfill-coaches -- --dry-run  # Replit only
pnpm --filter @workspace/api-spec run codegen          # regen Zod from OpenAPI
pnpm --filter @workspace/api-server run dev            # start API on 8080
```

---

## REST API

Base: `/api` on port 8080. Pagination: `?page=1&page_size=20` (max 100).

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/healthz` | Health |
| GET | `/api/clubs` | Paginated list |
| GET | `/api/clubs/search` | Advanced search |
| GET | `/api/clubs/:id` | One club + affiliations + aliases |
| GET | `/api/clubs/:id/related` | Related via shared affiliations |
| GET | `/api/clubs/:id/staff` | Coach discoveries for this club |
| GET | `/api/events/search` | Events with club/league/age/gender/season/date filters |
| GET | `/api/coaches/search` | Coaches: club, name, title, min confidence (ILIKE inputs escaped) |
| GET | `/api/leagues` | All leagues |
| GET | `/api/leagues/:id/clubs` | Clubs per league |
| GET | `/api/search` | Fuzzy club name |
| GET | `/api/analytics/duplicates` | Near-dup clusters |
| GET | `/api/analytics/coverage` | Per-state / per-league counts |
| GET | `/api/analytics/overlap` | Clubs in 2+ leagues |

---

## Operational Runbook (Replit)

After pulling master following a schema PR:

```bash
pnpm install                                                          # refresh workspace symlinks
pnpm --filter @workspace/db run push                                  # apply schema changes
pnpm --filter @workspace/scripts run backfill-coaches -- --dry-run    # preview
pnpm --filter @workspace/scripts run backfill-coaches                 # commit
psql "$DATABASE_URL" -c "SELECT count(*) FROM coach_discoveries WHERE coach_id IS NULL;"  # should be 0
```

Do NOT paste shell comments (lines starting with `#`) or em-dashes as CLI args — the shell will treat them as arguments and `drizzle-kit push` will reject them.

---

## Path A Status (as of April 2026)

- ✅ PR #2 — Schema: 18 new tables + 13 extension columns + tests + docs
- ✅ PR #3 — `/api/coaches/search` reads from `coach_discoveries`, escapes ILIKE inputs
- ✅ PR #6 — Backfill script (rescued from stacking-base accident in original PRs #4/#5)
- ✅ Replit backfill run: 2,647 discoveries → 2,603 coaches inserted, 44-row collision rate, `coach_id IS NULL` count = 0
- ✅ `club_coaches` dropped (this PR) — absorb step returned 0 rows on Replit, API route was rewired in PR #3
- ⏳ **Next:** rewire `/api/events/search` to read from `events` + `event_teams`, then drop `club_events` in the same PR

See `docs/path-a-data-model.md` for the full domain-by-domain spec + changelog.

---

## Review Gates

For any non-trivial change, before merge run:

1. **Code review** — line-level correctness, import resolution, lockfile drift
2. **Tech arch review** — contracts, deploy-ordering safety, idempotency, coupling
3. **Feature review** — does it deliver the promised capability end-to-end

Run the three in parallel. See prior sessions for the stacked-PR rescue pattern as a worked example.
