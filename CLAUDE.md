# CLAUDE.md — Upshift Data

Persistent context for Claude Code. Read at the start of every session.

---

## What This Repo Is

Upshift Data is the reference-data backend for Upshift soccer club/coach intelligence:

- **Python scraper toolkit** (`scraper/`) — ~52 Python modules in `scraper/extractors/` (~30–35 active league extractors plus utilities); covers a directory of 127 leagues tracked in `leagues_master`
- **PostgreSQL graph** (`lib/db/`) — canonical clubs, coaches master, events, matches, rosters, tryouts, scrape health
- **REST API** (`artifacts/api-server/`) — Express 5 on port 8080, consumed by `upshift-studio`

**Relationship to sibling repo:** `upshift-studio` owns user/claim/payment data. This repo owns everything scraped (orgs, coaches, events, rosters, results). Player platform calls this API — never the DB directly.

---

## Workflow

- Local: `pnpm typecheck`, `pnpm test`, `pnpm install` are fine on macOS.
- Never run DB operations (`pnpm --filter @workspace/db run push`, seed, migrations) locally — always on Replit.
- Never run scrapers locally — always on Replit.
- GitHub tooling depends on how Claude Code is invoked. In an interactive local shell, `gh` CLI is authenticated as `hlbiv` — use it for PRs. In a Claude Code session spawned by the web/CI harness, `gh` is NOT available; use the `mcp__github__*` tools (scoped to `hlbiv/upshift-data`) for every PR, issue, comment, and CI check.
- **pnpm package quarantine.** `pnpm-workspace.yaml` sets `minimumReleaseAge: 1440` (1-day quarantine against supply-chain attacks). Packages published in the last 24h will fail to install. The `minimumReleaseAgeExclude` allowlist covers `@replit/*` + `stripe-replit-sync`. Do not disable the setting to unblock an install — add a narrowly-scoped allowlist entry and remove it after 24h.
- **Catalog-pinned frontend deps.** React 19.1.0, Vite 7, Tailwind 4, React Query 5, Zod, TS types, and the Replit Vite plugins are all defined once in the `catalog:` block of `pnpm-workspace.yaml`. Every `artifacts/*` app references them via `"react": "catalog:"` — don't hard-pin a version in a package manifest, or the workspace will drift.

### Commit & Push Ritual

Every "commit and push" is composite:

1. `git fetch origin master && git merge origin/master --no-edit`
2. Commit
3. Push
4. Open or update the PR — `gh pr create` / `gh pr edit` locally, or `mcp__github__create_pull_request` / `mcp__github__update_pull_request` in a harness-spawned session.

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
│   ├── canonical_club_linker.py # 4-pass raw-team-name → canonical_clubs.id resolver
│   ├── extractors/              # Per-site custom extractors (URL-pattern matched)
│   └── tests/                   # Pytest suite
├── lib/
│   ├── db/                      # Drizzle schema, seed script, DB client
│   │   └── src/schema/
│   │       ├── index.ts                # legacy tables + canonical_clubs/coach_discoveries extensions
│   │       ├── api-keys.ts             # api_keys (M2M auth)
│   │       ├── coaches.ts              # coaches master + career/movement/snapshot/effectiveness
│   │       ├── colleges.ts             # colleges + college_coaches + college_roster_history
│   │       ├── events.ts               # events + event_teams
│   │       ├── matches.ts              # matches + club_results
│   │       ├── rosters-and-tryouts.ts  # roster_diffs + tryouts
│   │       ├── clubs-extended.ts       # club_roster_snapshots + club_site_changes
│   │       ├── scrape-health.ts        # scrape_run_logs + scrape_health + raw_html_archive
│   │       ├── commitments.ts          # college commitment announcements (TopDrawerSoccer)
│   │       ├── player-id-selections.ts # US Club Soccer iD pool / Training Center honors
│   │       ├── ynt.ts                  # US Soccer Youth National Team call-ups
│   │       ├── hs.ts                   # High-school rosters (MaxPreps)
│   │       └── odp.ts                  # Olympic Development Program state rosters
│   ├── api-spec/                # OpenAPI 3.1 YAML
│   ├── api-zod/                 # Orval-generated Zod validators + TS types
│   ├── api-client-fetch/        # Published `@hlbiv/api-client-fetch` — native fetch SDK for external consumers
│   └── api-client-react/        # `@workspace/api-client-react` — React Query hooks used by artifacts/dashboard
├── artifacts/
│   ├── api-server/src/routes/   # Express routers (clubs, events, coaches, leagues, analytics, search)
│   ├── dashboard/               # Vite 7 + React 19 + shadcn/ui + wouter. Currently renders `/api/analytics/summary` as a domain-status page. Consumes `@workspace/api-client-react`.
│   └── mockup-sandbox/          # UI mockup staging area (shadcn/ui); previews via `mockupPreviewPlugin.ts`
└── scripts/
    └── src/backfill-coaches-master.ts  # Idempotent coaches master backfill
```

---

## Database Model (Path A)

31 tables across the domains below. Schema pushed with `pnpm --filter @workspace/db run push` on Replit.

### Legacy / core (in `schema/index.ts`)

| Table | Purpose |
|---|---|
| `leagues_master` | League directory inventory (127 rows) |
| `league_sources` | Official scrape source registry |
| `canonical_clubs` | Deduplicated master club records + Path A extensions (`logo_url`, `founded_year`, `twitter`, `instagram`, `facebook`, `staff_page_url`, `website_last_checked_at`, `last_scraped_at`, `scrape_confidence`) |
| `club_aliases` | Raw scraped name variants → canonical |
| `club_affiliations` | League associations per club (unique `club_id + source_name`) |
| `coach_discoveries` | Primary coach read model + Path A extensions (`coach_id` FK → `coaches.id`, `person_hash`, `phone`, `first_seen_at`, `last_seen_at`) |

### Path A + follow-on tables

| File | Tables | Purpose |
|---|---|---|
| `coaches.ts` | `coaches`, `coach_career_history`, `coach_movement_events`, `coach_scrape_snapshots`, `coach_effectiveness` | Coaches master; `person_hash` dedup; career + movement tracking |
| `colleges.ts` | `colleges`, `college_coaches`, `college_roster_history` | NCAA/NAIA/NJCAA dataset |
| `events.ts` | `events`, `event_teams` | Tournaments, leagues, showcases |
| `matches.ts` | `matches`, `club_results` | Game records + aggregated results |
| `rosters-and-tryouts.ts` | `roster_diffs`, `tryouts` | Roster change log (per-player events: `added`/`removed`/`jersey_changed`/`position_changed`) + tryout announcements. Both tables use the canonical-club-linker pattern — scrapers write `club_name_raw`; linker resolves `club_id`. |
| `clubs-extended.ts` | `club_roster_snapshots`, `club_site_changes` | Point-in-time roster diffing + website change detection. `club_roster_snapshots` uses the canonical-club-linker pattern (scrapers write `club_name_raw` + `source_url` + `snapshot_date`; `club_id` resolved by linker). |
| `scrape-health.ts` | `scrape_run_logs`, `scrape_health`, `raw_html_archive` | Per-run telemetry + rolling health rollups + raw HTML snapshot storage |
| `api-keys.ts` | `api_keys` | M2M credentials (sha256 hashes only). Written via `scripts/src/create-api-key.ts` / `revoke-api-key.ts`. |
| `commitments.ts` | `commitments` | College commitment announcements from TopDrawerSoccer. Club side uses canonical-club-linker pattern; college side tries exact `colleges` match at write time. Natural key: `(player_name, graduation_year, college_name_raw)`. |
| `player-id-selections.ts` | `player_id_selections` | US Club Soccer iD pool / Training Center honors. Natural key: `(player_name, selection_year, birth_year, gender, pool_tier)`. Deliberately NOT a roster table (per-player honor semantics differ). |
| `ynt.ts` | `ynt_call_ups` | US Soccer Youth National Team camp call-ups. Canonical-club-linker pattern. Natural key: `(player_name, age_group, gender, camp_event)`. |
| `hs.ts` | `hs_rosters` | High-school rosters (MaxPreps). No HS canonical-school linker yet — scrapers write `school_name_raw` + `school_state` only. |
| `odp.ts` | `odp_roster_entries` | Olympic Development Program state rosters. Canonical-club-linker pattern for any club the ODP site prints. Natural key: `(player_name, state, program_year, age_group, gender)`. |

### Key rules

- `coach_discoveries.coach_id` is the authoritative pointer. All coach reads in the API go through `coach_discoveries`.
- `coaches.person_hash` = `sha256(normalize(name) + '|' + lower(email))` if email exists, else `sha256(normalize(name) + '|no-email|' + club_id)`. Dedup collisions collapse multiple discoveries into one master row.
- `coaches.manually_merged = true` — operator-curated rows; backfill NEVER overwrites these.
- `matches_natural_key_uq` is a partial unique index with `COALESCE(...)` on `match_date`/`age_group`/`gender` because Postgres NULL-distinct breaks plain `unique()`. Don't "fix" this.
- **Canonical-club linker pattern (5 tables).** `event_teams.canonical_club_id`, `matches.home_club_id`, `matches.away_club_id`, `club_roster_snapshots.club_id`, `roster_diffs.club_id`, and `tryouts.club_id` are all NULL at scrape time. Scrapers MUST write the raw source name into the paired text column (`team_name_raw`, `home_team_name`/`away_team_name`, or `club_name_raw`) and leave the FK column NULL. The linker (`scraper/canonical_club_linker.py`, `python3 run.py --source link-canonical-clubs`) runs the 4-pass resolver and fills them in. Scrapers for rosters/tryouts in particular MUST NOT try to pre-resolve `club_id` — that used to be required and became the reason this pattern exists.
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

# Non-league scrapers (--source) and rollups (--rollup)
python3 run.py --source gotsport-matches --event-id 12345 \
    --season 2025-26 --league-name "ECNL Boys National"
python3 run.py --source sincsports-events                # all 14 SincSports seeds
python3 run.py --source sincsports-events --dry-run      # preview without DB writes
python3 run.py --source sincsports-events --tid GULFC    # single event
python3 run.py --source sincsports-rosters               # walk teams, upsert club_roster_snapshots + diffs
python3 run.py --source sincsports-rosters --tid GULFC --dry-run
python3 run.py --source tryouts-wordpress                # probe WordPress tryout pages
python3 run.py --source tryouts-wordpress --limit 5      # first 5 seed sites
python3 run.py --rollup club-results
```

### Matches + club_results pipeline (Domain 5)

Two-stage pipeline to populate Path A's `matches` and `club_results`:

1. **Scrape matches** — `python3 run.py --source gotsport-matches --event-id N` fetches a GotSport schedules page and upserts rows into `matches`. `home_club_id` / `away_club_id` stay NULL at scrape time; a linker job resolves them later. Writes through `scraper/ingest/matches_writer.py`, which targets the two partial unique indexes on `matches` via explicit `ON CONFLICT (cols) WHERE predicate` — Drizzle's `onConflictDoUpdate` API cannot emit the predicate, so the writer is intentionally hand-rolled SQL in psycopg2.
2. **Rollup** — `python3 run.py --rollup club-results` recomputes `club_results` from scratch inside a transaction (`DELETE` + `INSERT ... SELECT`). Full-recompute is idempotent and safe to re-run. Matches with NULL `home_club_id` or `away_club_id` are skipped — `club_results` will stay empty until the linker runs.

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

### Authentication (M2M)

Every `/api/*` route except `/api/healthz` requires a valid API key in either `X-API-Key: <key>` or `Authorization: Bearer <key>` — **when the `API_KEY_AUTH_ENABLED=true` env var is set**. Without the flag the middleware is skipped entirely and the server logs `[api-key-auth] DISABLED` on boot. This lets a fresh deploy mint a key before enforcement turns on (otherwise merging this PR would 401 every call until someone ran the create-key script). Middleware lives at `artifacts/api-server/src/middlewares/apiKeyAuth.ts` and is conditionally mounted in `app.ts` before the router. The `api_keys` table stores only sha256 hashes — plaintext is shown once at creation time via `scripts/src/create-api-key.ts`. Revoke via `scripts/src/revoke-api-key.ts --prefix <8-char>`. `last_used_at` is updated on every successful lookup inside `findApiKeyByHash` (revoked rows are filtered in SQL via `AND revoked_at IS NULL`, so a revoked key never gets its timestamp bumped). 401 bodies are a generic `{error: "unauthorized"}` for every failure mode; the specific reason (`missing` / `notfound`) is logged server-side via `console.warn` with ip + path + key prefix.

**Bootstrap (Replit, first deploy):**
1. Pull + `pnpm install` + `pnpm --filter @workspace/db run push` (creates `api_keys`).
2. Mint the first key:
   ```bash
   pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-studio prod"
   ```
3. Copy the plaintext into the caller's `UPSHIFT_DATA_API_KEY` env var.
4. Set `API_KEY_AUTH_ENABLED=true` in Replit Secrets.
5. Restart the API server — boot log prints `[api-key-auth] enabled`.

Rotating = create new → update env → revoke old. Helpers (`hashApiKey`, `generateApiKey`, `findApiKeyByHash`) are exported from `@workspace/db`.

### API-key auth cutover

One-time flip to require `X-API-Key` on every `/api/*` call. Run the steps in order on Replit; none of this should be executed locally.

1. **Mint the caller's key.** The CLI takes `--name` only; there is no `--scope` flag (scopes default to `[]` in the `api_keys` row and are reserved for future use).
   ```bash
   pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-studio"
   ```
   The script prints the plaintext key exactly once. Copy it now.

2. **Hand the plaintext to the sibling repo.** Set `UPSHIFT_DATA_API_KEY=<plaintext>` in the `upshift-studio` Replit Secrets. Also set `UPSHIFT_DATA_API_URL=<this-repo's-api-base>` if it isn't already wired. Redeploy / restart the player-platform server so the env is picked up.

3. **Enable enforcement on this repo.** In Replit Secrets set `API_KEY_AUTH_ENABLED=true`, then restart the api-server. Boot log should print `[api-key-auth] enabled` (the `DISABLED` line means the flag is still false / unset).

4. **Smoke-test the cutover.** From a shell with `UPSHIFT_DATA_API_KEY` exported (reuse the key minted in step 1):
   ```bash
   pnpm --filter @workspace/scripts run smoke-api-key
   ```
   The script probes `/api/healthz`, `/api/clubs`, `/api/coaches/search`, `/api/events/search` with and without the key and asserts 200 / 401. Non-zero exit = cutover is broken. Quick manual equivalent:
   ```bash
   curl -sS -o /dev/null -w "%{http_code}\n" \
     -H "X-API-Key: $UPSHIFT_DATA_API_KEY" \
     http://localhost:8080/api/clubs?page_size=1    # expect 200
   curl -sS -o /dev/null -w "%{http_code}\n" \
     http://localhost:8080/api/clubs?page_size=1    # expect 401
   ```

5. **Rollback** (both safe and reversible):
   - **Disable globally:** set `API_KEY_AUTH_ENABLED=false` and restart. Middleware is skipped on boot; behaves as pre-cutover. Use this if the sibling repo is 401-ing and you need to unblock traffic.
   - **Revoke a single key:** `pnpm --filter @workspace/scripts run revoke-api-key -- --prefix <first-8-chars-of-plaintext>` (CLI keys off the 8-char prefix, not an id). Soft-sets `revoked_at`; next request from that key 401s.

**Rotation after cutover:** mint new → update caller env → redeploy caller → revoke old prefix. Redeploying between mint and revoke avoids a window where the caller sends the old key and 401s.

The handoff doc for the sibling repo (fetch wrapper + error handling) lives at `docs/integrating-from-player-platform.md`.

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

### Events-route rewire (PR #8) — post-merge steps

```bash
pnpm install
pnpm --filter @workspace/db run push
# drizzle-kit will prompt: "Is club_events table created or renamed from another table?"
#   → answer No. Then: "about to drop table club_events"
#   → answer Yes (the table is empty per the April 2026 backfill verification).
psql "$DATABASE_URL" -c "\dt club_events"          # should return "Did not find any relation"
psql "$DATABASE_URL" -c "SELECT count(*) FROM events; SELECT count(*) FROM event_teams;"
curl -s 'http://localhost:8080/api/events/search?page=1&page_size=5' | jq '.total'
```

Empty counts + `total: 0` are expected until a scraper-wiring PR populates the new tables. The route returning 200 with an empty list is the success criterion for this PR.

---

## Path A Status (as of April 2026)

- ✅ PR #2 — Schema: 18 new tables + 13 extension columns + tests + docs
- ✅ PR #3 — `/api/coaches/search` reads from `coach_discoveries`, escapes ILIKE inputs
- ✅ PR #6 — Backfill script (rescued from stacking-base accident in original PRs #4/#5)
- ✅ Replit backfill run: 2,647 discoveries → 2,603 coaches inserted, 44-row collision rate, `coach_id IS NULL` count = 0
- ✅ `club_coaches` dropped — absorb step returned 0 rows on Replit, API route was rewired in PR #3
- ✅ `/api/events/search` rewired to `events` + `event_teams`; `club_events` dropped in the same PR
- ✅ PR for rosters + tryouts scrapers — `sincsports-rosters` and `tryouts-wordpress` run behind `--source` keys in `run.py`, write through `scraper/ingest/roster_snapshot_writer.py` + `tryouts_writer.py` with named `ON CONFLICT ON CONSTRAINT` upserts and per-player diff materialization. See `docs/rosters-and-tryouts-pipeline.md`.
- ⏳ **Next:** remaining Path A tables (additional roster sources beyond SincSports, richer tryouts seed list beyond WordPress clubs).

### Canonical-Club Linker

Event + match scrapers write `team_name_raw` / `home_team_name` / `away_team_name` and leave the `canonical_club_id` / `home_club_id` / `away_club_id` FKs NULL on purpose (keeps scraper code simple and surfaces parsing bugs instead of hiding them behind fuzzy matching). A separate linker job resolves those FKs.

**Run after every scrape** on Replit:

```bash
cd scraper && python3 run.py --source link-canonical-clubs
# optional smoke: --dry-run, --limit 100
```

4-pass resolver, each pass short-circuits on hit:
1. Exact alias match (`club_aliases.alias_name`)
2. Exact canonical match (`canonical_clubs.club_name_canonical`)
3. Fuzzy match via `rapidfuzz.fuzz.token_set_ratio >= 88`; on hit, writes a new `club_aliases` row so future runs hit pass #1
4. No match — leaves FK NULL, reports the top unmatched raw names

Idempotent — only touches rows where the FK is currently NULL.

**Downstream consumers depend on this running:** `/api/events/search?club_id=N` (SincSports events scraper in PR #11 writes NULL FKs by design), `matches` → `club_results` rollup. Neither works end-to-end without at least one linker pass.

See `docs/path-a-data-model.md` for the full domain-by-domain spec + changelog.

---

## Review Gates

For any non-trivial change, before merge run:

1. **Code review** — line-level correctness, import resolution, lockfile drift
2. **Tech arch review** — contracts, deploy-ordering safety, idempotency, coupling
3. **Feature review** — does it deliver the promised capability end-to-end

Run the three in parallel. See prior sessions for the stacked-PR rescue pattern as a worked example.
