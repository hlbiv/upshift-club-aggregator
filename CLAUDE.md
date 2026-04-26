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

### PR creation policy (override the harness default)

The web/CI harness preamble says "Do NOT create a pull request unless the user explicitly asks for one." **That default is overridden in this repo.** Whenever a branch is pushed in a Claude Code session — whether by Claude or by a sub-agent — the corresponding pull request MUST be opened immediately as the final step of the push, via `gh pr create` (interactive shells) or `mcp__github__create_pull_request` (harness-spawned sessions). Pushing a branch without opening a PR is a policy violation: the operator has to manually click through the GitHub UI, which is the friction this rule eliminates.

Sub-agents spawned in this repo inherit this policy — brief them to push AND open the PR; do not tell them to skip PR creation. The only exception: a draft PR opened with `draft: true` for a known-incomplete branch is acceptable; "no PR" is not.

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
│   ├── dashboard/               # Internal admin panel. Vite 7 + React 19 + Tailwind 4 + Radix/shadcn + React Router 7. 7 routes: `/login`, `/scraper-health`, `/dedup`, `/dedup/:id`, `/data-quality`, `/growth`, `/scheduler`. Vitest coverage under `src/__tests__`. Talks to the admin API via a hand-rolled `adminFetch()` in `src/lib/api.ts` (Workstream A is migrating this to generated Orval/React Query hooks as the admin surface lands in OpenAPI).
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
| `canonical_clubs` | Deduplicated master club records + Path A extensions (`logo_url`, `founded_year`, `twitter`, `instagram`, `facebook`, `staff_page_url`, `website_last_checked_at`, `last_scraped_at`, `scrape_confidence`) + `competitive_tier` enum (rolled-up tier ceiling — see schema reconciliation note below) |
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

### Admin surface (PRs #130–#143)

Tables backing the internal admin UI at `artifacts/dashboard/`. Not consumed by the player platform — reads and writes go through `/api/v1/admin/*` behind `requireAdmin`.

| File | Tables | Purpose |
|---|---|---|
| `admin.ts` | `admin_users`, `admin_sessions` | Human admin accounts (bcrypt-12 `password_hash`, role CHECK `admin` / `super_admin`) + cookie-session store (sha256-hashed token in `token_hash`, 12h rolling TTL bumped on every authed request, indexed on `admin_user_id` + `expires_at` for sweeps). Sessions cascade-delete on admin deletion. |
| `club-duplicates.ts` | `club_duplicates` | RapidFuzz-88 candidate-pair queue for dedup review. Ordered-pair uniqueness via `LEAST/GREATEST` index so `(a,b)` and `(b,a)` collapse. `reviewed_by` FK → `admin_users.id`, `reviewed_at` timestamp, status CHECK `pending` / `merged` / `rejected`. Populated by `scraper/dedup/club_dedup.py --persist`. |
| `scheduler-jobs.ts` | `scheduler_jobs` | Queue backing the admin "Run now" scheduler button. `job_key` is a hard-coded allow-list (see `admin/scheduler.ts`: `nightly_tier1`, `weekly_state`, `hourly_linker`). Status CHECK `pending` / `running` / `success` / `failed` / `canceled`. `requested_by` FK is `ON DELETE SET NULL` so audit rows survive admin deletion. Worker runs in-process inside the api-server. |
| `rosters-and-tryouts.ts` | `roster_quality_flags` | Per-snapshot data-quality flags on `club_roster_snapshots` (FK `snapshot_id`, `ON DELETE CASCADE`). `flag_type` is text + CHECK (NOT pgEnum) so new flag types are a CHECK-list extension. Per-flag-type metadata contract lives in the schema-file docstring: `nav_leaked_name: { leaked_strings: string[], snapshot_roster_size: number }`. Unique on `(snapshot_id, flag_type)` so the detector is idempotent. `resolved_by` FK → `admin_users.id` (`ON DELETE SET NULL`). **Snapshot-supersession rule:** flags on earlier snapshots STAY flagged when a later snapshot replaces them — they are explicitly resolved by an operator, not auto-expired. Phase 2 adds a scraper-side detector; Phase 3+ adds the resolve-flag UI. |
| `coach-quality-flags.ts` | `coach_quality_flags` | Canary / audit-trail table for the coach-pollution remediation effort. Per-discovery data-quality flags on `coach_discoveries` (FK `discovery_id`, `ON DELETE CASCADE`). `flag_type` is text + CHECK (`looks_like_name_reject` / `role_label_as_name` / `corrupt_email` / `nav_leaked`). Per-flag-type metadata contract in schema-file docstring: `looks_like_name_reject: { reject_reason, raw_name }`, `role_label_as_name: { raw_name, matched_pattern }`, `corrupt_email: { raw_email, corruption_kind }`, `nav_leaked: { leaked_strings, raw_name }`. Unique on `(discovery_id, flag_type)` so the shared guard / purge script is idempotent. Index on `flagged_at`. Optional `resolution_note` captures operator / purge-script context. `resolved_by` FK → `admin_users.id` (`ON DELETE SET NULL`). Written by upcoming PR 1 (shared guard) + PR 2 (purge script audit rows); PR 3 (this one) ships the table + admin list/resolve API. |

**Auth model.** `requireAdmin` (at `artifacts/api-server/src/middlewares/requireAdmin.ts`) accepts two credential paths, checked in order:
1. **Session cookie** (primary). Cookie name: `upshift_admin_sid` (httpOnly, secure, sameSite=lax). Raw token shown once at login; only the sha256 hash is stored in `admin_sessions.token_hash`.
2. **`X-API-Key`** (M2M fallback). The `api_keys` row must carry the `admin` scope in `api_keys.scopes`. If an API-key header is present but invalid, the middleware 401s without falling through to the cookie.

If both paths fail the response is a generic `{error: "unauthorized"}`; the specific reason (`no-credentials`, `apikey-notfound`, `apikey-missing-scope`, `session-notfound`, `session-user-missing`, `session-role-invalid`) is logged server-side via `console.warn`. `requireSuperAdmin` layers on top for mutation routes that need the `super_admin` role.

**Bootstrap.** Mint the first admin with `pnpm --filter @workspace/scripts run create-admin-user -- --email you@example.com --role admin` on Replit. The CLI hashes the password with bcryptjs (12 rounds, wire-compatible with the Player repo) and writes to `admin_users`. Password source precedence: `--password` flag → `ADMIN_PASSWORD` env var → TTY prompt (echo NOT hidden — prefer the env-var path for non-interactive flows). Super-admin bootstrap: pass `--role super_admin` on the first invocation.

### Schema reconciliation (table-name aliases in older briefs)

Older planning docs / briefs sometimes reference `organizations` and `regional_leagues`. Those tables don't exist in this repo. The live names are:

- **`canonical_clubs`** — the "orgs" / clubs table. Carries the rolled-up `competitive_tier` enum (`recreational | recreational_plus | competitive | elite | academy`, NOT NULL DEFAULT `competitive`) — the single value downstream code reads when it just wants to ask "what tier is this club?". Per-program tier granularity still lives in `leagues_master.tier_label`. Backfill via `pnpm --filter @workspace/scripts run backfill-competitive-tier`. See migration `0005_add_competitive_tier.sql`.
- **`leagues_master`** — the "regional_leagues" / league directory. Tier signal lives in `tier_label` + `tier_numeric` (lower = more elite); `league_family` distinguishes academy-pathway families (`MLS NEXT`, `NWSL Academy`, `USL Academy`) from same-tier non-academy leagues (`Girls Academy`, `Elite 64`).
- **`club_affiliations`** — the M2M between them. FKs are `club_id` and `league_id` (NOT `organization_id` / `regional_league_id`). `league_id` was added later; legacy rows fall back to matching on `source_name = league_name` (see `lib/db/src/backfill-affiliations-league-id.ts`).

When you see "organizations" in a plan, translate to `canonical_clubs`. When you see "regional_leagues", translate to `leagues_master`. When in doubt, the source of truth is `lib/db/src/schema/index.ts`.

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

**`@hlbiv/api-zod` requires a build step.** The `lib/api-zod/dist/` folder is not checked in. After editing any source under `lib/api-zod/src/`, run `pnpm --filter @hlbiv/api-zod run build` before consumers can import the new exports. Root `pnpm typecheck` handles this automatically via project references; per-package `tsc --noEmit` does not and will surface TS6305 errors.

---

## REST API

Base: `/api` on port 8080. Pagination: `?page=1&page_size=20` (max 100). Admin endpoints (`/api/v1/admin/*`) are documented in `lib/api-spec/openapi.yaml` alongside the public routes — new dashboard pages should prefer the Orval-generated React Query hooks from `@workspace/api-client-react` over the hand-rolled `adminFetch()` helper (`ScraperHealth.tsx` is the POC; other pages will migrate one at a time).

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

### `/api/v1/admin/*` — admin surface

Mounted under a separate router family (`artifacts/api-server/src/routes/admin/index.ts`) with its own auth stack. Two top-level routers:

- **`unauthAdminRouter`** — only `POST /auth/login`. Must live outside `requireAdmin` since it IS the auth entry point. Carries its own 10/min rate limiter.
- **`authedAdminRouter`** — everything else, mounted behind `requireAdmin` + the 120/min read-tier limiter; mutation routes layer an additional 30/min mutation limiter.

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/admin/auth/login` | Email + password → sets `upshift_admin_sid` cookie |
| POST | `/api/v1/admin/auth/logout` | Invalidates the session row; clears the cookie |
| GET | `/api/v1/admin/me` | Current admin (role + email); used by `ProtectedRoute` |
| GET | `/api/v1/admin/scrape-runs` | Paginated scraper telemetry |
| GET | `/api/v1/admin/scrape-runs/:id` | Single run detail |
| GET | `/api/v1/admin/scrape-health` | Rolling health rollups across all entities |
| GET | `/api/v1/admin/scrape-health/:entity_type/:entity_id` | Per-entity health |
| GET | `/api/v1/admin/dedup/clubs` | Paginated `club_duplicates` pending-review queue |
| GET | `/api/v1/admin/dedup/clubs/:id` | One pair + both snapshots |
| POST | `/api/v1/admin/dedup/clubs/:id/merge` | Mark pair `merged` + trigger merger |
| POST | `/api/v1/admin/dedup/clubs/:id/reject` | Mark pair `rejected` |
| POST | `/api/v1/admin/data-quality/ga-premier-orphans` | Scan / delete nav-token orphan rows in `club_roster_snapshots` (dry-run by default) |
| GET | `/api/v1/admin/data-quality/empty-staff-pages` | Clubs with `staff_page_url` set but zero coach discoveries in window |
| GET | `/api/v1/admin/data-quality/stale-scrapes` | `scrape_health` rows whose `last_scraped_at` is stale or NULL |
| GET | `/api/v1/admin/data-quality/nav-leaked-names` | Roster snapshots flagged as containing nav-menu strings instead of player names. Joins `roster_quality_flags` → `club_roster_snapshots` → `canonical_clubs` → `admin_users`. Extracts `leaked_strings` + `snapshot_roster_size` out of the flag's jsonb `metadata` into typed response fields; also returns `resolutionReason` (`resolved` / `dismissed` / null). The `state` query param is tri-state + escape hatch — `open` (default), `resolved`, `dismissed`, `all` — and replaces the prior boolean `include_resolved` param. Phase 2 populates the table via `scraper/nav_leaked_names_detector.py` (`python3 run.py --source nav-leaked-names-detect`). |
| PATCH | `/api/v1/admin/data-quality/roster-quality-flags/:id/resolve` | Operator triage: stamps `resolved_at = NOW()`, `resolved_by = <admin user id>` (NULL for API-key callers — same pattern as the dedup PATCH endpoints), and `resolution_reason = <reason>` on a `roster_quality_flags` row. Required body `{ reason: "resolved" \| "dismissed" }` — "resolved" = legitimate leak cleaned up out of band; "dismissed" = false positive. Snapshot rows are never mutated. Returns 204 on first resolve, 400 if the body is missing/invalid or the flag is already resolved (either reason — second attempts always 400), 404 if the id is unknown. |
| GET | `/api/v1/admin/data-quality/coach-quality-flags` | Canary panel for the coach-pollution remediation. Paginated list of `coach_quality_flags` rows joined to `coach_discoveries` (for `coachName` / `coachEmail`), the discovery's `canonical_clubs` resolution (for `clubDisplayName` / `clubId`), and `admin_users` (for `resolvedByEmail`). Query params: `flag_type?` (enum of the 4 CHECK values), `resolved?` (tri-state bool — omit for both), `page`, `page_size` (max 100). `metadata` returned as raw jsonb (typed-column promotion deferred until the pollution investigation settles a stable contract). NOTE: PATCH resolve currently uses empty-body; will be harmonized with the `{reason}` pattern in a follow-up PR. |
| PATCH | `/api/v1/admin/data-quality/coach-quality-flags/:id/resolve` | Operator triage: stamps `resolved_at = NOW()` and `resolved_by = <admin user id>` on a `coach_quality_flags` row (NULL for API-key callers — same pattern as the dedup PATCH endpoints). Empty body. Returns 204 on first resolve, 400 if already resolved, 404 if the id is unknown. NOTE: follow-up PR will harmonize this with the `{reason: "resolved" \| "dismissed"}` pattern established by `roster-quality-flags/:id/resolve`. |
| GET | `/api/v1/admin/growth/scraped-counts` | Counts of clubs / coaches / events / roster snapshots / matches added since a given ISO timestamp |
| GET | `/api/v1/admin/growth/coverage-trend` | Day-bucketed scrape-run counts + records touched over a windowed range |
| GET | `/api/v1/admin/scraper-schedules` | All known schedules with metadata (`description`, `cronExpression`) + recent runs inlined. Source of truth is `JOB_METADATA` in `admin/scheduler.ts` — the dashboard Scheduler page renders dynamically from this payload. Adding a new allow-listed jobKey server-side makes it appear in the UI with no code change. |
| GET | `/api/v1/admin/scraper-schedules/:jobKey/runs` | Last N `scheduler_jobs` rows for a single jobKey (legacy — subsumed by the combined list endpoint above; kept for scripts/smoke tests). |
| POST | `/api/v1/admin/scraper-schedules/:jobKey/run` | Enqueue a "Run now" (**super_admin gated**, jobKey allow-listed to `nightly_tier1` / `weekly_state` / `hourly_linker`) |
| GET | `/api/v1/admin/scheduler-jobs/:id` | Single scheduler-job row |

Planning-doc pointer: the authoritative specs for the admin UI and the admin API contract live in the sibling repo at `upshift-player-platform/docs/planning/upshift-data-admin-ui.md` and `upshift-player-platform/docs/planning/upshift-data-admin-api-contract.md`. Consult those before adding new admin routes or reshaping the dashboard layout.

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

### Admin schema / first admin user (on Replit)

Admin-surface schema changes (touching `admin_users`, `admin_sessions`, `club_duplicates`, `scheduler_jobs`) require the same push-then-seed rhythm as the rest of the Path A tables, plus a one-time bootstrap of a real admin before the dashboard can be used:

```bash
pnpm install
pnpm --filter @workspace/db run push
# Mint the first admin. --role defaults to "admin"; pass super_admin for the
# initial operator so "Run now" on the scheduler page is accessible.
pnpm --filter @workspace/scripts run create-admin-user -- \
    --email you@example.com --role super_admin
# Password source (first match wins): --password flag, ADMIN_PASSWORD env
# var, or TTY prompt (echo NOT hidden — prefer env-var for CI/non-interactive).
```

Rotate / add more admins with repeated `create-admin-user` invocations. Email is unique (`admin_users_email_uq`); a collision fails with a clear message rather than overwriting.

### Purge polluted `coach_discoveries` (on Replit)

One-shot cleanup for rows flagged by `coach-pollution-detect` (writes `coach_quality_flags` entries of type `looks_like_name_reject`). Dry-run is the default; `--commit` actually deletes. The FK `coach_quality_flags.discovery_id ON DELETE CASCADE` drops the flag rows automatically — no manual cleanup needed. `coach_discoveries.coach_id ON DELETE SET NULL` means the `coaches` master table is preserved (orphan-coach sweep is deliberately out of scope).

```bash
# 0. Pre-flight — how many rows will this purge?
psql "$DATABASE_URL" -c "
  SELECT COUNT(DISTINCT discovery_id)
  FROM coach_quality_flags
  WHERE flag_type = 'looks_like_name_reject' AND resolved_at IS NULL;
"

# 1. Dry-run — writes JSONL audit artifact to /tmp, rolls back the txn.
pnpm --filter @workspace/scripts run purge-polluted-coach-discoveries

# 2. Commit — writes JSONL, deletes, verifies cascade, commits.
pnpm --filter @workspace/scripts run purge-polluted-coach-discoveries -- --commit

# 3. Post-check — discoveries should drop by the pre-flight count;
#    flag rows should hit 0.
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM coach_discoveries;"
psql "$DATABASE_URL" -c "
  SELECT flag_type, COUNT(*) FROM coach_quality_flags GROUP BY flag_type;
"
```

Flags an operator has already marked `resolved_at IS NOT NULL` (via the admin canary UI) are skipped — operator triage wins over bulk purge. To purge a different flag type later, pass `--flag-type <name>` (the CHECK constraint on `coach_quality_flags.flag_type` enumerates the valid values: `looks_like_name_reject`, `role_label_as_name`, `corrupt_email`, `nav_leaked`, `ui_fragment_as_name`). Override the audit dir with `--audit-dir /path/to/dir` (default `/tmp`). The JSONL artifact is the source of truth for reconstruction — keep it even after a successful purge.

### Sweep orphan `coaches` master rows (on Replit)

Follow-up pass to the purge script above. After the April 2026 purge, the `coach_discoveries.coach_id ON DELETE SET NULL` FK left ~200 `coaches` master rows with zero referencing discoveries — unreferenced husks that have no remaining purpose. This script deletes them. **Never touches `manually_merged = true` rows** (SELECT filter + redundant predicate on the DELETE statement itself, so a concurrent flip cannot mass-delete curated rows).

The cascade FKs on `coach_career_history`, `coach_movement_events`, and `coach_effectiveness` drop children automatically. Post-cascade residual counts are verified; a non-zero count aborts the transaction.

```bash
# 0. Pre-flight — how many orphan masters will this sweep?
psql "$DATABASE_URL" -c "
  SELECT COUNT(*) FROM coaches c
  WHERE c.manually_merged = false
    AND NOT EXISTS (
      SELECT 1 FROM coach_discoveries cd WHERE cd.coach_id = c.id
    );
"

# 1. Dry-run — writes JSONL audit artifact to /tmp, rolls back the txn.
pnpm --filter @workspace/scripts run sweep-orphan-coaches

# 2. Commit — writes JSONL, deletes, verifies cascade, commits.
pnpm --filter @workspace/scripts run sweep-orphan-coaches -- --commit

# 3. Post-check — orphan count should be 0.
psql "$DATABASE_URL" -c "
  SELECT COUNT(*) FROM coaches c
  WHERE c.manually_merged = false
    AND NOT EXISTS (
      SELECT 1 FROM coach_discoveries cd WHERE cd.coach_id = c.id
    );
"
```

Override the audit dir with `--audit-dir /path/to/dir` (default `/tmp`). The JSONL bundles each orphan coach with its cascade-tied `coach_career_history`, `coach_movement_events`, and `coach_effectiveness` rows so a full rebuild is possible from the artifact alone.

The DELETE row-count check is **strict equality** as of PR 13: the deleted count must equal `targetIds.length` exactly. A short or long count rolls the transaction back. This catches concurrent `manually_merged` flips between the SELECT and DELETE that would have left the audit JSONL out of sync with the actual deletions. A `--relink` flag is available — see the next section.

### Coach person_hash rehash — RESEARCH DRY-RUN ONLY (cutover deferred)

**Auto-merge is locked.** PR 13 originally proposed dropping `clubId` from
the email-less `person_hash` to auto-merge same-name coaches across clubs.
That fix shifts the bug rather than removing it: in youth soccer, common
names are common and email capture is spotty, so a name-only hash
collapses real strangers (two different "John Smith" coaches at two
different clubs) into one row. The proper fix is a candidate-pair review
queue — see `docs/coach-merge-candidate-queue.md`.

`--commit --allow-rehash` now hard-exits with an error. The dry-run path
remains available for cardinality analysis only.

**Dry-run procedure (research only):**

```bash
pnpm --filter @workspace/scripts run backfill-coaches-master -- \
    --dry-run --allow-rehash
# Audit at /tmp/coach-rehash-cutover-<ts>.jsonl lists every pair the
# (unsafe) auto-merge would have collapsed. Use this to decide how
# urgent the candidate-queue work is — e.g. how many real cross-club
# matches vs. how many same-name strangers.
```

**Do NOT:**
- Run with `--commit --allow-rehash` (locked at the script entry).
- Strip the lock without first shipping the candidate-queue infrastructure.

When the candidate-queue infrastructure lands, the rehash flag can be
repurposed to write candidate rows instead of merging. The lock comes
out as part of that PR.

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
- ✅ **Scraping-infrastructure review (April 2026)** — 20 PRs (#235–#257, minus closed-redundant #249) closed out 38 review findings across writers, extractors, detectors, schema, and tests. Highlights:
  - **Writer transaction safety** (#235): per-row `SAVEPOINT`/`ROLLBACK TO SAVEPOINT` in `matches_writer` + `roster_snapshot_writer`; prior-snapshot lookup is now fail-loud.
  - **Schema/index COALESCE alignment**: `club_roster_snapshots` (#238) and `tryouts` (#240, also added `season` column). Both required Replit `pnpm push` post-merge — confirm those have run before relying on the new dedup behavior. Migration files at `scripts/src/migrations/0006_*.sql`.
  - **`FailureKind` unified** (#242): single canonical enum in `scrape_run_logger.py`. The duplicate enum in `run.py` is gone; `from run import FailureKind` is the same object as `from scrape_run_logger import FailureKind` (locked by `test_run_py_failure_kind_is_logger_failure_kind`).
  - **Coach-pipeline rehash deferred** (#250): the auto-merge approach was caught as wrong in-session — a name-only hash collapses same-name strangers in youth soccer. `--commit --allow-rehash` is locked at the script entry. The `--dry-run --allow-rehash` path stays open for cardinality analysis. Proper fix lives at `docs/coach-merge-candidate-queue.md` (mirror the `club_duplicates` candidate-queue pattern).
  - **Structural guards** (#257): `scraper/tests/test_writer_savepoint_lint.py` fails CI if any writer in `scraper/ingest/` reintroduces `conn.rollback()` inside a row loop (12 pre-existing offenders are noqa-allowlisted as a TODO list — removing the marker re-enables enforcement). `scraper/tests/test_partial_index_writer_alignment.py` fails CI if `matches.ts`'s `uniqueIndex(...).on(...)` COALESCE expressions diverge from the consumer writer (currently scoped to `matches`; extension to `club_roster_snapshots` + `tryouts` is a follow-up).
- ⏳ **Next:** remaining Path A tables (additional roster sources beyond SincSports, richer tryouts seed list beyond WordPress clubs); coach-merge candidate-queue infrastructure when prioritized (see `docs/coach-merge-candidate-queue.md`).

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

The analogous `link-canonical-schools` job (resolves `hs_rosters.school_id` via `canonical_schools` + `school_aliases`) is scheduled via `.replit` as `nightly-canonical-school-linker` at `30 3 * * *` UTC; it runs nightly and picks up whatever the operator-invoked MaxPreps scraper wrote since the last pass.

The Phase 2 nav-leaked-names detector (`scraper/nav_leaked_names_detector.py`) is scheduled via `.replit` as `nightly-nav-leaked-names-detect` at `35 3 * * *` UTC (5 minutes after the canonical-school linker). It scans `club_roster_snapshots` grouped by `(club_name_raw, season, age_group, gender)` for `player_name` values whose case-folded full string exactly matches one of the 39 nav tokens (e.g. "Home", "Contact", "Sitemap"), and upserts one `roster_quality_flags` row of type `nav_leaked_name` per offending group. Idempotent via `roster_quality_flags_snapshot_type_uq` + a `metadata IS DISTINCT FROM` no-op guard; `resolved_at`/`resolved_by` are preserved across re-runs so an operator's prior triage is not undone by the nightly pass.

See `docs/path-a-data-model.md` for the full domain-by-domain spec + changelog.

---

## Review Gates

For any non-trivial change, before merge run:

1. **Code review** — line-level correctness, import resolution, lockfile drift
2. **Tech arch review** — contracts, deploy-ordering safety, idempotency, coupling
3. **Feature review** — does it deliver the promised capability end-to-end

Run the three in parallel. See prior sessions for the stacked-PR rescue pattern as a worked example.
