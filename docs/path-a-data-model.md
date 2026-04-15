# Path A Data Model

**Status:** Approved. Ready for migration.
**Reviewed by:** Replit agent (2026-04-14)
**Goal:** The full 8-domain data model for `upshift-club-aggregator`, the scraped reference-data backend for both this repo's API and the `upshift-player-platform` product.

---

## Context

### Two repos, one product

- **`upshift-club-aggregator`** (this repo) — Python scraper toolkit + Postgres + Express API. Current state: 8 tables, 3,960 canonical clubs, 2,647 coach discoveries. Clean architecture, tier taxonomy, RapidFuzz dedup, FailureKind error reporting.
- **`upshift-player-platform`** — the consumer product (player/coach/recruiter app). Has its own scrapers (150+ TS files), its own reference-data tables (`organizations`, `schools`, `coach_profiles_shadow`, `events`, `games`, etc.). Scraper audit found 2 of 36 roster jobs have ever successfully touched records.

### The decision: Path A

**Aggregator absorbs the reference-data half of upshift-player-platform.**

- **Aggregator owns:** clubs, colleges, youth coaches, college coaches, events, matches, rosters, tryouts, scrape health — all scraped reference data.
- **upshift-player-platform keeps:** users, players (claim layer), claimed cards, highlights, performance_clients, messages, payments — all product/user data.
- **Integration:** upshift-player-platform reads reference data from aggregator API. Bridged via nullable FKs on the upshift side (e.g. `organizations.aggregator_canonical_club_id`).
- **Migration strategy:** strangler pattern, not big-bang. Pre-launch timing makes this safe.

---

## Conventions (locked)

1. **TS camelCase ↔ SQL snake_case.** Matches existing style (`clubNameCanonical` ↔ `club_name_canonical`).
2. **Timestamps:**
   - `scrapedAt` — event of record (when this row was scraped)
   - `firstSeenAt` / `lastSeenAt` — change detection
   - `createdAt` / `updatedAt` — only for records humans touch
3. **Polymorphic refs:** `(entity_type, entity_id)` pattern for cross-entity tables. No nullable-FK-per-type bloat. App-layer validation.
4. **Natural keys = unique constraints** on every scraped table, named, idempotent upserts.
5. **Check constraints for enums.** Avoid Postgres native enums (harder to migrate).
6. **`player_name` is text, never an FK.** Aggregator does not have player identity. Cross-roster attribution is a fuzzy derived job.
7. **Aggregator stops at "identified scraped fact."** Coach effectiveness, roster churn, movement events are derived tables, regenerated nightly.

---

## Entity graph

```
leagues_master (98)
       │
       ▼
canonical_clubs (3,960) ──┬── club_aliases
                          ├── club_affiliations
                          ├── club_roster_snapshots   (NEW) season-over-season
                          ├── club_site_changes       (NEW) site monitor diffs
                          ├── club_tryouts            (NEW) derived from site monitor
                          └── event_teams             (NEW) per-tournament participation

colleges (NEW) ──┬── college_coaches (NEW)
                 └── college_roster_history (NEW)

events (NEW) ── event_teams (NEW) ── matches (NEW) ── club_results (derived, NEW)

coaches (NEW — master) ──┬── coach_discoveries (existing, adds coach_id FK + absorbs club_coaches)
                         ├── coach_career_history  (NEW, polymorphic to club|college)
                         ├── coach_movement_events (NEW, derived)
                         ├── coach_scrape_snapshots(NEW, raw source-of-truth)
                         └── coach_effectiveness   (NEW, derived)

scrape_run_logs (NEW)  — every scraper invocation, append-only
scrape_health   (NEW)  — rollup, polymorphic (entity_type, entity_id)
```

**Totals for migration:**
- **New tables:** 18 (Domain 1: 2, Domain 2: 3, Domain 3: 5, Domain 4: 2, Domain 5: 2, Domain 6: 1, Domain 7: 1, Domain 8: 2)
- **Extended tables:** 2 (`canonical_clubs`, `coach_discoveries`)
- **Dropped tables (deferred):** 2 (`club_events` — empty wrong-shape, drops once the new `events`/`event_teams` linker covers it; `club_coaches` — merged into `coach_discoveries`, drops after the backfill run reports 0 remaining orphan rows). Both remain defined in `schema/index.ts` until their respective drop PRs so `drizzle-kit push` does not touch them.

---

## Tables by domain

### Domain 1 — Clubs

**Extend `canonical_clubs`** (9 new cols):
```
logo_url, founded_year, twitter, instagram, facebook, staff_page_url
website_status        check: active|dead|redirected|no_staff_page|unchecked
website_last_checked_at, last_scraped_at, scrape_confidence
```

*Migration note:* `websiteStatus` already exists without a check constraint. Sanitize existing rows to match the enum (or normalize to `'unchecked'`) before applying the check constraint.

**New `club_roster_snapshots`:**
```
id, club_id FK, season, age_group, gender, division
player_name (text, no FK), jersey_number, position
scraped_at, source, event_id FK nullable → events
UNIQUE (club_id, season, age_group, gender, player_name)
```

**New `club_site_changes`:**
```
id, club_id FK
change_type  check: staff_added|staff_removed|tryout_posted|announcement|site_redesign|page_404
change_detail jsonb, detected_at, page_url, snapshot_hash_before, snapshot_hash_after
UNIQUE (club_id, snapshot_hash_before, snapshot_hash_after, change_type)
```

---

### Domain 2 — Colleges (new entity)

**New `colleges`:**
```
id, name, slug unique, ncaa_id
division         check: D1|D2|D3|NAIA|NJCAA
conference, state, city
website, soccer_program_url
gender_program   check: mens|womens|both
enrollment, scholarship_available bool
logo_url, twitter, last_scraped_at, scrape_confidence
UNIQUE (name, division, gender_program)
```

**New `college_coaches`:**
```
id, college_id FK, coach_id FK → coaches nullable (nullable until linker runs)
name, title, email, phone, twitter, linkedin
is_head_coach bool, source, source_url, scraped_at, confidence
first_seen_at, last_seen_at
UNIQUE (college_id, name, title)
```

**New `college_roster_history`:**
```
id, college_id FK
player_name, position
year            check: freshman|sophomore|junior|senior|grad
academic_year   (e.g. "2024-25")
hometown, prev_club, jersey_number, scraped_at
UNIQUE (college_id, player_name, academic_year)
```

---

### Domain 3 — Youth Coaches

**New `coaches` master table:**
```
id, person_hash unique, display_name, primary_email nullable
first_seen_at, last_seen_at, manually_merged bool default false
```
`person_hash` = sha256 of normalized name + lowercased email if present. One real coach = one row, regardless of how many clubs.

**Extend `coach_discoveries`** (4 cols; absorbs `club_coaches`):
```
coach_id FK → coaches (nullable until linker runs)
phone          (absorbed from club_coaches)
first_seen_at, last_seen_at
```

*Migration note:* `club_coaches` is read-only from the API route and has no scraper writes. Migration batch:
1. Add `phone` column to `coach_discoveries`
2. `INSERT INTO coach_discoveries (...) SELECT ... FROM club_coaches ON CONFLICT (club_id, name, title) DO NOTHING`
3. **Update `artifacts/api-server/src/routes/coaches.ts`** to read from `coach_discoveries` (otherwise API breaks on drop)
4. Drop `club_coaches`

**New `coach_career_history`:**
```
id, coach_id FK → coaches
entity_type    check: club|college
entity_id      (polymorphic: canonical_clubs.id or colleges.id)
role           check: head_coach|assistant|doc|gk_coach|fitness|club_director|other
start_year, end_year nullable, is_current bool
source, source_url, confidence
UNIQUE (coach_id, entity_type, entity_id, role, start_year)
```

**New `coach_movement_events`** (append-only, derived from weekly diffs):
```
id, coach_id FK
event_type  check: joined|departed|promoted|role_changed|vanished
from_entity_type, from_entity_id (polymorphic)
to_entity_type, to_entity_id (polymorphic)
from_role, to_role, detected_at, scrape_run_log_id FK, confidence
UNIQUE (coach_id, event_type, detected_at, from_entity_type, from_entity_id)
```

**New `coach_scrape_snapshots`** (raw per-scrape blobs, source of truth for diffs):
```
id, club_id FK, scraped_at, raw_staff jsonb, parse_confidence, staff_count
UNIQUE (club_id, scraped_at)
```

**New `coach_effectiveness`** (materialized; recomputed nightly):
```
id, coach_id FK
players_placed_d1, players_placed_d2, players_placed_d3
players_placed_naia, players_placed_njcaa
players_placed_total, clubs_coached, seasons_tracked
last_calculated_at
UNIQUE (coach_id)
```

---

### Domain 4 — Events

**Drop existing `club_events` (empty, wrong-shaped).**

**New `events`** (tournament/showcase itself):
```
id, name, slug unique, league_name, season
age_group nullable, gender nullable, division nullable
  — nullable because multi-bracket events (e.g. Jefferson Cup U13-U19 both genders) can't
    have a single bracket value. Scrapers populate these only for single-bracket events.
    eventTeams carries the real per-team bracket.
location_city, location_state
start_date, end_date, registration_url, source_url
source            check: gotsport|sincsports|manual|other
platform_event_id (e.g. GotSport's event ID)
last_scraped_at
UNIQUE (source, platform_event_id)
```

**New `event_teams`:**
```
id, event_id FK, canonical_club_id FK nullable (null = unmatched scraped team)
team_name_raw, team_name_canonical, age_group, gender, division_code
registered_at, source_url, source
UNIQUE (event_id, team_name_raw)
```

*Matching `team_name_raw` → `canonical_club_id` is a deferred batch job, not inline at scrape time.*

---

### Domain 5 — Matches

**`matches` is the superset — covers league play AND event games.**

**New `matches`:**
```
id, event_id FK nullable (null = league match, not tournament)
home_club_id FK → canonical_clubs
away_club_id FK → canonical_clubs
home_team_name, away_team_name
home_score, away_score
match_date, age_group, gender, division, season, league
status            check: scheduled|final|cancelled|forfeit
source, source_url, platform_match_id
scraped_at
UNIQUE (source, platform_match_id) WHERE platform_match_id IS NOT NULL
UNIQUE (home_team_name, away_team_name, match_date, age_group, gender) WHERE platform_match_id IS NULL
```

**New `club_results`** (materialized; recomputed nightly from matches):
```
id, club_id FK, season, league, division, age_group, gender
wins, losses, draws, goals_for, goals_against, matches_played
last_calculated_at
UNIQUE (club_id, season, league, division, age_group, gender)
```

---

### Domain 6 — Historical Rosters

No new raw tables — uses `club_roster_snapshots` (Domain 1) + `college_roster_history` (Domain 2).

**New `roster_diffs`** (materialized; recomputed when new snapshot lands):
```
id, club_id FK, season_from, season_to, age_group, gender
players_joined jsonb      (stores a JSON array, not jsonb[])
players_departed jsonb
players_retained jsonb
retention_rate real, calculated_at
UNIQUE (club_id, season_from, season_to, age_group, gender)
```

---

### Domain 7 — Tryouts

**New `tryouts`:**
```
id, club_id FK, age_group, gender, division
tryout_date, registration_deadline
location_name, location_address, location_city, location_state
cost, url, notes
source  check: site_monitor|gotsport|manual|other
status  check: active|expired|cancelled|unknown
detected_at, scraped_at, expires_at
site_change_id FK nullable → club_site_changes
UNIQUE (club_id, tryout_date, age_group, gender)
```

---

### Domain 8 — Scrape Health

**New `scrape_run_logs`** — one row per scraper invocation, append-only:
```
id, scraper_key, league_name nullable
started_at, completed_at nullable
status        check: running|ok|partial|failed
failure_kind  check: NULL | timeout|network|parse_error|zero_results|unknown
records_created, records_updated, records_failed
records_touched GENERATED ALWAYS AS (records_created + records_updated) STORED
error_message, source_url
```

**New `scrape_health`** — polymorphic current-state rollup:
```
id, entity_type  check: club|league|college|coach|event|match|tryout
entity_id, last_scraped_at, last_success_at
status      check: ok|stale|failed|never
confidence, consecutive_failures, last_error
next_scheduled_at, priority check: 1..4
UNIQUE (entity_type, entity_id)
```

---

## Cross-domain joins (the critical graph)

| Join | Path | Purpose |
|---|---|---|
| Club → coach history | `canonical_clubs ← coach_career_history (entity_type='club')` | "who has coached here" |
| College → coach history | `colleges ← coach_career_history (entity_type='college')` | coach college stints |
| Coach → placements | `coach_career_history.entity_id (club) → club_roster_snapshots.player_name → college_roster_history.player_name` (fuzzy) | the moat: D1 placement rates |
| Event → results | `events → matches (event_id)` | tournament games |
| Club → season record | `canonical_clubs ← matches.home_club_id/away_club_id → club_results` | W/L/D trend |

---

## Resolved decisions (13)

| # | Decision | Rationale |
|---|---|---|
| 1 | Add `coaches` master table; `coach_discoveries` + all career/movement/effectiveness tables FK to it | One real coach at N clubs needs one identity row, not N |
| 2 | Drop `eventSchedule`; `matches` is the superset | `matches.event_id` nullable covers tournament + league cleanly |
| 3 | Drop `club_events` | Empty, wrong-shaped, replaced by `event_teams` |
| 4 | Merge `club_coaches` INTO `coach_discoveries` (add `phone` col, INSERT ON CONFLICT DO NOTHING, drop table, update API route) | Prototype table, no scraper writes, 95% schema overlap |
| 5 | Polymorphic `(entity_type, entity_id)` with app-layer validation | Industry standard for audit/log/history tables; alternative is nullable-FK-per-type bloat |
| 6 | `club_roster_snapshots` unique key includes gender | Prevents collision between U13G "Alex" and U13B "Alex" same season |
| 7 | `tryouts` UNIQUE (club_id, tryout_date, age_group, gender) | URLs not always stable across edits; date+bracket is reliable |
| 8 | `roster_diffs.players_*` as `jsonb`, not `jsonb[]` | Postgres array-of-jsonb awkward to query; JSON array in `jsonb` is same semantics, better ergonomics |
| 9 | `scrape_run_logs.records_touched` as `GENERATED ALWAYS AS (records_created + records_updated) STORED` | Avoids derived-value drift; zero cost |
| 10 | `coach_movement_events` UNIQUE (coach_id, event_type, detected_at, from_entity_type, from_entity_id) | Rerunning diff job would double-insert without this |
| 11 | `club_site_changes` UNIQUE (club_id, snapshot_hash_before, snapshot_hash_after, change_type) | Same: hash pair only occurs once, rerun-safe |
| 12 | `events.age_group/gender/division` nullable | Multi-bracket events (Jefferson Cup U13–U19) can't have a single value |
| 13 | Apply check constraint to `canonical_clubs.website_status` only after sanitizing existing rows | Migration must normalize or fail cleanly, not silently break |

---

## Operational follow-ups (not blocking migration)

These need answers before Phase 2+ scrapers ship, but do not block the initial migration:

1. **Fuzzy cross-roster matching reliability.** "John Smith at FC Dallas 2022" → "John Smith at UNC 2024" — false-match rate for `coach_effectiveness`? Do we need a manual review queue, or is the aggregate statistic robust enough despite individual errors?

2. **`person_hash` edge cases.** What about coaches with no email and common names? "Mike Smith" at 8 clubs might collapse to one row when they're 8 people. Suggest: require `manually_merged=true` for hashes with >1 distinct email across sources; otherwise keep separate.

3. **Materialized views vs scheduled jobs.** `coach_effectiveness`, `club_results`, `roster_diffs` are all nightly-regenerated. Postgres `MATERIALIZED VIEW ... REFRESH` vs regular tables filled by a cron job. Lean: regular tables (more control, better testability, Drizzle-friendly). Confirm before building.

4. **Retention policies:**
   - `scrape_run_logs` — how long do we keep individual runs? Suggest: rolling 90 days, archive to S3 after.
   - `coach_scrape_snapshots` — largest table (jsonb blobs). Suggest: keep last 5 per club, aggregate the rest.
   - `coach_movement_events` — append-only, small. Keep forever.

5. **`scrape_health` roll-up logic:**
   - `ok → stale` thresholds per entity type: rosters 90d, coaches 30d, tryouts 7d, events 14d.
   - `ok → failed` after N consecutive failures: suggest 3.
   - Who writes: the scraper writes `scrape_run_logs`; a post-run reconciler writes `scrape_health`.

6. **Indexes beyond unique constraints.** Query patterns that need additional indexes:
   - `matches(home_club_id, match_date DESC)`, `matches(away_club_id, match_date DESC)` — "all matches for this club"
   - `coach_movement_events(detected_at DESC)` — recent movement feed
   - `scrape_health(status, last_scraped_at)` — `/admin/stale` endpoint
   - `club_roster_snapshots(club_id, season)` — "roster for a season"

7. **Partition strategy for `matches`.** US youth soccer ~500K matches/year. Monolithic for now; revisit at 5M rows or if queries slow measurably. Partition key candidates: `season` (string, manual management) or `match_date` (native range partition).

8. **Backfill plan for `coaches` master:**
   - Iterate 2,647 `coach_discoveries` rows → compute `person_hash` → INSERT ON CONFLICT DO NOTHING into `coaches` → update `coach_discoveries.coach_id`
   - Absorb `club_coaches` rows in same pass
   - Ambiguous cases (same name, no email) get separate `coaches` rows and flagged for admin review queue

9. **`event_teams` → `canonical_club_id` linker.**
   - Nightly batch, fuzzy threshold 0.88 (reuse existing RapidFuzz setting for club dedup)
   - Unmatched rows surface in `/analytics/unlinked-teams` for manual review
   - Consider a confidence column for linker-assigned IDs

10. **Upshift-side bridge:** `upshift-player-platform` needs these FK columns added (tracked separately as post-migration work):
    - `organizations.aggregator_canonical_club_id` (integer, nullable, FK to `canonical_clubs.id` across DBs — enforced at app layer, not DB)
    - `schools.aggregator_college_id`
    - `coach_profiles_shadow.aggregator_coach_id`
    - `events.aggregator_event_id`
    - `games.aggregator_match_id`

---

## What's intentionally NOT in scope

- **User accounts, authentication.** Aggregator has no user model. API is either open or API-keyed.
- **Claim flow.** Stays in upshift-player-platform. Aggregator produces rosters; upshift decides who can claim them.
- **Payments, subscriptions, messaging.** Product layer.
- **Video, highlights, jersey recognition.** Product layer.
- **Player performance data, combines, wearables.** Product layer.
- **Admin dedup UI.** Lives in upshift-player-platform's admin panel, but reads aggregator API.

---

## Next steps

1. ~~Replit sign-off on 4 structural decisions~~ **Done. 13 resolutions accepted.**
2. **Draft single migration** that applies all changes in section "Tables by domain" + the 4 drops/extensions. Order matters — create `coaches` before altering `coach_discoveries`, create `events` before altering `club_roster_snapshots` (event_id FK), etc.
3. **Update `artifacts/api-server/src/routes/coaches.ts`** to read from `coach_discoveries` instead of `club_coaches` in the same PR as the `club_coaches` drop.
4. **Backfill job** for `coaches` master from existing `coach_discoveries` + `club_coaches`.
5. **Wire Python `FailureKind` into `scrape_run_logs`.** Smallest end-to-end slice; validates the schema is usable before building on top.
6. **Phase 1 scrapers** — events scraper first, since `events`/`event_teams` are the blocking tables for Phase 3+ (matches, results).

## Post-migration operational checklist

Items that are code-complete but must happen on the live DB in order:

1. `pnpm --filter @workspace/db run push` on Replit to apply the 15 new tables + extension columns.
2. Run `pnpm --filter @workspace/scripts run backfill-coaches -- --dry-run`, sanity-check the summary, then re-run without `--dry-run`.
3. After the backfill run reports 0 remaining rows with `coach_id IS NULL`, drop the legacy `club_coaches` table (separate PR; schema still defines it so push doesn't touch it).
4. Historical `scrape_run_logs` rows are NOT backfilled — the table starts empty and accumulates from the first scraper run post-deploy. This is intentional; the old `scraper_run_logs` shape in the legacy platform is not compatible.
5. The polymorphic tables (`coach_career_history`, `coach_movement_events`, `scrape_health`) have no FK-level orphan protection. Schedule a nightly reconciler that GCs rows whose `entity_id` no longer resolves against `entity_type`.
6. `club_events` is still defined in schema alongside the new `events` / `event_teams`. Keep it until the linker has enough coverage that a shadow-cutover is safe, then drop in its own PR.

---

## Changelog

- **2026-04-14 (initial):** Drafted with 4 structural decisions and 10 open questions. Sent for review.
- **2026-04-14 (resolved):** Replit agent reviewed. All 4 decisions accepted. 9 additional tightenings incorporated (items 6–13 in Resolved Decisions). Two drops confirmed (`club_events`, `club_coaches`). `clubCoaches` fate resolved via codebase audit: no scraper writes, read-only from API — merge into `coach_discoveries` safe. Status moved to **Approved, ready for migration**. Renamed from `path-a-data-model-review.md` to `path-a-data-model.md`.
- **2026-04-14 (tech arch review):** Parallel tech-arch + codebase-audit pass. Resolutions folded into the schema text at implementation time rather than re-listing in prose:
  - `canonical_clubs.website_status` check adds `'search'` — 544 existing rows use it (Brave Search API provenance). Enum becomes `active|dead|redirected|no_staff_page|search|unchecked`.
  - `club_roster_snapshots.player_name` is `NOT NULL` (already implied by UNIQUE; made explicit).
  - All `jsonb` columns use Drizzle `$type<…>()` annotations for type safety.
  - `event_teams` race policy: best-effort dedup on `(event_id, team_name_raw)`; `canonical_club_id` populated by nightly linker, never inline.
  - `scrape_health.priority` is `smallint` (1–4), not `integer`.
  - `matches.status` adds `'postponed'`. `tryouts.status` adds `'upcoming'`.
  - Extra indexes on `scrape_run_logs`: `(scraper_key, started_at DESC)`, `(status, started_at DESC)`.
  - FK `ON DELETE` policy: child rows that are meaningless without their parent CASCADE (`club_roster_snapshots`, `club_site_changes`, `tryouts`, `event_teams`, `coach_career_history`, `coach_scrape_snapshots`, `coach_effectiveness`, `college_coaches`, `college_roster_history`). FK `coach_id` on `coach_discoveries` is `SET NULL` (discovery row survives coach merges).
  - `coaches` master table gets `created_at`, `updated_at` (human-touched via manual merge).
  - `matches` partial uniques use Drizzle `uniqueIndex().on(…).where(sql\`…\`)`.
  - `scrape_run_logs.records_touched` uses `.generatedAlwaysAs(sql\`records_created + records_updated\`)` — Drizzle 0.45 emits `GENERATED ALWAYS AS (...) STORED` and Postgres has no VIRTUAL mode before PG 17, so no explicit mode option is passed.
  - `matches` natural-key unique index `COALESCE`s nullable columns (`match_date`, `age_group`, `gender`) to sentinels so rows with NULL fields don't silently re-insert every scrape. Same pattern on `tryouts_club_date_bracket_uq`.
  - `college_coaches.is_head_coach` is `.notNull().default(false)` so `WHERE is_head_coach = true` matches the intended set without NULL-trap.
  - `coaches.ts` backfill honors `manually_merged=true` — if an operator has pinned a discovery to a specific coach, subsequent backfills will not repoint it. Per-row errors are logged and counted, never aborting the run.
  - `scrape_run_logger.py` reuses a module-level autocommit connection instead of opening one per start/finish.
  - Python `_TIMEOUT_MARKERS` in `run.py` use the lowercased form `"timeouterror"` so the `.lower()`-comparison against `type(exc).__name__` matches.
