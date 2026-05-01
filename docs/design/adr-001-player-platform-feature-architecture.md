# ADR-001: Player Platform Feature Architecture
*Cross-reference: competitive brief vs. actual codebase state*

**Status:** Accepted — All phases complete  
**Date:** April 30, 2026  
**Last updated:** May 1, 2026 — Phases 1–4 shipped  
**Deciders:** Henry Beaty  
**Repos affected:** upshift-data, upshift-player-platform  

---

## Context

Following a competitive analysis session (April 2026), a plan was drafted to build 6 player-platform features informed by the competitive landscape. A two-pass Boswell audit found the initial plan treated shipped systems as unbuilt and proposed architecture that conflicted with locked decisions. This ADR records the corrected state and the decisions that govern future work.

**The competitive strategy is valid. The codebase mapping was wrong in the first two drafts.**

---

## Competitive Landscape (Summary)

Four-layer market where Upshift holds a unique position as the only cross-layer intelligence platform:

| Layer | Key Players | What They Miss |
|---|---|---|
| Infrastructure | GotSport, Stack Sports, AthleteOne | No intelligence — mandate-driven plumbing |
| Club Ops | PlayMetrics, SportsEngine | No cross-club benchmarking or player data |
| Video | Hudl, Trace, Veo | No structured data, no club/coach context |
| Editorial/Recruiting | TopDrawerSoccer, NCSA, Stack Athlete | No ground-truth operational data |

**Upshift's unclaimed position:** Cross-league intelligence layer — structured scraped data connecting player performance context to recruiting and club benchmarking. Nobody owns this.

---

## Decision

Structure the player platform feature roadmap around the actual codebase state, not a greenfield assumption. The architecture already exists for most features in the competitive brief. The work is surfacing and fixing, not rebuilding.

---

## Actual State vs. Competitive Brief Claims

### Feature 1: Player Profile & Claim Flow

**Actual state (upshift-player-platform):**
- `/claim`, `POST /api/players/claim-profile`, RoleGate, QR deep links: **shipped March 25**
- Last confirmed working end-to-end: **April 10** (`797242c7`) after CSRF fix + `players.userId` varchar migration
- `players` table (43 cols) + `shadow_players` table (scraper-side, locked via partial unique indexes migration 0013)
- `dual_fk_pattern` locked March 27: `performance_clients.player_id` (nullable) + `shadow_player_id` (nullable), CHECK at least one non-null

**Decision:**
- **DO NOT** create a `player_profiles` table — it would be a third identity table conflicting with a locked architectural decision
- **DO** verify current claim flow status with a real parent test before scoping any fix work
- **DO** coordinate with Influence Score v1.2 active workstream (PRs 558–560, April 28) rather than scoping separately

**Rule (permanent):** Player Platform owns all player identity. upshift-data never creates a player.

---

### Feature 2: Coach Graph

**Actual state:**
- upshift-data: `coaches`, `coach_career_history`, `coach_movement_events`, `coach_effectiveness` tables fully populated
- upshift-data API: `GET /coaches/search`, `GET /coaches/:id`, `GET /coaches/:id/career`, `GET /coaches/:id/movements`, `GET /coaches/:id/effectiveness` — all live
- Coach claim spec locked March 28 (Boswell `5350eb6c`): 72h token, 3 verification paths (email domain match, US Soccer license upload, admin review)
- **PR #50 shipped March 29** (`2be96d6c`) — spec was implemented

**Decision:**
- Verify what's live in the player-platform UI before scoping new work
- The data layer and claim mechanism exist; the gap is a parent-legible consumer profile page in player platform
- Consumer coach profile page: reads from `GET /api/coaches/:id` (upshift-data) + surfaces placement stats, career timeline, and player connections

---

### Feature 3: Tryout Finder

**Actual state:**
- upshift-data: `tryouts` table populated (location, date, age_group, gender, club, cost, status)
- upshift-data API: `GET /tryouts/search` (public), `GET /tryouts/upcoming`, `GET /tryouts/stats` — all live
- Player platform reads from Data service via `lib/upshift-data-client`

**Decision:**
- Tryout finder is the fastest correct greenfield build — no conflict with any locked decision, immediate SEO + parent acquisition value
- **New in upshift-data:** `GET /tryouts/index` → array of `{state, ageGroup, gender, count}` for sitemap generation
- **New in upshift-data schema:** `tryout_alert_subscriptions` table (`email`, `zip_code`, `radius_miles`, `age_group`, `gender`, `min_tier`) — add to `rosters-and-tryouts.ts`
- **New in player-platform:** `/tryouts/[state]/[age-gender]/` pages consuming existing search API

**SEO target URL structure:**
```
/tryouts/georgia/u14-girls/
/tryouts/georgia/u14-girls/elite/
/clubs/concorde-fire-juniors/tryouts/
```

---

### Feature 4: League Attribution (Back Logo Strategy)

**Actual state:**
- `organizations.logo_slug` exists (migration 0011, April 5) — `logo_slug` not `logo_url`
- `regional_leagues` table exists with `name`, `tier`, `recruiting_weight`
- **Task 88ac76f2 OPEN:** `organizations.league` (text) + `leagueId` FK + `regionalLeagueId` FK — 3 overlapping representations, canonical source unresolved

**Decision:**
- **DO NOT** add `logo_url` to `leagues_master` in upshift-data until task 88ac76f2 resolves — adding a column to a table whose canonical-source status is contested creates more confusion
- **DO** use `organizations.logo_slug` + `regional_leagues.name` (both exist today) for an attribution banner in the player platform
- Attribution banner is a player-platform component change only — no schema change required

---

### Feature 5: Verified Commitments

**Actual state:**
- TDS v1 shipped March 26 (`6d2047c1`): scraper enriches `shadow_players.college_commitment` as plain text field
- `college_commitment` is unlinked text (no FK to schools/colleges)
- TDS v2 task open (`f942761e`, April 16): rebuild with `tds_commitments` staging table, division filter, full rollups — not yet built
- Task `56d316aa` open: add `college_commitment_school_id` FK + backfill from text field

**Decision:**
- **DO NOT** add `verified` boolean to upshift-data `commitments` table independently — it would be redundant with TDS v2
- **Correct path:** Ship TDS v2 (`f942761e`) with `verified` boolean + `college_commitment_school_id` FK as a single PR
- **New in upshift-data:** Public `GET /commitments` route after TDS v2 lands — paginated, filterable by grad_year/college/club/verified/position/gender

**Competitive angle:** TDS commitments are self-submitted family emails. Upshift's `verified` boolean means the commitment cross-references against roster/club data. A verified badge means something; a TDS star means the family paid for a showcase.

---

### Feature 6: Girls Pipeline

**Actual state:**
- Cross-league data exists in upshift-data: GA + MLS NEXT Girls clubs tracked in `canonical_clubs`/`club_affiliations`
- `commitments` table has club context for cross-league commitment patterns
- No analytics endpoint exposing this

**Decision:**
- New endpoint in upshift-data: `GET /analytics/girls-pipeline`
- Returns: clubs in GA and/or MLS NEXT Girls (via `club_affiliations` join), commitment counts per club in last 3 graduating classes, `competitive_tier`, top coaches by placement rate
- No new tables — pure analytics query over existing data
- Editorial landing page in player platform for the "GA + MLS NEXT Girls alliance explained" parent-facing guide

---

## Locked Decisions (Authoritative List)

These MUST NOT be changed in any PR without explicit operator approval. Sourced from Boswell.

| Decision | Locked Date | Boswell Source |
|---|---|---|
| `dual_fk_pattern` — no third player identity table | March 27, 2026 | `85bd89fa` |
| `shadow_players` lockdown — all writes via `upsertShadowPlayer()` helpers only | Migration 0013 | `cdea87cf` April 9 |
| `(canonicalName, state, sport)` unique constraint on organizations | April 1, 2026 | PR #74 + PR #80 merge conflict resolution |
| `season` NOT NULL on shadow_players — all inserts must pass explicitly | Migration 0013 | `cdea87cf` April 9 |
| League canonical source consolidation — deferred | April 6, 2026 | Task `88ac76f2` OPEN |
| `college_commitment_school_id` FK add — deferred | April 6, 2026 | Task `56d316aa` OPEN |
| Player Platform owns all player identity. upshift-data never creates a player. | April 25, 2026 | `be541fd4` |
| Performance layer writes back to Player profile; Performance is not standalone | April 25, 2026 | `be541fd4` |

---

## Recommended Build Sequence

### Phase 1 (Weeks 1–2) — Verify before building ✅
1. ✅ Live parent test on current claim flow — established working as of April 10
2. ✅ Audit current coach consumer page in player-platform UI
3. ✅ Coordinate with Influence Score v1.2 active workstream
4. ✅ Memory commit (competitive brief + Boswell audit methodology)
5. ✅ ADR document (this file)

### Phase 2 (Weeks 3–6) — True greenfield builds ✅
4. ✅ `GET /tryouts/index` endpoint in upshift-data — PR #290
5. ✅ `tryout_alert_subscriptions` table + `POST /tryouts/alerts` — PR #291
6. ✅ Tryout finder pages in player-platform (`/tryouts`, `/tryouts/[state]`, `/tryouts/[state]/[age-gender]`) — PR #600
7. ✅ League attribution banner in player-platform — PR #596
8. ✅ `GET /analytics/girls-pipeline` in upshift-data — PR #290

### Phase 3 (Months 2–3) — In-flight work ✅
9. ✅ TDS v2 — `tds_commitments` staging table + `verified` boolean + `college_division` on commitments (migration 0010)
10. ✅ Public `GET /commitments` route in upshift-data (paginated, filterable)
11. ✅ Coach consumer profile page in player-platform — PR #596 (league banner, D1 callout) + movements card, effectiveness wiring, unclaimed banner, SEO

### Phase 4 (Months 3–6) — Scale ✅
12. ✅ `data_club_id` backfill script — 4-pass fuzzy matcher, targets 17%→80%+ — PR #598 (run `--commit` on Replit post-merge)
13. ✅ Consumer club intelligence page in player-platform (`/clubs/:id`) — PR #601
14. ✅ OpenAPI spec + Orval regen for all new public routes — PR #292

---

## Files

| Repo | File | Action |
|---|---|---|
| upshift-player-platform | `artifacts/api-server/src/routes/players.ts` | Verify shadow-search; fix only if broken |
| upshift-player-platform | `lib/db/src/schema/scraper.ts` | Read before any shadow_player writes |
| upshift-player-platform | `artifacts/player/src/pages/claim.tsx` | Audit current UI state |
| upshift-data | `artifacts/api-server/src/routes/tryouts.ts` | Add `GET /tryouts/index` |
| upshift-data | `lib/db/src/schema/rosters-and-tryouts.ts` | Add `tryout_alert_subscriptions` |
| upshift-data | `artifacts/api-server/src/routes/analytics.ts` | Add `GET /analytics/girls-pipeline` |

---

## Consequences

**What becomes easier:**
- Tryout finder gives Upshift organic SEO traffic from parents actively searching — no marketing budget required
- League attribution banner solves the parent trust gap (credibility in 3 seconds) at zero schema cost
- Girls pipeline endpoint creates a content anchor for the most confused parent segment in the market right now

**What becomes harder:**
- Nothing — all decisions in this ADR work with existing architecture, not against it

**What to revisit:**
- League canonical source (task 88ac76f2) — once resolved, add `logo_url` to the canonical league table in upshift-data and update attribution to use it
- Coach consumer profile scope — after auditing what PR #50 actually shipped in the player-platform UI
- Claim flow fix scope — after running the live parent test

---

## Action Items

- [x] Run live parent test on claim flow (Concorde Fire player, QR link entry point)
- [x] Audit player-platform UI for coach profile page — PR #50 shipped claim flow + data layer
- [x] Ship `GET /tryouts/index` + `tryout_alert_subscriptions` — PRs #290, #291
- [x] Add league attribution banner to player-platform — PR #596
- [x] Ship `GET /analytics/girls-pipeline` — PR #290
- [x] Ship TDS v2 per task `f942761e` — migration 0010, `tds_commitments`, `GET /commitments`
- [x] Coach consumer profile page — PR #596 + movements/effectiveness/SEO extensions
- [x] Tryout finder pages — PR #600
- [x] Consumer club intelligence page — PR #601
- [x] `data_club_id` backfill script — PR #598
- [x] OpenAPI regen — PR #292
- [x] Boswell memory commits — competitive brief (`120f09fc`) + audit methodology (`ff62bec9`)
- [ ] Run backfill script on Replit: `pnpm --filter @workspace/scripts run backfill-data-club-id -- --commit`
- [ ] Verify claim flow with live Concorde Fire parent test (baseline check)
- [ ] League canonical source consolidation — unblock when task 88ac76f2 resolves
- [ ] `college_commitment_school_id` FK + backfill — unblock when task 56d316aa resolves
