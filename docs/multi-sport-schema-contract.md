# Multi-sport schema contract

**Status:** Draft — planned for PR-29 (2026-04-23). Commit this doc to source control alongside the migration.

**Why this doc exists:** the NCAA enumeration arc started as soccer-only. April 2026 session formalized the multi-sport design so future basketball / football / volleyball handlers don't relitigate foundational decisions. This is the durable contract — read it before shipping a new-sport handler; don't re-derive it from chat history.

---

## The four non-negotiables

### 1. `colleges.sport TEXT NOT NULL` — **no DEFAULT after back-fill**

```sql
-- Step 1: add column with DEFAULT to back-fill existing rows
ALTER TABLE colleges ADD COLUMN sport TEXT NOT NULL DEFAULT 'soccer';

-- Step 2: DROP the DEFAULT immediately after the back-fill commits
ALTER TABLE colleges ALTER COLUMN sport DROP DEFAULT;
```

**Why the two-step.** Leaving `DEFAULT 'soccer'` permanently is a footgun: a future basketball seed handler that forgets to pass `sport` would silently write basketball rows as `sport='soccer'`, colliding on the natural key with the actual soccer program of the same school. Detection would be near-impossible (query returns a "double count" that looks like data rather than a bug).

Dropping the default enforces explicit sport at the DB layer. The Python writer (`ncaa_roster_writer.upsert_college`) keeps `sport='soccer'` as a Python-level default for existing-caller back-compat, but the DB rejects any INSERT that reaches the DB without an explicit column value. Belt-and-suspenders against the exact failure mode that'd be hardest to detect in analytics.

### 2. `sport` and `gender_program` are **separate axes**

Natural key:

```sql
UNIQUE (name, division, gender_program, sport)
```

- `sport TEXT IN ('soccer', 'basketball', 'football', 'volleyball', ...)` — no gender baked into the value
- `gender_program TEXT IN ('mens', 'womens')` — stays as-is; already existed pre-migration
- Football edge case: `gender_program='mens', sport='football'`. The constraint still holds — womens' football rows just never get created.

**Do NOT** collapse to `sport='mens-soccer'`. That double-encodes gender and forces either a redundant `gender_program` column or losing the existing one. Every downstream query that filters `WHERE gender_program='mens'` would need rewriting.

### 3. `--sport` required on new handlers, optional-with-default on existing

When PR-30 lands, every existing handler (`ncaa-seed-d1`, `ncaa-rosters`, `ncaa-resolve-urls`, `naia-seed-official`, etc.) accepts `--sport SPORT` as **optional** with default `soccer`. Existing runbooks don't break the day PR-30 merges.

For any handler shipped **after** PR-30 (future basketball handlers, future volleyball handlers, ...): `argparse` sets `required=True`. The basketball PR author cannot copy-paste a soccer handler and forget to flip the default — argparse rejects the call at the first invocation.

This is the forcing function against the exact class of bug that would produce cross-sport contamination.

### 4. `url_needs_review.reason` is a **6-value enum**

`college_roster_quality_flags.metadata->>'reason'` when `flag_type = 'url_needs_review'` must be one of:

| Value | Meaning | Kid action |
|---|---|---|
| `no_url_at_all` | `colleges.soccer_program_url IS NULL` | Google school, paste athletics site + roster URL |
| `static_404` | URL exists but page 404'd (school migrated CMS) | Verify + update URL |
| `playwright_exhausted` | Static fail + Playwright fail (JS-only page or auth wall) | Manual investigation; may be abandonable |
| `partial_parse` | URL works but returned `<SMALL_ROSTER_THRESHOLD` players | Parser regression — check selectors |
| `historical_no_data` | URL works for current season but not for the historical season path | Archive doesn't exist; likely abandon |
| `current_zero_parse` | Current-season URL fetched 200 but parser found 0 players | Selector broke *now*; distinct from historical-no-data |

This is the contract that the admin kid-workflow UI depends on for prioritization. Operator dashboards filter flags by reason to feed kid worklists. See `artifacts/dashboard/src/pages/DataQuality.tsx` (post-PR-24) for UI rendering.

Scraper write path picks exactly one value at flag-write time based on what actually failed. `write_college_flag` helper enforces the enum via a CHECK constraint at the column level.

---

## PR-29 migration SQL

```sql
BEGIN;

-- 1. Add column; back-fill existing rows
ALTER TABLE colleges ADD COLUMN sport TEXT NOT NULL DEFAULT 'soccer';

-- 2. DROP the default
ALTER TABLE colleges ALTER COLUMN sport DROP DEFAULT;

-- 3. Replace natural-key constraint
ALTER TABLE colleges DROP CONSTRAINT colleges_name_division_gender_uq;
ALTER TABLE colleges ADD CONSTRAINT colleges_name_division_gender_sport_uq
  UNIQUE (name, division, gender_program, sport);

-- 4. Index for common filter
CREATE INDEX colleges_sport_idx ON colleges (sport);

COMMIT;
```

Downstream tables (`college_roster_history`, `college_coaches`, `college_coach_tenures`) **do not** need a `sport` column — they inherit the partition via `college_id` FK. A row's sport is reachable through `college_id → colleges.sport` JOIN.

---

## PR-29 rollback plan

Schema migrations cannot be `git revert`-ed — once data is in the new shape, rolling back means another **forward** migration, not a revert.

```sql
-- PR-29b rollback (new migration, not a git revert)
BEGIN;
ALTER TABLE colleges DROP CONSTRAINT colleges_name_division_gender_sport_uq;
ALTER TABLE colleges ADD CONSTRAINT colleges_name_division_gender_uq
  UNIQUE (name, division, gender_program);
ALTER TABLE colleges DROP COLUMN sport;
COMMIT;
```

**The cheap-rollback window closes the first time a non-soccer row is written.** After any `sport='basketball'` (or `football`, etc.) row lands, dropping the column would lose data. Forcing function: PR-29 + PR-30 should go through a **full Replit sweep cycle on soccer-only data** (verify nothing regressed; counts match pre-migration baseline) before any basketball-handler PR ships. If the soccer-only sweep verifies clean, the schema contract is committed long-term.

Pre-flight checklist for the soccer-only verification run:

```bash
# Pre-migration baseline
psql "$DATABASE_URL" -c "
  SELECT division, gender_program, count(*)
  FROM colleges GROUP BY division, gender_program
  ORDER BY division, gender_program;
"

# Post-migration (should match the baseline — no row count drift)
# Same query, same output expected.

# Post-migration sport column invariant
psql "$DATABASE_URL" -c "
  SELECT sport, count(*) FROM colleges GROUP BY sport;
"
# Expected: only 'soccer' until basketball/football lands.
```

---

## New-sport handler checklist (future you, reading this)

When basketball (or any new sport) arrives, the work is bounded:

1. **New extractor module** — `scraper/extractors/ncaa_basketball_rosters.py`. Owns URL patterns, parser strategies, coach-extraction selectors. Reuses `CollegeSeed`, `upsert_college`, `retry_with_backoff`, `should_scrape`, `write_college_flag`, `ScrapeRunLogger`.

2. **New seed handlers** with the right source URLs. Parallel to `ncaa-seed-d1`, `ncaa-seed-wikipedia`, etc. Each accepts `--sport basketball` as **required** (not default).

3. **`--sport basketball` passed to every handler.** DB rejects the write if not supplied (DEFAULT was dropped).

4. **No schema changes needed.** `colleges` already has `sport`. `college_roster_quality_flags` already writes sport-neutral. Admin API already sport-agnostic.

5. **No shared-code changes.** `should_scrape()`, `write_college_flag()`, `upsert_college()` all sport-neutral.

6. **Sport allow-list update** — add `'basketball'` to the per-division allow-list in `scraper/run.py` (or a shared `scraper/sports.py` if it grows).

Validate with: soccer-only sweep still produces identical counts post-basketball-handler merge. If basketball seeded correctly, you should see `sport='basketball'` rows appear alongside existing `sport='soccer'` rows with no soccer regression.

---

## Design decisions deliberately rejected

- **`SportConfig` registry class** — premature abstraction. Wait for the second sport to drive the actual shape of what varies (URL templates? parser strategies? seed sources? coach selectors?). Building the abstraction before observing ≥2 instances of variation inevitably abstracts the wrong dimensions.
- **Sport column on every table** (`college_roster_history.sport`, `college_coaches.sport`, etc.) — redundant. Reachable via FK to `colleges.sport`. Denormalization buys nothing downstream; the JOIN cost is trivial for these table sizes.
- **Tiered retry queue** — YAGNI. Failure causes that benefit from progressive-retry-strategy are ~5% of the universe. The `url_needs_review.reason` enum already supports workflow-level tiering (kid filters by reason). Revisit if a failure class emerges where different strategies genuinely help.
- **Sport-aware admin UI filters at PR-24 ship time** — keep initial UI simple. Add `?sport=basketball` filter when second sport actually lands and the operator has a real reason to scope views.

---

## References

- BACKLOG.md items 12–16 — PRs that implement this contract
- `scraper/extractors/ncaa_rosters.py` — current soccer extractor (becomes `ncaa_soccer_rosters.py` in PR-30)
- `scraper/ingest/ncaa_roster_writer.py:upsert_college` — natural-key writer; gains `sport` parameter in PR-29
- `lib/db/src/schema/colleges.ts` — Drizzle schema; adds `sport` column in PR-29
- `lib/db/src/schema/coach-quality-flags.ts` — reference for flag-table pattern used by PR-24
- `scraper/nav_leaked_names_detector.py:198-241` — reference for idempotent `ON CONFLICT ... DO UPDATE` flag-write pattern
