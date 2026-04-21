# Rosters and Tryouts Pipeline

How the aggregator ingests roster snapshots and tryout postings, plus
how the per-player diff history is materialized.

Applies to these Path A tables:

- `club_roster_snapshots` — one row per (club, season, age_group, gender, player)
- `roster_diffs` — per-player events materialized from consecutive snapshots
- `tryouts` — club tryout postings

All three participate in the canonical-club-linker pattern: scrapers
write `club_name_raw` and leave `club_id` NULL; the linker
(`scraper/canonical_club_linker.py`) resolves the FK in a follow-up pass.

---

## Run Order

```bash
cd scraper

# 1. Rosters: walk every SincSports tournament, upsert snapshots and
#    materialize diffs against the previous snapshot of the same group.
python3 run.py --source sincsports-rosters --dry-run
python3 run.py --source sincsports-rosters

# 2. Tryouts: probe WordPress club websites for tryout pages.
python3 run.py --source tryouts-wordpress --dry-run
python3 run.py --source tryouts-wordpress

# 3. Linker: resolve `club_id` FKs on all three tables.
python3 run.py --source link-canonical-clubs
```

Linker runs *after* the scrapers. Running it in between is fine too —
it's idempotent and only touches rows where the FK is NULL. `matches`
and `event_teams` also benefit from the same pass.

Verify on Replit:

```sql
-- New roster rows since last run:
SELECT count(*) FROM club_roster_snapshots WHERE scraped_at > now() - interval '1 hour';

-- Diff events materialized in the last run:
SELECT diff_type, count(*) FROM roster_diffs
WHERE detected_at > now() - interval '1 hour'
GROUP BY diff_type;

-- Tryouts rows upserted in the last run:
SELECT count(*) FROM tryouts WHERE scraped_at > now() - interval '1 hour';

-- Linker coverage (after step 3):
SELECT count(*) FROM club_roster_snapshots WHERE club_id IS NULL;
SELECT count(*) FROM tryouts WHERE club_id IS NULL;
```

---

## How the Diff Computation Works

`scraper/ingest/roster_snapshot_writer.py::insert_roster_snapshots`
upserts the batch, then for each `(club_name_raw, season, age_group,
gender)` group:

1. Reads the previous snapshot — the rows with `MAX(snapshot_date) <
   the current snapshot_date` for that group.
2. Compares prior vs current by `player_name`:
   - present now + absent prior → `diff_type='added'`
   - absent now + present prior → `diff_type='removed'`
   - both, different `jersey_number` → `diff_type='jersey_changed'`
   - both, different `position` → `diff_type='position_changed'`
3. Inserts diff rows via `ON CONFLICT ON CONSTRAINT
   roster_diffs_name_season_age_gender_player_type_uq DO NOTHING` —
   diffs are append-only history.

Edge cases:

- **First snapshot for a group** — no prior data, no diffs emitted.
- **Re-running the same scrape** — the snapshot `DO UPDATE ... WHERE`
  predicate short-circuits unchanged rows. Diffs against the identical
  prior snapshot produce zero events.
- **Player re-joins after leaving** — produces two historical events
  (`removed` then later `added`) because the unique index keys on
  `(..., player_name, diff_type)`.

---

## Adding a New WordPress Tryouts Site

1. Find a club website hosted on WordPress (usually obvious in the
   page's HTML — look for `/wp-content/` in asset URLs).
2. Verify the club publishes tryout info at one of the probed paths:
   `/tryouts/`, `/tryouts`, `/register/`, `/registration/`, `/join/`,
   `/join`.
3. Confirm the page has at least a date in the prose in one of the
   supported formats:
   - `August 5, 2026` or `Aug 5, 2026`
   - `August 5-7, 2026` (date ranges keep the first day)
   - `8/5/26` or `08/05/2026`
4. Add an entry to `scraper/extractors/tryouts_wordpress_seed.py`:

   ```python
   TRYOUTS_WORDPRESS_SEED.append({
       "club_name_raw": "Foley FC",
       "website": "https://foleyfc.example.com",
   })
   ```

5. Dry-run to confirm:

   ```bash
   python3 run.py --source tryouts-wordpress --dry-run --limit 1
   ```

The seed list starts empty — operator populates it from
`canonical_clubs WHERE staff_page_url IS NOT NULL` on Replit after the
first real linker pass. Over time, scheduled runs will exercise every
seeded site; sites that produce zero rows for several seasons should
be pruned.

---

## Idempotency Guarantees

Both writers use named `ON CONFLICT ON CONSTRAINT <name>` so they are
resilient to Drizzle reformatting the stored index-expression text (see
`scraper/ingest/matches_writer.py` doc block for the full rationale).

The snapshot upsert's `DO UPDATE ... WHERE` predicate fires only when
one of `jersey_number`, `position`, `source_url`, `division` changed.
The tryouts upsert's `DO UPDATE ... WHERE` fires only when
`location_name`, `url`, or `notes` changed. Re-running either scraper
with no upstream change is a no-op and reports zero updates.

---

## Nav-leaked names detector (`roster_quality_flags`)

The roster scrapers occasionally write a navigation-menu string ("Home",
"Contact", "Sitemap", …) into the `player_name` column when the source
site's menu HTML appears inside the same selector as the roster table.
Phase 1 (read-only API + dashboard panel, PR #175) shipped the
`roster_quality_flags` table and the
`/api/v1/admin/data-quality/nav-leaked-names` GET endpoint. Phase 2 (this
change) adds:

- `scraper/nav_leaked_names_detector.py` — groups
  `club_roster_snapshots` rows by `(club_name_raw, season, age_group,
  gender)`, case-folds each `player_name` against a 39-token
  navigation-menu list, and upserts one
  `roster_quality_flags` row of type `nav_leaked_name` per offending
  group. Match is exact-on-full-string (no substring), so a real player
  named "Tom Sitemap" does not trip a flag. The flag's `metadata`
  jsonb stores `{leaked_strings: string[], snapshot_roster_size: number}`.
- `python3 run.py --source nav-leaked-names-detect [--dry-run] [--limit N]`
  CLI entry point; idempotent — the
  `roster_quality_flags_snapshot_type_uq` unique constraint plus the
  `WHERE metadata IS DISTINCT FROM EXCLUDED.metadata` clause means a
  re-run on unchanged data writes nothing.
- Nightly cron — `nightly-nav-leaked-names-detect` at `35 3 * * *`,
  five minutes after the canonical-school linker. (Add this block to
  `.replit` `[[deployment.scheduledJobs]]` manually — the agent cannot
  edit `.replit`.)
- `PATCH /api/v1/admin/data-quality/roster-quality-flags/:id/resolve`
  — operator triage endpoint. Stamps `resolved_at = NOW()` and
  `resolved_by = <admin user id>` (NULL for API-key callers). Idempotent
  on re-resolve; 404 only when the id is unknown. The dashboard panel
  surfaces a per-row Resolve button that issues this PATCH and
  invalidates the nav-leaked-names query so the row disappears (or
  flips to the Resolved badge when "Include resolved flags" is on).

## HS canonical-schools seed (NCES CCD)

The HS-roster scraper (MaxPreps) feeds `hs_rosters`, which uses the
canonical-schools linker pattern — scrapers write `school_name_raw` +
`school_state` and leave `school_id` NULL. The linker
(`scraper/canonical_school_linker.py`,
`python3 run.py --source link-canonical-schools`) can only match against
rows already present in `canonical_schools`. That table starts empty; the
NCES Common Core of Data (CCD) public-school universe is what populates
it.

Run on Replit once per year when NCES publishes a new CCD extract:

```bash
# 1. Download ccd_sch_029_YYYY_w_1a_DATE.zip from
#    https://nces.ed.gov/ccd/files.asp (most recent file). Unzip.

# 2. Seed the canonical table (idempotent — re-runnable).
pnpm --filter @workspace/scripts run seed-canonical-schools-nces \
    -- --csv /tmp/ccd_sch_029_YYYY_w_1a_DATE.csv

# 3. After the next MaxPreps HS scrape, resolve school_id FKs:
python3 run.py --source link-canonical-schools
```

Filter predicate applied by the seeder:

- `SCH_TYPE = 1` (regular school — excludes special-ed / vocational /
  alternative).
- Grade span (`GSLO`..`GSHI`) overlaps 9-12.
- 2-letter state code present on the row. Private schools are not in CCD
  (PSS is a separate NCES product and is out of scope here).

Idempotency: keyed on `canonical_schools.ncessch` (the 12-char NCES
identifier), which has a partial unique index `ncessch IS NOT NULL`. A
re-run UPDATEs the matching row. If a row already exists with the same
`(school_name_canonical, school_state)` but no `ncessch`
(operator-curated, or created by the linker from a MaxPreps alias), the
seeder backfills the NCES id onto that existing row rather than creating
a duplicate.

Dry-run / smoke:

```bash
pnpm --filter @workspace/scripts run seed-canonical-schools-nces \
    -- --csv /tmp/ccd.csv --dry-run --limit 1000
pnpm --filter @workspace/scripts run test:seed-canonical-schools-nces
```

The test runs fully offline against a fixture CSV; no real network or
Postgres required.
