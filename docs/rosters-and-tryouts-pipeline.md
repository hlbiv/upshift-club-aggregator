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
