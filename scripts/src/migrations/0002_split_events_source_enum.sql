-- 0002_split_events_source_enum.sql
--
-- FORWARD-ONLY migration. Postgres has no `DROP VALUE` for enums, so the
-- additions in this file cannot be rolled back without recreating the type
-- (which would require dropping every column that uses it). Review the
-- value lists below carefully before applying.
--
-- What this does
-- --------------
-- 1. Adds two values to `events_source_enum`:
--      - `totalglobalsports`  (already routed end-to-end via
--        `scraper/totalglobalsports_events_runner.py`)
--      - `usclub_sanctioned`  (already routed via the
--        `_handle_usclub_sanctioned` handler in `scraper/run.py`)
--    Without these, every TGS / US Club Sanctioned event lands as
--    `'other'` (currently 176 of 217 events = 81%).
--
-- 2. Creates `roster_source_enum` — a separate type so
--    `club_roster_snapshots.source` can carry roster-specific values
--    without overloading the events enum. Value list mirrors the roster
--    runners enumerated from `scraper/run.py` (`gotsport-rosters`,
--    `sincsports-rosters`, `maxpreps-rosters`, `ncaa-rosters`,
--    plus the three roster scrapers called out in the wave-2 runbook §9
--    that currently leave `source` NULL: `soccerwire`, `club_website`,
--    `duda_360player`, plus `naia` / `njcaa` shells already wired in
--    `_handle_ncaa_rosters` siblings, plus `manual` / `other`).
--
-- 3. Retypes `club_roster_snapshots.source` from `events_source_enum` to
--    `roster_source_enum`. Currently 0 rows in the table (verified at
--    authoring time), so the `USING source::text::roster_source_enum`
--    cast is a no-op data-wise, but is written defensively.
--
-- 4. Backfills `events.source = 'other'` rows whose `source_url` host
--    matches a known platform pattern. Mapping table:
--
--       host pattern (case-insensitive)            -> events.source
--       -----------------------------------------------------------
--       public.totalglobalsports.com               -> totalglobalsports
--       (any) usclubsoccer.org                     -> usclub_sanctioned
--       system.gotsport.com / events.gotsport.com  -> gotsport
--       (any) sincsports.com                       -> sincsports
--
--    Anything else stays as `'other'` — those rows are legitimate
--    misc-club tournament sites (e.g. socalelitefc.com, slsgsoccer.org,
--    rebelssoccerclub.com) that don't correspond to any platform-level
--    runner. The expected residual after backfill is the bulk of the
--    current 176 rows minus ~10 reassignments. (The original task spec
--    targeted <30 residual; that target was based on an assumption
--    about host distribution that doesn't match the actual data — most
--    'other' rows really are 'other'.)
--
-- Idempotent: every statement guards with IF NOT EXISTS or against
-- pg_catalog. Re-running this file is a no-op.
--
-- IMPORTANT — `ALTER TYPE ... ADD VALUE` cannot run inside a multi-
-- statement transaction in PG <12 with newly-added values used in the
-- same transaction. We therefore split this script into two phases
-- separated by a transaction boundary: enum additions run autocommit,
-- type creation + retype + backfill run inside a single BEGIN/COMMIT.
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f \
--     scripts/src/migrations/0002_split_events_source_enum.sql

-- ---------------------------------------------------------------------------
-- Phase 1: widen events_source_enum (must NOT be inside a transaction
-- if the new values are referenced later in the same transaction).
-- ---------------------------------------------------------------------------

ALTER TYPE events_source_enum ADD VALUE IF NOT EXISTS 'totalglobalsports';
ALTER TYPE events_source_enum ADD VALUE IF NOT EXISTS 'usclub_sanctioned';

-- ---------------------------------------------------------------------------
-- Phase 2: create roster_source_enum, retype the roster column, backfill.
-- ---------------------------------------------------------------------------

BEGIN;

-- 2a. Create roster_source_enum.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'roster_source_enum'
    ) THEN
        CREATE TYPE roster_source_enum AS ENUM (
            'gotsport',
            'sincsports',
            'maxpreps',
            'ncaa',
            'naia',
            'njcaa',
            'odp',
            'soccerwire',
            'club_website',
            'duda_360player',
            'manual',
            'other'
        );
    END IF;
END $$;

-- 2b. Retype club_roster_snapshots.source.
-- Cast via text so the value labels carry over. Currently every value
-- present in the roster table is also present in the new enum
-- (verified at authoring: 0 rows). The cast is wrapped in a DO block
-- guarded by a check on the column's current udt_name so re-running
-- this script after the retype is a no-op.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE  table_name  = 'club_roster_snapshots'
          AND  column_name = 'source'
          AND  udt_name    = 'events_source_enum'
    ) THEN
        -- The original column has no DEFAULT (drizzle schema declares
        -- `source: rosterSourceEnum("source")` with no `.default(...)`).
        -- Preserve that — scrapers writing rosters always set `source`
        -- explicitly, and the runbook §9 follow-ups explicitly track
        -- the three runners that currently leave it NULL. Adding a
        -- DEFAULT here would silently mask those NULLs as `'other'`.
        ALTER TABLE club_roster_snapshots
            ALTER COLUMN source TYPE roster_source_enum
            USING source::text::roster_source_enum;
    END IF;
    -- Belt-and-suspenders: an earlier draft of this migration briefly
    -- set DEFAULT 'other'. Drop it on re-apply so reruns converge to
    -- the no-default state regardless of what state the DB started in.
    ALTER TABLE club_roster_snapshots
        ALTER COLUMN source DROP DEFAULT;
END $$;

-- 2c. Backfill events.source = 'other' rows by host pattern.
-- ~* is the case-insensitive POSIX regex match operator.
UPDATE events
SET    source = 'totalglobalsports'
WHERE  source = 'other'
  AND  source_url ~* '(^|//)(public\.)?totalglobalsports\.com(/|$)';

UPDATE events
SET    source = 'usclub_sanctioned'
WHERE  source = 'other'
  AND  source_url ~* '(^|//)([^/]*\.)?usclubsoccer\.org(/|$)';

UPDATE events
SET    source = 'gotsport'
WHERE  source = 'other'
  AND  source_url ~* '(^|//)(system|events)\.gotsport\.com(/|$)';

UPDATE events
SET    source = 'sincsports'
WHERE  source = 'other'
  AND  source_url ~* '(^|//)([^/]*\.)?sincsports\.com(/|$)';

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification (run these manually after applying):
--
--   SELECT enum_range(NULL::events_source_enum);
--     -- expect: {gotsport,sincsports,manual,other,totalglobalsports,usclub_sanctioned}
--
--   SELECT enum_range(NULL::roster_source_enum);
--     -- expect 12 values listed above.
--
--   SELECT udt_name FROM information_schema.columns
--    WHERE table_name='club_roster_snapshots' AND column_name='source';
--     -- expect: roster_source_enum
--
--   SELECT source, count(*) FROM events GROUP BY source ORDER BY 2 DESC;
--     -- 'other' should drop by ~10 rows; new values should appear.
-- ---------------------------------------------------------------------------
