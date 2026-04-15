-- 0001_rosters_tryouts_linker_columns.sql
--
-- Evolves `club_roster_snapshots`, `roster_diffs`, and `tryouts` to match
-- the canonical-club-linker pattern used by `event_teams` and `matches`:
-- scrapers write `club_name_raw` and leave `club_id` NULL; the linker
-- (`scraper/canonical_club_linker.py`) resolves the FK in a follow-up pass.
--
-- Because drizzle-kit push cannot safely emit a DROP NOT NULL + column
-- rename + unique-key swap in one atomic step (and because `roster_diffs`
-- is being reshaped from an aggregate per-(season_from,season_to) row into
-- a per-player event row), this migration is hand-rolled SQL run BEFORE
-- `pnpm --filter @workspace/db run push`.
--
-- Idempotent: every statement guards with IF EXISTS / IF NOT EXISTS /
-- conditional lookups against pg_catalog. Re-running this file is a no-op.
--
-- Runbook: see PR description.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. club_roster_snapshots
-- ---------------------------------------------------------------------------

-- 1a. Drop NOT NULL from club_id.
ALTER TABLE club_roster_snapshots
    ALTER COLUMN club_id DROP NOT NULL;

-- 1b. Add club_name_raw. Initially nullable so existing rows survive;
-- backfill from `canonical_clubs.club_name_canonical`; then enforce NOT NULL.
ALTER TABLE club_roster_snapshots
    ADD COLUMN IF NOT EXISTS club_name_raw text;

UPDATE club_roster_snapshots crs
SET    club_name_raw = cc.club_name_canonical
FROM   canonical_clubs cc
WHERE  crs.club_id = cc.id
  AND  crs.club_name_raw IS NULL;

-- Any row with no club_id AND no backfill source gets a placeholder that
-- the linker will try to resolve (and likely fail, which is fine — the
-- goal here is to not block the NOT NULL swap).
UPDATE club_roster_snapshots
SET    club_name_raw = '(unknown)'
WHERE  club_name_raw IS NULL;

ALTER TABLE club_roster_snapshots
    ALTER COLUMN club_name_raw SET NOT NULL;

-- 1c. Add source_url (nullable).
ALTER TABLE club_roster_snapshots
    ADD COLUMN IF NOT EXISTS source_url text;

-- 1d. Add snapshot_date (nullable, defaults to now()).
ALTER TABLE club_roster_snapshots
    ADD COLUMN IF NOT EXISTS snapshot_date timestamp DEFAULT now();

-- 1e. Replace the old unique constraint with the name-keyed one.
ALTER TABLE club_roster_snapshots
    DROP CONSTRAINT IF EXISTS club_roster_snapshots_unique;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE  conname = 'club_roster_snapshots_name_season_age_gender_player_uq'
    ) THEN
        ALTER TABLE club_roster_snapshots
            ADD CONSTRAINT club_roster_snapshots_name_season_age_gender_player_uq
            UNIQUE (club_name_raw, season, age_group, gender, player_name);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 2. roster_diffs — shape change from aggregate (jsonb arrays) to per-player event row
-- ---------------------------------------------------------------------------
-- Safe to drop aggregate columns: no code reads them (verified by grep).

-- 2a. Drop the old unique + FK-required NOT NULL.
ALTER TABLE roster_diffs
    DROP CONSTRAINT IF EXISTS roster_diffs_unique;

ALTER TABLE roster_diffs
    ALTER COLUMN club_id DROP NOT NULL;

-- 2b. Drop the old aggregate columns.
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS season_from;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS season_to;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS players_joined;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS players_departed;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS players_retained;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS retention_rate;
ALTER TABLE roster_diffs DROP COLUMN IF EXISTS calculated_at;

-- 2c. Relax previously-NOT-NULL ancillary columns to nullable for the new shape.
ALTER TABLE roster_diffs ALTER COLUMN age_group DROP NOT NULL;
ALTER TABLE roster_diffs ALTER COLUMN gender DROP NOT NULL;

-- 2d. Add the new per-player event shape columns.
ALTER TABLE roster_diffs
    ADD COLUMN IF NOT EXISTS club_name_raw text;
UPDATE roster_diffs rd
SET    club_name_raw = cc.club_name_canonical
FROM   canonical_clubs cc
WHERE  rd.club_id = cc.id
  AND  rd.club_name_raw IS NULL;
UPDATE roster_diffs SET club_name_raw = '(unknown)' WHERE club_name_raw IS NULL;
ALTER TABLE roster_diffs ALTER COLUMN club_name_raw SET NOT NULL;

ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS season text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS player_name text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS diff_type text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS from_jersey_number text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS to_jersey_number text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS from_position text;
ALTER TABLE roster_diffs ADD COLUMN IF NOT EXISTS to_position text;
ALTER TABLE roster_diffs
    ADD COLUMN IF NOT EXISTS detected_at timestamp NOT NULL DEFAULT now();

-- 2e. Enforce NOT NULL on required identifier columns for new rows. Any
-- legacy row (pre-reshape) will have NULL player_name/diff_type and will
-- need to be reseeded; gate the constraint with a safe default for the
-- migration and document that roster_diffs is rebuilt from snapshots.
UPDATE roster_diffs SET player_name = '(legacy)' WHERE player_name IS NULL;
UPDATE roster_diffs SET diff_type   = 'added'    WHERE diff_type   IS NULL;
ALTER TABLE roster_diffs ALTER COLUMN player_name SET NOT NULL;
ALTER TABLE roster_diffs ALTER COLUMN diff_type   SET NOT NULL;

-- 2f. Add the diff_type CHECK constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'roster_diffs_diff_type_enum'
    ) THEN
        ALTER TABLE roster_diffs
            ADD CONSTRAINT roster_diffs_diff_type_enum
            CHECK (diff_type IN ('added','removed','jersey_changed','position_changed'));
    END IF;
END $$;

-- 2g. Add the natural-key unique index (COALESCE nullable cols).
CREATE UNIQUE INDEX IF NOT EXISTS roster_diffs_name_season_age_gender_player_type_uq
    ON roster_diffs (
        club_name_raw,
        COALESCE(season, ''),
        COALESCE(age_group, ''),
        COALESCE(gender, ''),
        player_name,
        diff_type
    );

-- 2h. Keep the club_idx.
CREATE INDEX IF NOT EXISTS roster_diffs_club_idx ON roster_diffs (club_id);

-- ---------------------------------------------------------------------------
-- 3. tryouts
-- ---------------------------------------------------------------------------

-- 3a. Drop the expression-indexed natural-key (keyed on club_id).
DROP INDEX IF EXISTS tryouts_club_date_bracket_uq;

-- 3b. Drop NOT NULL from club_id.
ALTER TABLE tryouts ALTER COLUMN club_id DROP NOT NULL;

-- 3c. Add club_name_raw with backfill.
ALTER TABLE tryouts ADD COLUMN IF NOT EXISTS club_name_raw text;
UPDATE tryouts t
SET    club_name_raw = cc.club_name_canonical
FROM   canonical_clubs cc
WHERE  t.club_id = cc.id
  AND  t.club_name_raw IS NULL;
UPDATE tryouts SET club_name_raw = '(unknown)' WHERE club_name_raw IS NULL;
ALTER TABLE tryouts ALTER COLUMN club_name_raw SET NOT NULL;

-- 3d. New natural-key unique index (COALESCE nullable cols).
CREATE UNIQUE INDEX IF NOT EXISTS tryouts_name_date_bracket_uq
    ON tryouts (
        club_name_raw,
        COALESCE(tryout_date, 'epoch'::timestamp),
        COALESCE(age_group, ''),
        COALESCE(gender, '')
    );

COMMIT;

-- ---------------------------------------------------------------------------
-- Rollback (run manually if needed — NOT part of the forward migration)
-- ---------------------------------------------------------------------------
--
-- BEGIN;
--
-- -- 1. club_roster_snapshots
-- ALTER TABLE club_roster_snapshots
--     DROP CONSTRAINT IF EXISTS club_roster_snapshots_name_season_age_gender_player_uq;
-- ALTER TABLE club_roster_snapshots DROP COLUMN IF EXISTS snapshot_date;
-- ALTER TABLE club_roster_snapshots DROP COLUMN IF EXISTS source_url;
-- ALTER TABLE club_roster_snapshots DROP COLUMN IF EXISTS club_name_raw;
-- ALTER TABLE club_roster_snapshots ALTER COLUMN club_id SET NOT NULL;
-- ALTER TABLE club_roster_snapshots
--     ADD CONSTRAINT club_roster_snapshots_unique
--     UNIQUE (club_id, season, age_group, gender, player_name);
--
-- -- 2. roster_diffs — losing aggregate shape is destructive; rollback drops the new shape
-- -- and recreates the old columns (empty). Data cannot be restored from per-player rows.
-- DROP INDEX IF EXISTS roster_diffs_name_season_age_gender_player_type_uq;
-- ALTER TABLE roster_diffs DROP CONSTRAINT IF EXISTS roster_diffs_diff_type_enum;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS detected_at;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS to_position;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS from_position;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS to_jersey_number;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS from_jersey_number;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS diff_type;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS player_name;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS season;
-- ALTER TABLE roster_diffs DROP COLUMN IF EXISTS club_name_raw;
-- ALTER TABLE roster_diffs ADD COLUMN season_from text;
-- ALTER TABLE roster_diffs ADD COLUMN season_to text;
-- ALTER TABLE roster_diffs ADD COLUMN players_joined jsonb;
-- ALTER TABLE roster_diffs ADD COLUMN players_departed jsonb;
-- ALTER TABLE roster_diffs ADD COLUMN players_retained jsonb;
-- ALTER TABLE roster_diffs ADD COLUMN retention_rate real;
-- ALTER TABLE roster_diffs ADD COLUMN calculated_at timestamp NOT NULL DEFAULT now();
-- -- (add back the NOT NULLs + old unique only if data is restored from backup)
--
-- -- 3. tryouts
-- DROP INDEX IF EXISTS tryouts_name_date_bracket_uq;
-- ALTER TABLE tryouts DROP COLUMN IF EXISTS club_name_raw;
-- ALTER TABLE tryouts ALTER COLUMN club_id SET NOT NULL;
-- CREATE UNIQUE INDEX tryouts_club_date_bracket_uq ON tryouts (
--     club_id,
--     COALESCE(tryout_date, 'epoch'::timestamp),
--     COALESCE(age_group, ''),
--     COALESCE(gender, '')
-- );
--
-- COMMIT;
