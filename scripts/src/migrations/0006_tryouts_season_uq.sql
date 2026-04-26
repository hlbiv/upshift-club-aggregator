-- 0006_tryouts_season_uq.sql
--
-- PR 4: Add `season` to the tryouts unique index so same-club / same-date
-- U-14 tryouts in different seasons (2025-26 vs 2026-27) don't collide
-- and silently UPDATE each other, losing season-scoped history.
--
-- Also adds the `season` column to the `tryouts` table (nullable text).
-- The writer (`scraper/ingest/tryouts_writer.py`) already passes a
-- `%(season)s` placeholder through `_normalize_row`, so no scraper-side
-- backfill is needed: legacy rows simply have NULL `season`, which
-- COALESCEs to '' for the unique index — i.e. the index treats them
-- as "season-unknown" and they collapse with each other on (club,
-- date, age, gender), preserving today's behavior for legacy rows.
--
-- The constraint name `tryouts_name_date_bracket_uq` is preserved.
-- The writer references it by name in `ON CONFLICT ON CONSTRAINT …`,
-- so renaming would break the upsert.
--
-- Idempotent: re-running the file is a no-op.

BEGIN;

-- 1. Add the column (nullable; legacy rows stay NULL).
ALTER TABLE tryouts
    ADD COLUMN IF NOT EXISTS season text;

-- 2. Swap the unique index to include COALESCE(season, '').
DROP INDEX IF EXISTS tryouts_name_date_bracket_uq;
CREATE UNIQUE INDEX tryouts_name_date_bracket_uq
    ON tryouts (
        club_name_raw,
        COALESCE(tryout_date, 'epoch'::timestamp),
        COALESCE(age_group, ''),
        COALESCE(gender, ''),
        COALESCE(season, '')
    );

COMMIT;

-- ---------------------------------------------------------------------------
-- ROLLBACK (paste-ready — NOT part of the forward migration)
-- ---------------------------------------------------------------------------
--
-- BEGIN;
--
-- DROP INDEX IF EXISTS tryouts_name_date_bracket_uq;
-- CREATE UNIQUE INDEX tryouts_name_date_bracket_uq
--     ON tryouts (
--         club_name_raw,
--         COALESCE(tryout_date, 'epoch'::timestamp),
--         COALESCE(age_group, ''),
--         COALESCE(gender, '')
--     );
--
-- -- Optional: only drop the column if nothing has started writing it.
-- -- ALTER TABLE tryouts DROP COLUMN IF EXISTS season;
--
-- COMMIT;
