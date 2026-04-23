-- 0005_add_competitive_tier.sql
--
-- FORWARD-ONLY migration. Adds `canonical_clubs.competitive_tier` — a
-- single rolled-up enum column representing the highest tier across a
-- club's active affiliations. Per-program tier granularity still lives
-- in `leagues_master.tier_label` via `club_affiliations`; this column
-- is the *ceiling* downstream code (acquirer metrics, pricing gates,
-- scout filters, Influence Score calibration) reads when it just wants
-- to ask "what tier is this club?". See task-78.
--
-- What this does
-- --------------
-- 1. Creates the `competitive_tier` enum:
--      recreational | recreational_plus | competitive | elite | academy
--    `recreational` / `recreational_plus` are placeholders for future
--    AYSO / US Club rec scrapers; nothing populates them today (per the
--    "Out of scope" section of task-78).
--
-- 2. Adds `canonical_clubs.competitive_tier` as NOT NULL DEFAULT
--    'competitive'. Existing rows therefore land at 'competitive' until
--    `scripts/src/backfill-competitive-tier.ts` rolls them up to their
--    true tier ceiling.
--
-- Note on schema reconciliation: the originating brief referenced
-- `organizations` and `regional_leagues` tables. Those don't exist in
-- this repo. The live tables are `canonical_clubs`, `leagues_master`,
-- and the M2M `club_affiliations` (with `club_id` / `league_id` FKs).
-- This migration targets the live schema. See CLAUDE.md.
--
-- Idempotent: every statement guards with IF NOT EXISTS / DO $$ blocks.
-- Re-running this file is a no-op.
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f \
--     scripts/src/migrations/0005_add_competitive_tier.sql
--
-- After applying, run the backfill:
--   pnpm --filter @workspace/scripts exec tsx src/backfill-competitive-tier.ts

BEGIN;

-- 1. Create the enum.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type WHERE typname = 'competitive_tier'
    ) THEN
        CREATE TYPE competitive_tier AS ENUM (
            'recreational',
            'recreational_plus',
            'competitive',
            'elite',
            'academy'
        );
    END IF;
END $$;

-- 2. Add the column. NOT NULL DEFAULT 'competitive' so existing rows
--    are populated atomically by the ALTER itself; the backfill then
--    raises rows whose affiliations warrant 'elite' / 'academy'.
ALTER TABLE canonical_clubs
    ADD COLUMN IF NOT EXISTS competitive_tier competitive_tier
        NOT NULL DEFAULT 'competitive';

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification (run these manually after applying):
--
--   SELECT enum_range(NULL::competitive_tier);
--     -- expect: {recreational,recreational_plus,competitive,elite,academy}
--
--   \d+ canonical_clubs
--     -- expect new column: competitive_tier competitive_tier NOT NULL
--     --                    DEFAULT 'competitive'::competitive_tier
--
--   SELECT competitive_tier, count(*) FROM canonical_clubs GROUP BY 1;
--     -- before backfill: every row at 'competitive'.
-- ---------------------------------------------------------------------------
