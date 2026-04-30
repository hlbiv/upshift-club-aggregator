-- 0006_add_mlsnext_source_enum.sql
--
-- FORWARD-ONLY migration. Postgres has no `DROP VALUE` for enums, so this
-- addition cannot be rolled back without recreating the type.
--
-- What this does
-- --------------
-- Adds `mlsnext` to `events_source_enum` so the five MLS NEXT event
-- scrapers (Cup=72, Qualifiers=74, Fest=75, GA Cup=80, Flex=88) can
-- write their source column correctly.
--
-- Without this, every mlsnext-events row fails with:
--   invalid input value for enum events_source_enum: "mlsnext"
--
-- Apply on Replit:
--   psql "$DATABASE_URL" -f scripts/src/migrations/0006_add_mlsnext_source_enum.sql

ALTER TYPE events_source_enum ADD VALUE IF NOT EXISTS 'mlsnext';
