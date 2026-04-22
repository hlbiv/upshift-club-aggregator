-- 0003_coverage_history.sql
--
-- FORWARD-ONLY migration. Creates `coverage_history` — a daily snapshot
-- of the global coverage rollup that powers the Coverage page's KpiStrip
-- sparklines + week-over-week delta badges (#64).
--
-- Schema mirrors `lib/db/src/schema/scrape-health.ts::coverageHistory`:
-- six counters (same shape as the live `summarizeLeagues` rollup), one
-- row per UTC day keyed by `snapshot_date`. The summary endpoint upserts
-- today's row on each call (`ON CONFLICT (snapshot_date) DO UPDATE`),
-- so reads stay cheap and the trend series stays in sync with whatever
-- the strip is showing right now.
--
-- Idempotent: every statement guards with IF NOT EXISTS. Re-running this
-- file is a no-op.
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f \
--     scripts/src/migrations/0003_coverage_history.sql

BEGIN;

CREATE TABLE IF NOT EXISTS coverage_history (
    id                          SERIAL       PRIMARY KEY,
    snapshot_date               DATE         NOT NULL,
    leagues_total               INTEGER      NOT NULL,
    clubs_total                 INTEGER      NOT NULL,
    clubs_with_roster_snapshot  INTEGER      NOT NULL,
    clubs_with_coach_discovery  INTEGER      NOT NULL,
    clubs_never_scraped         INTEGER      NOT NULL,
    clubs_stale_14d             INTEGER      NOT NULL,
    recorded_at                 TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS coverage_history_snapshot_date_uq
    ON coverage_history (snapshot_date);

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification (run these manually after applying):
--
--   \d+ coverage_history
--     -- expect 8 columns + the unique index above.
--
--   -- After hitting GET /api/v1/admin/coverage/leagues/summary once:
--   SELECT * FROM coverage_history ORDER BY snapshot_date DESC LIMIT 5;
-- ---------------------------------------------------------------------------
