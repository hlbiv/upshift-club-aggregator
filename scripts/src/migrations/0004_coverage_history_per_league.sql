-- 0004_coverage_history_per_league.sql
--
-- FORWARD-ONLY migration. Creates `coverage_history_per_league` — a daily
-- snapshot of the per-league coverage rollup, paired with `coverage_history`
-- (0003) which stores the global rollup. Powers per-league sparkline +
-- week-over-week delta on the Coverage drilldown page (#75).
--
-- Schema mirrors `lib/db/src/schema/scrape-health.ts::coverageHistoryPerLeague`:
-- five per-league counters (same shape as the live `listLeagues` rollup,
-- minus `leaguesTotal` which is global), one row per (UTC day, league_id).
-- The summary endpoint bulk-upserts every league's row for today on each
-- call (`ON CONFLICT (snapshot_date, league_id) DO UPDATE`).
--
-- Idempotent: every statement guards with IF NOT EXISTS. Re-running this
-- file is a no-op.
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f \
--     scripts/src/migrations/0004_coverage_history_per_league.sql

BEGIN;

CREATE TABLE IF NOT EXISTS coverage_history_per_league (
    id                          SERIAL       PRIMARY KEY,
    snapshot_date               DATE         NOT NULL,
    league_id                   INTEGER      NOT NULL,
    clubs_total                 INTEGER      NOT NULL,
    clubs_with_roster_snapshot  INTEGER      NOT NULL,
    clubs_with_coach_discovery  INTEGER      NOT NULL,
    clubs_never_scraped         INTEGER      NOT NULL,
    clubs_stale_14d             INTEGER      NOT NULL,
    recorded_at                 TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS coverage_history_per_league_date_league_uq
    ON coverage_history_per_league (snapshot_date, league_id);

CREATE INDEX IF NOT EXISTS coverage_history_per_league_league_date_idx
    ON coverage_history_per_league (league_id, snapshot_date DESC);

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification (run these manually after applying):
--
--   \d+ coverage_history_per_league
--     -- expect 9 columns + the unique index + the league_id idx above.
--
--   -- After hitting GET /api/v1/admin/coverage/leagues/summary once:
--   SELECT snapshot_date, league_id, clubs_total, clubs_never_scraped
--   FROM coverage_history_per_league
--   ORDER BY snapshot_date DESC, league_id ASC LIMIT 10;
-- ---------------------------------------------------------------------------
