/**
 * `/api/v1/admin/growth` — read-only admin view for the growth dashboard.
 *
 *   GET /api/v1/admin/growth/scraped-counts?since=<iso>
 *   GET /api/v1/admin/growth/coverage-trend?days=<n>
 *
 * Both routes are inherited behind `requireAdmin` + the 120/min read-tier
 * limiter by the parent admin router.
 *
 * The per-table "added-since" timestamp picks differ — see the doc comment
 * on `GrowthDeps.countSince` for the column chosen for each of the five
 * ingest tables. That choice is load-bearing: `events` has no dedicated
 * `first_seen_at` today, so `last_scraped_at` is the best available proxy.
 *
 * Status enum — `scrape_run_logs.status` is the DB's four-value vocab
 * ('running' | 'ok' | 'partial' | 'failed'). Contract-side we collapse:
 *   successes := status = 'ok'
 *   failures  := status IN ('partial', 'failed')
 * 'running' rows are counted in `runs` but not in successes/failures.
 *
 * Handlers are factory-built so tests can drive them with an in-memory
 * `GrowthDeps` instead of spinning up a real Postgres — same pattern as
 * admin/dedup.ts.
 *
 * No caching for this PR; both endpoints hit the DB on every request. If
 * the UI surfaces them in a polling loop we'll add node-cache (5-min TTL
 * on scraped-counts, 1-hour on coverage-trend) in a follow-up.
 */
import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { sql } from "drizzle-orm";
import {
  db,
  canonicalClubs,
  coaches,
  events,
  clubRosterSnapshots,
  matches,
  scrapeRunLogs,
} from "@workspace/db";
import {
  ScrapedCountsDelta,
  CoverageTrendResponse,
} from "@hlbiv/api-zod/admin";

/**
 * Per-table "added since <date>" counts. Handlers consume this so tests
 * can inject a fake without touching Postgres.
 */
export type GrowthDeps = {
  countSince: (args: {
    table: "clubs" | "coaches" | "events" | "rosterSnapshots" | "matches";
    since: Date;
  }) => Promise<number>;
  coverageTrend: (args: {
    days: number;
  }) => Promise<Array<{
    date: string;
    runs: number;
    successes: number;
    failures: number;
    rowsTouched: number;
  }>>;
};

// ---------------------------------------------------------------------------
// Production dep bound to the shared `db`.
// ---------------------------------------------------------------------------

/**
 * DB-backed dep. Per-table timestamp-column choice:
 *   - clubs           → canonical_clubs.last_scraped_at  (no first_seen col today)
 *   - coaches         → coaches.first_seen_at
 *   - events          → events.last_scraped_at            (no first_seen col today)
 *   - rosterSnapshots → club_roster_snapshots.snapshot_date
 *   - matches         → matches.scraped_at
 */
export const prodGrowthDeps: GrowthDeps = {
  countSince: async ({ table, since }) => {
    if (table === "clubs") {
      const [row] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(canonicalClubs)
        .where(sql`${canonicalClubs.lastScrapedAt} > ${since}`);
      return row?.count ?? 0;
    }
    if (table === "coaches") {
      const [row] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(coaches)
        .where(sql`${coaches.firstSeenAt} > ${since}`);
      return row?.count ?? 0;
    }
    if (table === "events") {
      const [row] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(events)
        .where(sql`${events.lastScrapedAt} > ${since}`);
      return row?.count ?? 0;
    }
    if (table === "rosterSnapshots") {
      const [row] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(clubRosterSnapshots)
        .where(sql`${clubRosterSnapshots.snapshotDate} > ${since}`);
      return row?.count ?? 0;
    }
    // matches
    const [row] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(matches)
      .where(sql`${matches.scrapedAt} > ${since}`);
    return row?.count ?? 0;
  },

  coverageTrend: async ({ days }) => {
    if (days <= 0) return [];
    // Use make_interval so the day count is bound as an integer, not a
    // string literal concatenation. `records_touched` is a STORED generated
    // column — select it directly, don't recompute.
    const rows = await db.execute<{
      date: string;
      runs: number;
      successes: number;
      failures: number;
      rows_touched: number;
    }>(sql`
      SELECT date_trunc('day', ${scrapeRunLogs.startedAt})::date::text AS date,
             count(*)::int AS runs,
             count(*) FILTER (WHERE ${scrapeRunLogs.status} = 'ok')::int AS successes,
             count(*) FILTER (WHERE ${scrapeRunLogs.status} IN ('partial', 'failed'))::int AS failures,
             COALESCE(sum(${scrapeRunLogs.recordsTouched}), 0)::int AS rows_touched
      FROM ${scrapeRunLogs}
      WHERE ${scrapeRunLogs.startedAt} > now() - make_interval(days => ${days})
      GROUP BY 1
      ORDER BY 1
    `);
    // Drizzle's .execute() returns the driver's rows in `.rows` on node-pg.
    const list = (rows as unknown as { rows?: Array<Record<string, unknown>> }).rows
      ?? (rows as unknown as Array<Record<string, unknown>>);
    return (list ?? []).map((r) => ({
      date: String(r.date),
      runs: Number(r.runs ?? 0),
      successes: Number(r.successes ?? 0),
      failures: Number(r.failures ?? 0),
      rowsTouched: Number(r.rows_touched ?? 0),
    }));
  },
};

// ---------------------------------------------------------------------------
// Handlers (factory-built so tests can inject a fake GrowthDeps).
// ---------------------------------------------------------------------------

export function makeScrapedCountsHandler(deps: GrowthDeps) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const sinceParam = req.query.since;
      if (typeof sinceParam !== "string" || sinceParam.length === 0) {
        res.status(400).json({ error: "missing_since" });
        return;
      }
      const since = new Date(sinceParam);
      if (Number.isNaN(since.getTime())) {
        res.status(400).json({ error: "invalid_since" });
        return;
      }

      const [
        clubsAdded,
        coachesAdded,
        eventsAdded,
        rosterSnapshotsAdded,
        matchesAdded,
      ] = await Promise.all([
        deps.countSince({ table: "clubs", since }),
        deps.countSince({ table: "coaches", since }),
        deps.countSince({ table: "events", since }),
        deps.countSince({ table: "rosterSnapshots", since }),
        deps.countSince({ table: "matches", since }),
      ]);

      res.json(
        ScrapedCountsDelta.parse({
          since: since.toISOString(),
          clubsAdded,
          coachesAdded,
          eventsAdded,
          rosterSnapshotsAdded,
          matchesAdded,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeCoverageTrendHandler(deps: GrowthDeps) {
  return async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    try {
      const daysRaw = req.query.days;
      // Default window: 30 days. Matches the admin UI default.
      let days = 30;
      if (typeof daysRaw === "string" && daysRaw.length > 0) {
        const parsed = Number(daysRaw);
        if (!Number.isFinite(parsed) || parsed < 0) {
          res.status(400).json({ error: "invalid_days" });
          return;
        }
        days = Math.floor(parsed);
      }

      const points = await deps.coverageTrend({ days });

      res.json(
        CoverageTrendResponse.parse({
          points,
          windowDays: days,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Router — prod-wired.
// ---------------------------------------------------------------------------

const growthRouter: IRouter = Router();
growthRouter.get("/scraped-counts", makeScrapedCountsHandler(prodGrowthDeps));
growthRouter.get("/coverage-trend", makeCoverageTrendHandler(prodGrowthDeps));

export default growthRouter;
