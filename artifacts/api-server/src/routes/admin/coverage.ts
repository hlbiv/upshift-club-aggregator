/**
 * `/api/v1/admin/coverage/*` — per-league operations-visibility panels.
 *
 *   GET /api/v1/admin/coverage/leagues
 *   GET /api/v1/admin/coverage/leagues/:leagueId
 *
 * Both routes are inherited behind `requireAdmin` + the 120/min read-tier
 * limiter by the parent admin router in `./index.ts`.
 *
 * Join assumption
 * ---------------
 * The join between `leagues_master` and the clubs that belong to each
 * league runs through `club_affiliations.league_id = leagues_master.id`
 * — a stable id, not the league name. This means a label rename on
 * `leagues_master.league_name` (e.g. `"ECNL Boys"` → `"ECNL — Boys,
 * 2025"`) cannot silently drop the affected league out of the per-league
 * rollup or under-count the global "With roster / With coach" counters.
 * Affiliation rows whose `league_id` is NULL (legacy rows that predate
 * the column, or scraper writes that couldn't resolve a league) are
 * intentionally excluded from coverage rollups — run
 * `lib/db/src/backfill-affiliations-league-id.ts` to fill them in.
 *
 * Scrape-health coupling
 * ----------------------
 * Coverage staleness runs off `scrape_health` with `entity_type='club'`,
 * keyed by `canonical_clubs.id`. A club with no `scrape_health` row counts
 * as `never_scraped` (same semantic as a NULL `last_scraped_at`).
 *
 * Ordering
 * --------
 * - `/leagues`: `clubs_never_scraped DESC, clubs_stale_14d DESC,
 *   league_name ASC` — the worst-covered leagues bubble to page 1.
 * - `/leagues/:leagueId`: `last_scraped_at ASC NULLS FIRST,
 *   club_name_canonical ASC` — oldest + never-scraped first.
 *
 * Testing
 * -------
 * Handlers are factory-built (same pattern as `admin/growth.ts` and
 * `admin/data-quality.ts`) so unit tests can drive them with an in-memory
 * `CoverageDeps` fake. The prod wiring lives at the bottom of this file.
 */
import {
  Router,
  type IRouter,
  type Request,
  type Response,
  type NextFunction,
} from "express";
import { sql } from "drizzle-orm";
import { db as defaultDb } from "@workspace/db";
import {
  CoverageLeaguesRequest,
  CoverageLeaguesResponse,
  CoverageLeaguesSummaryResponse,
  CoverageLeaguesHistoryRequest,
  CoverageLeaguesHistoryResponse,
  CoverageLeagueDetailRequest,
  CoverageLeagueDetailResponse,
  CoverageLeagueHistoryRequest,
  CoverageLeagueHistoryResponse,
  type CoverageLeagueDetailStatus,
} from "@hlbiv/api-zod/admin";

// ---------------------------------------------------------------------------
// DI surface.
// ---------------------------------------------------------------------------

export interface CoverageLeagueAggRow {
  leagueId: number;
  leagueName: string;
  clubsTotal: number;
  clubsWithRosterSnapshot: number;
  clubsWithCoachDiscovery: number;
  clubsNeverScraped: number;
  clubsStale14d: number;
}

export interface CoverageLeagueDetailAggRow {
  clubId: number;
  clubNameCanonical: string;
  lastScrapedAt: Date | string | null;
  consecutiveFailures: number;
  coachCount: number;
  hasRosterSnapshot: boolean;
  staffPageUrl: string | null;
  scrapeConfidence: number | null;
}

export interface CoverageLeaguesSummary {
  leaguesTotal: number;
  clubsTotal: number;
  clubsWithRosterSnapshot: number;
  clubsWithCoachDiscovery: number;
  clubsNeverScraped: number;
  clubsStale14d: number;
}

export interface CoverageHistoryRow {
  /** ISO calendar date (YYYY-MM-DD) — one row per UTC day. */
  snapshotDate: string;
  leaguesTotal: number;
  clubsTotal: number;
  clubsWithRosterSnapshot: number;
  clubsWithCoachDiscovery: number;
  clubsNeverScraped: number;
  clubsStale14d: number;
}

export interface CoverageLeagueHistoryRow {
  /** ISO calendar date (YYYY-MM-DD) — one row per UTC day. */
  snapshotDate: string;
  clubsTotal: number;
  clubsWithRosterSnapshot: number;
  clubsWithCoachDiscovery: number;
  clubsNeverScraped: number;
  clubsStale14d: number;
}

export interface CoverageDeps {
  listLeagues: (args: {
    page: number;
    pageSize: number;
  }) => Promise<{ rows: CoverageLeagueAggRow[]; total: number }>;

  /**
   * Aggregate coverage rollup across every league. Counts are
   * deduplicated by canonical club so a club that appears in N
   * leagues counts once.
   *
   * Production also persists today's rollup into `coverage_history`
   * via an idempotent upsert (one row per UTC day) so the trends
   * series stays in sync with whatever the strip is showing right
   * now. Tests are free to skip the upsert.
   */
  summarizeLeagues: () => Promise<CoverageLeaguesSummary>;

  /**
   * Daily timeseries of the global coverage rollup, oldest-first,
   * capped at `days`. Returns an empty array if the history table is
   * empty (fresh deploy, before the first summary call).
   */
  getCoverageHistory: (args: {
    days: number;
  }) => Promise<CoverageHistoryRow[]>;

  /**
   * Daily timeseries of one league's coverage rollup, oldest-first,
   * capped at `days`. Returns an empty array when the per-league
   * history table has no rows for the league yet (fresh deploy or a
   * league that's never been included in a summary call).
   */
  getLeagueCoverageHistory: (args: {
    leagueId: number;
    days: number;
  }) => Promise<CoverageLeagueHistoryRow[]>;

  /**
   * Resolves the `(id, name)` pair for the requested league. Returns null
   * if the id is unknown — the handler translates that into a 404.
   */
  findLeague: (args: {
    leagueId: number;
  }) => Promise<{ id: number; name: string } | null>;

  listClubsInLeague: (args: {
    leagueId: number;
    leagueName: string;
    status: CoverageLeagueDetailStatus;
    page: number;
    pageSize: number;
  }) => Promise<{ rows: CoverageLeagueDetailAggRow[]; total: number }>;
}

// ---------------------------------------------------------------------------
// Helpers — ISO coercion lifted from admin/data-quality.ts.
// ---------------------------------------------------------------------------

function toIsoOrNull(value: Date | string | null): string | null {
  if (value === null) return null;
  if (value instanceof Date) return value.toISOString();
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toNumberOrUndefined(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function toStringOrUndefined(raw: unknown): string | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  return String(raw);
}

// ---------------------------------------------------------------------------
// Handler factories.
// ---------------------------------------------------------------------------

export function makeListLeaguesHandler(deps: CoverageDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const parsed = CoverageLeaguesRequest.safeParse({
        page: toNumberOrUndefined(req.query.page),
        pageSize:
          toNumberOrUndefined(req.query.page_size) ??
          toNumberOrUndefined(req.query.pageSize),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { page, pageSize } = parsed.data;

      const { rows, total } = await deps.listLeagues({ page, pageSize });

      res.json(
        CoverageLeaguesResponse.parse({
          rows,
          total,
          page,
          pageSize,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeSummaryHandler(deps: CoverageDeps) {
  return async (
    _req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const summary = await deps.summarizeLeagues();
      res.json(CoverageLeaguesSummaryResponse.parse(summary));
    } catch (err) {
      next(err);
    }
  };
}

export function makeHistoryHandler(deps: CoverageDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const parsed = CoverageLeaguesHistoryRequest.safeParse({
        days: toNumberOrUndefined(req.query.days),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { days } = parsed.data;
      const rows = await deps.getCoverageHistory({ days });
      res.json(CoverageLeaguesHistoryResponse.parse({ rows }));
    } catch (err) {
      next(err);
    }
  };
}

export function makeLeagueHistoryHandler(deps: CoverageDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const leagueId = Number(req.params.leagueId);
      if (!Number.isFinite(leagueId) || leagueId <= 0) {
        res.status(400).json({ error: "Invalid leagueId" });
        return;
      }
      const parsed = CoverageLeagueHistoryRequest.safeParse({
        days: toNumberOrUndefined(req.query.days),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { days } = parsed.data;

      const league = await deps.findLeague({ leagueId });
      if (league === null) {
        res.status(404).json({ error: "League not found" });
        return;
      }

      const rows = await deps.getLeagueCoverageHistory({ leagueId, days });
      res.json(CoverageLeagueHistoryResponse.parse({ league, rows }));
    } catch (err) {
      next(err);
    }
  };
}

export function makeLeagueDetailHandler(deps: CoverageDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const leagueId = Number(req.params.leagueId);
      if (!Number.isFinite(leagueId) || leagueId <= 0) {
        res.status(400).json({ error: "Invalid leagueId" });
        return;
      }

      const parsed = CoverageLeagueDetailRequest.safeParse({
        page: toNumberOrUndefined(req.query.page),
        pageSize:
          toNumberOrUndefined(req.query.page_size) ??
          toNumberOrUndefined(req.query.pageSize),
        status: toStringOrUndefined(req.query.status),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { page, pageSize, status } = parsed.data;

      const league = await deps.findLeague({ leagueId });
      if (league === null) {
        res.status(404).json({ error: "League not found" });
        return;
      }

      const { rows, total } = await deps.listClubsInLeague({
        leagueId: league.id,
        leagueName: league.name,
        status,
        page,
        pageSize,
      });

      res.json(
        CoverageLeagueDetailResponse.parse({
          league,
          rows: rows.map((r) => ({
            clubId: r.clubId,
            clubNameCanonical: r.clubNameCanonical,
            lastScrapedAt: toIsoOrNull(r.lastScrapedAt),
            consecutiveFailures: r.consecutiveFailures,
            coachCount: r.coachCount,
            hasRosterSnapshot: r.hasRosterSnapshot,
            staffPageUrl: r.staffPageUrl,
            scrapeConfidence: r.scrapeConfidence,
          })),
          total,
          page,
          pageSize,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Production DB wiring.
// ---------------------------------------------------------------------------

const STALE_THRESHOLD_DAYS = 14;

export const prodCoverageDeps: CoverageDeps = {
  listLeagues: async ({ page, pageSize }) => {
    const offset = (page - 1) * pageSize;

    // Per-league aggregate. The join key is the stable
    // `club_affiliations.league_id = leagues_master.id` — never the
    // league name — so a `leagues_master.league_name` rename can't drop
    // a league out of the rollup. Distinct clubs only — a league with a
    // duplicate affiliation row must not double-count. Subset flags use
    // EXISTS so a club with 10 snapshots still counts 1.
    const result = await defaultDb.execute<{
      league_id: number;
      league_name: string;
      clubs_total: string;
      clubs_with_roster_snapshot: string;
      clubs_with_coach_discovery: string;
      clubs_never_scraped: string;
      clubs_stale_14d: string;
      total: string;
    }>(sql`
      WITH league_clubs AS (
        SELECT
          lm.id         AS league_id,
          lm.league_name,
          cc.id         AS club_id,
          EXISTS (
            SELECT 1 FROM club_roster_snapshots crs WHERE crs.club_id = cc.id
          ) AS has_roster_snapshot,
          EXISTS (
            SELECT 1 FROM coach_discoveries cd WHERE cd.club_id = cc.id
          ) AS has_coach_discovery,
          sh.last_scraped_at
        FROM leagues_master lm
        LEFT JOIN club_affiliations ca
          ON ca.league_id = lm.id
        LEFT JOIN canonical_clubs cc
          ON cc.id = ca.club_id
        LEFT JOIN scrape_health sh
          ON sh.entity_type = 'club' AND sh.entity_id = cc.id
      ),
      league_agg AS (
        SELECT
          league_id,
          league_name,
          COUNT(DISTINCT club_id) AS clubs_total,
          COUNT(DISTINCT club_id) FILTER (WHERE has_roster_snapshot)
            AS clubs_with_roster_snapshot,
          COUNT(DISTINCT club_id) FILTER (WHERE has_coach_discovery)
            AS clubs_with_coach_discovery,
          COUNT(DISTINCT club_id) FILTER (WHERE club_id IS NOT NULL AND last_scraped_at IS NULL)
            AS clubs_never_scraped,
          COUNT(DISTINCT club_id) FILTER (
            WHERE last_scraped_at IS NOT NULL
              AND last_scraped_at < now() - make_interval(days => ${STALE_THRESHOLD_DAYS})
          ) AS clubs_stale_14d
        FROM league_clubs
        GROUP BY league_id, league_name
      )
      SELECT
        league_id,
        league_name,
        clubs_total::text                AS clubs_total,
        clubs_with_roster_snapshot::text AS clubs_with_roster_snapshot,
        clubs_with_coach_discovery::text AS clubs_with_coach_discovery,
        clubs_never_scraped::text        AS clubs_never_scraped,
        clubs_stale_14d::text            AS clubs_stale_14d,
        COUNT(*) OVER ()                 AS total
      FROM league_agg
      ORDER BY
        clubs_never_scraped DESC,
        clubs_stale_14d DESC,
        league_name ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const list = Array.from(
      result as unknown as Array<{
        league_id: number;
        league_name: string;
        clubs_total: string;
        clubs_with_roster_snapshot: string;
        clubs_with_coach_discovery: string;
        clubs_never_scraped: string;
        clubs_stale_14d: string;
        total: string;
      }>,
    );

    const total = Number(list[0]?.total ?? 0);
    const rows: CoverageLeagueAggRow[] = list.map((r) => ({
      leagueId: Number(r.league_id),
      leagueName: r.league_name,
      clubsTotal: Number(r.clubs_total ?? 0),
      clubsWithRosterSnapshot: Number(r.clubs_with_roster_snapshot ?? 0),
      clubsWithCoachDiscovery: Number(r.clubs_with_coach_discovery ?? 0),
      clubsNeverScraped: Number(r.clubs_never_scraped ?? 0),
      clubsStale14d: Number(r.clubs_stale_14d ?? 0),
    }));

    return { rows, total };
  },

  summarizeLeagues: async () => {
    // Single statement so the dashboard's KpiStrip + per-league
    // table stay consistent: both queries see the same snapshot of
    // `scrape_health.last_scraped_at` (it ticks during runs).
    //
    // `leagues_master` is left of the affiliation join so leagues
    // with zero affiliated clubs still count in `leaguesTotal`.
    // Subset counts use FILTER on the canonical-club id so a club
    // that appears in N league affiliations counts exactly once.
    const result = await defaultDb.execute<{
      leagues_total: string;
      clubs_total: string;
      clubs_with_roster_snapshot: string;
      clubs_with_coach_discovery: string;
      clubs_never_scraped: string;
      clubs_stale_14d: string;
    }>(sql`
      WITH joined AS (
        SELECT
          lm.id        AS league_id,
          cc.id        AS club_id,
          EXISTS (
            SELECT 1 FROM club_roster_snapshots crs WHERE crs.club_id = cc.id
          ) AS has_roster_snapshot,
          EXISTS (
            SELECT 1 FROM coach_discoveries cd WHERE cd.club_id = cc.id
          ) AS has_coach_discovery,
          sh.last_scraped_at
        FROM leagues_master lm
        LEFT JOIN club_affiliations ca
          ON ca.league_id = lm.id
        LEFT JOIN canonical_clubs cc
          ON cc.id = ca.club_id
        LEFT JOIN scrape_health sh
          ON sh.entity_type = 'club' AND sh.entity_id = cc.id
      )
      SELECT
        COUNT(DISTINCT league_id)::text AS leagues_total,
        COUNT(DISTINCT club_id)::text   AS clubs_total,
        COUNT(DISTINCT club_id) FILTER (WHERE has_roster_snapshot)::text
          AS clubs_with_roster_snapshot,
        COUNT(DISTINCT club_id) FILTER (WHERE has_coach_discovery)::text
          AS clubs_with_coach_discovery,
        COUNT(DISTINCT club_id) FILTER (
          WHERE club_id IS NOT NULL AND last_scraped_at IS NULL
        )::text AS clubs_never_scraped,
        COUNT(DISTINCT club_id) FILTER (
          WHERE last_scraped_at IS NOT NULL
            AND last_scraped_at < now() - make_interval(days => ${STALE_THRESHOLD_DAYS})
        )::text AS clubs_stale_14d
      FROM joined
    `);

    const row = Array.from(
      result as unknown as Array<{
        leagues_total: string;
        clubs_total: string;
        clubs_with_roster_snapshot: string;
        clubs_with_coach_discovery: string;
        clubs_never_scraped: string;
        clubs_stale_14d: string;
      }>,
    )[0];

    const summary: CoverageLeaguesSummary = {
      leaguesTotal: Number(row?.leagues_total ?? 0),
      clubsTotal: Number(row?.clubs_total ?? 0),
      clubsWithRosterSnapshot: Number(row?.clubs_with_roster_snapshot ?? 0),
      clubsWithCoachDiscovery: Number(row?.clubs_with_coach_discovery ?? 0),
      clubsNeverScraped: Number(row?.clubs_never_scraped ?? 0),
      clubsStale14d: Number(row?.clubs_stale_14d ?? 0),
    };

    // Persist today's snapshot. Idempotent within the UTC day — repeated
    // calls just rewrite the same row with the latest counters. We
    // intentionally don't fail the read if the upsert errors (e.g. the
    // table is missing on a deploy that hasn't migrated yet); the
    // KpiStrip is more important than the trend strip.
    try {
      await defaultDb.execute(sql`
        INSERT INTO coverage_history (
          snapshot_date,
          leagues_total,
          clubs_total,
          clubs_with_roster_snapshot,
          clubs_with_coach_discovery,
          clubs_never_scraped,
          clubs_stale_14d
        ) VALUES (
          (now() AT TIME ZONE 'UTC')::date,
          ${summary.leaguesTotal},
          ${summary.clubsTotal},
          ${summary.clubsWithRosterSnapshot},
          ${summary.clubsWithCoachDiscovery},
          ${summary.clubsNeverScraped},
          ${summary.clubsStale14d}
        )
        ON CONFLICT (snapshot_date) DO UPDATE SET
          leagues_total = EXCLUDED.leagues_total,
          clubs_total = EXCLUDED.clubs_total,
          clubs_with_roster_snapshot = EXCLUDED.clubs_with_roster_snapshot,
          clubs_with_coach_discovery = EXCLUDED.clubs_with_coach_discovery,
          clubs_never_scraped = EXCLUDED.clubs_never_scraped,
          clubs_stale_14d = EXCLUDED.clubs_stale_14d,
          recorded_at = now()
      `);
    } catch (err) {
      console.error("[coverage] failed to upsert coverage_history", err);
    }

    // Per-league snapshot — bulk-upsert today's row for every league in
    // one statement. Uses the same `league_clubs`/`league_agg` shape as
    // `listLeagues`, but feeds into INSERT instead of paginated SELECT
    // so all 127 leagues get a row per call (not just page 1). Same
    // best-effort try/catch policy as the global upsert above: a missing
    // table on a deploy that hasn't migrated yet must not break the
    // KpiStrip read.
    try {
      await defaultDb.execute(sql`
        WITH league_clubs AS (
          SELECT
            lm.id         AS league_id,
            cc.id         AS club_id,
            EXISTS (
              SELECT 1 FROM club_roster_snapshots crs WHERE crs.club_id = cc.id
            ) AS has_roster_snapshot,
            EXISTS (
              SELECT 1 FROM coach_discoveries cd WHERE cd.club_id = cc.id
            ) AS has_coach_discovery,
            sh.last_scraped_at
          FROM leagues_master lm
          LEFT JOIN club_affiliations ca
            ON ca.source_name = lm.league_name
          LEFT JOIN canonical_clubs cc
            ON cc.id = ca.club_id
          LEFT JOIN scrape_health sh
            ON sh.entity_type = 'club' AND sh.entity_id = cc.id
        ),
        league_agg AS (
          SELECT
            league_id,
            COUNT(DISTINCT club_id) AS clubs_total,
            COUNT(DISTINCT club_id) FILTER (WHERE has_roster_snapshot)
              AS clubs_with_roster_snapshot,
            COUNT(DISTINCT club_id) FILTER (WHERE has_coach_discovery)
              AS clubs_with_coach_discovery,
            COUNT(DISTINCT club_id) FILTER (
              WHERE club_id IS NOT NULL AND last_scraped_at IS NULL
            ) AS clubs_never_scraped,
            COUNT(DISTINCT club_id) FILTER (
              WHERE last_scraped_at IS NOT NULL
                AND last_scraped_at < now() - make_interval(days => ${STALE_THRESHOLD_DAYS})
            ) AS clubs_stale_14d
          FROM league_clubs
          GROUP BY league_id
        )
        INSERT INTO coverage_history_per_league (
          snapshot_date,
          league_id,
          clubs_total,
          clubs_with_roster_snapshot,
          clubs_with_coach_discovery,
          clubs_never_scraped,
          clubs_stale_14d
        )
        SELECT
          (now() AT TIME ZONE 'UTC')::date,
          league_id,
          clubs_total,
          clubs_with_roster_snapshot,
          clubs_with_coach_discovery,
          clubs_never_scraped,
          clubs_stale_14d
        FROM league_agg
        ON CONFLICT (snapshot_date, league_id) DO UPDATE SET
          clubs_total = EXCLUDED.clubs_total,
          clubs_with_roster_snapshot = EXCLUDED.clubs_with_roster_snapshot,
          clubs_with_coach_discovery = EXCLUDED.clubs_with_coach_discovery,
          clubs_never_scraped = EXCLUDED.clubs_never_scraped,
          clubs_stale_14d = EXCLUDED.clubs_stale_14d,
          recorded_at = now()
      `);
    } catch (err) {
      console.error(
        "[coverage] failed to upsert coverage_history_per_league",
        err,
      );
    }

    // Retention sweep — keep ~1 year of daily snapshots. The dashboard
    // only ever asks for the last 30 days, and there's no product
    // reason to hold multi-year history. Piggy-backed on the same
    // call path that writes today's row so we don't need a separate
    // scheduler entry; the DELETE is an indexed range scan over
    // `snapshot_date` and a no-op on every call after the first one
    // each day. Best-effort: a missing table on a fresh deploy must
    // not break the read.
    try {
      await defaultDb.execute(sql`
        DELETE FROM coverage_history
        WHERE snapshot_date < (now() AT TIME ZONE 'UTC')::date - INTERVAL '365 days'
      `);
    } catch (err) {
      console.error("[coverage] failed to prune coverage_history", err);
    }
    try {
      await defaultDb.execute(sql`
        DELETE FROM coverage_history_per_league
        WHERE snapshot_date < (now() AT TIME ZONE 'UTC')::date - INTERVAL '365 days'
      `);
    } catch (err) {
      console.error(
        "[coverage] failed to prune coverage_history_per_league",
        err,
      );
    }

    return summary;
  },

  getCoverageHistory: async ({ days }) => {
    // Oldest-first so the dashboard can pass straight into a sparkline.
    // Fetch the most recent `days` rows, then reverse to ASC.
    const result = await defaultDb.execute<{
      snapshot_date: Date | string;
      leagues_total: string;
      clubs_total: string;
      clubs_with_roster_snapshot: string;
      clubs_with_coach_discovery: string;
      clubs_never_scraped: string;
      clubs_stale_14d: string;
    }>(sql`
      SELECT
        snapshot_date,
        leagues_total::text                AS leagues_total,
        clubs_total::text                  AS clubs_total,
        clubs_with_roster_snapshot::text   AS clubs_with_roster_snapshot,
        clubs_with_coach_discovery::text   AS clubs_with_coach_discovery,
        clubs_never_scraped::text          AS clubs_never_scraped,
        clubs_stale_14d::text              AS clubs_stale_14d
      FROM coverage_history
      ORDER BY snapshot_date DESC
      LIMIT ${days}
    `);
    const list = Array.from(
      result as unknown as Array<{
        snapshot_date: Date | string;
        leagues_total: string;
        clubs_total: string;
        clubs_with_roster_snapshot: string;
        clubs_with_coach_discovery: string;
        clubs_never_scraped: string;
        clubs_stale_14d: string;
      }>,
    );
    const rows: CoverageHistoryRow[] = list.map((r) => ({
      snapshotDate:
        r.snapshot_date instanceof Date
          ? r.snapshot_date.toISOString().slice(0, 10)
          : String(r.snapshot_date).slice(0, 10),
      leaguesTotal: Number(r.leagues_total ?? 0),
      clubsTotal: Number(r.clubs_total ?? 0),
      clubsWithRosterSnapshot: Number(r.clubs_with_roster_snapshot ?? 0),
      clubsWithCoachDiscovery: Number(r.clubs_with_coach_discovery ?? 0),
      clubsNeverScraped: Number(r.clubs_never_scraped ?? 0),
      clubsStale14d: Number(r.clubs_stale_14d ?? 0),
    }));
    return rows.reverse();
  },

  getLeagueCoverageHistory: async ({ leagueId, days }) => {
    // Same shape as getCoverageHistory: pull most recent `days` rows in
    // DESC order (index hit on coverage_history_per_league_league_date_idx)
    // then reverse to ASC so the dashboard can pass straight into a
    // sparkline.
    const result = await defaultDb.execute<{
      snapshot_date: Date | string;
      clubs_total: string;
      clubs_with_roster_snapshot: string;
      clubs_with_coach_discovery: string;
      clubs_never_scraped: string;
      clubs_stale_14d: string;
    }>(sql`
      SELECT
        snapshot_date,
        clubs_total::text                  AS clubs_total,
        clubs_with_roster_snapshot::text   AS clubs_with_roster_snapshot,
        clubs_with_coach_discovery::text   AS clubs_with_coach_discovery,
        clubs_never_scraped::text          AS clubs_never_scraped,
        clubs_stale_14d::text              AS clubs_stale_14d
      FROM coverage_history_per_league
      WHERE league_id = ${leagueId}
      ORDER BY snapshot_date DESC
      LIMIT ${days}
    `);
    const list = Array.from(
      result as unknown as Array<{
        snapshot_date: Date | string;
        clubs_total: string;
        clubs_with_roster_snapshot: string;
        clubs_with_coach_discovery: string;
        clubs_never_scraped: string;
        clubs_stale_14d: string;
      }>,
    );
    const rows: CoverageLeagueHistoryRow[] = list.map((r) => ({
      snapshotDate:
        r.snapshot_date instanceof Date
          ? r.snapshot_date.toISOString().slice(0, 10)
          : String(r.snapshot_date).slice(0, 10),
      clubsTotal: Number(r.clubs_total ?? 0),
      clubsWithRosterSnapshot: Number(r.clubs_with_roster_snapshot ?? 0),
      clubsWithCoachDiscovery: Number(r.clubs_with_coach_discovery ?? 0),
      clubsNeverScraped: Number(r.clubs_never_scraped ?? 0),
      clubsStale14d: Number(r.clubs_stale_14d ?? 0),
    }));
    return rows.reverse();
  },

  findLeague: async ({ leagueId }) => {
    const result = await defaultDb.execute<{
      id: number;
      league_name: string;
    }>(sql`
      SELECT id, league_name
      FROM leagues_master
      WHERE id = ${leagueId}
      LIMIT 1
    `);
    const list = Array.from(
      result as unknown as Array<{ id: number; league_name: string }>,
    );
    const hit = list[0];
    if (!hit) return null;
    return { id: Number(hit.id), name: hit.league_name };
  },

  listClubsInLeague: async ({ leagueId, status, page, pageSize }) => {
    const offset = (page - 1) * pageSize;

    const statusPredicate =
      status === "never_scraped"
        ? sql`sh.last_scraped_at IS NULL`
        : status === "stale"
          ? sql`sh.last_scraped_at IS NOT NULL AND sh.last_scraped_at < now() - make_interval(days => ${STALE_THRESHOLD_DAYS})`
          : sql`TRUE`;

    const result = await defaultDb.execute<{
      club_id: number;
      club_name_canonical: string;
      last_scraped_at: Date | string | null;
      consecutive_failures: number | null;
      coach_count: string;
      has_roster_snapshot: boolean;
      staff_page_url: string | null;
      scrape_confidence: string | number | null;
      total: string;
    }>(sql`
      SELECT
        cc.id AS club_id,
        cc.club_name_canonical,
        sh.last_scraped_at,
        COALESCE(sh.consecutive_failures, 0) AS consecutive_failures,
        (
          SELECT COUNT(DISTINCT cd.coach_id)
          FROM coach_discoveries cd
          WHERE cd.club_id = cc.id
        )::text AS coach_count,
        EXISTS (
          SELECT 1 FROM club_roster_snapshots crs WHERE crs.club_id = cc.id
        ) AS has_roster_snapshot,
        cc.staff_page_url,
        cc.scrape_confidence,
        COUNT(*) OVER () AS total
      FROM club_affiliations ca
      JOIN canonical_clubs cc ON cc.id = ca.club_id
      LEFT JOIN scrape_health sh
        ON sh.entity_type = 'club' AND sh.entity_id = cc.id
      WHERE ca.league_id = ${leagueId}
        AND (${statusPredicate})
      GROUP BY
        cc.id,
        cc.club_name_canonical,
        sh.last_scraped_at,
        sh.consecutive_failures,
        cc.staff_page_url,
        cc.scrape_confidence
      ORDER BY sh.last_scraped_at ASC NULLS FIRST, cc.club_name_canonical ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const list = Array.from(
      result as unknown as Array<{
        club_id: number;
        club_name_canonical: string;
        last_scraped_at: Date | string | null;
        consecutive_failures: number | null;
        coach_count: string;
        has_roster_snapshot: boolean;
        staff_page_url: string | null;
        scrape_confidence: string | number | null;
        total: string;
      }>,
    );

    const total = Number(list[0]?.total ?? 0);
    const rows: CoverageLeagueDetailAggRow[] = list.map((r) => ({
      clubId: Number(r.club_id),
      clubNameCanonical: r.club_name_canonical,
      lastScrapedAt: r.last_scraped_at,
      consecutiveFailures: Number(r.consecutive_failures ?? 0),
      coachCount: Number(r.coach_count ?? 0),
      hasRosterSnapshot: Boolean(r.has_roster_snapshot),
      staffPageUrl: r.staff_page_url,
      scrapeConfidence:
        r.scrape_confidence === null || r.scrape_confidence === undefined
          ? null
          : Number(r.scrape_confidence),
    }));

    return { rows, total };
  },
};

// ---------------------------------------------------------------------------
// Router — prod-wired.
// ---------------------------------------------------------------------------

export function makeCoverageRouter(deps: CoverageDeps): IRouter {
  const router: IRouter = Router();
  router.get("/leagues", makeListLeaguesHandler(deps));
  // Static paths must be registered before the `:leagueId` param route
  // so Express doesn't capture "summary"/"history" as the leagueId.
  router.get("/leagues/summary", makeSummaryHandler(deps));
  router.get("/leagues/history", makeHistoryHandler(deps));
  // The `:leagueId/history` static suffix must be registered before the
  // bare `:leagueId` param route so Express doesn't capture "history"
  // as part of the param.
  router.get(
    "/leagues/:leagueId/history",
    makeLeagueHistoryHandler(deps),
  );
  router.get("/leagues/:leagueId", makeLeagueDetailHandler(deps));
  return router;
}

const coverageRouter: IRouter = makeCoverageRouter(prodCoverageDeps);

export default coverageRouter;
