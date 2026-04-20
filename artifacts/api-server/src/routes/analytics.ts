import { Router, type IRouter, type Request } from "express";
import { db } from "@workspace/db";
import { sql } from "drizzle-orm";
import { AnalyticsDuplicatesReviewBody } from "@hlbiv/api-zod";
import { parsePagination } from "../lib/pagination";
import { normalizeClubName, PG_NORMALIZE_EXPR } from "../lib/analytics";

const router: IRouter = Router();

type Row = Record<string, unknown>;

/**
 * Module-level query executor. Default wraps the live `db`; tests can
 * swap it via `__setExecRowsForTests` to exercise this router against
 * an in-memory fake without needing a real Postgres. Kept here rather
 * than threaded through every handler signature so the route bodies
 * stay readable. Mirrors the testing pattern used by `apiKeyAuth`
 * (which accepts a lookup fn via a factory) — only difference is we
 * have many handlers, so we share one module-local hook instead of
 * rewriting the router into a factory.
 */
type ExecRowsFn = (query: ReturnType<typeof sql>) => Promise<Row[]>;

const defaultExecRows: ExecRowsFn = async (query) => {
  const result = await db.execute(query);
  if (Array.isArray(result)) return result as Row[];
  if (result && typeof result === "object" && "rows" in result) {
    return (result as { rows: Row[] }).rows;
  }
  return [];
};

let currentExecRows: ExecRowsFn = defaultExecRows;

async function execRows(query: ReturnType<typeof sql>): Promise<Row[]> {
  return currentExecRows(query);
}

/** Test hook — swap the query executor. Production code must not call this. */
export function __setExecRowsForTests(fn: ExecRowsFn | null): void {
  currentExecRows = fn ?? defaultExecRows;
}

const REVIEW_STATUS_VALUES = ["pending", "all", "rejected", "merged"] as const;
type ReviewStatus = (typeof REVIEW_STATUS_VALUES)[number];

function parseReviewStatus(raw: unknown): ReviewStatus {
  if (typeof raw === "string" && (REVIEW_STATUS_VALUES as readonly string[]).includes(raw)) {
    return raw as ReviewStatus;
  }
  return "pending";
}

router.get("/analytics/duplicates", async (req, res, next): Promise<void> => {
  try {
    const stateFilter = req.query.state as string | undefined;
    const minClubs = Math.max(2, Number(req.query.min_clubs) || 2);
    const status = parseReviewStatus(req.query.status);
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const stateCondition = stateFilter
      ? sql`AND lower(state) = lower(${stateFilter})`
      : sql``;

    // Per-status review filter applied to the pair stream:
    //   pending  — exclude pairs that have a merged/rejected decision
    //   all      — no filter
    //   rejected — only pairs with decision = 'rejected'
    //   merged   — only pairs with decision = 'merged'
    //
    // A pair with decision = 'pending' is treated the same as a pair with
    // no row at all: both fall into the "pending" default view. That keeps
    // the write API simple (decision=pending is a no-op as far as filtering
    // is concerned but still records the reviewer's touch).
    const reviewFilter =
      status === "pending"
        ? sql`(drd.decision IS NULL OR drd.decision = 'pending')`
        : status === "all"
          ? sql`TRUE`
          : status === "rejected"
            ? sql`drd.decision = 'rejected'`
            : sql`drd.decision = 'merged'`;

    // Build the pair stream once. Clusters of N clubs expand to C(N, 2)
    // pairs, normalized so a.id < b.id. We LEFT JOIN the review row,
    // apply the status filter, and page the result.
    //
    // Kept as one SQL block for correctness: COUNT(*) and the paged SELECT
    // must see identical predicates, including the review-status filter.
    const pairCte = sql`
      WITH normalized AS (
        SELECT
          id,
          club_name_canonical,
          state,
          ${sql.raw(PG_NORMALIZE_EXPR)} AS normalized_name
        FROM canonical_clubs
        WHERE status = 'active' OR status IS NULL
      ),
      clusters AS (
        SELECT
          normalized_name,
          state,
          array_agg(id ORDER BY id) AS club_ids,
          array_agg(club_name_canonical ORDER BY id) AS club_names,
          COUNT(*)::int AS club_count
        FROM normalized
        WHERE length(normalized_name) >= 2
        ${stateCondition}
        GROUP BY normalized_name, state
        HAVING COUNT(*) >= ${minClubs}
      ),
      cluster_pairs AS (
        SELECT
          c.normalized_name,
          c.state,
          c.club_count,
          c.club_ids,
          c.club_names,
          a.id AS club_a_id,
          b.id AS club_b_id,
          a.club_name_canonical AS club_a_name,
          b.club_name_canonical AS club_b_name
        FROM clusters c
        JOIN normalized a
          ON a.id = ANY(c.club_ids)
        JOIN normalized b
          ON b.id = ANY(c.club_ids)
         AND b.id > a.id
      ),
      pairs_with_review AS (
        SELECT
          cp.*,
          drd.decision,
          drd.decided_by,
          drd.decided_at,
          drd.notes
        FROM cluster_pairs cp
        LEFT JOIN duplicate_review_decisions drd
          ON drd.club_a_id = cp.club_a_id
         AND drd.club_b_id = cp.club_b_id
        WHERE ${reviewFilter}
      )
    `;

    const countRows = await execRows(sql`
      ${pairCte}
      SELECT COUNT(*)::int AS total FROM pairs_with_review
    `);
    const total = Number(countRows[0]?.total ?? 0);

    const dataRows = await execRows(sql`
      ${pairCte}
      SELECT
        pwr.*,
        (
          SELECT array_agg(DISTINCT ca.source_name ORDER BY ca.source_name)
          FROM club_affiliations ca
          WHERE ca.club_id = ANY(pwr.club_ids)
            AND ca.source_name IS NOT NULL
        ) AS sources
      FROM pairs_with_review pwr
      ORDER BY pwr.club_count DESC, pwr.normalized_name ASC, pwr.club_a_id ASC, pwr.club_b_id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    res.json({
      duplicates: dataRows.map((r) => ({
        normalized_name: r.normalized_name,
        state: r.state ?? null,
        club_count: Number(r.club_count),
        club_ids: r.club_ids,
        club_names: r.club_names,
        sources: (r.sources as string[] | null) ?? [],
        club_a_id: Number(r.club_a_id),
        club_b_id: Number(r.club_b_id),
        club_a_name: r.club_a_name as string,
        club_b_name: r.club_b_name as string,
        review: r.decision
          ? {
              decision: r.decision as string,
              decided_by: (r.decided_by as string | null) ?? null,
              decided_at: r.decided_at ?? null,
              notes: (r.notes as string | null) ?? null,
            }
          : null,
      })),
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

// -----------------------------------------------------------------------
// POST /api/analytics/duplicates/review
// Record (upsert) a review decision for a pair of canonical clubs. This
// endpoint is additive and usable whether API-key auth is enabled or not;
// `decided_by` is populated from `req.apiKey?.name` when the middleware
// has run, else left null. A follow-up PR will gate this endpoint with
// `requireScope('admin')`.
// -----------------------------------------------------------------------

router.post(
  "/analytics/duplicates/review",
  async (req: Request, res, next): Promise<void> => {
    try {
      const parsed = AnalyticsDuplicatesReviewBody.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({
          error: "invalid_body",
          details: parsed.error.issues,
        });
        return;
      }
      const body = parsed.data;
      if (body.club_a_id === body.club_b_id) {
        res.status(400).json({ error: "self_pair_not_allowed" });
        return;
      }

      // Normalize so club_a_id < club_b_id. This lets the UI POST the pair
      // in whichever order it received from GET; the DB unique index + CHECK
      // only accept the normalized form.
      const [clubA, clubB] =
        body.club_a_id < body.club_b_id
          ? [body.club_a_id, body.club_b_id]
          : [body.club_b_id, body.club_a_id];

      const decidedBy = req.apiKey?.name ?? null;
      const notes = body.notes ?? null;

      // Verify both clubs exist — otherwise the FK would fail with a
      // 500-level Postgres error, which is a poor UX. A pre-check is
      // cheap here.
      const existsRows = await execRows(sql`
        SELECT id FROM canonical_clubs WHERE id IN (${clubA}, ${clubB})
      `);
      if (existsRows.length < 2) {
        res.status(400).json({ error: "unknown_club_id" });
        return;
      }

      // Upsert keyed on the normalized pair. `decided_at` is refreshed on
      // update so the UI can show "most recently touched".
      const rows = await execRows(sql`
        INSERT INTO duplicate_review_decisions
          (club_a_id, club_b_id, decision, decided_by, notes)
        VALUES (${clubA}, ${clubB}, ${body.decision}, ${decidedBy}, ${notes})
        ON CONFLICT (club_a_id, club_b_id) DO UPDATE SET
          decision   = EXCLUDED.decision,
          decided_by = EXCLUDED.decided_by,
          decided_at = NOW(),
          notes      = EXCLUDED.notes
        RETURNING id, club_a_id, club_b_id, decision, decided_by,
                  decided_at, notes
      `);

      const row = rows[0];
      if (!row) {
        // Should be unreachable — the INSERT ... ON CONFLICT DO UPDATE
        // always returns a row. Kept for defensive safety.
        res.status(500).json({ error: "write_failed" });
        return;
      }

      res.json({
        id: Number(row.id),
        club_a_id: Number(row.club_a_id),
        club_b_id: Number(row.club_b_id),
        decision: row.decision as string,
        decided_by: (row.decided_by as string | null) ?? null,
        decided_at: row.decided_at ?? null,
        notes: (row.notes as string | null) ?? null,
      });
    } catch (err) {
      next(err);
    }
  },
);

router.get("/analytics/coverage", async (req, res, next): Promise<void> => {
  try {
    const minClubs = Math.max(1, Number(req.query.min_clubs) || 5);

    const [stateRows, leagueRows, totalRows, websiteRows] = await Promise.all([
      execRows(sql`
        SELECT
          COALESCE(state, 'Unknown') AS state,
          COUNT(*)::int AS club_count
        FROM canonical_clubs
        WHERE status = 'active' OR status IS NULL
        GROUP BY state
        ORDER BY club_count ASC, state ASC
      `),
      execRows(sql`
        SELECT
          source_name AS league,
          COUNT(DISTINCT club_id)::int AS club_count
        FROM club_affiliations
        WHERE source_name IS NOT NULL
        GROUP BY source_name
        ORDER BY club_count DESC, source_name ASC
      `),
      execRows(sql`
        SELECT COUNT(*)::int AS total
        FROM canonical_clubs
        WHERE status = 'active' OR status IS NULL
      `),
      execRows(sql`
        SELECT
          COUNT(*)::int AS total,
          COUNT(CASE WHEN website IS NOT NULL AND website <> '' THEN 1 END)::int AS with_website
        FROM canonical_clubs
        WHERE status = 'active' OR status IS NULL
      `),
    ]);

    const totalClubs = Number(totalRows[0]?.total ?? 0);
    const withWebsite = Number(websiteRows[0]?.with_website ?? 0);
    const withoutWebsite = totalClubs - withWebsite;

    const stateSummary = stateRows.map((r) => ({
      state: r.state as string,
      club_count: Number(r.club_count),
      below_threshold: Number(r.club_count) < minClubs,
    }));

    const gapCount = stateSummary.filter((s) => s.below_threshold).length;

    res.json({
      summary: {
        total_clubs: totalClubs,
        total_states: stateSummary.length,
        states_below_threshold: gapCount,
        threshold: minClubs,
        website_coverage: {
          with_website: withWebsite,
          without_website: withoutWebsite,
          pct: totalClubs > 0
            ? Math.round((withWebsite / totalClubs) * 1000) / 10
            : 0,
        },
      },
      states: stateSummary,
      leagues: leagueRows.map((r) => ({
        league: r.league as string,
        club_count: Number(r.club_count),
      })),
    });
  } catch (err) {
    next(err);
  }
});

router.get("/analytics/overlap", async (req, res, next): Promise<void> => {
  try {
    const minLeagues = Math.max(2, Number(req.query.min_leagues) || 2);
    const stateFilter = req.query.state as string | undefined;
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const stateCondition = stateFilter
      ? sql`AND lower(cc.state) = lower(${stateFilter})`
      : sql``;

    const [countRows, dataRows] = await Promise.all([
      execRows(sql`
        WITH multi_league AS (
          SELECT club_id, COUNT(DISTINCT source_name)::int AS league_count
          FROM club_affiliations
          WHERE source_name IS NOT NULL
          GROUP BY club_id
          HAVING COUNT(DISTINCT source_name) >= ${minLeagues}
        )
        SELECT COUNT(*)::int AS total
        FROM multi_league ml
        JOIN canonical_clubs cc ON cc.id = ml.club_id
        WHERE 1=1 ${stateCondition}
      `),
      execRows(sql`
        WITH multi_league AS (
          SELECT
            club_id,
            COUNT(DISTINCT source_name)::int AS league_count,
            array_agg(DISTINCT source_name ORDER BY source_name) AS leagues,
            array_agg(DISTINCT COALESCE(gender_program, 'unknown') ORDER BY COALESCE(gender_program, 'unknown')) AS gender_programs
          FROM club_affiliations
          WHERE source_name IS NOT NULL
          GROUP BY club_id
          HAVING COUNT(DISTINCT source_name) >= ${minLeagues}
        )
        SELECT
          cc.id,
          cc.club_name_canonical,
          ${sql.raw(PG_NORMALIZE_EXPR)} AS normalized_name,
          cc.city,
          cc.state,
          ml.league_count,
          ml.leagues,
          ml.gender_programs
        FROM multi_league ml
        JOIN canonical_clubs cc ON cc.id = ml.club_id
        WHERE 1=1 ${stateCondition}
        ORDER BY ml.league_count DESC, cc.club_name_canonical ASC
        LIMIT ${pageSize} OFFSET ${offset}
      `),
    ]);

    const total = Number(countRows[0]?.total ?? 0);

    res.json({
      clubs: dataRows.map((r) => ({
        id: Number(r.id),
        club_name_canonical: r.club_name_canonical as string,
        normalized_name: r.normalized_name as string,
        city: r.city ?? null,
        state: r.state ?? null,
        league_count: Number(r.league_count),
        leagues: r.leagues as string[],
        gender_programs: r.gender_programs as string[],
      })),
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

router.get("/analytics/summary", async (_req, res, next): Promise<void> => {
  try {
    const safeCount = async (table: string): Promise<number> => {
      try {
        const rows = await execRows(
          sql.raw(`SELECT COUNT(*)::int AS n FROM ${table}`),
        );
        return Number(rows[0]?.n ?? 0);
      } catch {
        return 0;
      }
    };

    const [
      canonicalClubs,
      clubAffiliations,
      clubAliases,
      coachDiscoveries,
      leagues,
      events,
      eventTeams,
      matches,
      clubResults,
      rosterSnapshots,
      rosterDiffs,
      tryouts,
      colleges,
      collegeCoaches,
      scrapeRunLogs,
      scrapeHealth,
    ] = await Promise.all([
      execRows(
        sql`SELECT COUNT(*)::int AS n FROM canonical_clubs WHERE status = 'active' OR status IS NULL`,
      ).then((r) => Number(r[0]?.n ?? 0)),
      safeCount("club_affiliations"),
      safeCount("club_aliases"),
      safeCount("coach_discoveries"),
      safeCount("leagues_master"),
      safeCount("events"),
      safeCount("event_teams"),
      safeCount("matches"),
      safeCount("club_results"),
      safeCount("club_roster_snapshots"),
      safeCount("roster_diffs"),
      safeCount("tryouts"),
      safeCount("colleges"),
      safeCount("college_coaches"),
      safeCount("scrape_run_logs"),
      safeCount("scrape_health"),
    ]);

    res.json({
      generated_at: new Date().toISOString(),
      domains: {
        d1_clubs: { canonical_clubs: canonicalClubs, club_affiliations: clubAffiliations, club_aliases: clubAliases },
        d2_colleges: { colleges, college_coaches: collegeCoaches },
        d3_coaches: { coach_discoveries: coachDiscoveries },
        d4_events: { events, event_teams: eventTeams },
        d5_matches: { matches, club_results: clubResults },
        d6_rosters: { roster_snapshots: rosterSnapshots, roster_diffs: rosterDiffs },
        d7_tryouts: { tryouts },
        d8_scrape_health: { scrape_run_logs: scrapeRunLogs, scrape_health: scrapeHealth },
      },
      totals: { leagues },
    });
  } catch (err) {
    next(err);
  }
});

router.get(
  "/analytics/scrape-health",
  async (req, res, next): Promise<void> => {
    try {
      const now = sql`NOW()`;

      // 1. Domain freshness from scrape_health
      const domainRows = await execRows(sql`
        SELECT entity_type, status, COUNT(*)::int AS cnt
        FROM scrape_health
        GROUP BY entity_type, status
        ORDER BY entity_type, status
      `);

      // Pivot into per-domain objects
      const domainMap = new Map<
        string,
        { total: number; ok: number; stale: number; failed: number; never: number }
      >();
      for (const r of domainRows) {
        const et = r.entity_type as string;
        const st = r.status as string;
        const cnt = Number(r.cnt);
        if (!domainMap.has(et)) {
          domainMap.set(et, { total: 0, ok: 0, stale: 0, failed: 0, never: 0 });
        }
        const d = domainMap.get(et)!;
        d.total += cnt;
        if (st === "ok" || st === "stale" || st === "failed" || st === "never") {
          d[st] += cnt;
        }
      }

      // Per-domain last_successful_run from scrape_health itself
      const domainSuccessRows = await execRows(sql`
        SELECT entity_type, MAX(last_success_at) AS last_successful_run
        FROM scrape_health
        GROUP BY entity_type
      `);

      const domainSuccessMap = new Map<string, unknown>();
      for (const r of domainSuccessRows) {
        domainSuccessMap.set(r.entity_type as string, r.last_successful_run);
      }

      // Global run stats from scrape_run_logs (no entity_type column, so these are cross-domain)
      const globalRunRows = await execRows(sql`
        SELECT
          COUNT(CASE WHEN started_at >= ${now} - INTERVAL '24 hours' THEN 1 END)::int AS runs_24h,
          COUNT(CASE WHEN started_at >= ${now} - INTERVAL '7 days' THEN 1 END)::int AS runs_7d,
          COALESCE(SUM(CASE WHEN started_at >= ${now} - INTERVAL '24 hours' THEN records_touched ELSE 0 END), 0)::int AS records_touched_24h,
          COALESCE(SUM(CASE WHEN started_at >= ${now} - INTERVAL '7 days' THEN records_touched ELSE 0 END), 0)::int AS records_touched_7d
        FROM scrape_run_logs
      `);

      const globalRun = globalRunRows[0] ?? {};

      // SLA thresholds per entity type (mirrors scraper/config/freshness_sla.py)
      const SLA_HOURS: Record<string, number> = {
        club: 168, league: 720, college: 168, coach: 168,
        event: 24, match: 24, tryout: 168,
      };
      const DEFAULT_SLA = 168;

      const domains = Array.from(domainMap.entries()).map(([et, d]) => {
        const freshness_pct =
          d.total > 0
            ? Math.round((d.ok / d.total) * 1000) / 10
            : 0;
        const sla_hours = SLA_HOURS[et] ?? DEFAULT_SLA;
        return {
          entity_type: et,
          total: d.total,
          ok: d.ok,
          stale: d.stale,
          failed: d.failed,
          never: d.never,
          freshness_pct,
          sla_hours,
          sla_breached: freshness_pct < 100,
          last_successful_run: domainSuccessMap.get(et) ?? null,
          runs_24h: Number(globalRun.runs_24h ?? 0),
          runs_7d: Number(globalRun.runs_7d ?? 0),
          records_touched_24h: Number(globalRun.records_touched_24h ?? 0),
          records_touched_7d: Number(globalRun.records_touched_7d ?? 0),
        };
      });

      // 2. Per-scraper run history
      const scraperRows = await execRows(sql`
        WITH latest AS (
          SELECT DISTINCT ON (scraper_key)
            scraper_key,
            league_name,
            started_at AS last_run_at,
            status AS last_status,
            failure_kind AS last_failure_kind
          FROM scrape_run_logs
          ORDER BY scraper_key, started_at DESC
        ),
        agg AS (
          SELECT
            scraper_key,
            COUNT(CASE WHEN started_at >= ${now} - INTERVAL '24 hours' THEN 1 END)::int AS runs_24h,
            COUNT(CASE WHEN started_at >= ${now} - INTERVAL '7 days' THEN 1 END)::int AS runs_7d,
            COALESCE(SUM(CASE WHEN started_at >= ${now} - INTERVAL '7 days' THEN records_touched ELSE 0 END), 0)::int AS total_records_touched_7d
          FROM scrape_run_logs
          GROUP BY scraper_key
        )
        SELECT
          l.scraper_key,
          l.league_name,
          l.last_run_at,
          l.last_status,
          l.last_failure_kind,
          COALESCE(a.runs_24h, 0)::int AS runs_24h,
          COALESCE(a.runs_7d, 0)::int AS runs_7d,
          COALESCE(a.total_records_touched_7d, 0)::int AS total_records_touched_7d
        FROM latest l
        LEFT JOIN agg a ON a.scraper_key = l.scraper_key
        ORDER BY l.last_run_at DESC
      `);

      const scraper_runs = scraperRows.map((r) => ({
        scraper_key: r.scraper_key as string,
        league_name: (r.league_name as string) ?? null,
        last_run_at: r.last_run_at ?? null,
        last_status: r.last_status as string,
        last_failure_kind: (r.last_failure_kind as string) ?? null,
        runs_24h: Number(r.runs_24h),
        runs_7d: Number(r.runs_7d),
        total_records_touched_7d: Number(r.total_records_touched_7d),
      }));

      // 3. Summary derived from domain data
      const totalEntities = domains.reduce((s, d) => s + d.total, 0);
      const totalOk = domains.reduce((s, d) => s + d.ok, 0);
      const scrapersFailing = scraper_runs.filter(
        (r) => r.last_status === "failed",
      ).length;
      const lastAnyRun =
        scraper_runs.length > 0 ? scraper_runs[0].last_run_at : null;

      res.json({
        domains,
        scraper_runs,
        summary: {
          total_entities: totalEntities,
          healthy_pct:
            totalEntities > 0
              ? Math.round((totalOk / totalEntities) * 1000) / 10
              : 0,
          scrapers_failing: scrapersFailing,
          last_any_run: lastAnyRun,
        },
      });
    } catch (err) {
      next(err);
    }
  },
);

export default router;
