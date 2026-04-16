import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { sql } from "drizzle-orm";
import { parsePagination } from "../lib/pagination";
import { normalizeClubName, PG_NORMALIZE_EXPR } from "../lib/analytics";

const router: IRouter = Router();

type Row = Record<string, unknown>;

async function execRows(query: ReturnType<typeof sql>): Promise<Row[]> {
  const result = await db.execute(query);
  if (Array.isArray(result)) return result as Row[];
  if (result && typeof result === "object" && "rows" in result) {
    return (result as { rows: Row[] }).rows;
  }
  return [];
}

router.get("/analytics/duplicates", async (req, res, next): Promise<void> => {
  try {
    const stateFilter = req.query.state as string | undefined;
    const minClubs = Math.max(2, Number(req.query.min_clubs) || 2);
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const stateCondition = stateFilter
      ? sql`AND lower(state) = lower(${stateFilter})`
      : sql``;

    const countRows = await execRows(sql`
      WITH normalized AS (
        SELECT
          id,
          ${sql.raw(PG_NORMALIZE_EXPR)} AS normalized_name,
          state
        FROM canonical_clubs
        WHERE status = 'active' OR status IS NULL
      ),
      clusters AS (
        SELECT normalized_name, state, COUNT(*) AS club_count
        FROM normalized
        WHERE length(normalized_name) >= 2
        ${stateCondition}
        GROUP BY normalized_name, state
        HAVING COUNT(*) >= ${minClubs}
      )
      SELECT COUNT(*)::int AS total FROM clusters
    `);

    const total = Number(countRows[0]?.total ?? 0);

    const dataRows = await execRows(sql`
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
          COUNT(*)::int AS club_count,
          array_agg(id ORDER BY id) AS club_ids,
          array_agg(club_name_canonical ORDER BY id) AS club_names
        FROM normalized
        WHERE length(normalized_name) >= 2
        ${stateCondition}
        GROUP BY normalized_name, state
        HAVING COUNT(*) >= ${minClubs}
      )
      SELECT
        c.*,
        (
          SELECT array_agg(DISTINCT ca.source_name ORDER BY ca.source_name)
          FROM club_affiliations ca
          WHERE ca.club_id = ANY(c.club_ids) AND ca.source_name IS NOT NULL
        ) AS sources
      FROM clusters c
      ORDER BY club_count DESC, normalized_name ASC
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
      })),
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

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

      const domains = Array.from(domainMap.entries()).map(([et, d]) => ({
        entity_type: et,
        total: d.total,
        ok: d.ok,
        stale: d.stale,
        failed: d.failed,
        never: d.never,
        freshness_pct:
          d.total > 0
            ? Math.round((d.ok / d.total) * 1000) / 10
            : 0,
        last_successful_run: domainSuccessMap.get(et) ?? null,
        runs_24h: Number(globalRun.runs_24h ?? 0),
        runs_7d: Number(globalRun.runs_7d ?? 0),
        records_touched_24h: Number(globalRun.records_touched_24h ?? 0),
        records_touched_7d: Number(globalRun.records_touched_7d ?? 0),
      }));

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
