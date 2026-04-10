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
        WHERE normalized_name <> ''
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
        WHERE normalized_name <> ''
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
      duplicates: dataRows
        .filter((r) => normalizeClubName(String(r.normalized_name ?? "")).length >= 2)
        .map((r) => ({
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

export default router;
