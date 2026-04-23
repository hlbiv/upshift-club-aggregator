/**
 * `/api/v1/admin/pro-academies/*` — operator-edit surface for the
 * `canonical_clubs.is_pro_academy` curated allow-list.
 *
 *   GET   /api/v1/admin/pro-academies
 *   PATCH /api/v1/admin/pro-academies/:clubId
 *
 * Why this exists
 * ---------------
 * The curated pro-academy allow-list previously lived in
 * `scripts/src/seed-pro-academies.ts` (PRO_ACADEMY_NAMES). Adding or
 * removing a club required a code change + redeploy. Operators
 * reconciling the ~205 borderline candidates surfaced by
 * `scripts/src/backfill-competitive-tier.ts` need a dashboard view that:
 *
 *   1. Lists every canonical club with a tier-1 academy-family
 *      affiliation (MLS NEXT / NWSL Academy / USL Academy).
 *   2. Shows the current `is_pro_academy` flag + rolled-up
 *      `competitive_tier`.
 *   3. Lets them flip the flag inline; the flip persists and re-runs the
 *      per-club tier rollup so `competitive_tier` stays consistent.
 *
 * Decision rule (mirrors backfill-competitive-tier.ts, kept in sync by
 * test). For one club:
 *
 *   - top_tn = MIN(tier_numeric) over the club's mapped affiliations
 *     (`norm_tier IS NOT NULL`).
 *   - academy iff cc.is_pro_academy = TRUE AND top_tn = 1 AND at least
 *     one of those tier-1 affiliations is in ACADEMY_FAMILIES.
 *   - else 'elite' if any top-tier mapped affiliation is normalized to
 *     'elite'; otherwise 'competitive' (the default).
 *
 * `TIER_LABEL_TO_ENUM` and `ACADEMY_FAMILIES` are duplicated from the
 * backfill script with a top-of-file pointer comment — the api-server
 * package can't import from `@workspace/scripts`. The duplication is
 * tested by `__tests__/adminProAcademies.test.ts` to catch drift.
 */
import { Router, type IRouter, type RequestHandler } from "express";
import { sql } from "drizzle-orm";
import { db as defaultDb } from "@workspace/db";
import {
  ProAcademiesRequest,
  ProAcademiesResponse,
  UpdateProAcademyRequest,
  UpdateProAcademyResponse,
  type CompetitiveTier,
} from "@hlbiv/api-zod/admin";

// ---------------------------------------------------------------------------
// Tier rollup constants — MIRRORED from
// scripts/src/backfill-competitive-tier.ts. Keep the two in sync; drift is
// caught by the tier-rollup parity test.
// ---------------------------------------------------------------------------

/** league_family labels that flip a tier-1 affiliation to 'academy' (when the curated flag is also TRUE). */
export const ACADEMY_FAMILIES = ["MLS NEXT", "NWSL Academy", "USL Academy"];

/** leagues_master.tier_label → competitive_tier. Unmapped → NULL (no contribution). */
export const TIER_LABEL_TO_ENUM: Record<string, "elite" | "competitive"> = {
  "National Elite": "elite",
  "National Elite / High National": "elite",
  "National Elite / Pro Pathway": "elite",
  "National / Regional High Performance": "elite",
  "Pre-Elite Development": "elite",
  "NPL Member League": "competitive",
  "Regional Power League": "competitive",
  "Regional Tournament": "competitive",
  "State Association / League Hub": "competitive",
};

/** Build the SQL CASE expression from TIER_LABEL_TO_ENUM. */
function buildNormalizedCaseSql() {
  const cases = Object.entries(TIER_LABEL_TO_ENUM).map(
    ([label, tier]) => sql`WHEN ${label} THEN ${tier}`,
  );
  return sql`CASE lm.tier_label ${sql.join(cases, sql` `)} ELSE NULL END`;
}

const NORMALIZED_CASE_SQL = buildNormalizedCaseSql();
const ACADEMY_FAMILIES_LITERAL = sql`ARRAY[${sql.join(
  ACADEMY_FAMILIES.map((f) => sql`${f}`),
  sql`, `,
)}]::text[]`;

// ---------------------------------------------------------------------------
// Result-shape helper.
// ---------------------------------------------------------------------------
//
// `drizzle-orm/node-postgres` returns the underlying `pg.QueryResult` from
// `db.execute(sql\`...\`)` — i.e. `{ rows: [...], rowCount, fields, ... }`.
// The QueryResult is NOT iterable, so `Array.from(result)` silently yields
// `[]`. Other admin handlers in this repo use the `Array.from(result as
// unknown as Array<...>)` pattern; that pattern only happens to "work"
// when the underlying queries also return zero rows in test environments.
// We extract via this helper so the handler is correct regardless of
// which shape execute() returns now or in the future.
export function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  if (result && typeof result === "object" && "rows" in result) {
    const rows = (result as { rows: unknown }).rows;
    if (Array.isArray(rows)) return rows as T[];
  }
  return [];
}

// ---------------------------------------------------------------------------
// LIST handler.
// ---------------------------------------------------------------------------

interface ListRowSql extends Record<string, unknown> {
  club_id: number;
  club_name_canonical: string;
  city: string | null;
  state: string | null;
  is_pro_academy: boolean;
  competitive_tier: CompetitiveTier;
  // jsonb arrays from json_agg — tolerated as unknown at the boundary.
  families: unknown;
  affiliations: unknown;
  affiliation_count: number | string;
  total: number | string;
}

interface AffiliationRowSql {
  league_id: number | null;
  league_name: string;
  league_family: string;
  gender_program: string | null;
}

/**
 * GET /v1/admin/pro-academies?flag=all|flagged|unflagged&page=&page_size=
 */
export const listProAcademiesHandler: RequestHandler = async (
  req,
  res,
  next,
) => {
  try {
    const parsed = ProAcademiesRequest.safeParse({
      flag: toStringOrUndefined(req.query.flag),
      page: toNumberOrUndefined(req.query.page),
      pageSize:
        toNumberOrUndefined(req.query.page_size) ??
        toNumberOrUndefined(req.query.pageSize),
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid query params" });
      return;
    }
    const { flag, page, pageSize } = parsed.data;
    const offset = (page - 1) * pageSize;

    // Filter by current is_pro_academy flag, applied AFTER the
    // academy-family affiliation predicate so the row set is always
    // bounded to candidates worth reviewing.
    const flagPredicate =
      flag === "flagged"
        ? sql`AND cc.is_pro_academy = TRUE`
        : flag === "unflagged"
          ? sql`AND cc.is_pro_academy = FALSE`
          : sql``;

    // The `candidates` CTE is the universe of academy-family affiliations
    // at tier 1 (the only tier that can flip a row to 'academy'). We
    // aggregate to a per-club summary, then page over it. `flaggedTotal`
    // is computed alongside via a window — single query, no extra
    // round-trip.
    const result = await defaultDb.execute<ListRowSql>(sql`
      WITH normalized AS (
        SELECT
          ca.club_id,
          ca.gender_program,
          lm.id            AS league_id,
          lm.league_name,
          lm.league_family,
          lm.tier_numeric,
          ${NORMALIZED_CASE_SQL} AS norm_tier
        FROM club_affiliations ca
        JOIN leagues_master lm
          ON ca.league_id = lm.id
          OR (ca.league_id IS NULL AND ca.source_name = lm.league_name)
        WHERE ca.club_id IS NOT NULL
      ),
      academy_aff AS (
        SELECT
          n.club_id,
          n.league_id,
          n.league_name,
          n.league_family,
          n.gender_program
        FROM normalized n
        WHERE n.tier_numeric = 1
          AND n.league_family = ANY(${ACADEMY_FAMILIES_LITERAL})
      ),
      per_club AS (
        SELECT
          cc.id                              AS club_id,
          cc.club_name_canonical,
          cc.city,
          cc.state,
          cc.is_pro_academy,
          cc.competitive_tier,
          (
            SELECT json_agg(DISTINCT a.league_family ORDER BY a.league_family)
            FROM academy_aff a WHERE a.club_id = cc.id
          ) AS families,
          (
            SELECT json_agg(json_build_object(
              'league_id', a.league_id,
              'league_name', a.league_name,
              'league_family', a.league_family,
              'gender_program', a.gender_program
            ) ORDER BY a.league_family, a.league_name, a.gender_program)
            FROM academy_aff a WHERE a.club_id = cc.id
          ) AS affiliations,
          (SELECT COUNT(*) FROM academy_aff a WHERE a.club_id = cc.id)
            AS affiliation_count
        FROM canonical_clubs cc
        WHERE EXISTS (SELECT 1 FROM academy_aff a WHERE a.club_id = cc.id)
      ),
      filtered AS (
        SELECT pc.*
        FROM per_club pc, canonical_clubs cc
        WHERE cc.id = pc.club_id ${flagPredicate}
      )
      SELECT
        f.*,
        COUNT(*) OVER () AS total
      FROM filtered f
      ORDER BY
        f.is_pro_academy DESC,
        f.club_name_canonical ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const list = rowsOf<ListRowSql>(result);
    const total = Number(list[0]?.total ?? 0);

    // Compute flaggedTotal in a separate small query — keeps the main
    // query simple, and the cardinality is small enough that a second
    // round-trip is cheap.
    const flaggedRes = await defaultDb.execute<{ n: string }>(sql`
      SELECT COUNT(*)::text AS n FROM canonical_clubs WHERE is_pro_academy = TRUE
    `);
    const flaggedTotal = Number(rowsOf<{ n: string }>(flaggedRes)[0]?.n ?? 0);

    const rows = list.map((r) => ({
      clubId: r.club_id,
      clubNameCanonical: r.club_name_canonical,
      city: r.city,
      state: r.state,
      isProAcademy: r.is_pro_academy,
      competitiveTier: r.competitive_tier,
      families: Array.isArray(r.families) ? (r.families as string[]) : [],
      affiliations: Array.isArray(r.affiliations)
        ? (r.affiliations as AffiliationRowSql[]).map((a) => ({
            leagueId: a.league_id,
            leagueName: a.league_name,
            leagueFamily: a.league_family,
            genderProgram: a.gender_program,
          }))
        : [],
      affiliationCount: Number(r.affiliation_count ?? 0),
    }));

    res.json(
      ProAcademiesResponse.parse({
        rows,
        total,
        flaggedTotal,
        page,
        pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
};

// ---------------------------------------------------------------------------
// PATCH handler.
// ---------------------------------------------------------------------------

/**
 * Compute the post-update competitive_tier for one club using the
 * supplied `nextFlag` value. Mirrors the rollup decision rule in
 * `scripts/src/backfill-competitive-tier.ts`. `null` is returned when
 * the club has no mapped affiliations at all — the caller should keep
 * the default 'competitive' tier in that case so a flag-only flip on a
 * brand-new row doesn't drop it below default.
 */
/**
 * Minimal tx-or-db shape we need for the rollup. Avoids depending on
 * drizzle's PgTransaction generics — the body of `db.transaction(fn)`
 * passes a slightly narrower type than `defaultDb`, but both expose
 * `.execute(sql)`.
 */
type SqlRunner = Pick<typeof defaultDb, "execute">;

export async function computeTierForClub(
  clubId: number,
  nextFlag: boolean,
  tx: SqlRunner,
): Promise<CompetitiveTier> {
  const decision = await tx.execute<{ final_tier: CompetitiveTier }>(sql`
    WITH normalized AS (
      SELECT
        lm.tier_numeric,
        lm.league_family,
        ${NORMALIZED_CASE_SQL} AS norm_tier
      FROM club_affiliations ca
      JOIN leagues_master lm
        ON ca.league_id = lm.id
        OR (ca.league_id IS NULL AND ca.source_name = lm.league_name)
      WHERE ca.club_id = ${clubId}
    ),
    top_tier AS (
      SELECT MIN(tier_numeric) AS min_tn FROM normalized WHERE norm_tier IS NOT NULL
    ),
    flags AS (
      SELECT
        bool_or(n.league_family = ANY(${ACADEMY_FAMILIES_LITERAL})) AS any_academy,
        bool_or(n.norm_tier = 'elite') AS has_elite
      FROM normalized n, top_tier t
      WHERE n.norm_tier IS NOT NULL AND n.tier_numeric = t.min_tn
    )
    SELECT (
      CASE
        WHEN (SELECT min_tn FROM top_tier) IS NULL THEN 'competitive'
        WHEN (SELECT min_tn FROM top_tier) = 1
          AND (SELECT any_academy FROM flags) = TRUE
          AND ${nextFlag}::boolean = TRUE THEN 'academy'
        WHEN (SELECT has_elite FROM flags) = TRUE THEN 'elite'
        ELSE 'competitive'
      END
    )::competitive_tier AS final_tier
  `);
  const arr = rowsOf<{ final_tier: CompetitiveTier }>(decision);
  return arr[0]?.final_tier ?? "competitive";
}

/**
 * PATCH /v1/admin/pro-academies/:clubId — body { isProAcademy: boolean }.
 * 200 on success, 400 on bad input, 404 if the club doesn't exist.
 */
export const updateProAcademyHandler: RequestHandler = async (
  req,
  res,
  next,
) => {
  try {
    const clubId = Number(req.params.clubId);
    if (!Number.isFinite(clubId) || clubId <= 0) {
      res.status(400).json({ error: "Invalid clubId" });
      return;
    }
    const parsed = UpdateProAcademyRequest.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid request body" });
      return;
    }
    const { isProAcademy } = parsed.data;

    const result = await defaultDb.transaction(async (tx) => {
      const existing = await tx.execute<{
        competitive_tier: CompetitiveTier;
      }>(sql`
        SELECT competitive_tier FROM canonical_clubs
        WHERE id = ${clubId}
        FOR UPDATE
      `);
      const existingArr = rowsOf<{ competitive_tier: CompetitiveTier }>(existing);
      if (existingArr.length === 0) {
        return { outcome: "not_found" as const };
      }
      const previousCompetitiveTier = existingArr[0].competitive_tier;

      // Two writes in one tx: flag, then rollup. Order matters for the
      // safety property — if the rollup write fails, the flag write rolls
      // back too and the row stays internally consistent.
      await tx.execute(sql`
        UPDATE canonical_clubs SET is_pro_academy = ${isProAcademy}
        WHERE id = ${clubId}
      `);
      const nextTier = await computeTierForClub(clubId, isProAcademy, tx);
      await tx.execute(sql`
        UPDATE canonical_clubs SET competitive_tier = ${nextTier}::competitive_tier
        WHERE id = ${clubId}
      `);

      return {
        outcome: "ok" as const,
        previousCompetitiveTier,
        competitiveTier: nextTier,
      };
    });

    if (result.outcome === "not_found") {
      res.status(404).json({ error: "Club not found" });
      return;
    }

    res.json(
      UpdateProAcademyResponse.parse({
        clubId,
        isProAcademy,
        competitiveTier: result.competitiveTier,
        previousCompetitiveTier: result.previousCompetitiveTier,
      }),
    );
  } catch (err) {
    next(err);
  }
};

// ---------------------------------------------------------------------------
// Helpers + router.
// ---------------------------------------------------------------------------

function toNumberOrUndefined(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function toStringOrUndefined(raw: unknown): string | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  return String(raw);
}

export const proAcademiesRouter: IRouter = Router();
proAcademiesRouter.get("/", listProAcademiesHandler);
proAcademiesRouter.patch("/:clubId", updateProAcademyHandler);

export default proAcademiesRouter;
