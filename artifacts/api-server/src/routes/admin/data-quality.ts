/**
 * `/api/v1/admin/data-quality/*` — admin-surface data-quality operations.
 *
 *   POST /api/v1/admin/data-quality/ga-premier-orphans
 *
 * GA Premier orphan cleanup
 * -------------------------
 * `club_roster_snapshots` has accumulated rows whose `club_name_raw` is
 * actually a site-nav token (FACILITIES, STAFF, NEWS, etc.) rather than
 * a real club name — scraped off navigation menus before parser fixes
 * landed. This endpoint lets an operator identify and remove those rows.
 *
 * Patterns (case-insensitive, matched against `club_name_raw`):
 *   FACILITIES, STAFF, NEWS, EVENTS, CONTACT, ABOUT,
 *   HOME, TEAMS, COACHES, REGISTRATION, TRYOUTS
 *
 * A row is flagged when the UPPER'd `club_name_raw` equals one of these
 * tokens exactly, OR starts with one of them followed by a non-letter
 * character (matches e.g. "STAFF - Meet the Team" but leaves "STAFFORD SC"
 * alone).
 *
 * Mode contract
 * -------------
 *   dryRun=true  (default) — SELECT ... LIMIT <limit>; return counts +
 *                up to 20 sample `club_name_raw` values. No writes.
 *   dryRun=false           — DELETE ... LIMIT <limit> inside a tx and
 *                return counts; sampleNames is still populated (pre-delete
 *                capture) so the operator can confirm what went.
 *
 * Auth
 * ----
 * Mounted under `authedAdminRouter` — requireAdmin + rate limiter already
 * applied upstream in app.ts. Handler is a plain factory so the unit test
 * can feed it fake DB deps without spinning up Postgres.
 *
 * Future panels (empty-staff-pages, nav-leaked-names, stale-scrapes) will
 * land alongside this one as sibling handlers on the same router.
 */
import { Router, type IRouter, type RequestHandler } from "express";
import { sql } from "drizzle-orm";
import { db as defaultDb, clubRosterSnapshots } from "@workspace/db";
import {
  GaPremierOrphanCleanupRequest,
  GaPremierOrphanCleanupResponse,
} from "@hlbiv/api-zod/admin";

// ---------------------------------------------------------------------------
// Bad-token patterns.
// ---------------------------------------------------------------------------

/**
 * Nav tokens that masquerade as club names in orphan `club_roster_snapshots`
 * rows. Uppercase here so the SQL comparison can normalize both sides.
 * Future PRs can extend this list — the 11 below are a conservative baseline.
 */
export const GA_PREMIER_ORPHAN_TOKENS: readonly string[] = [
  "FACILITIES",
  "STAFF",
  "NEWS",
  "EVENTS",
  "CONTACT",
  "ABOUT",
  "HOME",
  "TEAMS",
  "COACHES",
  "REGISTRATION",
  "TRYOUTS",
];

/** Max sample names surfaced on the response, per contract. */
const SAMPLE_NAME_CAP = 20;

// ---------------------------------------------------------------------------
// Dependency injection surface.
// ---------------------------------------------------------------------------

/**
 * Tests drive the handler with a fake `DataQualityDeps`. The surface is
 * narrow on purpose — no Drizzle types leak through, so fakes can be
 * plain object literals.
 */
export interface DataQualityDeps {
  /**
   * Return (scanned, flagged, sampleNames) for the given limit. `scanned`
   * is the upper bound inspected (min(limit, total-rows)); `flagged` is
   * the count matching the patterns; `sampleNames` is a first-N sample of
   * `club_name_raw` values, capped at `SAMPLE_NAME_CAP`.
   */
  scanOrphans: (args: {
    tokens: readonly string[];
    limit: number;
  }) => Promise<{
    scanned: number;
    flagged: number;
    sampleNames: string[];
  }>;
  /**
   * Delete up to `limit` matching rows inside a transaction. Returns
   * (deleted, sampleNames) — sampleNames captured before the delete so the
   * caller sees what went.
   */
  deleteOrphans: (args: {
    tokens: readonly string[];
    limit: number;
  }) => Promise<{
    scanned: number;
    flagged: number;
    deleted: number;
    sampleNames: string[];
  }>;
}

// ---------------------------------------------------------------------------
// Handler factory.
// ---------------------------------------------------------------------------

export function makeGaPremierOrphanHandler(
  deps: DataQualityDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const parsed = GaPremierOrphanCleanupRequest.safeParse(req.body ?? {});
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid request body" });
        return;
      }
      const { dryRun, limit } = parsed.data;

      if (dryRun) {
        const { scanned, flagged, sampleNames } = await deps.scanOrphans({
          tokens: GA_PREMIER_ORPHAN_TOKENS,
          limit,
        });
        res.json(
          GaPremierOrphanCleanupResponse.parse({
            scanned,
            flagged,
            deleted: 0,
            sampleNames: sampleNames.slice(0, SAMPLE_NAME_CAP),
          }),
        );
        return;
      }

      const { scanned, flagged, deleted, sampleNames } =
        await deps.deleteOrphans({
          tokens: GA_PREMIER_ORPHAN_TOKENS,
          limit,
        });
      res.json(
        GaPremierOrphanCleanupResponse.parse({
          scanned,
          flagged,
          deleted,
          sampleNames: sampleNames.slice(0, SAMPLE_NAME_CAP),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Router factory + default wiring.
// ---------------------------------------------------------------------------

export function makeDataQualityRouter(deps: DataQualityDeps): IRouter {
  const router: IRouter = Router();
  router.post("/ga-premier-orphans", makeGaPremierOrphanHandler(deps));
  return router;
}

// ---------------------------------------------------------------------------
// Production (live DB) dependency wiring.
// ---------------------------------------------------------------------------

/**
 * Build the `WHERE` predicate that flags a row as orphaned by a nav token.
 *
 * For each token T the row matches if:
 *   UPPER(club_name_raw) = T
 *   OR club_name_raw ILIKE 'T%' AND substring at position len(T)+1 is non-letter
 *
 * We express this as: `UPPER(club_name_raw) = T OR club_name_raw ~* '^T[^A-Za-z]'`.
 * The regex form guarantees the token is followed by a non-letter (digit,
 * space, punctuation, end) — so "STAFFORD SC" doesn't match "STAFF" but
 * "STAFF - Meet the Team" does.
 */
function orphanPredicateSql(tokens: readonly string[]) {
  const equalityList = sql.join(
    tokens.map((t) => sql`${t}`),
    sql`, `,
  );
  const regexClauses = tokens.map(
    (t) => sql`${clubRosterSnapshots.clubNameRaw} ~* ${`^${t}[^A-Za-z]`}`,
  );
  return sql`(
    UPPER(${clubRosterSnapshots.clubNameRaw}) IN (${equalityList})
    OR ${sql.join(regexClauses, sql` OR `)}
  )`;
}

async function scanOrphans(args: {
  tokens: readonly string[];
  limit: number;
}): Promise<{ scanned: number; flagged: number; sampleNames: string[] }> {
  const predicate = orphanPredicateSql(args.tokens);
  const rows = await defaultDb.execute<{ club_name_raw: string }>(
    sql`
      SELECT club_name_raw
      FROM ${clubRosterSnapshots}
      WHERE ${predicate}
      LIMIT ${args.limit}
    `,
  );
  const flaggedRows = Array.from(rows as unknown as Array<{ club_name_raw: string }>);
  const sampleNames = flaggedRows
    .slice(0, SAMPLE_NAME_CAP)
    .map((r) => r.club_name_raw);
  return {
    scanned: flaggedRows.length,
    flagged: flaggedRows.length,
    sampleNames,
  };
}

async function deleteOrphans(args: {
  tokens: readonly string[];
  limit: number;
}): Promise<{
  scanned: number;
  flagged: number;
  deleted: number;
  sampleNames: string[];
}> {
  return defaultDb.transaction(async (tx) => {
    const predicate = orphanPredicateSql(args.tokens);
    // Capture sample names first so the operator sees what's about to go.
    const sampleRows = await tx.execute<{ club_name_raw: string }>(
      sql`
        SELECT club_name_raw
        FROM ${clubRosterSnapshots}
        WHERE ${predicate}
        LIMIT ${SAMPLE_NAME_CAP}
      `,
    );
    const sampleNames = Array.from(
      sampleRows as unknown as Array<{ club_name_raw: string }>,
    ).map((r) => r.club_name_raw);

    // Postgres DELETE doesn't support LIMIT directly — use a CTE on the
    // primary key to cap the delete.
    const result = await tx.execute<{ id: number }>(
      sql`
        WITH doomed AS (
          SELECT id
          FROM ${clubRosterSnapshots}
          WHERE ${predicate}
          LIMIT ${args.limit}
        )
        DELETE FROM ${clubRosterSnapshots}
        WHERE id IN (SELECT id FROM doomed)
        RETURNING id
      `,
    );
    const deletedRows = Array.from(result as unknown as Array<{ id: number }>);
    const deleted = deletedRows.length;

    return {
      scanned: deleted,
      flagged: deleted,
      deleted,
      sampleNames,
    };
  });
}

export const dataQualityRouter: IRouter = makeDataQualityRouter({
  scanOrphans,
  deleteOrphans,
});

export default dataQualityRouter;
