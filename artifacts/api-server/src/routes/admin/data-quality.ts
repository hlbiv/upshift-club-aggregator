/**
 * `/api/v1/admin/data-quality/*` — admin-surface data-quality operations.
 *
 *   POST /api/v1/admin/data-quality/ga-premier-orphans
 *   GET  /api/v1/admin/data-quality/empty-staff-pages
 *   GET  /api/v1/admin/data-quality/stale-scrapes
 *   GET  /api/v1/admin/data-quality/nav-leaked-names
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
 * Empty staff pages
 * -----------------
 * Clubs with `staff_page_url IS NOT NULL` but zero distinct coach
 * discoveries inside the `windowDays` window. Pure derived SQL — no
 * schema changes. See EmptyStaffPagesRequest in lib/api-zod for the
 * rationale behind `windowDays`.
 *
 * Stale scrapes
 * -------------
 * `scrape_health` rows where `last_scraped_at < now() - thresholdDays`
 * or is NULL. `entity_name` is joined best-effort from
 * canonical_clubs / leagues_master / colleges / coaches by `entity_type`;
 * null is returned if the join fails rather than fabricating a label.
 *
 * Auth
 * ----
 * Mounted under `authedAdminRouter` — requireAdmin + rate limiter already
 * applied upstream in app.ts. GA Premier handler is a DI factory so the
 * unit test can feed it fake DB deps without spinning up Postgres; the
 * read-only panels below are inline handlers following the scrape-runs /
 * scrape-health pattern (tested end-to-end via the dashboard test suite).
 *
 * Nav-leaked-names
 * ----------------
 * Phase 1 read-only panel for `roster_quality_flags` rows with
 * `flag_type = 'nav_leaked_name'`. Joins the flag to its snapshot, the
 * snapshot's canonical club (nullable — linker pattern), and the resolver
 * admin (if resolved). `leakedStrings` and `snapshotRosterSize` are
 * extracted from the jsonb `metadata` into typed response fields at the
 * API boundary — the caller never sees raw jsonb. Phase 2 (scraper
 * detection) ships separately; the table is empty at Phase-1 merge time
 * and this endpoint is the regression guard for the jsonb → typed
 * extraction shape.
 */
import { Router, type IRouter, type RequestHandler } from "express";
import { sql } from "drizzle-orm";
import {
  db as defaultDb,
  clubRosterSnapshots,
  canonicalClubs,
  coachDiscoveries,
  scrapeHealth,
  leaguesMaster,
  colleges,
  coaches,
  rosterQualityFlags,
  adminUsers,
} from "@workspace/db";
import {
  GaPremierOrphanCleanupRequest,
  GaPremierOrphanCleanupResponse,
  EmptyStaffPagesRequest,
  EmptyStaffPagesResponse,
  StaleScrapesRequest,
  StaleScrapesResponse,
  NavLeakedNamesRequest,
  NavLeakedNamesResponse,
  ResolveRosterQualityFlagRequest,
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
  router.get("/empty-staff-pages", emptyStaffPagesHandler);
  router.get("/stale-scrapes", staleScrapesHandler);
  router.get(
    "/nav-leaked-names",
    makeNavLeakedNamesHandler(prodNavLeakedNamesDeps),
  );
  router.patch(
    "/roster-quality-flags/:id/resolve",
    makeResolveRosterQualityFlagHandler(prodResolveRosterQualityFlagDeps),
  );
  return router;
}

// ---------------------------------------------------------------------------
// Nav-leaked-names — DI surface + handler factory + prod wiring.
// ---------------------------------------------------------------------------

/**
 * Raw row shape returned by the DB layer. The handler is responsible for
 * mapping this into the typed response (extracting fields out of jsonb,
 * ISO-normalizing timestamps). Keeping this explicit in the DI surface
 * lets the unit test drive the handler with a plain object literal.
 */
export interface NavLeakedNamesRawRow {
  id: number;
  snapshotId: number;
  clubId: number | null;
  clubNameCanonical: string | null;
  // jsonb payload as returned by node-pg — an arbitrary object. The
  // handler extracts typed fields out of this at the API boundary.
  metadata: unknown;
  flaggedAt: Date | string;
  resolvedAt: Date | string | null;
  resolvedByEmail: string | null;
}

export interface NavLeakedNamesDeps {
  listNavLeakedNames: (args: {
    page: number;
    pageSize: number;
    includeResolved: boolean;
  }) => Promise<{ rows: NavLeakedNamesRawRow[]; total: number }>;
}

/**
 * Extract `leaked_strings` + `snapshot_roster_size` from a
 * `roster_quality_flags.metadata` jsonb payload. Tolerant of missing /
 * malformed fields — defaults to [] / 0 rather than throwing, so one
 * malformed row cannot take out the whole panel.
 */
function extractNavLeakedMetadata(raw: unknown): {
  leakedStrings: string[];
  snapshotRosterSize: number;
} {
  if (raw === null || typeof raw !== "object") {
    return { leakedStrings: [], snapshotRosterSize: 0 };
  }
  const m = raw as Record<string, unknown>;
  const leakedStrings = Array.isArray(m.leaked_strings)
    ? m.leaked_strings.filter((x): x is string => typeof x === "string")
    : [];
  const sizeRaw = m.snapshot_roster_size;
  const snapshotRosterSize =
    typeof sizeRaw === "number" && Number.isFinite(sizeRaw)
      ? Math.trunc(sizeRaw)
      : 0;
  return { leakedStrings, snapshotRosterSize };
}

export function makeNavLeakedNamesHandler(
  deps: NavLeakedNamesDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const parsed = NavLeakedNamesRequest.safeParse({
        page: toNumberOrUndefined(req.query.page),
        pageSize:
          toNumberOrUndefined(req.query.page_size) ??
          toNumberOrUndefined(req.query.pageSize) ??
          toNumberOrUndefined(req.query.limit),
        includeResolved:
          toBooleanOrUndefined(req.query.include_resolved) ??
          toBooleanOrUndefined(req.query.includeResolved),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { page, pageSize, includeResolved } = parsed.data;

      const { rows, total } = await deps.listNavLeakedNames({
        page,
        pageSize,
        includeResolved,
      });

      const mapped = rows.map((r) => {
        const { leakedStrings, snapshotRosterSize } = extractNavLeakedMetadata(
          r.metadata,
        );
        return {
          id: r.id,
          snapshotId: r.snapshotId,
          clubId: r.clubId,
          clubNameCanonical: r.clubNameCanonical,
          leakedStrings,
          snapshotRosterSize,
          flaggedAt: toIsoRequired(r.flaggedAt),
          resolvedAt: toIsoOrNull(r.resolvedAt),
          resolvedByEmail: r.resolvedByEmail,
        };
      });

      res.json(
        NavLeakedNamesResponse.parse({
          rows: mapped,
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

/**
 * Production DB-backed dep. Single SQL query with a COUNT(*) OVER () window
 * to avoid a separate round-trip for the total. Orders unresolved flags
 * oldest-first (so the worst-aged offenders bubble up on page 1); when
 * `includeResolved=true`, resolved rows sort after unresolved.
 */
export const prodNavLeakedNamesDeps: NavLeakedNamesDeps = {
  listNavLeakedNames: async ({ page, pageSize, includeResolved }) => {
    const offset = (page - 1) * pageSize;
    const includeResolvedLit = includeResolved ? sql`true` : sql`false`;

    const result = await defaultDb.execute<{
      id: number;
      snapshot_id: number;
      club_id: number | null;
      club_name_canonical: string | null;
      metadata: unknown;
      flagged_at: Date | string;
      resolved_at: Date | string | null;
      resolved_by_email: string | null;
      total: string;
    }>(sql`
      SELECT
        rqf.id,
        rqf.snapshot_id,
        crs.club_id,
        cc.club_name_canonical,
        rqf.metadata,
        rqf.created_at AS flagged_at,
        rqf.resolved_at,
        au.email AS resolved_by_email,
        COUNT(*) OVER () AS total
      FROM ${rosterQualityFlags} rqf
      JOIN ${clubRosterSnapshots} crs ON crs.id = rqf.snapshot_id
      LEFT JOIN ${canonicalClubs} cc ON cc.id = crs.club_id
      LEFT JOIN ${adminUsers} au ON au.id = rqf.resolved_by
      WHERE rqf.flag_type = 'nav_leaked_name'
        AND (${includeResolvedLit} OR rqf.resolved_at IS NULL)
      ORDER BY
        (rqf.resolved_at IS NOT NULL) ASC,
        rqf.created_at ASC,
        rqf.id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const list = Array.from(
      result as unknown as Array<{
        id: number;
        snapshot_id: number;
        club_id: number | null;
        club_name_canonical: string | null;
        metadata: unknown;
        flagged_at: Date | string;
        resolved_at: Date | string | null;
        resolved_by_email: string | null;
        total: string;
      }>,
    );

    const total = Number(list[0]?.total ?? 0);
    const rows: NavLeakedNamesRawRow[] = list.map((r) => ({
      id: r.id,
      snapshotId: r.snapshot_id,
      clubId: r.club_id,
      clubNameCanonical: r.club_name_canonical,
      metadata: r.metadata,
      flaggedAt: r.flagged_at,
      resolvedAt: r.resolved_at,
      resolvedByEmail: r.resolved_by_email,
    }));

    return { rows, total };
  },
};

// ---------------------------------------------------------------------------
// Resolve roster_quality_flags — DI surface + handler factory + prod wiring.
// ---------------------------------------------------------------------------

export interface ResolveRosterQualityFlagDeps {
  resolveFlag: (args: {
    id: number;
    resolvedBy: number | null;
    notes?: string;
  }) => Promise<{ found: boolean }>;
}

/**
 * PATCH /api/v1/admin/data-quality/roster-quality-flags/:id/resolve
 *
 * Marks a `roster_quality_flags` row as resolved by stamping
 * `resolved_at = NOW()` and `resolved_by = <admin user id>`. The
 * underlying `club_roster_snapshots` row is NOT mutated — resolving
 * means "operator has triaged this leak", not "the data is fixed".
 *
 * Auth: API-key callers get `resolved_by = NULL` (no admin user
 * identity). Session callers get their admin user id stamped — same
 * pattern used by the dedup PATCH endpoints.
 *
 * Idempotent: re-resolving an already-resolved flag is a no-op
 * (returns 204), since the WHERE clause filters on
 * `resolved_at IS NULL`.
 *
 * Status codes:
 *   204 No Content — flag found (resolved or already-resolved)
 *   400 Bad Request — invalid id or invalid body
 *   404 Not Found  — no `nav_leaked_name` flag with that id
 */
export function makeResolveRosterQualityFlagHandler(
  deps: ResolveRosterQualityFlagDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      // Body is optional; when provided, validate the shape.
      const parsed = ResolveRosterQualityFlagRequest.safeParse(
        req.body && Object.keys(req.body as object).length > 0
          ? req.body
          : undefined,
      );
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid body" });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      const { found } = await deps.resolveFlag({
        id,
        resolvedBy: adminUserId,
        notes: parsed.data?.notes,
      });

      if (!found) {
        res.status(404).json({ error: "RosterQualityFlag not found" });
        return;
      }

      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };
}

/**
 * Production DB-backed dep. UPDATE … RETURNING surfaces whether the
 * row exists; we treat "row exists but is already resolved" as success
 * (idempotent) by checking existence with a separate SELECT when the
 * UPDATE matches zero rows.
 */
export const prodResolveRosterQualityFlagDeps: ResolveRosterQualityFlagDeps = {
  resolveFlag: async ({ id, resolvedBy }) => {
    const updated = await defaultDb.execute<{ id: number }>(sql`
      UPDATE ${rosterQualityFlags}
      SET resolved_at = NOW(),
          resolved_by = ${resolvedBy}
      WHERE id = ${id}
        AND flag_type = 'nav_leaked_name'
        AND resolved_at IS NULL
      RETURNING id
    `);
    const updatedList = Array.from(
      updated as unknown as Array<{ id: number }>,
    );
    if (updatedList.length > 0) {
      return { found: true };
    }
    // Either the id doesn't exist or it's already resolved — distinguish
    // so the API returns 404 only for non-existent flags.
    const existing = await defaultDb.execute<{ id: number }>(sql`
      SELECT id
      FROM ${rosterQualityFlags}
      WHERE id = ${id}
        AND flag_type = 'nav_leaked_name'
      LIMIT 1
    `);
    const existingList = Array.from(
      existing as unknown as Array<{ id: number }>,
    );
    return { found: existingList.length > 0 };
  },
};

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

// ---------------------------------------------------------------------------
// Read-only panel: empty-staff-pages
// ---------------------------------------------------------------------------

/**
 * GET /v1/admin/data-quality/empty-staff-pages?window_days=30&page=1&page_size=20
 *
 * Returns canonical_clubs rows where:
 *   - staff_page_url IS NOT NULL
 *   - AND COUNT(DISTINCT coach_id) FROM coach_discoveries where
 *     club_id = clubs.id AND last_seen_at > now() - window_days interval = 0
 *
 * `page_size` capped at 100 (repo convention). Snake-case query params are
 * accepted to match the rest of the public API; camelCase is accepted too.
 */
export const emptyStaffPagesHandler: RequestHandler = async (req, res, next) => {
  try {
    const parsed = EmptyStaffPagesRequest.safeParse({
      windowDays:
        toNumberOrUndefined(req.query.window_days) ??
        toNumberOrUndefined(req.query.windowDays),
      page: toNumberOrUndefined(req.query.page),
      pageSize:
        toNumberOrUndefined(req.query.page_size) ??
        toNumberOrUndefined(req.query.pageSize) ??
        toNumberOrUndefined(req.query.limit),
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid query params" });
      return;
    }
    const { windowDays, page, pageSize } = parsed.data;
    const offset = (page - 1) * pageSize;
    const windowDaysLiteral = Number(windowDays);

    // CTE: per-club coach count inside the window.
    // Outer predicate: staff_page_url present AND coach_count_window = 0.
    //
    // COUNT(DISTINCT coach_id) rather than COUNT(*) for the semantic of
    // "distinct real coaches" — trivially 0 here but preserved so the field
    // stays meaningful if a caller filters coach_count_window > 0 to
    // sanity-check recently-fixed clubs.
    const countRows = await defaultDb.execute<{ total: string }>(sql`
      WITH windowed AS (
        SELECT
          ${canonicalClubs.id} AS club_id,
          COUNT(DISTINCT ${coachDiscoveries.coachId}) AS coach_count_window
        FROM ${canonicalClubs}
        LEFT JOIN ${coachDiscoveries} ON ${coachDiscoveries.clubId} = ${canonicalClubs.id}
          AND ${coachDiscoveries.lastSeenAt} > now() - (${windowDaysLiteral}::text || ' days')::interval
        WHERE ${canonicalClubs.staffPageUrl} IS NOT NULL
        GROUP BY ${canonicalClubs.id}
      )
      SELECT COUNT(*)::text AS total FROM windowed WHERE coach_count_window = 0
    `);
    const countArr = Array.from(
      countRows as unknown as Array<{ total: string }>,
    );
    const total = Number(countArr[0]?.total ?? 0);

    const rowsResult = await defaultDb.execute<{
      club_id: number;
      club_name_canonical: string;
      staff_page_url: string;
      last_scraped_at: Date | string | null;
      coach_count_window: string;
    }>(sql`
      WITH windowed AS (
        SELECT
          ${canonicalClubs.id} AS club_id,
          ${canonicalClubs.clubNameCanonical} AS club_name_canonical,
          ${canonicalClubs.staffPageUrl} AS staff_page_url,
          ${canonicalClubs.lastScrapedAt} AS last_scraped_at,
          COUNT(DISTINCT ${coachDiscoveries.coachId}) AS coach_count_window
        FROM ${canonicalClubs}
        LEFT JOIN ${coachDiscoveries} ON ${coachDiscoveries.clubId} = ${canonicalClubs.id}
          AND ${coachDiscoveries.lastSeenAt} > now() - (${windowDaysLiteral}::text || ' days')::interval
        WHERE ${canonicalClubs.staffPageUrl} IS NOT NULL
        GROUP BY ${canonicalClubs.id}
      )
      SELECT
        club_id,
        club_name_canonical,
        staff_page_url,
        last_scraped_at,
        coach_count_window::text AS coach_count_window
      FROM windowed
      WHERE coach_count_window = 0
      ORDER BY last_scraped_at ASC NULLS FIRST, club_id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const rows = Array.from(rowsResult as unknown as Array<{
      club_id: number;
      club_name_canonical: string;
      staff_page_url: string;
      last_scraped_at: Date | string | null;
      coach_count_window: string;
    }>).map((r) => ({
      clubId: r.club_id,
      clubNameCanonical: r.club_name_canonical,
      staffPageUrl: r.staff_page_url,
      lastScrapedAt: toIsoOrNull(r.last_scraped_at),
      coachCountWindow: Number(r.coach_count_window ?? 0),
    }));

    res.json(
      EmptyStaffPagesResponse.parse({
        rows,
        total,
        page,
        pageSize,
        windowDays,
      }),
    );
  } catch (err) {
    next(err);
  }
};

// ---------------------------------------------------------------------------
// Read-only panel: stale-scrapes
// ---------------------------------------------------------------------------

/**
 * GET /v1/admin/data-quality/stale-scrapes?threshold_days=14&page=1&page_size=20
 *
 * Rows from `scrape_health` where `last_scraped_at < now() - threshold_days`
 * OR `last_scraped_at IS NULL` (never scraped). `entity_name` is resolved
 * by a type-switched LEFT JOIN; rows whose entity_type falls outside the
 * joinable set (match, tryout, etc.) get null rather than a fabricated label.
 *
 * Ordered oldest-first (NULLs sort first) so "never scraped" entities
 * surface at the top of page 1.
 */
export const staleScrapesHandler: RequestHandler = async (req, res, next) => {
  try {
    const parsed = StaleScrapesRequest.safeParse({
      thresholdDays:
        toNumberOrUndefined(req.query.threshold_days) ??
        toNumberOrUndefined(req.query.thresholdDays),
      page: toNumberOrUndefined(req.query.page),
      pageSize:
        toNumberOrUndefined(req.query.page_size) ??
        toNumberOrUndefined(req.query.pageSize) ??
        toNumberOrUndefined(req.query.limit),
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid query params" });
      return;
    }
    const { thresholdDays, page, pageSize } = parsed.data;
    const offset = (page - 1) * pageSize;
    const thresholdDaysLiteral = Number(thresholdDays);

    // Stale predicate: never scraped OR last_scraped_at older than threshold.
    const stalePredicate = sql`(
      ${scrapeHealth.lastScrapedAt} IS NULL
      OR ${scrapeHealth.lastScrapedAt} < now() - (${thresholdDaysLiteral}::text || ' days')::interval
    )`;

    const [countRow] = await defaultDb
      .select({ count: sql<number>`count(*)::int` })
      .from(scrapeHealth)
      .where(stalePredicate);
    const total = countRow?.count ?? 0;

    // Best-effort entity name via a polymorphic LEFT JOIN — one CASE per
    // joinable entity_type. Unknown types (e.g. 'match', 'tryout') fall
    // through to NULL rather than fabricating a label.
    const rowsResult = await defaultDb.execute<{
      entity_type: string;
      entity_id: number;
      entity_name: string | null;
      last_scraped_at: Date | string | null;
      last_status: string | null;
      consecutive_failures: number;
    }>(sql`
      SELECT
        sh.entity_type,
        sh.entity_id,
        CASE sh.entity_type
          WHEN 'club'    THEN cc.club_name_canonical
          WHEN 'league'  THEN lm.league_name
          WHEN 'college' THEN co.name
          WHEN 'coach'   THEN coa.display_name
          ELSE NULL
        END AS entity_name,
        sh.last_scraped_at,
        sh.status AS last_status,
        sh.consecutive_failures
      FROM ${scrapeHealth} sh
      LEFT JOIN ${canonicalClubs} cc ON sh.entity_type = 'club'    AND cc.id = sh.entity_id
      LEFT JOIN ${leaguesMaster} lm  ON sh.entity_type = 'league'  AND lm.id = sh.entity_id
      LEFT JOIN ${colleges} co       ON sh.entity_type = 'college' AND co.id = sh.entity_id
      LEFT JOIN ${coaches} coa       ON sh.entity_type = 'coach'   AND coa.id = sh.entity_id
      WHERE ${stalePredicate}
      ORDER BY sh.last_scraped_at ASC NULLS FIRST, sh.id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const rows = Array.from(
      rowsResult as unknown as Array<{
        entity_type: string;
        entity_id: number;
        entity_name: string | null;
        last_scraped_at: Date | string | null;
        last_status: string | null;
        consecutive_failures: number;
      }>,
    ).map((r) => ({
      entityType: r.entity_type,
      entityId: r.entity_id,
      entityName: r.entity_name,
      lastScrapedAt: toIsoOrNull(r.last_scraped_at),
      lastStatus: r.last_status,
      consecutiveFailures: Number(r.consecutive_failures ?? 0),
    }));

    res.json(
      StaleScrapesResponse.parse({
        rows,
        total,
        page,
        pageSize,
        thresholdDays,
      }),
    );
  } catch (err) {
    next(err);
  }
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function toNumberOrUndefined(raw: unknown): number | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  const n = Number(raw);
  return Number.isFinite(n) ? n : undefined;
}

function toBooleanOrUndefined(raw: unknown): boolean | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  if (typeof raw === "boolean") return raw;
  const s = String(raw).toLowerCase();
  if (s === "true" || s === "1") return true;
  if (s === "false" || s === "0") return false;
  return undefined;
}

function toIsoOrNull(value: Date | string | null): string | null {
  if (value === null) return null;
  if (value instanceof Date) return value.toISOString();
  // Postgres `timestamp` columns arrive as strings via raw `db.execute`.
  // Coerce via Date round-trip so the response format is stable ISO-8601.
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toIsoRequired(value: Date | string): string {
  if (value instanceof Date) return value.toISOString();
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new Error(`toIsoRequired: unparseable timestamp ${String(value)}`);
  }
  return d.toISOString();
}

export const dataQualityRouter: IRouter = makeDataQualityRouter({
  scanOrphans,
  deleteOrphans,
});

export default dataQualityRouter;
