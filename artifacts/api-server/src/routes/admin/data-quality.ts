/**
 * `/api/v1/admin/data-quality/*` — admin-surface data-quality operations.
 *
 *   POST  /api/v1/admin/data-quality/ga-premier-orphans
 *   GET   /api/v1/admin/data-quality/empty-staff-pages
 *   GET   /api/v1/admin/data-quality/stale-scrapes
 *   GET   /api/v1/admin/data-quality/nav-leaked-names
 *   PATCH /api/v1/admin/data-quality/roster-quality-flags/:id/resolve
 *   GET   /api/v1/admin/data-quality/coach-quality-flags
 *   PATCH /api/v1/admin/data-quality/coach-quality-flags/:id/resolve
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
 * Read + resolve surface for `roster_quality_flags` rows with
 * `flag_type = 'nav_leaked_name'`. The GET handler joins the flag to its
 * snapshot, the snapshot's canonical club (nullable — linker pattern),
 * and the resolver admin (if resolved). `leakedStrings` and
 * `snapshotRosterSize` are extracted from the jsonb `metadata` into typed
 * response fields at the API boundary — the caller never sees raw jsonb.
 *
 * The `state` query param is a tri-state + escape hatch:
 *   open       (default) — resolved_at IS NULL
 *   resolved              — resolution_reason = 'resolved'
 *   dismissed             — resolution_reason = 'dismissed'
 *   all                   — no filter
 *
 * The PATCH `/roster-quality-flags/:id/resolve` endpoint accepts a
 * required body `{ reason: "resolved" | "dismissed" }` — "resolved" means
 * the flag was legitimate and the leak was cleaned up out of band;
 * "dismissed" means the detector flagged a false positive. Snapshot rows
 * are never mutated by this endpoint.
 */
import { Router, type IRouter, type RequestHandler } from "express";
import { sql } from "drizzle-orm";
import {
  db as defaultDb,
  clubRosterSnapshots,
  canonicalClubs,
  coachDiscoveries,
  coachQualityFlags,
  scrapeHealth,
  coachMisses,
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
  CoachMissesRequest,
  CoachMissesResponse,
  NavLeakedNamesRequest,
  NavLeakedNamesResponse,
  NumericOnlyNamesRequest,
  NumericOnlyNamesResponse,
  ResolveRosterQualityFlagRequest,
  CoachQualityFlagsRequest,
  CoachQualityFlagsResponse,
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
  router.get("/coach-misses", coachMissesHandler);
  router.get(
    "/nav-leaked-names",
    makeNavLeakedNamesHandler(prodNavLeakedNamesDeps),
  );
  router.get(
    "/numeric-only-names",
    makeNumericOnlyNamesHandler(prodNumericOnlyNamesDeps),
  );
  router.patch(
    "/roster-quality-flags/:id/resolve",
    makeResolveRosterQualityFlagHandler(prodResolveRosterQualityFlagDeps),
  );
  router.get(
    "/coach-quality-flags",
    makeCoachQualityFlagsHandler(prodCoachQualityFlagsDeps),
  );
  router.patch(
    "/coach-quality-flags/:id/resolve",
    makeResolveCoachQualityFlagHandler(prodResolveCoachQualityFlagDeps),
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
  resolutionReason: "resolved" | "dismissed" | null;
}

export type NavLeakedNamesState = "open" | "resolved" | "dismissed" | "all";

export interface NavLeakedNamesDeps {
  listNavLeakedNames: (args: {
    page: number;
    pageSize: number;
    state: NavLeakedNamesState;
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
        state: toStringOrUndefined(req.query.state),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { page, pageSize, state } = parsed.data;

      const { rows, total } = await deps.listNavLeakedNames({
        page,
        pageSize,
        state,
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
          resolutionReason: r.resolutionReason,
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
 * oldest-first (so the worst-aged offenders bubble up on page 1); when the
 * state filter includes resolved rows, resolved rows sort after unresolved.
 *
 * `state` maps to a SQL predicate:
 *   open       → resolved_at IS NULL
 *   resolved   → resolved_at IS NOT NULL AND resolution_reason = 'resolved'
 *   dismissed  → resolved_at IS NOT NULL AND resolution_reason = 'dismissed'
 *   all        → (no extra predicate)
 */
export const prodNavLeakedNamesDeps: NavLeakedNamesDeps = {
  listNavLeakedNames: async ({ page, pageSize, state }) => {
    const offset = (page - 1) * pageSize;
    const statePredicate =
      state === "open"
        ? sql`rqf.resolved_at IS NULL`
        : state === "resolved"
          ? sql`rqf.resolved_at IS NOT NULL AND rqf.resolution_reason = 'resolved'`
          : state === "dismissed"
            ? sql`rqf.resolved_at IS NOT NULL AND rqf.resolution_reason = 'dismissed'`
            : sql`TRUE`;

    const result = await defaultDb.execute<{
      id: number;
      snapshot_id: number;
      club_id: number | null;
      club_name_canonical: string | null;
      metadata: unknown;
      flagged_at: Date | string;
      resolved_at: Date | string | null;
      resolved_by_email: string | null;
      resolution_reason: "resolved" | "dismissed" | null;
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
        rqf.resolution_reason,
        COUNT(*) OVER () AS total
      FROM ${rosterQualityFlags} rqf
      JOIN ${clubRosterSnapshots} crs ON crs.id = rqf.snapshot_id
      LEFT JOIN ${canonicalClubs} cc ON cc.id = crs.club_id
      LEFT JOIN ${adminUsers} au ON au.id = rqf.resolved_by
      WHERE rqf.flag_type = 'nav_leaked_name'
        AND (${statePredicate})
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
        resolution_reason: "resolved" | "dismissed" | null;
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
      resolutionReason: r.resolution_reason,
    }));

    return { rows, total };
  },
};

// ---------------------------------------------------------------------------
// Numeric-only-names — DI surface + handler factory + prod wiring.
// ---------------------------------------------------------------------------

/**
 * Raw row shape returned by the DB layer for the numeric-only-names panel.
 * Same shape as NavLeakedNamesRawRow; the handler extracts
 * `numeric_strings` + `snapshot_roster_size` out of the metadata jsonb at
 * the API boundary.
 */
export interface NumericOnlyNamesRawRow {
  id: number;
  snapshotId: number;
  clubId: number | null;
  clubNameCanonical: string | null;
  metadata: unknown;
  flaggedAt: Date | string;
  resolvedAt: Date | string | null;
  resolvedByEmail: string | null;
  resolutionReason: "resolved" | "dismissed" | null;
}

export type NumericOnlyNamesState =
  | "open"
  | "resolved"
  | "dismissed"
  | "all";

export interface NumericOnlyNamesDeps {
  listNumericOnlyNames: (args: {
    page: number;
    pageSize: number;
    state: NumericOnlyNamesState;
  }) => Promise<{ rows: NumericOnlyNamesRawRow[]; total: number }>;
}

/**
 * Extract `numeric_strings` + `snapshot_roster_size` from a
 * `roster_quality_flags.metadata` jsonb payload for `flag_type =
 * 'numeric_only_name'`. Tolerant of missing / malformed fields — defaults
 * to [] / 0 rather than throwing.
 */
function extractNumericOnlyMetadata(raw: unknown): {
  numericStrings: string[];
  snapshotRosterSize: number;
} {
  if (raw === null || typeof raw !== "object") {
    return { numericStrings: [], snapshotRosterSize: 0 };
  }
  const m = raw as Record<string, unknown>;
  const numericStrings = Array.isArray(m.numeric_strings)
    ? m.numeric_strings.filter((x): x is string => typeof x === "string")
    : [];
  const sizeRaw = m.snapshot_roster_size;
  const snapshotRosterSize =
    typeof sizeRaw === "number" && Number.isFinite(sizeRaw)
      ? Math.trunc(sizeRaw)
      : 0;
  return { numericStrings, snapshotRosterSize };
}

export function makeNumericOnlyNamesHandler(
  deps: NumericOnlyNamesDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const parsed = NumericOnlyNamesRequest.safeParse({
        page: toNumberOrUndefined(req.query.page),
        pageSize:
          toNumberOrUndefined(req.query.page_size) ??
          toNumberOrUndefined(req.query.pageSize) ??
          toNumberOrUndefined(req.query.limit),
        state: toStringOrUndefined(req.query.state),
      });
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid query params" });
        return;
      }
      const { page, pageSize, state } = parsed.data;

      const { rows, total } = await deps.listNumericOnlyNames({
        page,
        pageSize,
        state,
      });

      const mapped = rows.map((r) => {
        const { numericStrings, snapshotRosterSize } =
          extractNumericOnlyMetadata(r.metadata);
        return {
          id: r.id,
          snapshotId: r.snapshotId,
          clubId: r.clubId,
          clubNameCanonical: r.clubNameCanonical,
          numericStrings,
          snapshotRosterSize,
          flaggedAt: toIsoRequired(r.flaggedAt),
          resolvedAt: toIsoOrNull(r.resolvedAt),
          resolvedByEmail: r.resolvedByEmail,
          resolutionReason: r.resolutionReason,
        };
      });

      res.json(
        NumericOnlyNamesResponse.parse({
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
 * Production DB-backed dep for the numeric-only-names panel. Query is the
 * nav-leaked variant with the flag_type predicate swapped — same COUNT(*)
 * OVER () + ORDER BY + join topology.
 */
export const prodNumericOnlyNamesDeps: NumericOnlyNamesDeps = {
  listNumericOnlyNames: async ({ page, pageSize, state }) => {
    const offset = (page - 1) * pageSize;
    const statePredicate =
      state === "open"
        ? sql`rqf.resolved_at IS NULL`
        : state === "resolved"
          ? sql`rqf.resolved_at IS NOT NULL AND rqf.resolution_reason = 'resolved'`
          : state === "dismissed"
            ? sql`rqf.resolved_at IS NOT NULL AND rqf.resolution_reason = 'dismissed'`
            : sql`TRUE`;

    const result = await defaultDb.execute<{
      id: number;
      snapshot_id: number;
      club_id: number | null;
      club_name_canonical: string | null;
      metadata: unknown;
      flagged_at: Date | string;
      resolved_at: Date | string | null;
      resolved_by_email: string | null;
      resolution_reason: "resolved" | "dismissed" | null;
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
        rqf.resolution_reason,
        COUNT(*) OVER () AS total
      FROM ${rosterQualityFlags} rqf
      JOIN ${clubRosterSnapshots} crs ON crs.id = rqf.snapshot_id
      LEFT JOIN ${canonicalClubs} cc ON cc.id = crs.club_id
      LEFT JOIN ${adminUsers} au ON au.id = rqf.resolved_by
      WHERE rqf.flag_type = 'numeric_only_name'
        AND (${statePredicate})
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
        resolution_reason: "resolved" | "dismissed" | null;
        total: string;
      }>,
    );

    const total = Number(list[0]?.total ?? 0);
    const rows: NumericOnlyNamesRawRow[] = list.map((r) => ({
      id: r.id,
      snapshotId: r.snapshot_id,
      clubId: r.club_id,
      clubNameCanonical: r.club_name_canonical,
      metadata: r.metadata,
      flaggedAt: r.flagged_at,
      resolvedAt: r.resolved_at,
      resolvedByEmail: r.resolved_by_email,
      resolutionReason: r.resolution_reason,
    }));

    return { rows, total };
  },
};

// ---------------------------------------------------------------------------
// Resolve roster_quality_flags — DI surface + handler factory + prod wiring.
// ---------------------------------------------------------------------------

export type ResolveOutcome = "resolved" | "already_resolved" | "not_found";

export type ResolutionReason = "resolved" | "dismissed";

export interface ResolveRosterQualityFlagDeps {
  resolveFlag: (args: {
    id: number;
    resolvedBy: number | null;
    reason: ResolutionReason;
  }) => Promise<{ outcome: ResolveOutcome }>;
}

/**
 * PATCH /api/v1/admin/data-quality/roster-quality-flags/:id/resolve
 *
 * Body: { reason: "resolved" | "dismissed" } — required.
 *   resolved  — legitimate leak, operator cleaned it up out of band.
 *   dismissed — false positive.
 *
 * 204 on first resolve, 400 if body is missing/invalid or the flag is
 * already resolved (either reason), 404 if id unknown.
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

      const parsedBody = ResolveRosterQualityFlagRequest.safeParse(
        req.body ?? {},
      );
      if (!parsedBody.success) {
        res.status(400).json({ error: "Invalid request body" });
        return;
      }
      const { reason } = parsedBody.data;

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      const { outcome } = await deps.resolveFlag({
        id,
        resolvedBy: adminUserId,
        reason,
      });

      if (outcome === "not_found") {
        res.status(404).json({ error: "RosterQualityFlag not found" });
        return;
      }
      if (outcome === "already_resolved") {
        res.status(400).json({ error: "Flag is already resolved" });
        return;
      }
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };
}

/**
 * Production wiring for the roster-quality-flag resolve endpoint. Flag-type
 * agnostic by design: the URL is scoped to `roster_quality_flags` as a whole,
 * so any row whose id matches can be resolved regardless of `flag_type`. The
 * CHECK constraint on `flag_type` is currently a singleton, but we avoid
 * baking that assumption into the query so Phase 3+ flag types resolve
 * correctly the moment the CHECK list is extended.
 */
export const prodResolveRosterQualityFlagDeps: ResolveRosterQualityFlagDeps = {
  resolveFlag: async ({ id, resolvedBy, reason }) => {
    const updated = await defaultDb.execute<{ id: number }>(sql`
      UPDATE ${rosterQualityFlags}
      SET resolved_at = NOW(),
          resolved_by = ${resolvedBy},
          resolution_reason = ${reason}
      WHERE id = ${id}
        AND resolved_at IS NULL
      RETURNING id
    `);
    if (Array.from(updated as unknown as Array<{ id: number }>).length > 0) {
      return { outcome: "resolved" };
    }
    const existing = await defaultDb.execute<{ id: number }>(sql`
      SELECT id
      FROM ${rosterQualityFlags}
      WHERE id = ${id}
      LIMIT 1
    `);
    return {
      outcome:
        Array.from(existing as unknown as Array<{ id: number }>).length > 0
          ? "already_resolved"
          : "not_found",
    };
  },
};

// ---------------------------------------------------------------------------
// coach_quality_flags — DI surface + handler factory + prod wiring.
//
// Patterned 1:1 on the nav-leaked-names implementation above. The
// `coach_quality_flags` table is the canary / audit trail for the 3-PR
// coach-pollution remediation: PR 1 (shared guard) flags suspicious writes
// as scrapers run; PR 2 (purge script) writes an audit row before deleting
// any `coach_discoveries` entry. This panel is the operator's forensic
// surface — "show everything the canary caught, grouped by flag type /
// resolved state".
//
// Unlike nav-leaked-names we do NOT narrow metadata into typed columns at
// the API boundary yet — the per-flag-type jsonb shape is still being
// settled by the pollution investigation, and forcing it through typed
// fields now would lock us into a premature contract. The raw jsonb ships
// verbatim and the UI can narrow on `flagType` to read fields safely.
// ---------------------------------------------------------------------------

export interface CoachQualityFlagRawRow {
  id: number;
  discoveryId: number;
  flagType: string;
  metadata: unknown;
  flaggedAt: Date | string;
  resolvedAt: Date | string | null;
  resolvedByEmail: string | null;
  resolutionNote: string | null;
  coachName: string;
  coachEmail: string | null;
  clubNameRaw: string | null;
  clubId: number | null;
  clubDisplayName: string | null;
}

export interface CoachQualityFlagsDeps {
  listCoachQualityFlags: (args: {
    flagType: string | undefined;
    resolved: boolean | undefined;
    page: number;
    pageSize: number;
  }) => Promise<{ rows: CoachQualityFlagRawRow[]; total: number }>;
}

/**
 * Pass metadata through as a plain object or null. node-pg returns jsonb as
 * a parsed JS value; `Array.isArray` / primitive results are not valid
 * metadata contracts here, so we coerce non-object payloads to null rather
 * than throwing (the detector could, in theory, write bad rows — one bad
 * row must not take out the whole panel).
 */
function normalizeCoachQualityMetadata(
  raw: unknown,
): Record<string, unknown> | null {
  if (raw === null || raw === undefined) return null;
  if (typeof raw !== "object") return null;
  if (Array.isArray(raw)) return null;
  return raw as Record<string, unknown>;
}

/**
 * Narrow an arbitrary string to one of the four CHECK-list values we
 * advertise in the response schema. If a row ever slips through with a
 * flagType outside the CHECK list (CHECK migration lag, manual SQL poke),
 * fall back to `'nav_leaked'` rather than crashing the whole panel — the
 * DB-level CHECK constraint is the canonical guard; this is defense in
 * depth at the API boundary so one corrupt row cannot take out the UI.
 */
function coerceCoachQualityFlagType(
  raw: string,
): "looks_like_name_reject" | "role_label_as_name" | "corrupt_email" | "nav_leaked" {
  switch (raw) {
    case "looks_like_name_reject":
    case "role_label_as_name":
    case "corrupt_email":
    case "nav_leaked":
      return raw;
    default:
      return "nav_leaked";
  }
}

export function makeCoachQualityFlagsHandler(
  deps: CoachQualityFlagsDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const parsed = CoachQualityFlagsRequest.safeParse({
        flagType:
          typeof req.query.flag_type === "string"
            ? req.query.flag_type
            : typeof req.query.flagType === "string"
              ? req.query.flagType
              : undefined,
        resolved:
          toBooleanOrUndefined(req.query.resolved) ??
          toBooleanOrUndefined(req.query.is_resolved),
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
      const { flagType, resolved, page, pageSize } = parsed.data;

      const { rows, total } = await deps.listCoachQualityFlags({
        flagType,
        resolved,
        page,
        pageSize,
      });

      const items = rows.map((r) => ({
        id: r.id,
        discoveryId: r.discoveryId,
        flagType: coerceCoachQualityFlagType(r.flagType),
        metadata: normalizeCoachQualityMetadata(r.metadata),
        flaggedAt: toIsoRequired(r.flaggedAt),
        resolvedAt: toIsoOrNull(r.resolvedAt),
        resolvedByEmail: r.resolvedByEmail,
        resolutionNote: r.resolutionNote,
        coachName: r.coachName,
        coachEmail: r.coachEmail,
        clubNameRaw: r.clubNameRaw,
        clubId: r.clubId,
        clubDisplayName: r.clubDisplayName,
      }));

      res.json(
        CoachQualityFlagsResponse.parse({
          items,
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
 * Production DB-backed dep. COUNT(*) OVER () window so we don't make a
 * second round-trip for pagination total. Orders active flags oldest-first
 * so the stalest offenders bubble up; resolved rows sort after unresolved
 * when the caller asks for both.
 */
export const prodCoachQualityFlagsDeps: CoachQualityFlagsDeps = {
  listCoachQualityFlags: async ({ flagType, resolved, page, pageSize }) => {
    const offset = (page - 1) * pageSize;

    // Tri-state resolved filter: undefined → both, true → resolved only,
    // false → active only. Flag-type filter is optional; a nullish branch
    // matches every row.
    const flagTypePredicate = flagType
      ? sql`cqf.flag_type = ${flagType}`
      : sql`TRUE`;
    const resolvedPredicate =
      resolved === undefined
        ? sql`TRUE`
        : resolved
          ? sql`cqf.resolved_at IS NOT NULL`
          : sql`cqf.resolved_at IS NULL`;

    const result = await defaultDb.execute<{
      id: number;
      discovery_id: number;
      flag_type: string;
      metadata: unknown;
      flagged_at: Date | string;
      resolved_at: Date | string | null;
      resolved_by_email: string | null;
      resolution_note: string | null;
      coach_name: string;
      coach_email: string | null;
      club_name_raw: string | null;
      club_id: number | null;
      club_display_name: string | null;
      total: string;
    }>(sql`
      SELECT
        cqf.id,
        cqf.discovery_id,
        cqf.flag_type,
        cqf.metadata,
        cqf.flagged_at,
        cqf.resolved_at,
        au.email AS resolved_by_email,
        cqf.resolution_note,
        cd.name AS coach_name,
        cd.email AS coach_email,
        cd.club_id,
        cc.club_name_canonical AS club_display_name,
        /* The joined "raw" club name is the discovery's club_id-associated
           canonical row if linked; otherwise NULL. coach_discoveries has
           no club_name_raw column, so we surface the canonical name there
           too and let the UI differentiate via clubId presence. */
        cc.club_name_canonical AS club_name_raw,
        COUNT(*) OVER () AS total
      FROM ${coachQualityFlags} cqf
      JOIN ${coachDiscoveries} cd ON cd.id = cqf.discovery_id
      LEFT JOIN ${canonicalClubs} cc ON cc.id = cd.club_id
      LEFT JOIN ${adminUsers} au ON au.id = cqf.resolved_by
      WHERE ${flagTypePredicate}
        AND ${resolvedPredicate}
      ORDER BY
        (cqf.resolved_at IS NOT NULL) ASC,
        cqf.flagged_at ASC,
        cqf.id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const list = Array.from(
      result as unknown as Array<{
        id: number;
        discovery_id: number;
        flag_type: string;
        metadata: unknown;
        flagged_at: Date | string;
        resolved_at: Date | string | null;
        resolved_by_email: string | null;
        resolution_note: string | null;
        coach_name: string;
        coach_email: string | null;
        club_name_raw: string | null;
        club_id: number | null;
        club_display_name: string | null;
        total: string;
      }>,
    );

    const total = Number(list[0]?.total ?? 0);
    const rows: CoachQualityFlagRawRow[] = list.map((r) => ({
      id: r.id,
      discoveryId: r.discovery_id,
      flagType: r.flag_type,
      metadata: r.metadata,
      flaggedAt: r.flagged_at,
      resolvedAt: r.resolved_at,
      resolvedByEmail: r.resolved_by_email,
      resolutionNote: r.resolution_note,
      coachName: r.coach_name,
      coachEmail: r.coach_email,
      clubNameRaw: r.club_name_raw,
      clubId: r.club_id,
      clubDisplayName: r.club_display_name,
    }));

    return { rows, total };
  },
};

// ---------------------------------------------------------------------------
// Resolve coach_quality_flags — DI surface + handler factory + prod wiring.
// Mirrors the roster_quality_flags resolve endpoint one-for-one.
// ---------------------------------------------------------------------------

export interface ResolveCoachQualityFlagDeps {
  resolveFlag: (args: {
    id: number;
    resolvedBy: number | null;
  }) => Promise<{ outcome: ResolveOutcome }>;
}

/**
 * PATCH /api/v1/admin/data-quality/coach-quality-flags/:id/resolve
 * 204 on first resolve, 400 if already resolved, 404 if id unknown.
 */
export function makeResolveCoachQualityFlagHandler(
  deps: ResolveCoachQualityFlagDeps,
): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      const { outcome } = await deps.resolveFlag({
        id,
        resolvedBy: adminUserId,
      });

      if (outcome === "not_found") {
        res.status(404).json({ error: "CoachQualityFlag not found" });
        return;
      }
      if (outcome === "already_resolved") {
        res.status(400).json({ error: "Flag is already resolved" });
        return;
      }
      res.status(204).send();
    } catch (err) {
      next(err);
    }
  };
}

/**
 * Production wiring for the coach-quality-flag resolve endpoint. Scoped to
 * `coach_quality_flags` as a whole — any row with the matching id is
 * resolved regardless of `flag_type`, so new flag types added to the CHECK
 * list resolve correctly without a code change.
 */
export const prodResolveCoachQualityFlagDeps: ResolveCoachQualityFlagDeps = {
  resolveFlag: async ({ id, resolvedBy }) => {
    const updated = await defaultDb.execute<{ id: number }>(sql`
      UPDATE ${coachQualityFlags}
      SET resolved_at = NOW(),
          resolved_by = ${resolvedBy}
      WHERE id = ${id}
        AND resolved_at IS NULL
      RETURNING id
    `);
    if (Array.from(updated as unknown as Array<{ id: number }>).length > 0) {
      return { outcome: "resolved" };
    }
    const existing = await defaultDb.execute<{ id: number }>(sql`
      SELECT id
      FROM ${coachQualityFlags}
      WHERE id = ${id}
      LIMIT 1
    `);
    return {
      outcome:
        Array.from(existing as unknown as Array<{ id: number }>).length > 0
          ? "already_resolved"
          : "not_found",
    };
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
// Read-only panel: coach-misses
// ---------------------------------------------------------------------------

/**
 * GET /v1/admin/data-quality/coach-misses?division=D1&gender=womens&page=1&page_size=20
 *
 * Rows from `coach_misses` (populated by the NCAA roster scraper when env
 * `COACH_MISSES_REPORT_ENABLED=true`). For each (college, gender_program)
 * pair we surface the most recent miss only — operators care about the
 * current state of the queue, not historical re-misses on the same school.
 *
 * The newline-separated `probed_urls` text column is split into
 * `probedUrls: string[]` at the API boundary so the dashboard never sees
 * the storage detail.
 */
export const coachMissesHandler: RequestHandler = async (req, res, next) => {
  try {
    const parsed = CoachMissesRequest.safeParse({
      division: toStringOrUndefined(req.query.division),
      gender: toStringOrUndefined(req.query.gender),
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
    const { division, gender, page, pageSize } = parsed.data;
    const offset = (page - 1) * pageSize;

    // Build optional WHERE filters as composable raw SQL fragments so
    // the count and the page query share the exact same predicate.
    const divisionPredicate = division
      ? sql`AND cm.division = ${division}`
      : sql``;
    const genderPredicate = gender
      ? sql`AND cm.gender_program = ${gender}`
      : sql``;

    // Most-recent miss per (college, gender_program). DISTINCT ON keeps
    // the top row per partition after ORDER BY — Postgres-specific but
    // we're not portable. The total count uses the same DISTINCT key
    // so pagination math stays consistent.
    const countResult = await defaultDb.execute<{ count: number }>(sql`
      SELECT count(*)::int AS count
      FROM (
        SELECT DISTINCT ON (cm.college_id, cm.gender_program) cm.id
        FROM ${coachMisses} cm
        WHERE 1 = 1 ${divisionPredicate} ${genderPredicate}
        ORDER BY cm.college_id, cm.gender_program, cm.recorded_at DESC, cm.id DESC
      ) AS uniq
    `);
    const countRow = (countResult as unknown as Array<{ count: number }>)[0];
    const total = Number(countRow?.count ?? 0);

    const rowsResult = await defaultDb.execute<{
      college_id: number;
      college_name: string;
      division: string;
      gender_program: string;
      roster_url: string | null;
      probed_urls: string | null;
      scrape_run_log_id: number | null;
      recorded_at: Date | string;
    }>(sql`
      WITH latest AS (
        SELECT DISTINCT ON (cm.college_id, cm.gender_program)
          cm.college_id,
          cm.division,
          cm.gender_program,
          cm.roster_url,
          cm.probed_urls,
          cm.scrape_run_log_id,
          cm.recorded_at
        FROM ${coachMisses} cm
        WHERE 1 = 1 ${divisionPredicate} ${genderPredicate}
        ORDER BY cm.college_id, cm.gender_program, cm.recorded_at DESC, cm.id DESC
      )
      SELECT
        l.college_id,
        co.name AS college_name,
        l.division,
        l.gender_program,
        l.roster_url,
        l.probed_urls,
        l.scrape_run_log_id,
        l.recorded_at
      FROM latest l
      LEFT JOIN ${colleges} co ON co.id = l.college_id
      ORDER BY l.recorded_at DESC, l.college_id ASC
      LIMIT ${pageSize} OFFSET ${offset}
    `);

    const rows = Array.from(
      rowsResult as unknown as Array<{
        college_id: number;
        college_name: string | null;
        division: string;
        gender_program: string;
        roster_url: string | null;
        probed_urls: string | null;
        scrape_run_log_id: number | null;
        recorded_at: Date | string;
      }>,
    ).map((r) => ({
      collegeId: Number(r.college_id),
      collegeName: r.college_name ?? `college #${r.college_id}`,
      division: r.division,
      genderProgram: r.gender_program,
      rosterUrl: r.roster_url,
      probedUrls: r.probed_urls
        ? r.probed_urls.split("\n").filter((s) => s.length > 0)
        : [],
      scrapeRunLogId:
        r.scrape_run_log_id === null ? null : Number(r.scrape_run_log_id),
      recordedAt: toIsoRequired(r.recorded_at),
    }));

    res.json(
      CoachMissesResponse.parse({ rows, total, page, pageSize }),
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
  if (s === "true" || s === "1" || s === "yes") return true;
  if (s === "false" || s === "0" || s === "no") return false;
  return undefined;
}

function toStringOrUndefined(raw: unknown): string | undefined {
  if (raw === undefined || raw === null || raw === "") return undefined;
  return String(raw);
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
