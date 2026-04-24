/**
 * `/api/v1/admin/dedup/*` — canonical-club duplicate-review routes.
 *
 *   GET  /api/v1/admin/dedup/clubs?status=pending&limit=50&page=1
 *   GET  /api/v1/admin/dedup/clubs/:id
 *   POST /api/v1/admin/dedup/clubs/:id/merge
 *   POST /api/v1/admin/dedup/clubs/:id/reject
 *
 * The queue is populated by `scraper/dedup/club_dedup.py --persist` (see
 * `club_duplicates` schema). This router is the human-in-the-loop side:
 *  - list/detail reads the queue (plus live side-state for the detail view)
 *  - merge calls the transactional `mergeClubs` helper in @workspace/db
 *    and flips the queue row to `status='merged'` under the same tx
 *  - reject is a single UPDATE to `status='rejected'`
 *
 * Response projection for /merge
 * ------------------------------
 * `mergeClubs` returns an 18-field `MergeClubsResult` (per-table reparent
 * counts — useful for server-side logging / operator debugging). The
 * public contract (`ClubDuplicateMergeResponse` in `@hlbiv/api-zod/admin`)
 * only surfaces 5 of those fields: `ok`, `winnerId`, `loserAliasesCreated`,
 * `affiliationsReparented`, `rosterSnapshotsReparented`. We log the full
 * result server-side and project down to the 5-field contract shape so
 * the client never sees the extended internal detail.
 *
 * Auth / rate limits
 * ------------------
 * Mounted under `authedAdminRouter` in `./index.ts` — the admin-surface
 * `requireAdmin` guard + 120/min rate limiter already covers these routes.
 * Mutation-specific (30/min) limits are not applied per-route today: the
 * admin surface is human-operator-scale and the dedup queue is small.
 *
 * Factory shape
 * -------------
 * `makeDedupRouter(deps)` accepts injectable DB helpers so tests can drive
 * the handlers without a live Postgres. The default exported `dedupRouter`
 * is the production wiring.
 */
import { Router, type IRouter, type RequestHandler } from "express";
import { and, asc, eq, sql, type SQL } from "drizzle-orm";
import {
  db as defaultDb,
  clubDuplicates,
  canonicalClubs,
  clubAffiliations,
  clubRosterSnapshots,
  collegeDuplicates,
  colleges as collegesTable,
  mergeClubs as defaultMergeClubs,
  mergeColleges as defaultMergeColleges,
  type ClubDuplicate as ClubDuplicateRow,
  type CanonicalClub,
  type MergeClubsResult,
  type CollegeDuplicate as CollegeDuplicateRow,
  type MergeCollegesResult,
} from "@workspace/db";
import {
  ClubDuplicate,
  ClubDuplicateList,
  ClubDuplicateDetail,
  ClubDuplicateMergeRequest,
  ClubDuplicateMergeResponse,
  CollegeDuplicate,
  CollegeDuplicateList,
  CollegeDuplicateDetail,
  CollegeDuplicateMergeRequest,
  CollegeDuplicateMergeResponse,
} from "@hlbiv/api-zod/admin";
import { parsePagination } from "../../lib/pagination";

// Type for a colleges row (subset of fields we expose)
type CollegeRow = typeof collegesTable.$inferSelect;

// ---------------------------------------------------------------------------
// Dependency injection surface.
// ---------------------------------------------------------------------------

/**
 * The tests drive the handlers with a fake "DB facade" + fake mergeClubs.
 * Keeping the surface narrow means the fake can be a plain object literal;
 * we don't drag in Drizzle's `NodePgDatabase` type.
 */
export interface DedupDeps {
  listPairs: (args: {
    status: string;
    limit: number;
    offset: number;
  }) => Promise<{ rows: ClubDuplicateRow[]; total: number }>;
  getPairById: (id: number) => Promise<ClubDuplicateRow | null>;
  getClubById: (id: number) => Promise<CanonicalClub | null>;
  countAffiliations: (clubId: number) => Promise<number>;
  countRosterSnapshots: (clubId: number) => Promise<number>;
  mergeAndMarkReviewed: (args: {
    pairId: number;
    winnerId: number;
    loserId: number;
    reviewedBy: number | null;
    notes?: string;
  }) => Promise<MergeClubsResult>;
  rejectPair: (args: {
    pairId: number;
    reviewedBy: number | null;
    notes?: string;
  }) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Row → contract projections.
// ---------------------------------------------------------------------------

function toContractStatus(
  s: string,
): "pending" | "merged" | "rejected" {
  if (s === "merged" || s === "rejected") return s;
  return "pending";
}

function rowToContract(row: ClubDuplicateRow): unknown {
  return {
    id: row.id,
    leftClubId: row.leftClubId,
    rightClubId: row.rightClubId,
    score: row.score,
    method: row.method,
    status: toContractStatus(row.status),
    createdAt: row.createdAt.toISOString(),
    reviewedAt: row.reviewedAt ? row.reviewedAt.toISOString() : null,
    reviewedBy: row.reviewedBy ?? null,
    // Snapshots are JSONB in the DB — store-and-forward to the client. If
    // somehow null (shouldn't happen — column is NOT NULL), coerce to {}.
    leftSnapshot: (row.leftSnapshot as Record<string, unknown> | null) ?? {},
    rightSnapshot: (row.rightSnapshot as Record<string, unknown> | null) ?? {},
  };
}

function clubToRecord(
  club: CanonicalClub | null,
): Record<string, unknown> {
  if (!club) return {};
  // Serialize to a plain record — preserve ISO strings for timestamps.
  return {
    id: club.id,
    clubNameCanonical: club.clubNameCanonical,
    clubSlug: club.clubSlug,
    city: club.city,
    state: club.state,
    country: club.country,
    status: club.status,
    website: club.website,
    logoUrl: club.logoUrl,
    foundedYear: club.foundedYear,
    manuallyMerged: club.manuallyMerged,
    lastScrapedAt: club.lastScrapedAt
      ? club.lastScrapedAt.toISOString()
      : null,
    scrapeConfidence: club.scrapeConfidence,
  };
}

// ---------------------------------------------------------------------------
// Handler factories.
// ---------------------------------------------------------------------------

export function makeListHandler(deps: DedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const rawStatus = req.query.status;
      const status =
        typeof rawStatus === "string" && rawStatus.length > 0
          ? rawStatus
          : "pending";

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.limit ?? req.query.page_size,
      );

      const { rows, total } = await deps.listPairs({
        status,
        limit: pageSize,
        offset,
      });

      res.json(
        ClubDuplicateList.parse({
          pairs: rows.map(rowToContract),
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

export function makeDetailHandler(deps: DedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const pair = await deps.getPairById(id);
      if (!pair) {
        res.status(404).json({ error: "ClubDuplicate not found" });
        return;
      }

      const [leftCurrent, rightCurrent, leftAffil, rightAffil, leftRoster, rightRoster] =
        await Promise.all([
          deps.getClubById(pair.leftClubId),
          deps.getClubById(pair.rightClubId),
          deps.countAffiliations(pair.leftClubId),
          deps.countAffiliations(pair.rightClubId),
          deps.countRosterSnapshots(pair.leftClubId),
          deps.countRosterSnapshots(pair.rightClubId),
        ]);

      const base = rowToContract(pair) as Record<string, unknown>;
      res.json(
        ClubDuplicateDetail.parse({
          ...base,
          leftCurrent: clubToRecord(leftCurrent),
          rightCurrent: clubToRecord(rightCurrent),
          affiliations: {
            leftAffiliationCount: leftAffil,
            rightAffiliationCount: rightAffil,
          },
          rosters: {
            leftRosterSnapshotCount: leftRoster,
            rightRosterSnapshotCount: rightRoster,
          },
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeMergeHandler(deps: DedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const parsed = ClubDuplicateMergeRequest.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid request body" });
        return;
      }
      const { winnerId, loserId, notes } = parsed.data;

      const pair = await deps.getPairById(id);
      if (!pair) {
        res.status(404).json({ error: "ClubDuplicate not found" });
        return;
      }

      if (pair.status !== "pending") {
        res.status(409).json({
          error: "already_reviewed",
          status: pair.status,
        });
        return;
      }

      // Accept either orientation of (winner, loser) relative to (left, right).
      const matchesOrder =
        winnerId === pair.leftClubId && loserId === pair.rightClubId;
      const matchesReversed =
        winnerId === pair.rightClubId && loserId === pair.leftClubId;
      if (!matchesOrder && !matchesReversed) {
        res.status(400).json({
          error: "winner_loser_mismatch",
        });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      const result = await deps.mergeAndMarkReviewed({
        pairId: id,
        winnerId,
        loserId,
        reviewedBy: adminUserId,
        notes,
      });

      // Log the full 18-field helper result server-side for operator
      // debugging; only the 5-field contract shape goes to the client.
      // eslint-disable-next-line no-console
      console.info("[admin-dedup] merge completed", {
        pairId: id,
        winnerId,
        loserId,
        reviewedBy: adminUserId,
        result,
      });

      res.json(
        ClubDuplicateMergeResponse.parse({
          ok: true,
          winnerId: result.winnerId,
          loserAliasesCreated: result.loserAliasesCreated,
          affiliationsReparented: result.affiliationsReparented,
          rosterSnapshotsReparented: result.rosterSnapshotsReparented,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeRejectHandler(deps: DedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      // Body is optional. When present we accept an opaque `{notes?}`.
      const notes =
        req.body && typeof req.body === "object" && typeof (req.body as { notes?: unknown }).notes === "string"
          ? ((req.body as { notes: string }).notes)
          : undefined;

      const pair = await deps.getPairById(id);
      if (!pair) {
        res.status(404).json({ error: "ClubDuplicate not found" });
        return;
      }
      if (pair.status !== "pending") {
        res.status(409).json({
          error: "already_reviewed",
          status: pair.status,
        });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      await deps.rejectPair({
        pairId: id,
        reviewedBy: adminUserId,
        notes,
      });

      res.json({ ok: true, id });
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Router factory + default wiring.
// ---------------------------------------------------------------------------

export function makeDedupRouter(deps: DedupDeps): IRouter {
  const router: IRouter = Router();
  router.get("/clubs", makeListHandler(deps));
  router.get("/clubs/:id", makeDetailHandler(deps));
  router.post("/clubs/:id/merge", makeMergeHandler(deps));
  router.post("/clubs/:id/reject", makeRejectHandler(deps));
  return router;
}

// ---------------------------------------------------------------------------
// Production (live DB) dependency wiring.
// ---------------------------------------------------------------------------

async function listPairs(args: {
  status: string;
  limit: number;
  offset: number;
}): Promise<{ rows: ClubDuplicateRow[]; total: number }> {
  const conditions: SQL[] = [];
  // The contract uses the DB enum names directly (pending|merged|rejected).
  // Defensive: if the caller passes a freeform string, only filter when it
  // matches one of the known values — otherwise return nothing (safer than
  // returning everything under a typo).
  const { status } = args;
  if (status === "pending" || status === "merged" || status === "rejected") {
    conditions.push(eq(clubDuplicates.status, status));
  } else if (status === "all") {
    // No predicate — include every row.
  } else {
    // Unknown value: match nothing.
    conditions.push(sql`1 = 0`);
  }

  const where = conditions.length > 0 ? and(...conditions) : undefined;

  const [countRow] = await defaultDb
    .select({ count: sql<number>`count(*)::int` })
    .from(clubDuplicates)
    .where(where);

  const rows = await defaultDb
    .select()
    .from(clubDuplicates)
    .where(where)
    // Highest-score first within the page so reviewers see the strongest
    // candidates at the top of the queue.
    .orderBy(sql`${clubDuplicates.score} DESC`, asc(clubDuplicates.id))
    .limit(args.limit)
    .offset(args.offset);

  return { rows, total: countRow?.count ?? 0 };
}

async function getPairById(id: number): Promise<ClubDuplicateRow | null> {
  const [row] = await defaultDb
    .select()
    .from(clubDuplicates)
    .where(eq(clubDuplicates.id, id))
    .limit(1);
  return row ?? null;
}

async function getClubById(id: number): Promise<CanonicalClub | null> {
  const [row] = await defaultDb
    .select()
    .from(canonicalClubs)
    .where(eq(canonicalClubs.id, id))
    .limit(1);
  return row ?? null;
}

async function countAffiliations(clubId: number): Promise<number> {
  const [row] = await defaultDb
    .select({ count: sql<number>`count(*)::int` })
    .from(clubAffiliations)
    .where(eq(clubAffiliations.clubId, clubId));
  return row?.count ?? 0;
}

async function countRosterSnapshots(clubId: number): Promise<number> {
  const [row] = await defaultDb
    .select({ count: sql<number>`count(*)::int` })
    .from(clubRosterSnapshots)
    .where(eq(clubRosterSnapshots.clubId, clubId));
  return row?.count ?? 0;
}

async function mergeAndMarkReviewed(args: {
  pairId: number;
  winnerId: number;
  loserId: number;
  reviewedBy: number | null;
  notes?: string;
}): Promise<MergeClubsResult> {
  // Wrap the transactional `mergeClubs` helper + the club_duplicates row
  // update in a single outer transaction so both commit atomically. If the
  // status-flip fails, the merge rolls back; if the merge throws, the
  // row update never runs.
  return defaultDb.transaction(async (tx) => {
    const result = await defaultMergeClubs({
      // The helper wants a `NodePgDatabase<typeof schema>`; the tx object
      // has the same callable surface (`.transaction`, `.select`, `.execute`).
      // Cast is safe because mergeClubs only uses `.transaction(cb)` → we
      // nest but still commit together.
      db: tx as unknown as typeof defaultDb,
      winnerId: args.winnerId,
      loserId: args.loserId,
      reviewedBy: args.reviewedBy,
      notes: args.notes,
    });
    await tx
      .update(clubDuplicates)
      .set({
        status: "merged",
        reviewedAt: new Date(),
        reviewedBy: args.reviewedBy,
        notes: args.notes ?? null,
      })
      .where(eq(clubDuplicates.id, args.pairId));
    return result;
  });
}

async function rejectPair(args: {
  pairId: number;
  reviewedBy: number | null;
  notes?: string;
}): Promise<void> {
  await defaultDb
    .update(clubDuplicates)
    .set({
      status: "rejected",
      reviewedAt: new Date(),
      reviewedBy: args.reviewedBy,
      notes: args.notes ?? null,
    })
    .where(eq(clubDuplicates.id, args.pairId));
}

export const dedupRouter: IRouter = makeDedupRouter({
  listPairs,
  getPairById,
  getClubById,
  countAffiliations,
  countRosterSnapshots,
  mergeAndMarkReviewed,
  rejectPair,
});

export default dedupRouter;

// ===========================================================================
// College dedup routes
// ===========================================================================

/**
 * `/api/v1/admin/dedup/colleges/*` — college duplicate-review routes.
 *
 *   GET  /api/v1/admin/dedup/colleges?status=pending&limit=50&page=1
 *   GET  /api/v1/admin/dedup/colleges/:id
 *   POST /api/v1/admin/dedup/colleges/:id/merge
 *   POST /api/v1/admin/dedup/colleges/:id/reject
 *
 * Factory pattern mirrors the club dedup router above.
 */

export interface CollegeDedupDeps {
  listCollegePairs: (args: {
    status: string;
    limit: number;
    offset: number;
  }) => Promise<{ rows: CollegeDuplicateRow[]; total: number }>;
  getCollegePairById: (id: number) => Promise<CollegeDuplicateRow | null>;
  getCollegeById: (id: number) => Promise<CollegeRow | null>;
  mergeCollegesAndMarkReviewed: (args: {
    pairId: number;
    winnerId: number;
    loserId: number;
    reviewedBy: number | null;
    notes?: string;
  }) => Promise<MergeCollegesResult>;
  rejectCollegePair: (args: {
    pairId: number;
    reviewedBy: number | null;
    notes?: string;
  }) => Promise<void>;
}

// ---------------------------------------------------------------------------
// Row → contract projections for colleges.
// ---------------------------------------------------------------------------

function toContractCollegeStatus(s: string): "pending" | "merged" | "rejected" {
  if (s === "merged" || s === "rejected") return s;
  return "pending";
}

function collegeRowToContract(row: CollegeDuplicateRow): unknown {
  return {
    id: row.id,
    leftCollegeId: row.leftCollegeId,
    rightCollegeId: row.rightCollegeId,
    score: row.score,
    method: row.method,
    status: toContractCollegeStatus(row.status),
    createdAt: row.createdAt.toISOString(),
    reviewedAt: row.reviewedAt ? row.reviewedAt.toISOString() : null,
    reviewedBy: row.reviewedBy ?? null,
    leftSnapshot: (row.leftSnapshot as Record<string, unknown> | null) ?? {},
    rightSnapshot: (row.rightSnapshot as Record<string, unknown> | null) ?? {},
  };
}

function collegeToRecord(college: CollegeRow | null): Record<string, unknown> {
  if (!college) return {};
  return {
    id: college.id,
    name: college.name,
    slug: college.slug,
    division: college.division,
    genderProgram: college.genderProgram,
    conference: college.conference,
    state: college.state,
    city: college.city,
    website: college.website,
    logoUrl: college.logoUrl,
    lastScrapedAt: college.lastScrapedAt
      ? college.lastScrapedAt.toISOString()
      : null,
  };
}

// ---------------------------------------------------------------------------
// Handler factories for colleges.
// ---------------------------------------------------------------------------

export function makeCollegeListHandler(deps: CollegeDedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const rawStatus = req.query.status;
      const status =
        typeof rawStatus === "string" && rawStatus.length > 0
          ? rawStatus
          : "pending";

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.limit ?? req.query.page_size,
      );

      const { rows, total } = await deps.listCollegePairs({
        status,
        limit: pageSize,
        offset,
      });

      res.json(
        CollegeDuplicateList.parse({
          pairs: rows.map(collegeRowToContract),
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

export function makeCollegeDetailHandler(deps: CollegeDedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const pair = await deps.getCollegePairById(id);
      if (!pair) {
        res.status(404).json({ error: "CollegeDuplicate not found" });
        return;
      }

      const [leftCurrent, rightCurrent] = await Promise.all([
        deps.getCollegeById(pair.leftCollegeId),
        deps.getCollegeById(pair.rightCollegeId),
      ]);

      const base = collegeRowToContract(pair) as Record<string, unknown>;
      res.json(
        CollegeDuplicateDetail.parse({
          ...base,
          leftCurrent: collegeToRecord(leftCurrent),
          rightCurrent: collegeToRecord(rightCurrent),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeCollegeMergeHandler(deps: CollegeDedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const parsed = CollegeDuplicateMergeRequest.safeParse(req.body);
      if (!parsed.success) {
        res.status(400).json({ error: "Invalid request body" });
        return;
      }
      const { winnerId, loserId, notes } = parsed.data;

      const pair = await deps.getCollegePairById(id);
      if (!pair) {
        res.status(404).json({ error: "CollegeDuplicate not found" });
        return;
      }

      if (pair.status !== "pending") {
        res.status(409).json({
          error: "already_reviewed",
          status: pair.status,
        });
        return;
      }

      // Accept either orientation of (winner, loser) relative to (left, right).
      const matchesOrder =
        winnerId === pair.leftCollegeId && loserId === pair.rightCollegeId;
      const matchesReversed =
        winnerId === pair.rightCollegeId && loserId === pair.leftCollegeId;
      if (!matchesOrder && !matchesReversed) {
        res.status(400).json({ error: "winner_loser_mismatch" });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      const result = await deps.mergeCollegesAndMarkReviewed({
        pairId: id,
        winnerId,
        loserId,
        reviewedBy: adminUserId,
        notes,
      });

      // eslint-disable-next-line no-console
      console.info("[admin-dedup] college merge completed", {
        pairId: id,
        winnerId,
        loserId,
        reviewedBy: adminUserId,
        result,
      });

      res.json(
        CollegeDuplicateMergeResponse.parse({
          ok: true,
          winnerId: result.winnerId,
          loserAliasesCreated: result.loserAliasesCreated,
          coachesReparented: result.coachesReparented,
          rosterRowsReparented: result.rosterRowsReparented,
          tenuresReparented: result.tenuresReparented,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

export function makeCollegeRejectHandler(deps: CollegeDedupDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || id <= 0) {
        res.status(400).json({ error: "Invalid id" });
        return;
      }

      const notes =
        req.body &&
        typeof req.body === "object" &&
        typeof (req.body as { notes?: unknown }).notes === "string"
          ? (req.body as { notes: string }).notes
          : undefined;

      const pair = await deps.getCollegePairById(id);
      if (!pair) {
        res.status(404).json({ error: "CollegeDuplicate not found" });
        return;
      }
      if (pair.status !== "pending") {
        res.status(409).json({
          error: "already_reviewed",
          status: pair.status,
        });
        return;
      }

      const adminUserId =
        req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

      await deps.rejectCollegePair({
        pairId: id,
        reviewedBy: adminUserId,
        notes,
      });

      res.json({ ok: true, id });
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// College router factory + default wiring.
// ---------------------------------------------------------------------------

export function makeCollegeDedupRouter(deps: CollegeDedupDeps): IRouter {
  const router: IRouter = Router();
  router.get("/colleges", makeCollegeListHandler(deps));
  router.get("/colleges/:id", makeCollegeDetailHandler(deps));
  router.post("/colleges/:id/merge", makeCollegeMergeHandler(deps));
  router.post("/colleges/:id/reject", makeCollegeRejectHandler(deps));
  return router;
}

// ---------------------------------------------------------------------------
// Production (live DB) college dependency wiring.
// ---------------------------------------------------------------------------

async function listCollegePairs(args: {
  status: string;
  limit: number;
  offset: number;
}): Promise<{ rows: CollegeDuplicateRow[]; total: number }> {
  const conditions: SQL[] = [];
  const { status } = args;
  if (status === "pending" || status === "merged" || status === "rejected") {
    conditions.push(eq(collegeDuplicates.status, status));
  } else if (status === "all") {
    // No predicate — include every row.
  } else {
    conditions.push(sql`1 = 0`);
  }

  const where = conditions.length > 0 ? and(...conditions) : undefined;

  const [countRow] = await defaultDb
    .select({ count: sql<number>`count(*)::int` })
    .from(collegeDuplicates)
    .where(where);

  const rows = await defaultDb
    .select()
    .from(collegeDuplicates)
    .where(where)
    .orderBy(sql`${collegeDuplicates.score} DESC`, asc(collegeDuplicates.id))
    .limit(args.limit)
    .offset(args.offset);

  return { rows, total: countRow?.count ?? 0 };
}

async function getCollegePairById(id: number): Promise<CollegeDuplicateRow | null> {
  const [row] = await defaultDb
    .select()
    .from(collegeDuplicates)
    .where(eq(collegeDuplicates.id, id))
    .limit(1);
  return row ?? null;
}

async function getCollegeById(id: number): Promise<CollegeRow | null> {
  const [row] = await defaultDb
    .select()
    .from(collegesTable)
    .where(eq(collegesTable.id, id))
    .limit(1);
  return row ?? null;
}

async function mergeCollegesAndMarkReviewed(args: {
  pairId: number;
  winnerId: number;
  loserId: number;
  reviewedBy: number | null;
  notes?: string;
}): Promise<MergeCollegesResult> {
  return defaultDb.transaction(async (tx) => {
    const result = await defaultMergeColleges({
      db: tx as unknown as typeof defaultDb,
      winnerId: args.winnerId,
      loserId: args.loserId,
      reviewedBy: args.reviewedBy,
      notes: args.notes,
    });
    await tx
      .update(collegeDuplicates)
      .set({
        status: "merged",
        reviewedAt: new Date(),
        reviewedBy: args.reviewedBy,
        notes: args.notes ?? null,
      })
      .where(eq(collegeDuplicates.id, args.pairId));
    return result;
  });
}

async function rejectCollegePair(args: {
  pairId: number;
  reviewedBy: number | null;
  notes?: string;
}): Promise<void> {
  await defaultDb
    .update(collegeDuplicates)
    .set({
      status: "rejected",
      reviewedAt: new Date(),
      reviewedBy: args.reviewedBy,
      notes: args.notes ?? null,
    })
    .where(eq(collegeDuplicates.id, args.pairId));
}

export const collegeDedupRouter: IRouter = makeCollegeDedupRouter({
  listCollegePairs,
  getCollegePairById,
  getCollegeById,
  mergeCollegesAndMarkReviewed,
  rejectCollegePair,
});
