/**
 * admin/dedup routes — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminDedup.test.ts
 *
 * Harness mirrors adminAuth.test.ts — drive the factory-built handlers
 * directly with fake DB deps. No HTTP server, no real DB. Scenarios:
 *
 *   1. GET list returns pending rows paginated.
 *   2. GET detail enriches with current-state counts.
 *   3. POST merge (happy path) → calls mergeClubs + updates status.
 *   4. POST merge on non-pending row → 409.
 *   5. POST merge with mismatched winner/loser → 400.
 *   6. POST reject → marks status='rejected'.
 */
import type { Request, Response } from "express";
import {
  makeListHandler,
  makeDetailHandler,
  makeMergeHandler,
  makeRejectHandler,
  type DedupDeps,
} from "../routes/admin/dedup";
import type {
  ClubDuplicate as ClubDuplicateRow,
  CanonicalClub,
  MergeClubsResult,
} from "@workspace/db";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// ---------------------------------------------------------------------------
// Fake req/res.
// ---------------------------------------------------------------------------

type FakeRes = {
  statusCode: number;
  body: unknown;
  status: (code: number) => FakeRes;
  json: (body: unknown) => FakeRes;
};

function makeRes(): FakeRes {
  const res: FakeRes = {
    statusCode: 200,
    body: undefined,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
  return res;
}

function makeReq(opts: {
  params?: Record<string, string>;
  query?: Record<string, string>;
  body?: unknown;
  adminUserId?: number | null;
}): Request {
  const adminAuth =
    opts.adminUserId != null
      ? {
          kind: "session" as const,
          userId: opts.adminUserId,
          email: "ops@upshift.test",
          role: "admin" as const,
          sessionId: 7,
        }
      : {
          kind: "apiKey" as const,
          keyId: 1,
          keyName: "test-key",
          scopes: ["admin"],
        };
  return {
    params: opts.params ?? {},
    query: opts.query ?? {},
    body: opts.body ?? {},
    adminAuth,
  } as unknown as Request;
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

function pendingPair(overrides: Partial<ClubDuplicateRow> = {}): ClubDuplicateRow {
  return {
    id: 101,
    leftClubId: 11,
    rightClubId: 22,
    score: 0.91,
    method: "name_fuzzy_88",
    status: "pending",
    leftSnapshot: { id: 11, name: "FC Example" },
    rightSnapshot: { id: 22, name: "F.C. Example" },
    createdAt: new Date("2026-04-01T00:00:00Z"),
    reviewedAt: null,
    reviewedBy: null,
    notes: null,
    ...overrides,
  };
}

function club(id: number, name: string): CanonicalClub {
  return {
    id,
    clubNameCanonical: name,
    clubSlug: null,
    city: null,
    state: null,
    country: "USA",
    status: "active",
    website: null,
    websiteDiscoveredAt: null,
    websiteStatus: null,
    logoUrl: null,
    foundedYear: null,
    twitter: null,
    instagram: null,
    facebook: null,
    staffPageUrl: null,
    websiteLastCheckedAt: null,
    lastScrapedAt: null,
    scrapeConfidence: null,
    manuallyMerged: false,
  } as CanonicalClub;
}

function fullMergeResult(): MergeClubsResult {
  return {
    ok: true,
    winnerId: 11,
    loserAliasesCreated: 1,
    affiliationsReparented: 3,
    rosterSnapshotsReparented: 42,
    rosterDiffsReparented: 0,
    eventTeamsReparented: 2,
    matchesReparented: 0,
    clubResultsReparented: 0,
    commitmentsReparented: 0,
    yntReparented: 0,
    odpReparented: 0,
    coachCareerReparented: 0,
    tryoutsReparented: 0,
    siteChangesReparented: 0,
    clubDuplicatesMarked: 1,
    coachDiscoveriesReparented: 5,
  };
}

/**
 * Build a DedupDeps fake out of an in-memory "store" so individual test
 * scenarios only override the bits they exercise.
 */
function makeDeps(
  state: {
    pairs?: ClubDuplicateRow[];
    clubs?: CanonicalClub[];
    affiliationCounts?: Map<number, number>;
    rosterCounts?: Map<number, number>;
    mergeResult?: MergeClubsResult;
    mergeCalls?: Array<{
      pairId: number;
      winnerId: number;
      loserId: number;
      reviewedBy: number | null;
      notes?: string;
    }>;
    rejectCalls?: Array<{
      pairId: number;
      reviewedBy: number | null;
      notes?: string;
    }>;
  } = {},
): DedupDeps {
  const pairs = state.pairs ?? [];
  const clubs = state.clubs ?? [];
  const affiliationCounts = state.affiliationCounts ?? new Map<number, number>();
  const rosterCounts = state.rosterCounts ?? new Map<number, number>();

  return {
    listPairs: async ({ status, limit, offset }) => {
      const filtered =
        status === "all"
          ? pairs
          : pairs.filter((p) => p.status === status);
      return {
        rows: filtered.slice(offset, offset + limit),
        total: filtered.length,
      };
    },
    getPairById: async (id) => pairs.find((p) => p.id === id) ?? null,
    getClubById: async (id) => clubs.find((c) => c.id === id) ?? null,
    countAffiliations: async (id) => affiliationCounts.get(id) ?? 0,
    countRosterSnapshots: async (id) => rosterCounts.get(id) ?? 0,
    mergeAndMarkReviewed: async (args) => {
      state.mergeCalls?.push(args);
      // Mutate the pair in the in-memory store so a subsequent getPairById
      // sees 'merged'. Mirrors what the real transactional helper would do.
      const pair = pairs.find((p) => p.id === args.pairId);
      if (pair) {
        pair.status = "merged";
        pair.reviewedAt = new Date();
        pair.reviewedBy = args.reviewedBy;
      }
      return state.mergeResult ?? fullMergeResult();
    },
    rejectPair: async (args) => {
      state.rejectCalls?.push(args);
      const pair = pairs.find((p) => p.id === args.pairId);
      if (pair) {
        pair.status = "rejected";
        pair.reviewedAt = new Date();
        pair.reviewedBy = args.reviewedBy;
      }
    },
  };
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function run() {
  // --- 1. GET list returns pending rows paginated -------------------------
  {
    const pairs = [
      pendingPair({ id: 1, score: 0.95 }),
      pendingPair({ id: 2, score: 0.92 }),
      pendingPair({ id: 3, score: 0.88 }),
      pendingPair({ id: 99, status: "merged" }),
    ];
    const handler = makeListHandler(makeDeps({ pairs }));
    const req = makeReq({ query: { status: "pending", limit: "2", page: "1" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "list-pending", `expected 200, got ${res.statusCode}`);
    const body = res.body as {
      pairs?: unknown[];
      total?: number;
      page?: number;
      pageSize?: number;
    };
    assert(
      Array.isArray(body.pairs) && body.pairs.length === 2,
      "list-pending",
      `expected 2 rows on page 1 of pageSize=2, got ${body.pairs?.length}`,
    );
    assert(
      body.total === 3,
      "list-pending",
      `expected total=3 (merged row excluded), got ${body.total}`,
    );
    assert(body.page === 1, "list-pending", "page should be 1");
    assert(body.pageSize === 2, "list-pending", "pageSize should be 2");
    // Shape check: first row has expected contract fields.
    const first = body.pairs?.[0] as Record<string, unknown> | undefined;
    assert(
      first?.id === 1 && first?.leftClubId === 11 && first?.status === "pending",
      "list-pending",
      "first row contract shape mismatch",
    );
  }

  // --- 2. GET detail enriches with current-state counts -------------------
  {
    const pair = pendingPair({ id: 101, leftClubId: 11, rightClubId: 22 });
    const deps = makeDeps({
      pairs: [pair],
      clubs: [club(11, "FC Example"), club(22, "F.C. Example")],
      affiliationCounts: new Map([
        [11, 4],
        [22, 2],
      ]),
      rosterCounts: new Map([
        [11, 37],
        [22, 12],
      ]),
    });
    const handler = makeDetailHandler(deps);
    const req = makeReq({ params: { id: "101" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "detail-ok", `expected 200, got ${res.statusCode}`);
    const body = res.body as {
      id?: number;
      leftCurrent?: { clubNameCanonical?: string };
      rightCurrent?: { clubNameCanonical?: string };
      affiliations?: { leftAffiliationCount?: number; rightAffiliationCount?: number };
      rosters?: { leftRosterSnapshotCount?: number; rightRosterSnapshotCount?: number };
    };
    assert(body.id === 101, "detail-ok", "id should be echoed");
    assert(
      body.leftCurrent?.clubNameCanonical === "FC Example",
      "detail-ok",
      "leftCurrent should be re-fetched live",
    );
    assert(
      body.rightCurrent?.clubNameCanonical === "F.C. Example",
      "detail-ok",
      "rightCurrent should be re-fetched live",
    );
    assert(
      body.affiliations?.leftAffiliationCount === 4 &&
        body.affiliations?.rightAffiliationCount === 2,
      "detail-ok",
      "affiliation counts should come from countAffiliations()",
    );
    assert(
      body.rosters?.leftRosterSnapshotCount === 37 &&
        body.rosters?.rightRosterSnapshotCount === 12,
      "detail-ok",
      "roster counts should come from countRosterSnapshots()",
    );
  }

  // --- 3. POST merge (happy path) → calls mergeClubs + updates status -----
  {
    const pair = pendingPair({ id: 101, leftClubId: 11, rightClubId: 22 });
    const mergeCalls: Array<{
      pairId: number;
      winnerId: number;
      loserId: number;
      reviewedBy: number | null;
      notes?: string;
    }> = [];
    const deps = makeDeps({
      pairs: [pair],
      mergeCalls,
    });
    const handler = makeMergeHandler(deps);
    const req = makeReq({
      params: { id: "101" },
      body: { winnerId: 11, loserId: 22, notes: "ops confirmed" },
      adminUserId: 42,
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "merge-ok", `expected 200, got ${res.statusCode}`);
    const body = res.body as {
      ok?: boolean;
      winnerId?: number;
      loserAliasesCreated?: number;
      affiliationsReparented?: number;
      rosterSnapshotsReparented?: number;
      // Fields that MUST NOT leak through the projection:
      eventTeamsReparented?: unknown;
      matchesReparented?: unknown;
    };
    assert(body.ok === true, "merge-ok", "ok should be true");
    assert(body.winnerId === 11, "merge-ok", "winnerId should echo");
    assert(
      body.loserAliasesCreated === 1,
      "merge-ok",
      "loserAliasesCreated should come through",
    );
    assert(
      body.affiliationsReparented === 3,
      "merge-ok",
      "affiliationsReparented should come through",
    );
    assert(
      body.rosterSnapshotsReparented === 42,
      "merge-ok",
      "rosterSnapshotsReparented should come through",
    );
    // The 18-field helper result has additional counts — contract response
    // is strict at 5 fields. Zod .parse() strips the rest.
    assert(
      body.eventTeamsReparented === undefined,
      "merge-ok",
      "extra helper fields should be dropped by the Zod projection",
    );
    assert(
      body.matchesReparented === undefined,
      "merge-ok",
      "matchesReparented should not leak into the response",
    );
    // mergeClubs should have been invoked exactly once with the right args.
    assert(
      mergeCalls.length === 1,
      "merge-ok",
      `expected 1 merge call, got ${mergeCalls.length}`,
    );
    assert(
      mergeCalls[0]?.winnerId === 11 &&
        mergeCalls[0]?.loserId === 22 &&
        mergeCalls[0]?.reviewedBy === 42 &&
        mergeCalls[0]?.notes === "ops confirmed",
      "merge-ok",
      "mergeClubs args mismatch",
    );
    // The pair should be flipped to merged via the dep mutation.
    assert(pair.status === "merged", "merge-ok", "pair status should flip to merged");
  }

  // --- 4. POST merge on non-pending row → 409 -----------------------------
  {
    const pair = pendingPair({ id: 101, status: "merged" });
    const mergeCalls: Array<{
      pairId: number;
      winnerId: number;
      loserId: number;
      reviewedBy: number | null;
    }> = [];
    const deps = makeDeps({ pairs: [pair], mergeCalls });
    const handler = makeMergeHandler(deps);
    const req = makeReq({
      params: { id: "101" },
      body: { winnerId: 11, loserId: 22 },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 409, "merge-409", `expected 409, got ${res.statusCode}`);
    assert(
      mergeCalls.length === 0,
      "merge-409",
      "mergeClubs should NOT be called on a non-pending pair",
    );
    const body = res.body as { error?: string; status?: string };
    assert(
      body.error === "already_reviewed",
      "merge-409",
      `error should be 'already_reviewed', got ${body.error}`,
    );
    assert(body.status === "merged", "merge-409", "status should echo the existing state");
  }

  // --- 5. POST merge with mismatched winnerId/loserId → 400 ---------------
  {
    const pair = pendingPair({ id: 101, leftClubId: 11, rightClubId: 22 });
    const mergeCalls: Array<{
      pairId: number;
      winnerId: number;
      loserId: number;
      reviewedBy: number | null;
    }> = [];
    const deps = makeDeps({ pairs: [pair], mergeCalls });
    const handler = makeMergeHandler(deps);
    const req = makeReq({
      params: { id: "101" },
      // winnerId=99 doesn't match either side of the pair.
      body: { winnerId: 99, loserId: 22 },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 400, "merge-mismatch", `expected 400, got ${res.statusCode}`);
    assert(
      mergeCalls.length === 0,
      "merge-mismatch",
      "mergeClubs should NOT be called on a winner/loser mismatch",
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "winner_loser_mismatch",
      "merge-mismatch",
      `expected winner_loser_mismatch, got ${body.error}`,
    );
  }

  // Reverse orientation (winnerId=right, loserId=left) should succeed ------
  {
    const pair = pendingPair({ id: 202, leftClubId: 11, rightClubId: 22 });
    const mergeCalls: Array<{
      pairId: number;
      winnerId: number;
      loserId: number;
      reviewedBy: number | null;
    }> = [];
    const deps = makeDeps({ pairs: [pair], mergeCalls });
    const handler = makeMergeHandler(deps);
    const req = makeReq({
      params: { id: "202" },
      body: { winnerId: 22, loserId: 11 },
      adminUserId: 7,
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "merge-reverse-orientation",
      `expected 200 for reversed winner/loser, got ${res.statusCode}`,
    );
    assert(
      mergeCalls.length === 1 &&
        mergeCalls[0]?.winnerId === 22 &&
        mergeCalls[0]?.loserId === 11,
      "merge-reverse-orientation",
      "mergeClubs should receive the reversed orientation as-passed",
    );
  }

  // --- 6. POST reject → status='rejected' --------------------------------
  {
    const pair = pendingPair({ id: 101 });
    const rejectCalls: Array<{
      pairId: number;
      reviewedBy: number | null;
      notes?: string;
    }> = [];
    const deps = makeDeps({ pairs: [pair], rejectCalls });
    const handler = makeRejectHandler(deps);
    const req = makeReq({
      params: { id: "101" },
      body: { notes: "false positive — different cities" },
      adminUserId: 42,
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "reject-ok", `expected 200, got ${res.statusCode}`);
    const body = res.body as { ok?: boolean; id?: number };
    assert(body.ok === true, "reject-ok", "ok should be true");
    assert(body.id === 101, "reject-ok", "id should echo the pair id");
    assert(
      rejectCalls.length === 1 &&
        rejectCalls[0]?.pairId === 101 &&
        rejectCalls[0]?.reviewedBy === 42 &&
        rejectCalls[0]?.notes === "false positive — different cities",
      "reject-ok",
      "rejectPair args mismatch",
    );
    assert(pair.status === "rejected", "reject-ok", "pair status should flip to rejected");
  }

  // --- Reject on a non-pending row → 409 ---------------------------------
  {
    const pair = pendingPair({ id: 101, status: "rejected" });
    const rejectCalls: Array<{
      pairId: number;
      reviewedBy: number | null;
    }> = [];
    const deps = makeDeps({ pairs: [pair], rejectCalls });
    const handler = makeRejectHandler(deps);
    const req = makeReq({ params: { id: "101" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 409, "reject-409", `expected 409, got ${res.statusCode}`);
    assert(
      rejectCalls.length === 0,
      "reject-409",
      "rejectPair should NOT run on a non-pending pair",
    );
  }

  if (failures.length === 0) {
    console.log("[adminDedup-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminDedup-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
