/**
 * admin/data-quality routes — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminDataQuality.test.ts
 *
 * Harness mirrors adminDedup.test.ts — drive the factory-built handler
 * directly with fake DB deps. No HTTP server, no real DB. Scenarios:
 *
 *   1. Dry-run returns sampleNames + non-zero scanned/flagged, deleted=0.
 *   2. Real-run (dryRun=false) issues DELETE and returns deleted count.
 *   3. SampleNames are capped at 20 even when flagged > 20.
 */
import type { Request, Response } from "express";
import {
  makeCoachQualityFlagsHandler,
  makeGaPremierOrphanHandler,
  makeNavLeakedNamesHandler,
  makeResolveCoachQualityFlagHandler,
  makeResolveRosterQualityFlagHandler,
  type CoachQualityFlagRawRow,
  type CoachQualityFlagsDeps,
  type DataQualityDeps,
  type NavLeakedNamesDeps,
  type NavLeakedNamesRawRow,
  type ResolveCoachQualityFlagDeps,
  type ResolveRosterQualityFlagDeps,
} from "../routes/admin/data-quality";

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

function makeReq(
  opts: { body?: unknown; query?: Record<string, string> } = {},
): Request {
  return {
    params: {},
    query: opts.query ?? {},
    body: opts.body ?? {},
    adminAuth: {
      kind: "session" as const,
      userId: 42,
      email: "ops@upshift.test",
      role: "admin" as const,
      sessionId: 7,
    },
  } as unknown as Request;
}

// ---------------------------------------------------------------------------
// Fake deps factory.
// ---------------------------------------------------------------------------

type FakeState = {
  scanResult?: {
    scanned: number;
    flagged: number;
    sampleNames: string[];
  };
  deleteResult?: {
    scanned: number;
    flagged: number;
    deleted: number;
    sampleNames: string[];
  };
  scanCalls?: Array<{ tokens: readonly string[]; limit: number }>;
  deleteCalls?: Array<{ tokens: readonly string[]; limit: number }>;
};

function makeDeps(state: FakeState = {}): DataQualityDeps {
  return {
    scanOrphans: async (args) => {
      state.scanCalls?.push(args);
      return (
        state.scanResult ?? { scanned: 0, flagged: 0, sampleNames: [] }
      );
    },
    deleteOrphans: async (args) => {
      state.deleteCalls?.push(args);
      return (
        state.deleteResult ?? {
          scanned: 0,
          flagged: 0,
          deleted: 0,
          sampleNames: [],
        }
      );
    },
  };
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function run() {
  // --- 1. Dry-run returns samples + non-zero scanned/flagged, deleted=0 ---
  {
    const scanCalls: Array<{ tokens: readonly string[]; limit: number }> = [];
    const deleteCalls: Array<{ tokens: readonly string[]; limit: number }> = [];
    const deps = makeDeps({
      scanCalls,
      deleteCalls,
      scanResult: {
        scanned: 7,
        flagged: 7,
        sampleNames: [
          "STAFF",
          "FACILITIES",
          "NEWS",
          "TRYOUTS - Fall 2026",
          "TEAMS",
          "CONTACT",
          "ABOUT",
        ],
      },
    });
    const handler = makeGaPremierOrphanHandler(deps);
    const req = makeReq({ body: { dryRun: true, limit: 500 } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "dryrun",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      scanned?: number;
      flagged?: number;
      deleted?: number;
      sampleNames?: string[];
    };
    assert(
      body.scanned === 7,
      "dryrun",
      `scanned should be 7, got ${body.scanned}`,
    );
    assert(
      body.flagged === 7,
      "dryrun",
      `flagged should be 7, got ${body.flagged}`,
    );
    assert(
      body.deleted === 0,
      "dryrun",
      `deleted should be 0 on dry-run, got ${body.deleted}`,
    );
    assert(
      Array.isArray(body.sampleNames) && body.sampleNames.length === 7,
      "dryrun",
      `sampleNames should have 7 entries, got ${body.sampleNames?.length}`,
    );
    assert(
      body.sampleNames?.[0] === "STAFF",
      "dryrun",
      "sampleNames order should be preserved",
    );
    // Scan was called, delete was NOT.
    assert(
      scanCalls.length === 1 && scanCalls[0]?.limit === 500,
      "dryrun",
      `expected 1 scan call with limit=500, got ${scanCalls.length} calls`,
    );
    assert(
      deleteCalls.length === 0,
      "dryrun",
      "deleteOrphans must NOT run on a dry-run",
    );
    // Tokens plumbed through from the constant.
    assert(
      scanCalls[0]?.tokens.includes("STAFF") &&
        scanCalls[0]?.tokens.includes("FACILITIES") &&
        scanCalls[0]?.tokens.includes("TRYOUTS"),
      "dryrun",
      "scanOrphans should receive the GA_PREMIER_ORPHAN_TOKENS list",
    );
  }

  // --- 2. Real-run (dryRun=false) issues DELETE + returns deleted count ---
  {
    const scanCalls: Array<{ tokens: readonly string[]; limit: number }> = [];
    const deleteCalls: Array<{ tokens: readonly string[]; limit: number }> = [];
    const deps = makeDeps({
      scanCalls,
      deleteCalls,
      deleteResult: {
        scanned: 4,
        flagged: 4,
        deleted: 4,
        sampleNames: ["STAFF", "NEWS", "EVENTS", "HOME"],
      },
    });
    const handler = makeGaPremierOrphanHandler(deps);
    const req = makeReq({ body: { dryRun: false, limit: 100 } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "realrun",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      scanned?: number;
      flagged?: number;
      deleted?: number;
      sampleNames?: string[];
    };
    assert(
      body.deleted === 4,
      "realrun",
      `deleted should be 4, got ${body.deleted}`,
    );
    assert(
      body.scanned === 4,
      "realrun",
      `scanned should be 4, got ${body.scanned}`,
    );
    assert(
      body.flagged === 4,
      "realrun",
      `flagged should be 4, got ${body.flagged}`,
    );
    assert(
      Array.isArray(body.sampleNames) && body.sampleNames.length === 4,
      "realrun",
      "sampleNames should be populated on real-run for operator confirmation",
    );
    // Delete was called, scan was NOT.
    assert(
      deleteCalls.length === 1 && deleteCalls[0]?.limit === 100,
      "realrun",
      `expected 1 delete call with limit=100, got ${deleteCalls.length} calls`,
    );
    assert(
      scanCalls.length === 0,
      "realrun",
      "scanOrphans must NOT run when dryRun=false — delete path owns the read",
    );
  }

  // --- 3. SampleNames cap at 20 even when flagged > 20 --------------------
  {
    const manyNames = Array.from({ length: 50 }, (_, i) => `ORPHAN_${i}`);
    const deps = makeDeps({
      scanResult: {
        scanned: 50,
        flagged: 50,
        sampleNames: manyNames,
      },
    });
    const handler = makeGaPremierOrphanHandler(deps);
    const req = makeReq({ body: { dryRun: true, limit: 500 } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "sample-cap",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      scanned?: number;
      flagged?: number;
      sampleNames?: string[];
    };
    assert(
      body.flagged === 50,
      "sample-cap",
      `flagged should still reflect the full count (50), got ${body.flagged}`,
    );
    assert(
      Array.isArray(body.sampleNames) && body.sampleNames.length === 20,
      "sample-cap",
      `sampleNames must be capped at 20, got ${body.sampleNames?.length}`,
    );
    // Cap keeps the first 20 — assert ordering preserved.
    assert(
      body.sampleNames?.[0] === "ORPHAN_0" &&
        body.sampleNames?.[19] === "ORPHAN_19",
      "sample-cap",
      "cap should keep the first 20 in order",
    );
  }

  // --- Defaults: empty body → dryRun=true, limit=500 ---------------------
  {
    const scanCalls: Array<{ tokens: readonly string[]; limit: number }> = [];
    const deps = makeDeps({ scanCalls });
    const handler = makeGaPremierOrphanHandler(deps);
    const req = makeReq({ body: {} });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "defaults",
      `expected 200 with empty body (Zod defaults), got ${res.statusCode}`,
    );
    assert(
      scanCalls.length === 1 && scanCalls[0]?.limit === 500,
      "defaults",
      `empty body should apply defaults dryRun=true, limit=500`,
    );
  }

  // --- Invalid body: limit > 10_000 → 400 --------------------------------
  {
    const deps = makeDeps();
    const handler = makeGaPremierOrphanHandler(deps);
    const req = makeReq({ body: { dryRun: true, limit: 99_999 } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "validation",
      `expected 400 for limit > 10k, got ${res.statusCode}`,
    );
  }

  // --- Nav-leaked-names: metadata-to-typed-field extraction ---------------
  //
  // This is the load-bearing regression guard: until Phase 2 ships the
  // scraper detector, the prod table is empty — so this fake-DB test is
  // the only thing proving the handler correctly shreds the jsonb
  // `metadata` payload into typed `leakedStrings` + `snapshotRosterSize`
  // columns in the response.
  {
    const calls: Array<{
      page: number;
      pageSize: number;
      includeResolved: boolean;
    }> = [];
    const fakeRows: NavLeakedNamesRawRow[] = [
      {
        id: 1,
        snapshotId: 101,
        clubId: 42,
        clubNameCanonical: "Cactus Soccer Club",
        metadata: {
          leaked_strings: ["HOME", "ABOUT", "CONTACT US"],
          snapshot_roster_size: 24,
        },
        flaggedAt: new Date("2026-04-10T12:00:00Z"),
        resolvedAt: null,
        resolvedByEmail: null,
      },
      {
        id: 2,
        snapshotId: 202,
        // Unlinked snapshot (linker hasn't run) — must survive as null.
        clubId: null,
        clubNameCanonical: null,
        metadata: {
          leaked_strings: ["Register"],
          snapshot_roster_size: 1,
        },
        flaggedAt: "2026-04-11T09:00:00.000Z",
        resolvedAt: "2026-04-12T15:30:00.000Z",
        resolvedByEmail: "ops@upshift.test",
      },
    ];
    const deps: NavLeakedNamesDeps = {
      listNavLeakedNames: async (args) => {
        calls.push(args);
        return { rows: fakeRows, total: 2 };
      },
    };
    const handler = makeNavLeakedNamesHandler(deps);
    const req = makeReq({
      query: { page: "1", page_size: "20", include_resolved: "true" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "nav-leaked",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      calls.length === 1 &&
        calls[0]?.page === 1 &&
        calls[0]?.pageSize === 20 &&
        calls[0]?.includeResolved === true,
      "nav-leaked",
      `expected single call with page=1, pageSize=20, includeResolved=true; got ${JSON.stringify(calls)}`,
    );
    const body = res.body as {
      total?: number;
      page?: number;
      pageSize?: number;
      rows?: Array<{
        id: number;
        snapshotId: number;
        clubId: number | null;
        clubNameCanonical: string | null;
        leakedStrings: string[];
        snapshotRosterSize: number;
        flaggedAt: string;
        resolvedAt: string | null;
        resolvedByEmail: string | null;
      }>;
    };
    assert(body.total === 2, "nav-leaked", `total should be 2, got ${body.total}`);
    assert(
      Array.isArray(body.rows) && body.rows.length === 2,
      "nav-leaked",
      `rows should have 2 entries, got ${body.rows?.length}`,
    );
    // Row 0: typed fields extracted from metadata.
    const r0 = body.rows?.[0];
    assert(r0?.id === 1, "nav-leaked", `row0.id should be 1, got ${r0?.id}`);
    assert(
      Array.isArray(r0?.leakedStrings) &&
        r0?.leakedStrings.length === 3 &&
        r0?.leakedStrings[0] === "HOME" &&
        r0?.leakedStrings[2] === "CONTACT US",
      "nav-leaked",
      `row0 leakedStrings should be extracted from metadata.leaked_strings verbatim, got ${JSON.stringify(r0?.leakedStrings)}`,
    );
    assert(
      r0?.snapshotRosterSize === 24,
      "nav-leaked",
      `row0 snapshotRosterSize should be 24 (extracted from metadata.snapshot_roster_size), got ${r0?.snapshotRosterSize}`,
    );
    assert(
      r0?.clubId === 42 && r0?.clubNameCanonical === "Cactus Soccer Club",
      "nav-leaked",
      `row0 club fields should pass through`,
    );
    assert(
      r0?.flaggedAt === "2026-04-10T12:00:00.000Z",
      "nav-leaked",
      `row0 flaggedAt should be ISO-normalized, got ${r0?.flaggedAt}`,
    );
    assert(
      r0?.resolvedAt === null,
      "nav-leaked",
      `row0 resolvedAt should be null for active flag, got ${r0?.resolvedAt}`,
    );
    // Row 1: unlinked snapshot, resolved.
    const r1 = body.rows?.[1];
    assert(
      r1?.clubId === null && r1?.clubNameCanonical === null,
      "nav-leaked",
      `row1 unlinked snapshot should preserve nulls (clubId=${r1?.clubId}, clubNameCanonical=${r1?.clubNameCanonical})`,
    );
    assert(
      r1?.leakedStrings?.[0] === "Register" && r1?.snapshotRosterSize === 1,
      "nav-leaked",
      `row1 metadata extraction failed: ${JSON.stringify(r1)}`,
    );
    assert(
      r1?.resolvedAt === "2026-04-12T15:30:00.000Z",
      "nav-leaked",
      `row1 resolvedAt should be ISO, got ${r1?.resolvedAt}`,
    );
    assert(
      r1?.resolvedByEmail === "ops@upshift.test",
      "nav-leaked",
      `row1 resolvedByEmail should pass through, got ${r1?.resolvedByEmail}`,
    );
  }

  // --- Nav-leaked-names: malformed metadata is tolerated -------------------
  //
  // If a row slips through with missing / wrong-typed metadata fields, the
  // handler must NOT 500 — it should degrade to safe defaults ([] / 0) so
  // one bad row doesn't take out the whole panel.
  {
    const deps: NavLeakedNamesDeps = {
      listNavLeakedNames: async () => ({
        rows: [
          {
            id: 10,
            snapshotId: 1000,
            clubId: 1,
            clubNameCanonical: "Test FC",
            // Missing leaked_strings entirely; snapshot_roster_size is a string
            // instead of a number. Both should coerce to safe defaults.
            metadata: { snapshot_roster_size: "not-a-number" },
            flaggedAt: new Date("2026-04-13T00:00:00Z"),
            resolvedAt: null,
            resolvedByEmail: null,
          },
          {
            id: 11,
            snapshotId: 1001,
            clubId: 2,
            clubNameCanonical: "Other FC",
            // Null metadata — must not crash the mapper.
            metadata: null,
            flaggedAt: new Date("2026-04-13T01:00:00Z"),
            resolvedAt: null,
            resolvedByEmail: null,
          },
        ],
        total: 2,
      }),
    };
    const handler = makeNavLeakedNamesHandler(deps);
    const req = makeReq({ query: {} });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "nav-leaked-malformed",
      `expected 200 even with malformed metadata, got ${res.statusCode}`,
    );
    const body = res.body as {
      rows?: Array<{ leakedStrings: string[]; snapshotRosterSize: number }>;
    };
    assert(
      body.rows?.[0]?.leakedStrings.length === 0 &&
        body.rows?.[0]?.snapshotRosterSize === 0,
      "nav-leaked-malformed",
      `row with bad metadata should default to [] / 0, got ${JSON.stringify(body.rows?.[0])}`,
    );
    assert(
      body.rows?.[1]?.leakedStrings.length === 0 &&
        body.rows?.[1]?.snapshotRosterSize === 0,
      "nav-leaked-malformed",
      `row with null metadata should default to [] / 0, got ${JSON.stringify(body.rows?.[1])}`,
    );
  }

  // --- Nav-leaked-names: defaults applied when query is empty --------------
  {
    const calls: Array<{
      page: number;
      pageSize: number;
      includeResolved: boolean;
    }> = [];
    const deps: NavLeakedNamesDeps = {
      listNavLeakedNames: async (args) => {
        calls.push(args);
        return { rows: [], total: 0 };
      },
    };
    const handler = makeNavLeakedNamesHandler(deps);
    const req = makeReq({ query: {} });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "nav-leaked-defaults",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      calls[0]?.page === 1 &&
        calls[0]?.pageSize === 20 &&
        calls[0]?.includeResolved === false,
      "nav-leaked-defaults",
      `empty query should apply page=1/pageSize=20/includeResolved=false, got ${JSON.stringify(calls[0])}`,
    );
    const body = res.body as { total?: number; rows?: unknown[] };
    assert(
      body.total === 0 && body.rows?.length === 0,
      "nav-leaked-defaults",
      `empty response should round-trip`,
    );
  }

  // --- Nav-leaked-names: invalid page_size → 400 ---------------------------
  {
    const deps: NavLeakedNamesDeps = {
      listNavLeakedNames: async () => ({ rows: [], total: 0 }),
    };
    const handler = makeNavLeakedNamesHandler(deps);
    const req = makeReq({ query: { page_size: "500" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "nav-leaked-validation",
      `expected 400 for page_size > 100, got ${res.statusCode}`,
    );
  }

  // -----------------------------------------------------------------------
  // Resolve roster_quality_flags PATCH endpoint scenarios.
  // -----------------------------------------------------------------------

  // --- 1. 204 on first resolve; admin user id passed through ---
  {
    const calls: Array<{ id: number; resolvedBy: number | null }> = [];
    const deps: ResolveRosterQualityFlagDeps = {
      resolveFlag: async (args) => {
        calls.push({ id: args.id, resolvedBy: args.resolvedBy });
        return { outcome: "resolved" };
      },
    };
    const handler = makeResolveRosterQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "17",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 204,
      "resolve-success",
      `expected 204, got ${res.statusCode}`,
    );
    assert(
      calls.length === 1 && calls[0]?.id === 17 && calls[0]?.resolvedBy === 42,
      "resolve-success",
      `expected one call with id=17 resolvedBy=42, got ${JSON.stringify(calls)}`,
    );
  }

  // --- 2. 404 when flag id does not exist ---
  {
    const deps: ResolveRosterQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "not_found" }),
    };
    const handler = makeResolveRosterQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "99999",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 404,
      "resolve-404",
      `expected 404 for missing flag, got ${res.statusCode}`,
    );
  }

  // --- 3. 400 on invalid id (non-numeric / zero / negative) ---
  for (const badId of ["abc", "0", "-3"]) {
    const deps: ResolveRosterQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "resolved" }),
    };
    const handler = makeResolveRosterQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: badId,
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "resolve-400-id",
      `expected 400 for id=${badId}, got ${res.statusCode}`,
    );
  }

  // --- 4. 400 when flag is already resolved ---
  {
    const deps: ResolveRosterQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "already_resolved" }),
    };
    const handler = makeResolveRosterQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "1",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "resolve-400-already",
      `expected 400 for already_resolved, got ${res.statusCode}`,
    );
  }

  // --- 5. API-key caller (no session) → resolvedBy=null passthrough ---
  {
    const calls: Array<{ id: number; resolvedBy: number | null }> = [];
    const deps: ResolveRosterQualityFlagDeps = {
      resolveFlag: async (args) => {
        calls.push({ id: args.id, resolvedBy: args.resolvedBy });
        return { outcome: "resolved" };
      },
    };
    const handler = makeResolveRosterQualityFlagHandler(deps);
    const req = {
      params: { id: "5" },
      query: {},
      body: {},
      adminAuth: { kind: "apiKey" as const },
    } as unknown as Request;
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 204,
      "resolve-apikey",
      `expected 204 for API-key caller, got ${res.statusCode}`,
    );
    assert(
      calls[0]?.resolvedBy === null,
      "resolve-apikey",
      `expected resolvedBy=null for API-key caller, got ${calls[0]?.resolvedBy}`,
    );
  }

  // -----------------------------------------------------------------------
  // coach_quality_flags GET — shape + query-param pass-through.
  // -----------------------------------------------------------------------

  // --- Happy path: rows round-trip with joined coach/club context ---
  {
    const calls: Array<{
      flagType: string | undefined;
      resolved: boolean | undefined;
      page: number;
      pageSize: number;
    }> = [];
    const fakeRows: CoachQualityFlagRawRow[] = [
      {
        id: 10,
        discoveryId: 500,
        flagType: "nav_leaked",
        metadata: { leaked_strings: ["CONTACT"], raw_name: "CONTACT" },
        flaggedAt: new Date("2026-04-10T12:00:00Z"),
        resolvedAt: null,
        resolvedByEmail: null,
        resolutionNote: null,
        coachName: "CONTACT",
        coachEmail: "info@example.org",
        clubNameRaw: "Example FC",
        clubId: 77,
        clubDisplayName: "Example FC",
      },
      {
        id: 11,
        discoveryId: 501,
        flagType: "corrupt_email",
        metadata: null,
        flaggedAt: "2026-04-11T09:00:00.000Z",
        resolvedAt: "2026-04-12T15:30:00.000Z",
        resolvedByEmail: "ops@upshift.test",
        resolutionNote: "purged via PR 2",
        coachName: "Mon Apr 7 2026",
        coachEmail: null,
        clubNameRaw: null,
        clubId: null,
        clubDisplayName: null,
      },
    ];
    const deps: CoachQualityFlagsDeps = {
      listCoachQualityFlags: async (args) => {
        calls.push(args);
        return { rows: fakeRows, total: 2 };
      },
    };
    const handler = makeCoachQualityFlagsHandler(deps);
    const req = makeReq({
      query: {
        page: "1",
        page_size: "20",
        flag_type: "nav_leaked",
        resolved: "false",
      },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "coach-flags-happy",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      calls.length === 1 &&
        calls[0]?.page === 1 &&
        calls[0]?.pageSize === 20 &&
        calls[0]?.flagType === "nav_leaked" &&
        calls[0]?.resolved === false,
      "coach-flags-happy",
      `expected single call with page=1/pageSize=20/flagType=nav_leaked/resolved=false, got ${JSON.stringify(calls)}`,
    );
    const body = res.body as {
      total?: number;
      items?: Array<{
        id: number;
        discoveryId: number;
        flagType: string;
        metadata: Record<string, unknown> | null;
        flaggedAt: string;
        resolvedAt: string | null;
        resolvedByEmail: string | null;
        resolutionNote: string | null;
        coachName: string;
        coachEmail: string | null;
        clubNameRaw: string | null;
        clubId: number | null;
        clubDisplayName: string | null;
      }>;
    };
    assert(
      body.total === 2,
      "coach-flags-happy",
      `total should be 2, got ${body.total}`,
    );
    const r0 = body.items?.[0];
    assert(
      r0?.id === 10 && r0?.discoveryId === 500,
      "coach-flags-happy",
      `row0 id/discoveryId should round-trip, got ${JSON.stringify(r0)}`,
    );
    assert(
      r0?.flaggedAt === "2026-04-10T12:00:00.000Z",
      "coach-flags-happy",
      `row0 flaggedAt should ISO-normalize, got ${r0?.flaggedAt}`,
    );
    assert(
      r0?.metadata &&
        typeof r0.metadata === "object" &&
        !Array.isArray(r0.metadata) &&
        (r0.metadata as Record<string, unknown>).raw_name === "CONTACT",
      "coach-flags-happy",
      `row0 metadata should pass through as object, got ${JSON.stringify(r0?.metadata)}`,
    );
    assert(
      r0?.coachName === "CONTACT" &&
        r0?.clubId === 77 &&
        r0?.clubDisplayName === "Example FC",
      "coach-flags-happy",
      `row0 joined coach/club fields should pass through, got ${JSON.stringify(r0)}`,
    );
    const r1 = body.items?.[1];
    assert(
      r1?.metadata === null,
      "coach-flags-happy",
      `row1 null metadata should survive as null, got ${JSON.stringify(r1?.metadata)}`,
    );
    assert(
      r1?.resolvedAt === "2026-04-12T15:30:00.000Z" &&
        r1?.resolvedByEmail === "ops@upshift.test" &&
        r1?.resolutionNote === "purged via PR 2",
      "coach-flags-happy",
      `row1 resolved fields should pass through, got ${JSON.stringify(r1)}`,
    );
    assert(
      r1?.clubId === null && r1?.clubDisplayName === null,
      "coach-flags-happy",
      `row1 unlinked discovery should preserve nulls`,
    );
  }

  // --- Empty query → defaults (both resolved/unresolved; no flag-type filter) ---
  {
    const calls: Array<{
      flagType: string | undefined;
      resolved: boolean | undefined;
      page: number;
      pageSize: number;
    }> = [];
    const deps: CoachQualityFlagsDeps = {
      listCoachQualityFlags: async (args) => {
        calls.push(args);
        return { rows: [], total: 0 };
      },
    };
    const handler = makeCoachQualityFlagsHandler(deps);
    const req = makeReq({ query: {} });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "coach-flags-defaults",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      calls[0]?.page === 1 &&
        calls[0]?.pageSize === 20 &&
        calls[0]?.flagType === undefined &&
        calls[0]?.resolved === undefined,
      "coach-flags-defaults",
      `empty query should apply page=1/pageSize=20/flagType=undefined/resolved=undefined, got ${JSON.stringify(calls[0])}`,
    );
  }

  // --- Invalid flag_type → 400 ---
  {
    const deps: CoachQualityFlagsDeps = {
      listCoachQualityFlags: async () => ({ rows: [], total: 0 }),
    };
    const handler = makeCoachQualityFlagsHandler(deps);
    const req = makeReq({ query: { flag_type: "not-a-real-flag" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "coach-flags-bad-flag-type",
      `expected 400 for unknown flag_type, got ${res.statusCode}`,
    );
  }

  // --- Invalid page_size (over cap) → 400 ---
  {
    const deps: CoachQualityFlagsDeps = {
      listCoachQualityFlags: async () => ({ rows: [], total: 0 }),
    };
    const handler = makeCoachQualityFlagsHandler(deps);
    const req = makeReq({ query: { page_size: "500" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "coach-flags-bad-page-size",
      `expected 400 for page_size > 100, got ${res.statusCode}`,
    );
  }

  // -----------------------------------------------------------------------
  // coach_quality_flags PATCH — resolve outcomes.
  // -----------------------------------------------------------------------

  // --- 1. 204 on first resolve; admin user id passed through ---
  {
    const calls: Array<{ id: number; resolvedBy: number | null }> = [];
    const deps: ResolveCoachQualityFlagDeps = {
      resolveFlag: async (args) => {
        calls.push({ id: args.id, resolvedBy: args.resolvedBy });
        return { outcome: "resolved" };
      },
    };
    const handler = makeResolveCoachQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "33",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 204,
      "coach-resolve-success",
      `expected 204, got ${res.statusCode}`,
    );
    assert(
      calls.length === 1 && calls[0]?.id === 33 && calls[0]?.resolvedBy === 42,
      "coach-resolve-success",
      `expected id=33 resolvedBy=42, got ${JSON.stringify(calls)}`,
    );
  }

  // --- 2. 404 when flag id does not exist ---
  {
    const deps: ResolveCoachQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "not_found" }),
    };
    const handler = makeResolveCoachQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "88888",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 404,
      "coach-resolve-404",
      `expected 404, got ${res.statusCode}`,
    );
  }

  // --- 3. 400 on invalid id (non-numeric / zero / negative) ---
  for (const badId of ["abc", "0", "-3"]) {
    const deps: ResolveCoachQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "resolved" }),
    };
    const handler = makeResolveCoachQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: badId,
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "coach-resolve-400-id",
      `expected 400 for id=${badId}, got ${res.statusCode}`,
    );
  }

  // --- 4. 400 when flag is already resolved ---
  {
    const deps: ResolveCoachQualityFlagDeps = {
      resolveFlag: async () => ({ outcome: "already_resolved" }),
    };
    const handler = makeResolveCoachQualityFlagHandler(deps);
    const req = makeReq();
    (req as unknown as { params: Record<string, string> }).params = {
      id: "1",
    };
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "coach-resolve-400-already",
      `expected 400 for already_resolved, got ${res.statusCode}`,
    );
  }

  // --- 5. API-key caller → resolvedBy=null passthrough ---
  {
    const calls: Array<{ id: number; resolvedBy: number | null }> = [];
    const deps: ResolveCoachQualityFlagDeps = {
      resolveFlag: async (args) => {
        calls.push({ id: args.id, resolvedBy: args.resolvedBy });
        return { outcome: "resolved" };
      },
    };
    const handler = makeResolveCoachQualityFlagHandler(deps);
    const req = {
      params: { id: "5" },
      query: {},
      body: {},
      adminAuth: { kind: "apiKey" as const },
    } as unknown as Request;
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 204,
      "coach-resolve-apikey",
      `expected 204 for API-key caller, got ${res.statusCode}`,
    );
    assert(
      calls[0]?.resolvedBy === null,
      "coach-resolve-apikey",
      `expected resolvedBy=null for API-key caller, got ${calls[0]?.resolvedBy}`,
    );
  }

  if (failures.length === 0) {
    console.log("[adminDataQuality-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminDataQuality-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
