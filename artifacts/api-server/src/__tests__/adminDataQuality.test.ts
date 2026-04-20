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
  makeGaPremierOrphanHandler,
  type DataQualityDeps,
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

function makeReq(opts: { body?: unknown } = {}): Request {
  return {
    params: {},
    query: {},
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
