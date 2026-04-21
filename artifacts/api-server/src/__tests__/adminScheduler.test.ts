/**
 * admin/scheduler routes + requireSuperAdmin middleware — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminScheduler.test.ts
 *
 * Same factory-handler pattern as adminDedup.test.ts / adminGrowth.test.ts
 * — inject a fake SchedulerDeps so the handlers never reach Postgres. No
 * vitest, no HTTP server, no express router — the factories are called
 * directly with fake req/res/next.
 *
 * Scenarios:
 *   1. POST /scraper-schedules/nightly_tier1/run with super_admin session
 *      → 201 with {id, jobKey, status: 'pending', requestedAt}, row inserted.
 *   2. Same call with admin (non-super) session → requireSuperAdmin returns 403.
 *   3. Same call with X-API-Key (apiKey auth) → requireSuperAdmin returns 403.
 *   4. POST with unknown jobKey → 400 "unknown jobKey".
 *   5. GET /scheduler-jobs/:id for missing id → 404.
 *   6. requireSuperAdmin in isolation — admin role → 403.
 *   7. requireSuperAdmin in isolation — super_admin role → next().
 *   8. requireSuperAdmin in isolation — apiKey kind → 403.
 */
import type { Request, Response } from "express";
import type { SchedulerJob as SchedulerJobRow } from "@workspace/db";
import {
  makeRunNowHandler,
  makeGetJobHandler,
  makeListSchedulesHandler,
  JOB_METADATA,
  DEFAULT_SCHEDULES_RUN_LIMIT,
  type SchedulerDeps,
  type AllowedJobKey,
} from "../routes/admin/scheduler";
import { requireSuperAdmin } from "../middlewares/requireSuperAdmin";

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

type AdminAuth = Request["adminAuth"];

function makeReq(opts: {
  params?: Record<string, string>;
  query?: Record<string, string>;
  body?: unknown;
  adminAuth?: AdminAuth;
  path?: string;
}): Request {
  return {
    params: opts.params ?? {},
    query: opts.query ?? {},
    body: opts.body ?? {},
    adminAuth: opts.adminAuth,
    path: opts.path ?? "/scraper-schedules/nightly_tier1/run",
    ip: "127.0.0.1",
    headers: {},
    cookies: {},
  } as unknown as Request;
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

function fakeRow(overrides: Partial<SchedulerJobRow> = {}): SchedulerJobRow {
  const base: SchedulerJobRow = {
    id: 101,
    jobKey: "nightly_tier1",
    args: null,
    status: "pending",
    requestedBy: 42,
    requestedAt: new Date("2026-04-18T12:00:00.000Z"),
    startedAt: null,
    completedAt: null,
    exitCode: null,
    stdoutTail: null,
    stderrTail: null,
  };
  return { ...base, ...overrides };
}

function sessionAuth(role: "admin" | "super_admin" = "super_admin"): AdminAuth {
  return {
    kind: "session",
    userId: 42,
    email: "ops@upshift.test",
    role,
    sessionId: 7,
  };
}

function apiKeyAuth(): AdminAuth {
  return {
    kind: "apiKey",
    keyId: 99,
    keyName: "test-key",
    scopes: ["admin"],
  };
}

function makeDeps(overrides: Partial<SchedulerDeps> = {}): SchedulerDeps {
  return {
    enqueueJob: overrides.enqueueJob ?? (async () => fakeRow()),
    getJobById: overrides.getJobById ?? (async () => null),
    listJobsByKey: overrides.listJobsByKey ?? (async () => []),
  };
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function run() {
  // --- 1. POST /scraper-schedules/nightly_tier1/run — super_admin ---------
  //
  // The route middleware chain is `buildRateLimiter → requireSuperAdmin →
  // makeRunNowHandler(deps)`. Since we're testing the handler itself here,
  // we drive it directly with an adminAuth that has role='super_admin'.
  // Scenarios 2 and 3 cover the guard via the isolated middleware below.
  {
    type EnqueueCall = {
      jobKey: AllowedJobKey;
      args: Record<string, unknown> | null;
      requestedBy: number;
    };
    const insertedCalls: EnqueueCall[] = [];
    const deps = makeDeps({
      enqueueJob: async (input) => {
        insertedCalls.push(input);
        return fakeRow({
          id: 555,
          jobKey: input.jobKey,
          args: input.args,
          requestedBy: input.requestedBy,
        });
      },
    });
    const handler = makeRunNowHandler(deps);
    const req = makeReq({
      params: { jobKey: "nightly_tier1" },
      body: { args: { "dry-run": true } },
      adminAuth: sessionAuth("super_admin"),
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 201,
      "run-now-super-admin",
      `expected 201, got ${res.statusCode}`,
    );
    const body = res.body as {
      id?: number;
      jobKey?: string;
      status?: string;
      requestedAt?: string;
    };
    assert(body.id === 555, "run-now-super-admin", `id=555 expected, got ${body.id}`);
    assert(
      body.jobKey === "nightly_tier1",
      "run-now-super-admin",
      `jobKey=nightly_tier1 expected, got ${body.jobKey}`,
    );
    assert(
      body.status === "pending",
      "run-now-super-admin",
      `status=pending expected, got ${body.status}`,
    );
    assert(
      typeof body.requestedAt === "string" && body.requestedAt.length > 0,
      "run-now-super-admin",
      "requestedAt ISO string expected",
    );
    const last = insertedCalls[0];
    assert(
      last !== undefined &&
        last.jobKey === "nightly_tier1" &&
        last.requestedBy === 42,
      "run-now-super-admin",
      `enqueueJob called with wrong input: ${JSON.stringify(last)}`,
    );
    assert(
      last !== undefined &&
        last.args !== null &&
        (last.args as Record<string, unknown>)["dry-run"] === true,
      "run-now-super-admin",
      `args should forward dry-run=true, got ${JSON.stringify(last?.args)}`,
    );
  }

  // --- 2. Non-super admin session → requireSuperAdmin returns 403 ---------
  //
  // Exercises the middleware directly (the handler is never reached in the
  // real chain — the guard short-circuits). This tests the promise for
  // scenario 2 in the S.3 spec.
  {
    const req = makeReq({
      params: { jobKey: "nightly_tier1" },
      adminAuth: sessionAuth("admin"),
    });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "non-super-session", "next() should not be called");
    assert(
      res.statusCode === 403,
      "non-super-session",
      `expected 403, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "super_admin required",
      "non-super-session",
      `body.error mismatch, got ${body.error}`,
    );
  }

  // --- 3. API-key auth → requireSuperAdmin returns 403 --------------------
  {
    const req = makeReq({
      params: { jobKey: "nightly_tier1" },
      adminAuth: apiKeyAuth(),
    });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "apikey-not-super", "next() should not be called");
    assert(
      res.statusCode === 403,
      "apikey-not-super",
      `expected 403, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "super_admin required",
      "apikey-not-super",
      `body.error mismatch, got ${body.error}`,
    );
  }

  // --- 4. POST with unknown jobKey → 400 "unknown jobKey" ----------------
  {
    const deps = makeDeps();
    const handler = makeRunNowHandler(deps);
    const req = makeReq({
      params: { jobKey: "evil-arbitrary-string" },
      body: { args: {} },
      adminAuth: sessionAuth("super_admin"),
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "unknown-jobkey",
      `expected 400, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "unknown jobKey",
      "unknown-jobkey",
      `expected 'unknown jobKey', got ${body.error}`,
    );
  }

  // --- 5. GET /scheduler-jobs/:id for missing id → 404 -------------------
  {
    let calledWith: number | null = null;
    const deps = makeDeps({
      getJobById: async (id) => {
        calledWith = id;
        return null;
      },
    });
    const handler = makeGetJobHandler(deps);
    const req = makeReq({
      params: { id: "999999" },
      adminAuth: sessionAuth("admin"),
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 404,
      "get-job-missing",
      `expected 404, got ${res.statusCode}`,
    );
    assert(
      calledWith === 999999,
      "get-job-missing",
      `getJobById called with ${calledWith}`,
    );
    const body = res.body as { error?: string };
    assert(
      typeof body.error === "string" && body.error.includes("not found"),
      "get-job-missing",
      `expected 'not found' error, got ${body.error}`,
    );
  }

  // --- 6. requireSuperAdmin isolation: admin role → 403 ------------------
  //
  // Covers "role=admin" (explicit duplicate of scenario 2 but with no
  // routing context — the middleware must refuse on its own terms).
  {
    const req = makeReq({ adminAuth: sessionAuth("admin") });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "mw-admin-role", "next() should not be called");
    assert(res.statusCode === 403, "mw-admin-role", `expected 403, got ${res.statusCode}`);
  }

  // --- 7. requireSuperAdmin isolation: super_admin role → next() ---------
  {
    const req = makeReq({ adminAuth: sessionAuth("super_admin") });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "mw-super-admin-role", "next() should be called");
    // Status defaults to 200 since we never call res.status()/json().
    assert(
      res.statusCode === 200,
      "mw-super-admin-role",
      `no status should be set, got ${res.statusCode}`,
    );
  }

  // --- 8. requireSuperAdmin isolation: apiKey kind → 403 -----------------
  {
    const req = makeReq({ adminAuth: apiKeyAuth() });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "mw-apikey", "next() should not be called");
    assert(res.statusCode === 403, "mw-apikey", `expected 403, got ${res.statusCode}`);
  }

  // --- 9. GET /scraper-schedules returns all metadata + per-job runs -----
  //
  // The handler fans out one `listJobsByKey` per JOB_METADATA entry and
  // zips the results back with description + cronExpression. Verifies
  // order is preserved and each jobKey gets its own run set.
  {
    type ListCall = { jobKey: string; limit: number };
    const listCalls: ListCall[] = [];
    const runsByKey: Record<string, SchedulerJobRow[]> = {
      nightly_tier1: [fakeRow({ id: 11, jobKey: "nightly_tier1" })],
      weekly_state: [],
      hourly_linker: [
        fakeRow({ id: 31, jobKey: "hourly_linker" }),
        fakeRow({ id: 32, jobKey: "hourly_linker" }),
      ],
    };
    const deps = makeDeps({
      listJobsByKey: async ({ jobKey, limit }) => {
        listCalls.push({ jobKey, limit });
        return runsByKey[jobKey] ?? [];
      },
    });
    const handler = makeListSchedulesHandler(deps);
    const req = makeReq({
      adminAuth: sessionAuth("admin"),
      path: "/scraper-schedules",
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "list-schedules-ok",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      schedules?: Array<{
        jobKey: string;
        description: string;
        cronExpression: string | null;
        recentRuns: Array<{ id: number }>;
      }>;
    };
    assert(
      Array.isArray(body.schedules) &&
        body.schedules.length === JOB_METADATA.length,
      "list-schedules-ok",
      `expected ${JOB_METADATA.length} schedules, got ${body.schedules?.length}`,
    );
    // Order matches JOB_METADATA.
    body.schedules?.forEach((s, i) => {
      const meta = JOB_METADATA[i]!;
      assert(
        s.jobKey === meta.jobKey,
        "list-schedules-ok",
        `schedules[${i}].jobKey mismatch: expected ${meta.jobKey}, got ${s.jobKey}`,
      );
      assert(
        s.description === meta.description,
        "list-schedules-ok",
        `schedules[${i}].description mismatch for ${meta.jobKey}`,
      );
      assert(
        s.cronExpression === meta.cronExpression,
        "list-schedules-ok",
        `schedules[${i}].cronExpression mismatch for ${meta.jobKey}`,
      );
    });
    // Each JOB_METADATA entry triggered one listJobsByKey call with the
    // default limit.
    assert(
      listCalls.length === JOB_METADATA.length,
      "list-schedules-ok",
      `expected ${JOB_METADATA.length} listJobsByKey calls, got ${listCalls.length}`,
    );
    assert(
      listCalls.every((c) => c.limit === DEFAULT_SCHEDULES_RUN_LIMIT),
      "list-schedules-ok",
      `all calls should use default limit ${DEFAULT_SCHEDULES_RUN_LIMIT}`,
    );
    // Recent runs routed to the right schedule.
    assert(
      body.schedules?.[0]?.recentRuns.length === 1 &&
        body.schedules[0].recentRuns[0]!.id === 11,
      "list-schedules-ok",
      `nightly_tier1 should have 1 run with id=11`,
    );
    assert(
      body.schedules?.[1]?.recentRuns.length === 0,
      "list-schedules-ok",
      `weekly_state should have 0 runs`,
    );
    assert(
      body.schedules?.[2]?.recentRuns.length === 2,
      "list-schedules-ok",
      `hourly_linker should have 2 runs`,
    );
  }

  // --- 10. GET /scraper-schedules?limit=5 clamps + forwards the limit ----
  {
    const seenLimits: number[] = [];
    const deps = makeDeps({
      listJobsByKey: async ({ limit }) => {
        seenLimits.push(limit);
        return [];
      },
    });
    const handler = makeListSchedulesHandler(deps);
    const req = makeReq({
      adminAuth: sessionAuth("admin"),
      query: { limit: "5" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "list-schedules-custom-limit",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      seenLimits.every((l) => l === 5),
      "list-schedules-custom-limit",
      `each dep call should receive limit=5, got ${JSON.stringify(seenLimits)}`,
    );
  }

  // --- 11. GET /scraper-schedules?limit=abc → 400 ------------------------
  {
    const deps = makeDeps();
    const handler = makeListSchedulesHandler(deps);
    const req = makeReq({
      adminAuth: sessionAuth("admin"),
      query: { limit: "abc" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 400,
      "list-schedules-bad-limit",
      `expected 400, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "invalid limit",
      "list-schedules-bad-limit",
      `expected 'invalid limit', got ${body.error}`,
    );
  }

  // --- bonus: missing adminAuth (misconfigured route chain) → 403 --------
  {
    const req = makeReq({ adminAuth: undefined });
    const res = makeRes();
    let nextCalled = false;
    requireSuperAdmin(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(
      !nextCalled,
      "mw-no-auth",
      "next() should not be called when adminAuth missing",
    );
    assert(
      res.statusCode === 403,
      "mw-no-auth",
      `expected 403, got ${res.statusCode}`,
    );
  }

  if (failures.length === 0) {
    console.log("[adminScheduler-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminScheduler-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
