/**
 * requireAdmin middleware — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/requireAdmin.test.ts
 *
 * Same harness style as apiKeyAuth.test.ts — no vitest, no DB. All four
 * lookups are injected as fakes via makeRequireAdmin(). The middleware is
 * called directly with fake req/res/next; no HTTP server is started.
 */
import type { Request, Response } from "express";
import {
  makeRequireAdmin,
  ADMIN_SESSION_COOKIE,
} from "../middlewares/requireAdmin";
import {
  hashApiKey,
  hashSessionToken,
  type ApiKey,
  type AdminUser,
  type AdminSession,
} from "@workspace/db";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

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
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  cookies?: Record<string, string>;
}): Request {
  return {
    method: opts.method ?? "GET",
    path: opts.path ?? "/scrape-runs",
    headers: opts.headers ?? {},
    cookies: opts.cookies ?? {},
  } as unknown as Request;
}

function validApiKey(scopes: string[]): ApiKey {
  return {
    id: 1,
    name: "test-key",
    keyHash: hashApiKey("valid-plaintext"),
    keyPrefix: "valid-pl",
    createdAt: new Date("2026-01-01T00:00:00Z"),
    lastUsedAt: null,
    revokedAt: null,
    scopes,
  };
}

function validSession(id: number, userId: number): AdminSession {
  return {
    id,
    adminUserId: userId,
    tokenHash: hashSessionToken("valid-session-token"),
    expiresAt: new Date(Date.now() + 60 * 60 * 1000),
    createdAt: new Date("2026-01-01T00:00:00Z"),
    userAgent: "test-ua",
    ip: "127.0.0.1",
  };
}

function validAdminUser(role: "admin" | "super_admin" = "admin"): AdminUser {
  return {
    id: 42,
    email: "ops@upshift.test",
    passwordHash: "$2b$12$fake",
    role,
    createdAt: new Date("2026-01-01T00:00:00Z"),
    lastLoginAt: null,
  };
}

// Null lookups — used as defaults when a test only exercises one path.
const noApiKey = async (_h: string): Promise<ApiKey | null> => null;
const noSession = async (_h: string): Promise<AdminSession | null> => null;
const noUser = async (_id: number): Promise<AdminUser | null> => null;
const noopBump = async (_id: number): Promise<void> => {};

async function run() {
  // 1. No credentials → 401
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: noSession,
      adminUserLookup: noUser,
      bumpExpiry: noopBump,
    });
    const req = makeReq({});
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "no-creds", "next() should not be called");
    assert(res.statusCode === 401, "no-creds", `expected 401, got ${res.statusCode}`);
    assert(
      (res.body as { error?: string })?.error === "unauthorized",
      "no-creds",
      "body should be generic {error: 'unauthorized'}",
    );
  }

  // 2. Valid API key with 'admin' scope → next() + req.adminAuth populated
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: async () => validApiKey(["admin"]),
      sessionLookup: noSession,
      adminUserLookup: noUser,
      bumpExpiry: noopBump,
    });
    const req = makeReq({ headers: { "x-api-key": "valid-plaintext" } });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "apikey-admin", "next() must be called");
    assert(
      res.statusCode === 200,
      "apikey-admin",
      `status should not be set, got ${res.statusCode}`,
    );
    const reqTyped = req as Request & { adminAuth?: { kind: string } };
    assert(
      reqTyped.adminAuth?.kind === "apiKey",
      "apikey-admin",
      `req.adminAuth.kind should be 'apiKey', got ${reqTyped.adminAuth?.kind}`,
    );
  }

  // 3. Valid API key WITHOUT 'admin' scope → 401
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: async () => validApiKey(["read"]),
      sessionLookup: noSession,
      adminUserLookup: noUser,
      bumpExpiry: noopBump,
    });
    const req = makeReq({ headers: { "x-api-key": "valid-plaintext" } });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "apikey-noscope", "next() should not be called");
    assert(
      res.statusCode === 401,
      "apikey-noscope",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 4. Invalid API key (lookup returns null) → 401 without falling through
  //    to the session cookie. A broken caller shouldn't escalate to whatever
  //    session was on the same browser/IP.
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: async () => null,
      // If the middleware falls through to the session lookup, this would
      // return a valid session and the request would pass — the assertion
      // below catches that regression.
      sessionLookup: async () => validSession(1, 42),
      adminUserLookup: async () => validAdminUser(),
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      headers: { "x-api-key": "bogus" },
      cookies: { [ADMIN_SESSION_COOKIE]: "valid-session-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "apikey-invalid-no-fallthrough", "next() should not be called");
    assert(
      res.statusCode === 401,
      "apikey-invalid-no-fallthrough",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 5. Valid session cookie + admin role → next() + req.adminAuth.session
  {
    let bumpedId: number | null = null;
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => validSession(7, 42),
      adminUserLookup: async () => validAdminUser("admin"),
      bumpExpiry: async (id: number) => {
        bumpedId = id;
      },
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "valid-session-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "session-admin", "next() must be called");
    const reqTyped = req as Request & {
      adminAuth?: { kind: string; userId?: number };
    };
    assert(
      reqTyped.adminAuth?.kind === "session" && reqTyped.adminAuth?.userId === 42,
      "session-admin",
      "req.adminAuth should be session with userId=42",
    );
    assert(
      bumpedId === 7,
      "session-admin",
      `bumpExpiry should be called with sessionId=7, got ${bumpedId}`,
    );
  }

  // 6. Expired session (lookup returns null; SQL `expires_at > now()`
  //    predicate filters the row out) → 401
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => null, // DB filter dropped the expired row
      adminUserLookup: async () => validAdminUser(),
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "expired-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "session-expired", "next() should not be called");
    assert(
      res.statusCode === 401,
      "session-expired",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 7. Invalid session (lookup returns null for unknown hash) → 401
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => null,
      adminUserLookup: async () => validAdminUser(),
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "nonexistent" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "session-invalid", "next() should not be called");
    assert(
      res.statusCode === 401,
      "session-invalid",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 8. Session row exists but admin_users row does not (FK ghost) → 401
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => validSession(1, 42),
      adminUserLookup: async () => null,
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "valid-session-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(
      !nextCalled,
      "session-orphan",
      "next() should not be called when admin_users row is missing",
    );
    assert(
      res.statusCode === 401,
      "session-orphan",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 9. Session + admin role is NOT admin/super_admin → 401 (defense-in-depth
  //    — the CHECK constraint limits roles to those two, but the middleware
  //    doesn't trust that and checks anyway)
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => validSession(1, 42),
      adminUserLookup: async () =>
        ({ ...validAdminUser(), role: "bogus" }) as AdminUser,
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "valid-session-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "session-bad-role", "next() should not be called");
    assert(
      res.statusCode === 401,
      "session-bad-role",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 10. super_admin role passes
  {
    const mw = makeRequireAdmin({
      apiKeyLookup: noApiKey,
      sessionLookup: async () => validSession(1, 42),
      adminUserLookup: async () => validAdminUser("super_admin"),
      bumpExpiry: noopBump,
    });
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "valid-session-token" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "session-super-admin", "next() must be called");
  }

  // 11. Lookup throws → delegated to next(err)
  {
    const boom = new Error("db down");
    const mw = makeRequireAdmin({
      apiKeyLookup: async () => {
        throw boom;
      },
      sessionLookup: noSession,
      adminUserLookup: noUser,
      bumpExpiry: noopBump,
    });
    const req = makeReq({ headers: { "x-api-key": "x" } });
    const res = makeRes();
    let capturedErr: unknown;
    await mw(req, res as unknown as Response, (err?: unknown) => {
      capturedErr = err;
    });
    assert(
      capturedErr === boom,
      "lookup-throws",
      "next(err) should receive the error",
    );
  }

  if (failures.length === 0) {
    console.log("[requireAdmin-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[requireAdmin-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
