/**
 * apiKeyAuth middleware — unit tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx src/__tests__/apiKeyAuth.test.ts
 *
 * The repo has no vitest harness (see lib/db/src/schema/__tests__/smoke.ts
 * for the established pattern). These tests mount the middleware in a real
 * Express app and drive it with `supertest`-style raw HTTP — except we
 * don't have supertest either, so we call the handler directly with fake
 * req/res/next objects. No database is touched: a fake ApiKeyLookup is
 * injected via makeApiKeyAuth.
 */
import type { Request, Response } from "express";
import {
  makeApiKeyAuth,
} from "../middlewares/apiKeyAuth";
import { hashApiKey, type ApiKey } from "@workspace/db";

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
}): Request {
  return {
    method: opts.method ?? "GET",
    path: opts.path ?? "/api/clubs",
    headers: opts.headers ?? {},
  } as unknown as Request;
}

type Probe = { lookedUpHash?: string };

function makeLookup(
  row: ApiKey | null,
  probe?: Probe,
): (hash: string) => Promise<ApiKey | null> {
  return async (hash: string) => {
    if (probe) probe.lookedUpHash = hash;
    return row;
  };
}

function validRow(): ApiKey {
  return {
    id: 1,
    name: "test-key",
    keyHash: hashApiKey("valid-plaintext"),
    keyPrefix: "valid-pl",
    createdAt: new Date("2026-01-01T00:00:00Z"),
    lastUsedAt: null,
    revokedAt: null,
    scopes: [],
  };
}

async function run() {
  // 1. Missing header → 401
  {
    const mw = makeApiKeyAuth(makeLookup(null));
    const req = makeReq({});
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "missing-header", "next() should not be called");
    assert(
      res.statusCode === 401,
      "missing-header",
      `expected 401, got ${res.statusCode}`,
    );
  }

  // 2. Bad key → 401 (lookup returns null)
  {
    const probe: Probe = {};
    const mw = makeApiKeyAuth(makeLookup(null, probe));
    const req = makeReq({ headers: { "x-api-key": "bogus" } });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "bad-key", "next() should not be called");
    assert(res.statusCode === 401, "bad-key", `expected 401, got ${res.statusCode}`);
    assert(
      probe.lookedUpHash === hashApiKey("bogus"),
      "bad-key",
      "middleware should hash the plaintext before lookup",
    );
  }

  // 3. Revoked key → 401 (lookup already filters these; simulate by returning null)
  {
    const mw = makeApiKeyAuth(makeLookup(null));
    const req = makeReq({
      headers: { authorization: "Bearer revoked-key" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(!nextCalled, "revoked", "next() should not be called");
    assert(res.statusCode === 401, "revoked", `expected 401, got ${res.statusCode}`);
  }

  // 4. Valid key via X-API-Key → next() + req.apiKey populated
  {
    const row = validRow();
    const mw = makeApiKeyAuth(makeLookup(row));
    const req = makeReq({ headers: { "x-api-key": "valid-plaintext" } });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "valid-xapi", "next() must be called");
    assert(
      res.statusCode === 200,
      "valid-xapi",
      `status should not be set, got ${res.statusCode}`,
    );
    const reqWithKey = req as Request & { apiKey?: { id: number; name: string } };
    assert(
      reqWithKey.apiKey?.id === 1 && reqWithKey.apiKey?.name === "test-key",
      "valid-xapi",
      "req.apiKey should be populated",
    );
  }

  // 5. Valid key via Authorization: Bearer → next()
  {
    const row = validRow();
    const mw = makeApiKeyAuth(makeLookup(row));
    const req = makeReq({
      headers: { authorization: "Bearer valid-plaintext" },
    });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "valid-bearer", "next() must be called");
  }

  // 6a. /healthz (mounted form) passes without a key
  {
    const mw = makeApiKeyAuth(makeLookup(null));
    const req = makeReq({ path: "/healthz" });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "healthz-mounted", "next() must be called on /healthz");
    assert(
      res.statusCode === 200,
      "healthz-mounted",
      `no status expected, got ${res.statusCode}`,
    );
  }

  // 6b. /api/healthz (fully-qualified) also passes without a key
  {
    const mw = makeApiKeyAuth(makeLookup(null));
    const req = makeReq({ path: "/api/healthz" });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "healthz-full", "next() must be called on /api/healthz");
  }

  // 7. OPTIONS preflight passes without a key
  {
    const mw = makeApiKeyAuth(makeLookup(null));
    const req = makeReq({ method: "OPTIONS" });
    const res = makeRes();
    let nextCalled = false;
    await mw(req, res as unknown as Response, () => {
      nextCalled = true;
    });
    assert(nextCalled, "preflight", "next() must be called on OPTIONS");
  }

  // 8. Lookup throws → delegated to next(err) (Express error handler)
  {
    const boom = new Error("db down");
    const mw = makeApiKeyAuth(async () => {
      throw boom;
    });
    const req = makeReq({ headers: { "x-api-key": "x" } });
    const res = makeRes();
    let capturedErr: unknown;
    await mw(req, res as unknown as Response, (err?: unknown) => {
      capturedErr = err;
    });
    assert(capturedErr === boom, "lookup-throws", "next(err) should receive the error");
  }

  if (failures.length === 0) {
    console.log("[apiKeyAuth-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[apiKeyAuth-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
