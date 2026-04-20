/**
 * requireScope middleware — unit tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx src/__tests__/requireScope.test.ts
 *
 * Same tooling-free pattern as apiKeyAuth.test.ts: call the handler
 * directly with fake req/res/next objects, no DB, no supertest.
 */
import type { Request, Response } from "express";
import { requireScope } from "../middlewares/requireScope";
import type { ApiKey } from "@workspace/db";

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

type KeyLike = Pick<ApiKey, "id" | "name" | "keyPrefix" | "scopes" | "createdAt">;

function makeReq(opts: {
  path?: string;
  apiKey?: KeyLike;
}): Request {
  return {
    method: "GET",
    path: opts.path ?? "/api/admin/ping",
    headers: {},
    apiKey: opts.apiKey,
  } as unknown as Request;
}

function keyWithScopes(scopes: string[]): KeyLike {
  return {
    id: 1,
    name: "test-key",
    keyPrefix: "test-key",
    scopes,
    createdAt: new Date("2026-01-01T00:00:00Z"),
  };
}

/**
 * Swap env vars for the duration of a single test block. Captures prior
 * values so we don't leak state between cases.
 */
function withEnv<T>(overrides: Record<string, string | undefined>, fn: () => T): T {
  const prior: Record<string, string | undefined> = {};
  for (const k of Object.keys(overrides)) prior[k] = process.env[k];
  try {
    for (const [k, v] of Object.entries(overrides)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
    return fn();
  } finally {
    for (const [k, v] of Object.entries(prior)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  }
}

async function run() {
  // 1. Scope present → next() is called, no status set
  await withEnv(
    { API_KEY_AUTH_ENABLED: "true", NODE_ENV: "production" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: keyWithScopes(["admin:write", "other"]) });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(nextCalled, "scope-present", "next() must be called");
      assert(
        res.statusCode === 200,
        "scope-present",
        `no status expected, got ${res.statusCode}`,
      );
    },
  );

  // 2. Scope missing → 403 {error: "forbidden"}, next() NOT called
  await withEnv(
    { API_KEY_AUTH_ENABLED: "true", NODE_ENV: "production" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: keyWithScopes(["read"]) });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(!nextCalled, "scope-missing", "next() should not be called");
      assert(
        res.statusCode === 403,
        "scope-missing",
        `expected 403, got ${res.statusCode}`,
      );
      assert(
        (res.body as { error?: string })?.error === "forbidden",
        "scope-missing",
        "body should be generic {error: 'forbidden'}",
      );
      // No reason leaked in the body
      assert(
        (res.body as { reason?: string; scope?: string })?.reason === undefined &&
          (res.body as { reason?: string; scope?: string })?.scope === undefined,
        "scope-missing",
        "body should NOT leak reason or scope name",
      );
    },
  );

  // 3. Empty scopes array → treated as missing → 403
  await withEnv(
    { API_KEY_AUTH_ENABLED: "true", NODE_ENV: "production" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: keyWithScopes([]) });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(!nextCalled, "empty-scopes", "next() should not be called");
      assert(
        res.statusCode === 403,
        "empty-scopes",
        `expected 403, got ${res.statusCode}`,
      );
    },
  );

  // 4. Auth disabled via unset env → pass-through (no 403 even if scope missing)
  await withEnv(
    { API_KEY_AUTH_ENABLED: undefined, NODE_ENV: "production" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: undefined });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(nextCalled, "auth-disabled-unset", "next() must be called");
      assert(
        res.statusCode === 200,
        "auth-disabled-unset",
        `no status expected, got ${res.statusCode}`,
      );
    },
  );

  // 4b. Auth disabled in development mode → pass-through
  await withEnv(
    { API_KEY_AUTH_ENABLED: "true", NODE_ENV: "development" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: undefined });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(nextCalled, "auth-disabled-dev", "next() must be called in dev");
    },
  );

  // 5. Auth enabled but req.apiKey absent → 403 (fail closed)
  await withEnv(
    { API_KEY_AUTH_ENABLED: "true", NODE_ENV: "production" },
    async () => {
      const mw = requireScope("admin:write");
      const req = makeReq({ apiKey: undefined });
      const res = makeRes();
      let nextCalled = false;
      await mw(req, res as unknown as Response, () => {
        nextCalled = true;
      });
      assert(!nextCalled, "no-api-key", "next() should not be called");
      assert(
        res.statusCode === 403,
        "no-api-key",
        `expected 403, got ${res.statusCode}`,
      );
      assert(
        (res.body as { error?: string })?.error === "forbidden",
        "no-api-key",
        "body should be generic {error: 'forbidden'}",
      );
    },
  );

  if (failures.length === 0) {
    console.log("[requireScope-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[requireScope-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
