/**
 * Admin auth routes — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminAuth.test.ts
 *
 * Drives the login/logout handlers directly with fake DB deps. No HTTP
 * server is started and no DB is touched. Uses a real bcrypt hash generated
 * in `beforeAll` so the password-compare path exercises the same library
 * that production uses.
 */
import type { Request, Response } from "express";
import bcrypt from "bcryptjs";
import {
  makeLoginHandler,
  makeLogoutHandler,
  type LoginDeps,
  type LogoutDeps,
} from "../routes/admin/auth";
import { ADMIN_SESSION_COOKIE } from "../middlewares/requireAdmin";
import { hashSessionToken, type AdminUser } from "@workspace/db";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

interface CookieCall {
  name: string;
  value: string;
  options: Record<string, unknown>;
}

type FakeRes = {
  statusCode: number;
  body: unknown;
  cookies: CookieCall[];
  clearedCookies: CookieCall[];
  status: (code: number) => FakeRes;
  json: (body: unknown) => FakeRes;
  cookie: (name: string, value: string, options: Record<string, unknown>) => FakeRes;
  clearCookie: (name: string, options: Record<string, unknown>) => FakeRes;
};

function makeRes(): FakeRes {
  const res: FakeRes = {
    statusCode: 200,
    body: undefined,
    cookies: [],
    clearedCookies: [],
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
    cookie(name, value, options) {
      this.cookies.push({ name, value, options });
      return this;
    },
    clearCookie(name, options) {
      this.clearedCookies.push({ name, value: "", options });
      return this;
    },
  };
  return res;
}

function makeReq(opts: {
  body?: unknown;
  headers?: Record<string, string>;
  cookies?: Record<string, string>;
  ip?: string;
}): Request {
  return {
    body: opts.body ?? {},
    headers: opts.headers ?? {},
    cookies: opts.cookies ?? {},
    ip: opts.ip ?? "127.0.0.1",
  } as unknown as Request;
}

function userWithHash(hash: string, role: "admin" | "super_admin" = "admin"): AdminUser {
  return {
    id: 42,
    email: "ops@upshift.test",
    passwordHash: hash,
    role,
    createdAt: new Date("2026-01-01T00:00:00Z"),
    lastLoginAt: null,
  };
}

async function run() {
  // Real bcrypt hash so compare() is exercised end-to-end.
  const PASSWORD = "correcthorse!battery";
  const goodHash = await bcrypt.hash(PASSWORD, 4); // lower rounds for test speed

  // --- Login: correct credentials → 200 + cookie set + session created ---
  {
    let sessionCreatedFor: number | null = null;
    let lastLoginBumpedFor: number | null = null;
    const deps: LoginDeps = {
      findUserByEmail: async (email) =>
        email === "ops@upshift.test" ? userWithHash(goodHash) : null,
      createSession: async (adminUserId) => {
        sessionCreatedFor = adminUserId;
        return { token: "fresh-token", expiresAt: new Date(Date.now() + 10_000) };
      },
      bumpLastLogin: async (id) => {
        lastLoginBumpedFor = id;
      },
    };
    const handler = makeLoginHandler(deps);
    const req = makeReq({
      body: { email: "ops@upshift.test", password: PASSWORD },
      headers: { "user-agent": "test-ua" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "login-ok", `expected 200, got ${res.statusCode}`);
    const body = res.body as { id?: number; email?: string; role?: string };
    assert(body?.id === 42, "login-ok", "body.id should be 42");
    assert(body?.email === "ops@upshift.test", "login-ok", "body.email mismatch");
    assert(body?.role === "admin", "login-ok", "body.role should be admin");
    assert(
      res.cookies.length === 1 && res.cookies[0]?.name === ADMIN_SESSION_COOKIE,
      "login-ok",
      "exactly one Set-Cookie should target the admin session cookie",
    );
    assert(
      res.cookies[0]?.value === "fresh-token",
      "login-ok",
      "cookie value should be the plaintext token returned by createSession",
    );
    const opts = res.cookies[0]?.options ?? {};
    assert(opts.httpOnly === true, "login-ok", "cookie should be httpOnly");
    assert(opts.sameSite === "lax", "login-ok", "cookie should be sameSite=lax");
    assert(opts.path === "/", "login-ok", "cookie path should be /");
    assert(sessionCreatedFor === 42, "login-ok", "createSession should run for userId=42");
    assert(lastLoginBumpedFor === 42, "login-ok", "bumpLastLogin should run for userId=42");
  }

  // --- Login: wrong password → 401, no cookie, no session ---
  {
    let sessionCreated = false;
    const deps: LoginDeps = {
      findUserByEmail: async () => userWithHash(goodHash),
      createSession: async () => {
        sessionCreated = true;
        return { token: "should-not-run", expiresAt: new Date() };
      },
      bumpLastLogin: async () => {},
    };
    const handler = makeLoginHandler(deps);
    const req = makeReq({
      body: { email: "ops@upshift.test", password: "wrong-password-yo" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(
      res.statusCode === 401,
      "login-wrong-pw",
      `expected 401, got ${res.statusCode}`,
    );
    assert(
      (res.body as { error?: string })?.error === "unauthorized",
      "login-wrong-pw",
      "body should be {error: 'unauthorized'}",
    );
    assert(res.cookies.length === 0, "login-wrong-pw", "no cookie should be set");
    assert(!sessionCreated, "login-wrong-pw", "createSession should NOT be called");
  }

  // --- Login: unknown email → 401, no cookie, no session
  //     (and bcrypt.compare still runs against the dummy hash) ---
  {
    let findCalled = false;
    const deps: LoginDeps = {
      findUserByEmail: async () => {
        findCalled = true;
        return null;
      },
      createSession: async () => {
        throw new Error("should not be called");
      },
      bumpLastLogin: async () => {
        throw new Error("should not be called");
      },
    };
    const handler = makeLoginHandler(deps);
    const req = makeReq({
      body: { email: "ghost@upshift.test", password: PASSWORD },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(
      res.statusCode === 401,
      "login-unknown-email",
      `expected 401, got ${res.statusCode}`,
    );
    assert(findCalled, "login-unknown-email", "findUserByEmail should be called");
  }

  // --- Login: malformed body → 401, no DB call ---
  {
    let findCalled = false;
    const deps: LoginDeps = {
      findUserByEmail: async () => {
        findCalled = true;
        return null;
      },
      createSession: async () => ({ token: "x", expiresAt: new Date() }),
      bumpLastLogin: async () => {},
    };
    const handler = makeLoginHandler(deps);
    const req = makeReq({ body: { email: "not-an-email", password: "short" } });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(
      res.statusCode === 401,
      "login-bad-body",
      `expected 401, got ${res.statusCode}`,
    );
    assert(
      !findCalled,
      "login-bad-body",
      "findUserByEmail should NOT be called on a malformed body",
    );
  }

  // --- Logout: cookie present → session deleted + cookie cleared ---
  {
    let deletedHash: string | null = null;
    const deps: LogoutDeps = {
      deleteSession: async (hash) => {
        deletedHash = hash;
      },
    };
    const handler = makeLogoutHandler(deps);
    const req = makeReq({
      cookies: { [ADMIN_SESSION_COOKIE]: "session-token-xyz" },
    });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(res.statusCode === 200, "logout-ok", `expected 200, got ${res.statusCode}`);
    assert(
      (res.body as { ok?: boolean })?.ok === true,
      "logout-ok",
      "body.ok should be true",
    );
    assert(
      deletedHash === hashSessionToken("session-token-xyz"),
      "logout-ok",
      "deleteSession should be called with the SHA256 of the cookie value",
    );
    assert(
      res.clearedCookies.length === 1 &&
        res.clearedCookies[0]?.name === ADMIN_SESSION_COOKIE,
      "logout-ok",
      "clearCookie should target the admin session cookie",
    );
  }

  // --- Logout: no cookie → 200, idempotent (no delete attempt) ---
  {
    let deleteCalled = false;
    const deps: LogoutDeps = {
      deleteSession: async () => {
        deleteCalled = true;
      },
    };
    const handler = makeLogoutHandler(deps);
    const req = makeReq({ cookies: {} });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "logout-no-cookie",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      !deleteCalled,
      "logout-no-cookie",
      "deleteSession should NOT be called when cookie is absent",
    );
    assert(
      res.clearedCookies.length === 1,
      "logout-no-cookie",
      "clearCookie should still run (defense-in-depth cleanup)",
    );
  }

  if (failures.length === 0) {
    console.log("[adminAuth-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminAuth-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
