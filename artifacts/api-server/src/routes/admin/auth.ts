/**
 * `/api/v1/admin/auth/*` and `/api/v1/admin/me` — admin identity endpoints.
 *
 *   POST /api/v1/admin/auth/login   → loginRouter    (NOT behind requireAdmin)
 *   POST /api/v1/admin/auth/logout  → logoutRouter   (behind requireAdmin)
 *
 * `/api/v1/admin/me` lives in me.ts.
 *
 * The login route can't require admin auth (it IS the auth entry point) so
 * it is exported as a separate router and mounted OUTSIDE the requireAdmin
 * guard in app.ts, with its own tighter rate limiter (10/min vs. 120/min).
 *
 * Cookie flags: httpOnly, sameSite=lax, path=/. `secure` is only set when
 * NODE_ENV=production so the Replit preview pane works over HTTP in dev.
 *
 * Factory shape: `makeLoginRouter` + `makeLogoutRouter` accept injectable
 * dependencies so tests can fake DB lookups without a live Postgres.
 * The default exports `loginRouter`/`logoutRouter` are the production
 * wirings.
 */
import { Router, type IRouter, type RequestHandler } from "express";
// bcryptjs is the pure-JS port (no native binary). Matches the bcrypt API
// surface we use — hash() / compare() — so swapping back later (if we ever
// want the faster native version) is a one-line import change.
import bcrypt from "bcryptjs";
import { eq } from "drizzle-orm";
import {
  db,
  adminUsers,
  createAdminSession,
  deleteAdminSession,
  hashSessionToken,
  ADMIN_SESSION_TTL_MS,
  type AdminUser,
} from "@workspace/db";
import {
  AdminLoginRequest,
  AdminLoginResponse,
  AdminLogoutResponse,
} from "@hlbiv/api-zod/admin";
import { ADMIN_SESSION_COOKIE } from "../../middlewares/requireAdmin";

const UNAUTHORIZED_BODY = { error: "unauthorized" };

function cookieSecure(): boolean {
  // Always Secure: required when SameSite=None (browsers reject SameSite=None
  // without Secure). Replit's preview proxy is always HTTPS, so this is safe
  // in dev too. The only place this would break is plain http://localhost
  // testing — which we don't do here.
  return true;
}

function cookieSameSite(): "none" | "lax" {
  // The dashboard runs inside Replit's workspace iframe, which is a
  // cross-site context relative to the API origin in many browser
  // configurations. SameSite=Lax cookies are dropped on cross-site
  // sub-requests, which silently breaks login. SameSite=None lets the
  // session cookie ride along on the iframe's fetch calls.
  return "none";
}

function buildSessionCookie(token: string, expires: Date): string {
  const parts = [
    `${ADMIN_SESSION_COOKIE}=${encodeURIComponent(token)}`,
    "Path=/",
    `Expires=${expires.toUTCString()}`,
    `Max-Age=${Math.floor(ADMIN_SESSION_TTL_MS / 1000)}`,
    "HttpOnly",
    "SameSite=None",
    "Secure",
    "Partitioned",
  ];
  return parts.join("; ");
}

function buildClearSessionCookie(): string {
  const parts = [
    `${ADMIN_SESSION_COOKIE}=`,
    "Path=/",
    "Expires=Thu, 01 Jan 1970 00:00:00 GMT",
    "Max-Age=0",
    "HttpOnly",
    "SameSite=None",
    "Secure",
    "Partitioned",
  ];
  return parts.join("; ");
}

function isAdminRole(r: string | null | undefined): r is "admin" | "super_admin" {
  return r === "admin" || r === "super_admin";
}

// ---------------------------------------------------------------------------
// Login — NOT behind requireAdmin.
// ---------------------------------------------------------------------------

/**
 * Dummy bcrypt hash used on the "user not found" path so `bcrypt.compare`
 * still runs. Without this, a rapid 401 on an unknown email returns much
 * faster than an email-exists-but-wrong-password 401 — a timing oracle for
 * enumerating valid admin emails. Value = `bcrypt.hashSync("invalid", 12)`.
 */
const DUMMY_BCRYPT_HASH =
  "$2b$12$CwTycUXWue0Thq9StjUM0uJ8.z3v5rWYEO0iYabLqFJ8o1SsQKiLy";

export interface LoginDeps {
  findUserByEmail: (email: string) => Promise<AdminUser | null>;
  createSession: (
    adminUserId: number,
    userAgent: string | null,
    ip: string | null,
  ) => Promise<{ token: string; expiresAt: Date }>;
  bumpLastLogin: (adminUserId: number) => Promise<void>;
}

export function makeLoginHandler(deps: LoginDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const parsed = AdminLoginRequest.safeParse(req.body);
      if (!parsed.success) {
        // Don't echo the zod error — it would confirm field-level issues
        // (e.g. "password is too short" → reveals password policy). Same
        // generic body as every other 401 on the admin surface.
        res.status(401).json(UNAUTHORIZED_BODY);
        return;
      }
      const { email, password } = parsed.data;

      const user = await deps.findUserByEmail(email);
      const hash = user?.passwordHash ?? DUMMY_BCRYPT_HASH;
      const ok = await bcrypt.compare(password, hash);
      if (!ok || !user) {
        // eslint-disable-next-line no-console
        console.warn("[admin-login] failure", {
          ip: req.ip,
          emailPrefix: email.split("@")[0]?.slice(0, 3) ?? "",
        });
        res.status(401).json(UNAUTHORIZED_BODY);
        return;
      }
      if (!isAdminRole(user.role)) {
        // Row exists with a non-admin role — shouldn't happen, but be
        // defensive. Generic 401 so the failure mode isn't observable.
        // eslint-disable-next-line no-console
        console.warn("[admin-login] invalid role", {
          ip: req.ip,
          userId: user.id,
          role: user.role,
        });
        res.status(401).json(UNAUTHORIZED_BODY);
        return;
      }

      const ua = req.headers["user-agent"] ?? null;
      const ip = req.ip ?? null;
      const { token, expiresAt } = await deps.createSession(
        user.id,
        typeof ua === "string" ? ua : null,
        ip,
      );

      await deps.bumpLastLogin(user.id);

      // Build Set-Cookie manually so we can include the `Partitioned`
      // attribute (CHIPS — required for cookies served to a third-party
      // iframe under Chrome's third-party-cookie deprecation, which is the
      // exact context Replit's workspace preview iframe puts us in).
      // Express's `res.cookie()` doesn't yet emit Partitioned in 5.2.x.
      res.setHeader(
        "Set-Cookie",
        buildSessionCookie(token, expiresAt),
      );

      res.json(
        AdminLoginResponse.parse({
          id: user.id,
          email: user.email,
          role: user.role,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

async function defaultFindUserByEmail(email: string): Promise<AdminUser | null> {
  const rows = await db
    .select()
    .from(adminUsers)
    .where(eq(adminUsers.email, email))
    .limit(1);
  return rows[0] ?? null;
}

async function defaultBumpLastLogin(adminUserId: number): Promise<void> {
  await db
    .update(adminUsers)
    .set({ lastLoginAt: new Date() })
    .where(eq(adminUsers.id, adminUserId));
}

export function makeLoginRouter(deps: LoginDeps): IRouter {
  const router: IRouter = Router();
  router.post("/auth/login", makeLoginHandler(deps));
  return router;
}

export const loginRouter: IRouter = makeLoginRouter({
  findUserByEmail: defaultFindUserByEmail,
  createSession: createAdminSession,
  bumpLastLogin: defaultBumpLastLogin,
});

// ---------------------------------------------------------------------------
// Logout — behind requireAdmin (a logged-out caller cannot log out).
// ---------------------------------------------------------------------------

export interface LogoutDeps {
  deleteSession: (tokenHash: string) => Promise<void>;
}

export function makeLogoutHandler(deps: LogoutDeps): RequestHandler {
  return async (req, res, next): Promise<void> => {
    try {
      const cookies = (req as { cookies?: Record<string, string> }).cookies;
      const raw = cookies?.[ADMIN_SESSION_COOKIE];
      if (typeof raw === "string" && raw.length > 0) {
        await deps.deleteSession(hashSessionToken(raw));
      }
      res.setHeader("Set-Cookie", buildClearSessionCookie());
      res.json(AdminLogoutResponse.parse({ ok: true }));
    } catch (err) {
      next(err);
    }
  };
}

export function makeLogoutRouter(deps: LogoutDeps): IRouter {
  const router: IRouter = Router();
  router.post("/auth/logout", makeLogoutHandler(deps));
  return router;
}

export const logoutRouter: IRouter = makeLogoutRouter({
  deleteSession: deleteAdminSession,
});
