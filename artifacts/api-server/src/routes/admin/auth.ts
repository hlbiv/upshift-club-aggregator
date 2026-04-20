/**
 * `/v1/admin/auth/*` and `/v1/admin/me` — admin identity endpoints.
 *
 *   POST /v1/admin/auth/login   → loginRouter    (NOT behind requireAdmin)
 *   POST /v1/admin/auth/logout  → logoutRouter   (behind requireAdmin)
 *
 * `/v1/admin/me` lives in me.ts.
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
  return process.env.NODE_ENV === "production";
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

      res.cookie(ADMIN_SESSION_COOKIE, token, {
        httpOnly: true,
        secure: cookieSecure(),
        sameSite: "lax",
        path: "/",
        expires: expiresAt,
        // Mirror with maxAge so browsers that ignore `expires` still honor
        // the TTL. Some older clients prefer one over the other.
        maxAge: ADMIN_SESSION_TTL_MS,
      });

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
      res.clearCookie(ADMIN_SESSION_COOKIE, {
        httpOnly: true,
        secure: cookieSecure(),
        sameSite: "lax",
        path: "/",
      });
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
