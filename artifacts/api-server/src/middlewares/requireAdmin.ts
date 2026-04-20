/**
 * requireAdmin — authentication guard for `/v1/admin/*` routes.
 *
 * Two supported auth paths, checked in this order:
 *
 *   1. **Machine-to-machine.** `X-API-Key: <plaintext>` header validated
 *      against `api_keys.key_hash`. Must carry the `admin` scope in
 *      `api_keys.scopes` (granular scopes like `admin:scheduler` will be
 *      recognised in future phases — today the gate is the bare `admin`).
 *
 *   2. **Human session.** `upshift_admin_sid` cookie validated against
 *      `admin_sessions.token_hash` with `expires_at > now()`. The
 *      `admin_users.role` must be `admin` or `super_admin`. The session's
 *      idle-TTL is bumped on every successful hit.
 *
 * On success `req.adminAuth` is populated with a discriminated union so
 * downstream handlers can differentiate. On failure a generic 401 is
 * returned; the detailed reason is logged server-side via console.warn
 * (same posture as apiKeyAuth.ts — don't leak "this key existed once"
 * through client-visible response bodies).
 *
 * Cookie-vs-APIkey priority: if an API key header is present and valid, the
 * session cookie is IGNORED. This matches the intent of the M2M path (a
 * script running against the admin surface shouldn't be coerced into
 * whatever session happened to be on the same IP). If the API key header is
 * present but INVALID, we fall straight to 401 without consulting cookies —
 * a broken caller shouldn't accidentally escalate into a human session.
 */
import type { Request, Response, NextFunction, RequestHandler } from "express";
import { eq } from "drizzle-orm";
import {
  hashApiKey,
  findApiKeyByHash,
  hashSessionToken,
  findAdminSessionByTokenHash,
  bumpSessionExpiry,
  db,
  adminUsers,
  type ApiKey,
  type AdminSession,
  type AdminUser,
} from "@workspace/db";

/** Discriminated union attached to `req.adminAuth` after a successful check. */
export type AdminAuthContext =
  | {
      kind: "apiKey";
      keyId: number;
      keyName: string;
      scopes: readonly string[];
    }
  | {
      kind: "session";
      userId: number;
      email: string;
      role: "admin" | "super_admin";
      sessionId: number;
    };

// Module augmentation for `req.adminAuth` is centralized in
// src/types/express.d.ts — keep this file free of `declare module` so both
// the type declaration and this runtime file agree on one source.

/** Cookie name carrying the raw session token. Matches the contract spec. */
export const ADMIN_SESSION_COOKIE = "upshift_admin_sid";

/** Scope required on an API key for it to authenticate the admin surface. */
export const ADMIN_API_KEY_SCOPE = "admin";

type ApiKeyLookup = (hash: string) => Promise<ApiKey | null>;
type SessionLookup = (hash: string) => Promise<AdminSession | null>;
type AdminUserLookup = (id: number) => Promise<AdminUser | null>;
type SessionExpiryBump = (id: number) => Promise<void>;

interface RequireAdminDeps {
  apiKeyLookup: ApiKeyLookup;
  sessionLookup: SessionLookup;
  adminUserLookup: AdminUserLookup;
  bumpExpiry: SessionExpiryBump;
}

const UNAUTHORIZED_BODY = { error: "unauthorized" };

type FailureReason =
  | "no-credentials"
  | "apikey-notfound"
  | "apikey-missing-scope"
  | "session-notfound"
  | "session-user-missing"
  | "session-role-invalid";

function logAuthFailure(
  req: Request,
  reason: FailureReason,
  detail?: string,
): void {
  // eslint-disable-next-line no-console
  console.warn("[require-admin] auth failure", {
    ip: req.ip,
    path: req.path,
    reason,
    detail: detail ?? null,
  });
}

function extractApiKey(req: Request): string | null {
  const header = req.headers["x-api-key"];
  if (typeof header === "string" && header.length > 0) return header;
  if (Array.isArray(header) && header.length > 0 && header[0]) return header[0];

  const auth = req.headers["authorization"];
  if (typeof auth === "string" && auth.startsWith("Bearer ")) {
    const token = auth.slice("Bearer ".length).trim();
    if (token.length > 0) return token;
  }
  return null;
}

function extractSessionToken(req: Request): string | null {
  // `cookie-parser` populates req.cookies as a plain object keyed by
  // cookie name. If the middleware isn't mounted, `req.cookies` is
  // undefined — treat that as "no cookie".
  const cookies = (req as Request & { cookies?: Record<string, string> })
    .cookies;
  const raw = cookies?.[ADMIN_SESSION_COOKIE];
  if (typeof raw === "string" && raw.length > 0) return raw;
  return null;
}

function isAdminRole(role: string | null | undefined): role is "admin" | "super_admin" {
  return role === "admin" || role === "super_admin";
}

/**
 * Factory — build the middleware with injected lookups. Tests pass fakes so
 * no DB is touched. Production callers use `requireAdmin` (below).
 */
export function makeRequireAdmin(deps: RequireAdminDeps): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      // --- Path 1: API key ------------------------------------------------
      const plaintextKey = extractApiKey(req);
      if (plaintextKey) {
        const row = await deps.apiKeyLookup(hashApiKey(plaintextKey));
        if (!row) {
          logAuthFailure(req, "apikey-notfound", plaintextKey.slice(0, 8));
          res.status(401).json(UNAUTHORIZED_BODY);
          return;
        }
        if (!row.scopes?.includes(ADMIN_API_KEY_SCOPE)) {
          logAuthFailure(req, "apikey-missing-scope", row.keyPrefix);
          res.status(401).json(UNAUTHORIZED_BODY);
          return;
        }
        req.adminAuth = {
          kind: "apiKey",
          keyId: row.id,
          keyName: row.name,
          scopes: row.scopes,
        };
        next();
        return;
      }

      // --- Path 2: session cookie -----------------------------------------
      const cookieToken = extractSessionToken(req);
      if (cookieToken) {
        const sessionRow = await deps.sessionLookup(
          hashSessionToken(cookieToken),
        );
        if (!sessionRow) {
          logAuthFailure(req, "session-notfound");
          res.status(401).json(UNAUTHORIZED_BODY);
          return;
        }
        const user = await deps.adminUserLookup(sessionRow.adminUserId);
        if (!user) {
          // Shouldn't happen — FK cascade deletes sessions on admin-user
          // deletion — but guard anyway so a stale row can't impersonate.
          logAuthFailure(req, "session-user-missing", String(sessionRow.adminUserId));
          res.status(401).json(UNAUTHORIZED_BODY);
          return;
        }
        if (!isAdminRole(user.role)) {
          logAuthFailure(req, "session-role-invalid", user.role);
          res.status(401).json(UNAUTHORIZED_BODY);
          return;
        }
        // Bump expiry AFTER we know the session is usable. Fire-and-forget
        // via await — the write is small and keeping the request in the
        // same code path is easier to reason about than background.
        await deps.bumpExpiry(sessionRow.id);
        req.adminAuth = {
          kind: "session",
          userId: user.id,
          email: user.email,
          role: user.role,
          sessionId: sessionRow.id,
        };
        next();
        return;
      }

      // --- No credentials -------------------------------------------------
      logAuthFailure(req, "no-credentials");
      res.status(401).json(UNAUTHORIZED_BODY);
    } catch (err) {
      next(err);
    }
  };
}

async function defaultAdminUserLookup(id: number): Promise<AdminUser | null> {
  const rows = await db
    .select()
    .from(adminUsers)
    .where(eq(adminUsers.id, id))
    .limit(1);
  return rows[0] ?? null;
}

/** Default middleware wired to live DB lookups. Used by app.ts. */
export const requireAdmin: RequestHandler = makeRequireAdmin({
  apiKeyLookup: findApiKeyByHash,
  sessionLookup: findAdminSessionByTokenHash,
  adminUserLookup: defaultAdminUserLookup,
  bumpExpiry: bumpSessionExpiry,
});
