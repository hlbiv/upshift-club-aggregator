/**
 * apiKeyAuth — Machine-to-machine API key authentication.
 *
 * Every request under `/api/*` is rejected with 401 unless it presents a
 * valid, non-revoked API key in one of two headers:
 *   - `X-API-Key: <plaintext>`
 *   - `Authorization: Bearer <plaintext>`
 *
 * Exemptions: liveness probes (`/api/healthz`) and CORS preflights (OPTIONS).
 * The server only has M2M callers; there are no user sessions or cookies.
 *
 * On success `req.apiKey` is populated with the ApiKey row (minus secret
 * fields). `last_used_at` is updated transparently inside findApiKeyByHash.
 */
import type { Request, Response, NextFunction, RequestHandler } from "express";
import { hashApiKey, findApiKeyByHash, type ApiKey } from "@workspace/db";

// `req.apiKey` augmentation lives in `src/types/express.d.ts` so any
// middleware (rateLimit, requireScope, requireAdmin, …) can rely on the
// shape without importing this module just for types.

type ApiKeyLookup = (hash: string) => Promise<ApiKey | null>;

// Paths are compared against req.path, which inside a mounted sub-app
// excludes the mount prefix. Since this middleware is mounted at `/api`
// (see app.ts), `req.path` for /api/healthz is "/healthz". We also accept
// the fully-qualified path so the middleware is correct regardless of how
// callers mount it.
const EXEMPT_PATHS = new Set<string>(["/healthz", "/api/healthz"]);

function extractKey(req: Request): string | null {
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

/**
 * Factory — build a middleware with a custom lookup function. Used by
 * tests to inject a fake without hitting the database.
 */
/**
 * Client-facing 401 body. Intentionally identical for every failure mode
 * (missing header / bad key / revoked key) so an attacker probing the
 * endpoint can't distinguish "this key was once valid" from "never existed".
 * Detailed reason is logged server-side — see logAuthFailure below.
 */
const UNAUTHORIZED_BODY = { error: "unauthorized" };

type AuthFailureReason = "missing" | "notfound";

function logAuthFailure(
  req: Request,
  reason: AuthFailureReason,
  keyPrefix: string | null,
): void {
  // Use console.warn to stay consistent with the rest of the repo's logging
  // posture in middleware (pino-http handles request logging; this is a
  // separate security-event line).
  // eslint-disable-next-line no-console
  console.warn("[api-key-auth] auth failure", {
    ip: req.ip,
    path: req.path,
    prefix: keyPrefix ?? "(none)",
    reason,
  });
}

export function makeApiKeyAuth(lookup: ApiKeyLookup): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    if (req.method === "OPTIONS") {
      next();
      return;
    }
    if (EXEMPT_PATHS.has(req.path)) {
      next();
      return;
    }
    const plaintext = extractKey(req);
    if (!plaintext) {
      logAuthFailure(req, "missing", null);
      res.status(401).json(UNAUTHORIZED_BODY);
      return;
    }
    // Derive the key prefix (first 8 chars) for audit logging before we
    // hash. Safe to log — the prefix is stored in plaintext in the DB too.
    const keyPrefix = plaintext.slice(0, 8);
    try {
      const row = await lookup(hashApiKey(plaintext));
      if (!row) {
        // findApiKeyByHash filters revoked rows in SQL, so "not found" here
        // covers both "never existed" and "revoked" — indistinguishable by
        // design. Client sees the same generic 401 in either case.
        logAuthFailure(req, "notfound", keyPrefix);
        res.status(401).json(UNAUTHORIZED_BODY);
        return;
      }
      req.apiKey = {
        id: row.id,
        name: row.name,
        keyPrefix: row.keyPrefix,
        scopes: row.scopes,
        createdAt: row.createdAt,
      };
      next();
    } catch (err) {
      next(err);
    }
  };
}

/** Default middleware wired to the live database via findApiKeyByHash. */
export const apiKeyAuth: RequestHandler = makeApiKeyAuth(findApiKeyByHash);
