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

type ApiKeyLookup = (hash: string) => Promise<ApiKey | null>;

declare module "express" {
  interface Request {
    apiKey?: Pick<
      ApiKey,
      "id" | "name" | "keyPrefix" | "scopes" | "createdAt"
    >;
  }
}

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
      res.status(401).json({
        error: "unauthorized",
        message:
          "Missing API key. Pass it in the X-API-Key header or as Authorization: Bearer <key>.",
      });
      return;
    }
    try {
      const row = await lookup(hashApiKey(plaintext));
      if (!row) {
        res.status(401).json({
          error: "unauthorized",
          message: "Invalid or revoked API key.",
        });
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
