/**
 * requireScope — per-route scope authorization.
 *
 * Factory that returns an Express middleware rejecting the request with
 * 403 `{error: "forbidden"}` unless the authenticated API key's `scopes`
 * array contains the given scope.
 *
 * Contract:
 *   - MUST run AFTER `apiKeyAuth`, which populates `req.apiKey.scopes`.
 *   - Is a NO-OP when `API_KEY_AUTH_ENABLED` is falsy (or when running in
 *     development), mirroring the bypass in `app.ts` so dev loops keep
 *     working without minted keys. The scopes check would never have
 *     anything to match against in that mode anyway.
 *   - Body shape is a generic `{error: "forbidden"}` to match the
 *     intentionally-opaque 401 body from `apiKeyAuth`. The specific reason
 *     ("no req.apiKey" vs "scope X not in [...]") is logged server-side.
 *
 * Usage (wire per-route, not app-wide):
 *   router.post("/admin/reset", requireScope("admin:write"), handler);
 */
import type { Request, Response, NextFunction, RequestHandler } from "express";

const FORBIDDEN_BODY = { error: "forbidden" };

type ForbiddenReason = "no-api-key" | "missing-scope";

function logForbidden(
  req: Request,
  reason: ForbiddenReason,
  scope: string,
  keyPrefix: string | null,
): void {
  // eslint-disable-next-line no-console
  console.warn("[require-scope] forbidden", {
    ip: req.ip,
    path: req.path,
    prefix: keyPrefix ?? "(none)",
    reason,
    scope,
  });
}

/**
 * Mirror of the auth-enabled test in `app.ts`. When false, `requireScope`
 * is a no-op — same DX as `apiKeyAuth` in development / bootstrap.
 *
 * Evaluated lazily on each request so tests can mutate
 * `process.env.API_KEY_AUTH_ENABLED` between cases without having to
 * re-import the module.
 */
function isAuthEnabled(): boolean {
  return (
    process.env.API_KEY_AUTH_ENABLED === "true" &&
    process.env.NODE_ENV !== "development"
  );
}

export function requireScope(scope: string): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!isAuthEnabled()) {
      next();
      return;
    }
    const apiKey = req.apiKey;
    if (!apiKey) {
      // Should never happen in production — apiKeyAuth would have 401'd
      // first. If it does, something is misconfigured (e.g. requireScope
      // mounted on a path apiKeyAuth doesn't cover). Fail closed.
      logForbidden(req, "no-api-key", scope, null);
      res.status(403).json(FORBIDDEN_BODY);
      return;
    }
    if (!apiKey.scopes.includes(scope)) {
      logForbidden(req, "missing-scope", scope, apiKey.keyPrefix);
      res.status(403).json(FORBIDDEN_BODY);
      return;
    }
    next();
  };
}
