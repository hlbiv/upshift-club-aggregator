/**
 * requireSuperAdmin — secondary guard for admin mutations that must be
 * gated to human `super_admin` operators (e.g. "Run now" on the scheduler).
 *
 * Runs AFTER `requireAdmin`, which populates `req.adminAuth`. Only admits
 * requests whose adminAuth is a session with role === 'super_admin'. API-key
 * callers (no session, no role) are rejected with 403 — this action is
 * human-gated by design (see PR #132 design doc + S.3 scope).
 *
 * 403 body: `{error: "super_admin required"}`. The refusal is logged
 * server-side with ip + adminId + route so operators have an audit trail
 * when a lesser admin tries to trigger a mutation.
 */
import type { Request, Response, NextFunction, RequestHandler } from "express";

const FORBIDDEN_BODY = { error: "super_admin required" };

function logRefusal(
  req: Request,
  detail: { kind: string; adminId: number | null },
): void {
  // eslint-disable-next-line no-console
  console.warn("[require-super-admin] refused", {
    ip: req.ip,
    path: req.path,
    kind: detail.kind,
    adminId: detail.adminId,
  });
}

export const requireSuperAdmin: RequestHandler = (
  req: Request,
  res: Response,
  next: NextFunction,
) => {
  const auth = req.adminAuth;
  if (!auth) {
    // Should not happen — the route chain must always mount this AFTER
    // requireAdmin. If adminAuth is missing we fail closed.
    logRefusal(req, { kind: "none", adminId: null });
    res.status(403).json(FORBIDDEN_BODY);
    return;
  }
  if (auth.kind === "session" && auth.role === "super_admin") {
    next();
    return;
  }
  logRefusal(req, {
    kind: auth.kind,
    adminId: auth.kind === "session" ? auth.userId : null,
  });
  res.status(403).json(FORBIDDEN_BODY);
};
