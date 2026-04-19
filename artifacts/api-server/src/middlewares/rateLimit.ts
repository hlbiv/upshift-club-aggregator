/**
 * rateLimit — per-API-key (or per-IP) rate limiting.
 *
 * Runs AFTER apiKeyAuth so we can key off `req.apiKey.id` for authenticated
 * calls (the common case). Unauthenticated routes — `/api/healthz` and
 * OPTIONS preflights — fall back to IP-based limiting to keep the public
 * surface bounded.
 *
 * Uses the in-memory store that ships with `express-rate-limit`. That's
 * fine for a single-instance deploy (current shape on Replit). If we ever
 * scale horizontally, swap in a Redis-backed store via the `store` option
 * — the contract of `buildRateLimiter` intentionally exposes overrides so
 * that migration is a one-line change in `app.ts`.
 *
 * Response shape on 429:
 *   { error: "rate_limited", retry_after_seconds: N }
 * The `RateLimit-*` draft-7 headers are also set by the library so clients
 * can back off preemptively instead of tripping 429s.
 */
import type { Request, RequestHandler } from "express";
import {
  rateLimit,
  ipKeyGenerator,
  type Options as RateLimitOptions,
} from "express-rate-limit";

export interface BuildRateLimiterOptions {
  /** Window size in milliseconds. Default 60_000 (one minute). */
  windowMs?: number;
  /** Max requests per authenticated API key per window. Default 100. */
  authLimit?: number;
  /** Max requests per IP per window when no API key is present. Default 20. */
  ipLimit?: number;
  /**
   * Extra options passed straight through to `express-rate-limit`. Useful
   * in tests to inject a fake store or to disable headers.
   */
  passthrough?: Partial<RateLimitOptions>;
}

/**
 * Build a middleware that enforces two independent buckets per window:
 *  - one bucket per authenticated key (larger), keyed by `req.apiKey.id`
 *  - one bucket per IP (smaller), keyed by `req.ip`
 *
 * express-rate-limit takes a single `max` + `keyGenerator` + `skip` triple,
 * so we run TWO middleware instances in sequence via a tiny wrapper: the
 * key-limiter skips unauthenticated requests, and the IP-limiter skips
 * authenticated ones. Both share a `handler` that emits our error shape.
 */
export function buildRateLimiter(
  opts: BuildRateLimiterOptions = {},
): RequestHandler {
  const windowMs = opts.windowMs ?? 60_000;
  const authLimit = opts.authLimit ?? 100;
  const ipLimit = opts.ipLimit ?? 20;

  function hasAuthKey(req: Request): boolean {
    return typeof req.apiKey?.id === "number";
  }

  // Shared 429 body. `retry_after_seconds` is derived from the library's
  // own rate-limit headers so we stay consistent with what the client saw.
  const limitedHandler: NonNullable<RateLimitOptions["handler"]> = (
    req,
    res,
  ) => {
    const resetHeader = res.getHeader("RateLimit-Reset");
    let retryAfterSeconds: number;
    if (typeof resetHeader === "string" || typeof resetHeader === "number") {
      const parsed = Number(resetHeader);
      retryAfterSeconds = Number.isFinite(parsed) && parsed > 0
        ? Math.ceil(parsed)
        : Math.ceil(windowMs / 1000);
    } else {
      retryAfterSeconds = Math.ceil(windowMs / 1000);
    }
    res.status(429).json({
      error: "rate_limited",
      retry_after_seconds: retryAfterSeconds,
    });
  };

  const keyLimiter = rateLimit({
    windowMs,
    limit: authLimit,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    keyGenerator: (req: Request) => `key:${req.apiKey!.id}`,
    skip: (req: Request) => !hasAuthKey(req),
    handler: limitedHandler,
    ...opts.passthrough,
  });

  const ipLimiter = rateLimit({
    windowMs,
    limit: ipLimit,
    standardHeaders: "draft-7",
    legacyHeaders: false,
    // `ipKeyGenerator` handles IPv6 subnet folding per the library's
    // recommendation (see express-rate-limit docs on IPv6 hazards).
    keyGenerator: (req: Request) => `ip:${ipKeyGenerator(req.ip ?? "")}`,
    skip: (req: Request) => hasAuthKey(req),
    handler: limitedHandler,
    ...opts.passthrough,
  });

  return (req, res, next) => {
    if (hasAuthKey(req)) {
      keyLimiter(req, res, next);
    } else {
      ipLimiter(req, res, next);
    }
  };
}

/** Default instance with production defaults: 100/min/key, 20/min/ip. */
export const rateLimitMiddleware: RequestHandler = buildRateLimiter();
