/**
 * `/api/v1/admin` — scheduler routes (S.3).
 *
 * Three endpoints, mounted under two sub-paths by index.ts:
 *
 *   POST /scraper-schedules/:jobKey/run         — enqueue a "Run now"
 *     guarded by requireSuperAdmin (human-gated mutation). Validates the
 *     body against `RunNowRequest`, enforces the jobKey allow-list, and
 *     inserts a `scheduler_jobs` row at status='pending'. The in-process
 *     worker (see scheduler/worker.ts) picks it up on its next tick.
 *
 *   GET  /scraper-schedules/:jobKey/runs?limit= — last N rows for that
 *     jobKey, ordered by requested_at DESC. Default limit 20, cap 100.
 *     Open to any admin.
 *
 *   GET  /scheduler-jobs/:id                    — single row. 404 if
 *     missing. Open to any admin.
 *
 * Rate limiting: the mutation is tagged at 30/min (mutation tier) and the
 * reads at 120/min (read tier) by the individual route middlewares — see
 * index.ts for the wiring.
 *
 * jobKey allow-list is hard-coded rather than DB-driven because the three
 * keys are load-bearing in run.py's `--source` dispatch. Opening this to
 * arbitrary strings would let an operator paste any path into the shell-out
 * arg list and is explicitly out of scope for S.3.
 *
 * Factory pattern (same as admin/dedup.ts, admin/growth.ts): handlers
 * consume a `SchedulerDeps` so tests can drive them with an in-memory
 * fake — no Postgres, no DB pool, no mocks of Drizzle.
 */
import {
  Router,
  type IRouter,
  type Request,
  type Response,
  type NextFunction,
  type RequestHandler,
} from "express";
import { and, desc, eq } from "drizzle-orm";
import { db, schedulerJobs, type SchedulerJob as SchedulerJobRow } from "@workspace/db";
import {
  RunNowRequest,
  RunNowResponse,
  SchedulerJob,
  SchedulerJobList,
  ScraperSchedulesResponse,
} from "@hlbiv/api-zod/admin";
import { requireSuperAdmin } from "../../middlewares/requireSuperAdmin";
import { buildRateLimiter } from "../../middlewares/rateLimit";

/**
 * Hard-coded allow-list of jobKeys accepted by the "Run now" route.
 * Synchronized with the three scheduler entry points currently wired in
 * run.py's `--source` dispatch. Expanding this list is a deliberate,
 * code-review-gated change — do not open it to arbitrary strings.
 */
export const ALLOWED_JOB_KEYS = [
  "nightly_tier1",
  "weekly_state",
  "hourly_linker",
  "ga_matches",
] as const;
export type AllowedJobKey = (typeof ALLOWED_JOB_KEYS)[number];

function isAllowedJobKey(value: string): value is AllowedJobKey {
  return (ALLOWED_JOB_KEYS as readonly string[]).includes(value);
}

/**
 * Server-owned metadata for each allow-listed jobKey. The dashboard reads
 * this via GET /scraper-schedules so descriptions + cron hints are managed
 * server-side (one source of truth) instead of duplicated in React.
 *
 * `cronExpression` is a display-only string — it doesn't drive actual
 * scheduling. Cron jobs live in `.replit` as Replit Scheduled Deployments.
 * The three jobKeys here currently have no fixed cron (Run Now only), so
 * `cronExpression` is null for all three.
 */
export const JOB_METADATA: ReadonlyArray<{
  readonly jobKey: AllowedJobKey;
  readonly description: string;
  readonly cronExpression: string | null;
}> = [
  {
    jobKey: "nightly_tier1",
    description: "Nightly Tier 1 league scraper",
    cronExpression: null,
  },
  {
    jobKey: "weekly_state",
    description: "Weekly state associations sweep",
    cronExpression: null,
  },
  {
    jobKey: "hourly_linker",
    description: "Hourly canonical-club linker",
    cronExpression: null,
  },
  {
    jobKey: "ga_matches",
    description: "Girls Academy matches (GotSport event 42137) — requires GOTSPORT_SESSION_COOKIE in Replit Secrets",
    cronExpression: null,
  },
] as const;

/** Default limit for the runs-list endpoint. */
export const DEFAULT_RUNS_LIMIT = 20;
/** Upper bound on the `?limit=` query param. */
export const MAX_RUNS_LIMIT = 100;
/** Default recent-runs-per-schedule on GET /scraper-schedules. */
export const DEFAULT_SCHEDULES_RUN_LIMIT = 10;

/**
 * Minimal row shape the handlers rely on. Matches `typeof schedulerJobs.$inferSelect`
 * but listed explicitly so tests can construct fixtures without importing Drizzle.
 */
export type SchedulerJobDepRow = SchedulerJobRow;

export interface SchedulerDeps {
  /** Insert a new row at status='pending'. Returns the inserted row. */
  enqueueJob: (input: {
    jobKey: AllowedJobKey;
    args: Record<string, unknown> | null;
    requestedBy: number;
  }) => Promise<SchedulerJobDepRow>;
  /** Fetch one row by id, or null if missing. */
  getJobById: (id: number) => Promise<SchedulerJobDepRow | null>;
  /** Fetch the last N rows for a given jobKey, ordered by requested_at DESC. */
  listJobsByKey: (input: {
    jobKey: string;
    limit: number;
  }) => Promise<SchedulerJobDepRow[]>;
}

// ---------------------------------------------------------------------------
// Production deps — bind the DB-backed impl.
// ---------------------------------------------------------------------------

export const prodSchedulerDeps: SchedulerDeps = {
  async enqueueJob({ jobKey, args, requestedBy }) {
    const [row] = await db
      .insert(schedulerJobs)
      .values({
        jobKey,
        args,
        status: "pending",
        requestedBy,
      })
      .returning();
    if (!row) {
      // `returning()` on a single-row insert always yields one row, but the
      // type is an array so narrow for the compiler.
      throw new Error("scheduler_jobs insert returned no row");
    }
    return row;
  },
  async getJobById(id) {
    const [row] = await db
      .select()
      .from(schedulerJobs)
      .where(eq(schedulerJobs.id, id))
      .limit(1);
    return row ?? null;
  },
  async listJobsByKey({ jobKey, limit }) {
    return db
      .select()
      .from(schedulerJobs)
      .where(and(eq(schedulerJobs.jobKey, jobKey)))
      .orderBy(desc(schedulerJobs.requestedAt))
      .limit(limit);
  },
};

// ---------------------------------------------------------------------------
// Serializers — DB row → contract Zod shape.
// ---------------------------------------------------------------------------

function serializeJob(row: SchedulerJobDepRow) {
  return SchedulerJob.parse({
    id: row.id,
    jobKey: row.jobKey,
    args: (row.args ?? null) as Record<string, unknown> | null,
    status: row.status as
      | "pending"
      | "running"
      | "success"
      | "failed"
      | "canceled",
    requestedBy: row.requestedBy ?? null,
    requestedAt: row.requestedAt.toISOString(),
    startedAt: row.startedAt ? row.startedAt.toISOString() : null,
    completedAt: row.completedAt ? row.completedAt.toISOString() : null,
    exitCode: row.exitCode ?? null,
    stdoutTail: row.stdoutTail ?? null,
    stderrTail: row.stderrTail ?? null,
  });
}

// ---------------------------------------------------------------------------
// Handler factories.
// ---------------------------------------------------------------------------

/**
 * POST /scraper-schedules/:jobKey/run — guarded by requireSuperAdmin upstream.
 */
export function makeRunNowHandler(deps: SchedulerDeps): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const jobKeyRaw = req.params.jobKey;
      const jobKey = typeof jobKeyRaw === "string" ? jobKeyRaw : "";
      if (!jobKey || !isAllowedJobKey(jobKey)) {
        res.status(400).json({ error: "unknown jobKey" });
        return;
      }

      // Body is optional — RunNowRequest's `jobKey` lives in the path;
      // the body only carries `args`. We still parse the shape for safety:
      // any `jobKey` passed in the body must agree with the path.
      const body = req.body ?? {};
      const parsed = RunNowRequest.safeParse({
        jobKey,
        args: body.args,
      });
      if (!parsed.success) {
        res.status(400).json({
          error: "invalid body",
          issues: parsed.error.issues,
        });
        return;
      }
      if (body.jobKey && body.jobKey !== jobKey) {
        res.status(400).json({ error: "jobKey mismatch" });
        return;
      }

      // requireSuperAdmin ensures adminAuth.kind === 'session'. Narrow for
      // the compiler.
      const auth = req.adminAuth;
      if (!auth || auth.kind !== "session") {
        // Defense-in-depth — should be unreachable.
        res.status(403).json({ error: "super_admin required" });
        return;
      }

      const row = await deps.enqueueJob({
        jobKey,
        args: (parsed.data.args as Record<string, unknown> | undefined) ?? null,
        requestedBy: auth.userId,
      });

      res.status(201).json(
        RunNowResponse.parse({
          id: row.id,
          jobKey: row.jobKey,
          status: "pending",
          requestedAt: row.requestedAt.toISOString(),
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

/**
 * GET /scheduler-jobs/:id — open to any admin.
 */
export function makeGetJobHandler(deps: SchedulerDeps): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const id = Number(req.params.id);
      if (!Number.isFinite(id) || !Number.isInteger(id) || id <= 0) {
        res.status(400).json({ error: "invalid id" });
        return;
      }
      const row = await deps.getJobById(id);
      if (!row) {
        res.status(404).json({ error: "scheduler_job not found" });
        return;
      }
      res.json(serializeJob(row));
    } catch (err) {
      next(err);
    }
  };
}

/**
 * GET /scraper-schedules — open to any admin.
 *
 * Returns every allow-listed jobKey's metadata + its last N runs in a
 * single payload. Drives the dynamic Scheduler page.
 */
export function makeListSchedulesHandler(
  deps: SchedulerDeps,
): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const rawLimit = req.query.limit;
      let limit = DEFAULT_SCHEDULES_RUN_LIMIT;
      if (typeof rawLimit === "string" && rawLimit.length > 0) {
        const parsed = Number(rawLimit);
        if (
          !Number.isFinite(parsed) ||
          !Number.isInteger(parsed) ||
          parsed <= 0
        ) {
          res.status(400).json({ error: "invalid limit" });
          return;
        }
        limit = Math.min(parsed, MAX_RUNS_LIMIT);
      }

      const schedules = await Promise.all(
        JOB_METADATA.map(async (meta) => {
          const rows = await deps.listJobsByKey({
            jobKey: meta.jobKey,
            limit,
          });
          return {
            jobKey: meta.jobKey,
            description: meta.description,
            cronExpression: meta.cronExpression,
            recentRuns: rows.map(serializeJob),
          };
        }),
      );

      res.json(ScraperSchedulesResponse.parse({ schedules }));
    } catch (err) {
      next(err);
    }
  };
}

/**
 * GET /scraper-schedules/:jobKey/runs — open to any admin.
 */
export function makeListRunsHandler(deps: SchedulerDeps): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    try {
      const jobKeyRaw = req.params.jobKey;
      const jobKey = typeof jobKeyRaw === "string" ? jobKeyRaw : "";
      if (!jobKey || !isAllowedJobKey(jobKey)) {
        res.status(400).json({ error: "unknown jobKey" });
        return;
      }

      const rawLimit = req.query.limit;
      let limit = DEFAULT_RUNS_LIMIT;
      if (typeof rawLimit === "string" && rawLimit.length > 0) {
        const parsed = Number(rawLimit);
        if (
          !Number.isFinite(parsed) ||
          !Number.isInteger(parsed) ||
          parsed <= 0
        ) {
          res.status(400).json({ error: "invalid limit" });
          return;
        }
        limit = Math.min(parsed, MAX_RUNS_LIMIT);
      }

      const rows = await deps.listJobsByKey({ jobKey, limit });
      res.json(
        SchedulerJobList.parse({
          jobs: rows.map(serializeJob),
          total: rows.length,
        }),
      );
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Router construction — production wiring.
// ---------------------------------------------------------------------------

/**
 * Sub-router mounted at `/scraper-schedules`:
 *   GET  /             (any admin + 120/min)  — list all schedules
 *   POST /:jobKey/run  (super_admin + 30/min) — enqueue a one-off run
 *   GET  /:jobKey/runs (any admin + 120/min)  — last N runs for one jobKey
 */
export function buildScraperSchedulesRouter(
  deps: SchedulerDeps = prodSchedulerDeps,
): IRouter {
  const router: IRouter = Router();
  router.get(
    "/",
    buildRateLimiter({ authLimit: 120, ipLimit: 120 }),
    makeListSchedulesHandler(deps),
  );
  router.post(
    "/:jobKey/run",
    buildRateLimiter({ authLimit: 30, ipLimit: 30 }),
    requireSuperAdmin,
    makeRunNowHandler(deps),
  );
  router.get(
    "/:jobKey/runs",
    buildRateLimiter({ authLimit: 120, ipLimit: 120 }),
    makeListRunsHandler(deps),
  );
  return router;
}

/**
 * Sub-router mounted at `/scheduler-jobs`:
 *   GET /:id   (any admin + 120/min)
 */
export function buildSchedulerJobsRouter(
  deps: SchedulerDeps = prodSchedulerDeps,
): IRouter {
  const router: IRouter = Router();
  router.get(
    "/:id",
    buildRateLimiter({ authLimit: 120, ipLimit: 120 }),
    makeGetJobHandler(deps),
  );
  return router;
}
