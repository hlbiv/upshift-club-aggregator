/**
 * In-process scheduler worker — dequeues rows from `scheduler_jobs` and
 * shells out to `python3 scraper/run.py` with admin-supplied CLI flags.
 *
 * Design doc: docs/design/scheduler-queue-decision.md (PR #132).
 * Schema: lib/db/src/schema/scheduler-jobs.ts (PR #139).
 *
 * Runs inside the api-server Node process (no separate deploy) — see S.1
 * decision on 2026-04-20. One interval ticks every `POLL_INTERVAL_MS`.
 * Each tick:
 *
 *   BEGIN;
 *   SELECT id, job_key, args
 *     FROM scheduler_jobs
 *    WHERE status = 'pending'
 *    ORDER BY requested_at ASC
 *    LIMIT 1
 *    FOR UPDATE SKIP LOCKED;
 *   -- if a row was found:
 *   UPDATE scheduler_jobs
 *      SET status = 'running', started_at = now()
 *    WHERE id = $1;
 *   COMMIT;
 *
 * The FOR UPDATE SKIP LOCKED claim guarantees two workers (e.g. two api-
 * server replicas) never pick the same row. If no row is found the tx is
 * released immediately and the worker waits for the next tick.
 *
 * Hard timeout: 30 minutes per job. If exceeded, the child is SIGKILLed
 * and the row is marked `failed` with stderr noting the timeout.
 *
 * Crash semantics: if the worker process dies (deploy, OOM, crash) while a
 * job is at status='running', no process owns the row anymore. On the next
 * `startSchedulerWorker()` call, a one-shot reconciliation sweep runs
 * before the poll loop starts: rows still at status='running' whose
 * `started_at` is older than `JOB_TIMEOUT_MS` (30min — longer than any job
 * the worker would actually allow) are flipped to 'failed' with
 * `stderr_tail = 'orphaned (worker restart)'` and `exit_code = -1`. The
 * threshold guarantees a legitimately in-flight job on a sibling replica
 * is never reconciled out from under its owner. Errors in the sweep are
 * logged but do not block worker startup.
 *
 * This module is wired into app.ts by S.3 — until then it's dead code.
 */
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";

/** Poll interval between claim attempts. */
export const POLL_INTERVAL_MS = 5_000;

/** Hard kill a child process after this many ms. */
export const JOB_TIMEOUT_MS = 30 * 60 * 1000; // 30 minutes

/** Cap on stdout_tail / stderr_tail line count persisted to the DB. */
export const TAIL_LINES = 50;

/**
 * Minimal row shape returned by the claim query. Narrow by design — the
 * worker only needs the id, job_key, and args to shell out.
 */
export interface ClaimedJob {
  id: number;
  jobKey: string;
  args: Record<string, unknown> | null;
}

/**
 * DB surface the worker needs. A thin interface (rather than taking a
 * NodePgDatabase directly) so tests can inject a fake without mocking
 * Drizzle. Production wiring passes a small adapter in S.3.
 */
export interface WorkerDb {
  /**
   * Atomically claim one pending row: open a tx, SELECT ... FOR UPDATE
   * SKIP LOCKED LIMIT 1, flip status=running + started_at=now(), commit.
   * Returns null if the queue is empty (tx still released).
   */
  claimNextJob: () => Promise<ClaimedJob | null>;
  /** Mark a job terminal (`success` or `failed`) with tails + exit code. */
  finishJob: (id: number, result: JobResult) => Promise<void>;
  /**
   * One-shot sweep run at worker startup: flip any row still at
   * `status='running'` whose `started_at` is older than `olderThanMs`
   * into `status='failed'` with `stderr_tail = 'orphaned (worker
   * restart)'` and `exit_code = -1`. Returns the number of rows updated.
   *
   * Invariant: `olderThanMs` MUST be >= the worker's hard job timeout,
   * so any job legitimately running on a sibling replica (or the
   * current process, in a re-entry edge case) is never swept.
   */
  reconcileOrphanedJobs: (olderThanMs: number) => Promise<number>;
}

export interface JobResult {
  status: "success" | "failed";
  exitCode: number | null;
  stdoutTail: string;
  stderrTail: string;
}

/**
 * Spawn surface — tests inject a fake that returns a stub child process.
 * Production passes `child_process.spawn`.
 */
export type SpawnFn = (
  command: string,
  args: readonly string[],
) => ChildProcessWithoutNullStreams;

export interface StartSchedulerWorkerDeps {
  db: WorkerDb;
  /** Override for tests; defaults to node:child_process `spawn`. */
  spawnFn?: SpawnFn;
  /** Override for tests; defaults to POLL_INTERVAL_MS. */
  pollIntervalMs?: number;
  /** Override for tests; defaults to JOB_TIMEOUT_MS. */
  jobTimeoutMs?: number;
  /** Override for tests; used to simulate timeouts without real time passing. */
  now?: () => number;
}

let intervalHandle: ReturnType<typeof setInterval> | null = null;
let tickInFlight = false;

/**
 * Convert `{ 'event-id': '123', 'dry-run': true }` to
 * `['--event-id', '123', '--dry-run']`.
 *
 * Rules:
 *  - Boolean true  → presence flag (no value).
 *  - Boolean false → flag is omitted (no negative form).
 *  - null / undefined → flag omitted.
 *  - All other values stringified via String().
 */
export function argsToFlags(args: Record<string, unknown> | null | undefined): string[] {
  if (!args) return [];
  const out: string[] = [];
  for (const [key, value] of Object.entries(args)) {
    if (value === false || value == null) continue;
    const flag = `--${key}`;
    if (value === true) {
      out.push(flag);
    } else {
      out.push(flag, String(value));
    }
  }
  return out;
}

/**
 * Return the last `n` newline-delimited lines of `s`. Trailing newline is
 * preserved implicitly by split + slice + join — if the source ends in a
 * newline the result will too.
 *
 * Edge cases:
 *  - `n <= 0` → "".
 *  - Input shorter than `n` lines → return input unchanged.
 *  - Empty input → "".
 */
export function tailLines(s: string, n: number): string {
  if (!s) return "";
  if (n <= 0) return "";
  const lines = s.split("\n");
  if (lines.length <= n) return s;
  return lines.slice(-n).join("\n");
}

/**
 * Execute one claimed job: spawn python3, capture stdout/stderr, apply the
 * timeout, await exit. Returns the JobResult ready for `finishJob`.
 *
 * Separated from the tick loop so tests can drive it directly without
 * setting up a poller.
 */
export function executeJob(
  job: ClaimedJob,
  deps: {
    spawnFn: SpawnFn;
    jobTimeoutMs: number;
  },
): Promise<JobResult> {
  return new Promise<JobResult>((resolve) => {
    const flags = argsToFlags(job.args);
    const child = deps.spawnFn("python3", [
      "scraper/run.py",
      "--source",
      job.jobKey,
      ...flags,
    ]);
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let settled = false;

    const timer = setTimeout(() => {
      timedOut = true;
      try {
        child.kill("SIGKILL");
      } catch {
        // child already exited — nothing to do.
      }
    }, deps.jobTimeoutMs);

    child.stdout.on("data", (chunk: Buffer | string) => {
      stdout += typeof chunk === "string" ? chunk : chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk: Buffer | string) => {
      stderr += typeof chunk === "string" ? chunk : chunk.toString("utf8");
    });

    const settle = (result: JobResult) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(result);
    };

    child.on("error", (err) => {
      // Spawn error (e.g. python3 not on PATH) — record and fail.
      const note = `[scheduler] spawn error: ${err.message}\n`;
      settle({
        status: "failed",
        exitCode: null,
        stdoutTail: tailLines(stdout, TAIL_LINES),
        stderrTail: tailLines(stderr + note, TAIL_LINES),
      });
    });

    child.on("close", (code) => {
      if (timedOut) {
        const note = `[scheduler] killed after ${deps.jobTimeoutMs}ms timeout\n`;
        settle({
          status: "failed",
          exitCode: code,
          stdoutTail: tailLines(stdout, TAIL_LINES),
          stderrTail: tailLines(stderr + note, TAIL_LINES),
        });
        return;
      }
      settle({
        status: code === 0 ? "success" : "failed",
        exitCode: code,
        stdoutTail: tailLines(stdout, TAIL_LINES),
        stderrTail: tailLines(stderr, TAIL_LINES),
      });
    });
  });
}

/**
 * Start the polling worker. Idempotent — calling twice is a no-op (the
 * second call logs and returns).
 */
export function startSchedulerWorker(deps: StartSchedulerWorkerDeps): void {
  if (intervalHandle !== null) {
    console.warn("[scheduler] worker already started; ignoring second start call");
    return;
  }
  const spawnFn = deps.spawnFn ?? (spawn as unknown as SpawnFn);
  const pollIntervalMs = deps.pollIntervalMs ?? POLL_INTERVAL_MS;
  const jobTimeoutMs = deps.jobTimeoutMs ?? JOB_TIMEOUT_MS;

  // One-shot orphan sweep. Any row still at status='running' whose
  // started_at predates the hard job timeout is definitionally abandoned
  // — a live worker would have killed + marked it failed by now. The
  // sweep must not block startup: if it throws (e.g. transient DB
  // outage), log and continue. The poll loop itself will retry the DB on
  // its next tick.
  void (async () => {
    try {
      const swept = await deps.db.reconcileOrphanedJobs(jobTimeoutMs);
      if (swept > 0) {
        console.log(
          `[scheduler] reconciled ${swept} orphaned job${swept === 1 ? "" : "s"} on startup`,
        );
      }
    } catch (err) {
      console.error("[scheduler] orphan reconciliation failed on startup:", err);
    }
  })();

  const tick = async () => {
    if (tickInFlight) return; // single-flight — don't overlap ticks.
    tickInFlight = true;
    try {
      const job = await deps.db.claimNextJob();
      if (!job) return;
      const result = await executeJob(job, { spawnFn, jobTimeoutMs });
      await deps.db.finishJob(job.id, result);
    } catch (err) {
      // Never let a tick throw out of the interval — log and continue.
      console.error("[scheduler] tick failed:", err);
    } finally {
      tickInFlight = false;
    }
  };

  intervalHandle = setInterval(() => {
    void tick();
  }, pollIntervalMs);
  // Allow the process to exit even if the worker is idle.
  if (typeof intervalHandle === "object" && intervalHandle !== null) {
    (intervalHandle as unknown as { unref?: () => void }).unref?.();
  }
  console.log(
    `[scheduler] worker started (poll=${pollIntervalMs}ms, timeout=${jobTimeoutMs}ms)`,
  );
}

/** Stop the worker. Safe to call when not started. */
export function stopSchedulerWorker(): void {
  if (intervalHandle !== null) {
    clearInterval(intervalHandle);
    intervalHandle = null;
  }
  tickInFlight = false;
}

/**
 * Production factory for `WorkerDb` backed by NodePgDatabase. Kept here
 * (rather than at the call site in app.ts) so both the wiring PR and
 * future refactors have one spot to touch.
 *
 * Uses raw SQL via the underlying pg Pool because:
 *   - Drizzle's ORM surface doesn't emit `FOR UPDATE SKIP LOCKED`.
 *   - The claim has to happen inside a single transaction to be correct.
 */
export function makeWorkerDb(db: NodePgDatabase<Record<string, never>>): WorkerDb {
  // Drizzle's NodePgDatabase holds the underlying pool on an internal
  // symbol; the typed `.$client` accessor exposes it at runtime. We use
  // it to open a dedicated client for the claim transaction — pool
  // connections cannot span multiple statements safely without one.
  //
  // The S.3 PR will wire this up; meanwhile keeping it here keeps the
  // worker surface tidy and testable.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const client = (db as any).$client;
  return {
    async claimNextJob() {
      const conn = await client.connect();
      try {
        await conn.query("BEGIN");
        const claim = await conn.query(
          `SELECT id, job_key, args
             FROM scheduler_jobs
            WHERE status = 'pending'
            ORDER BY requested_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED`,
        );
        if (claim.rows.length === 0) {
          await conn.query("COMMIT");
          return null;
        }
        const row = claim.rows[0] as {
          id: number;
          job_key: string;
          args: Record<string, unknown> | null;
        };
        await conn.query(
          `UPDATE scheduler_jobs
              SET status = 'running',
                  started_at = now()
            WHERE id = $1`,
          [row.id],
        );
        await conn.query("COMMIT");
        return {
          id: row.id,
          jobKey: row.job_key,
          args: row.args,
        };
      } catch (err) {
        try {
          await conn.query("ROLLBACK");
        } catch {
          // ignore
        }
        throw err;
      } finally {
        conn.release();
      }
    },
    async finishJob(id, result) {
      await client.query(
        `UPDATE scheduler_jobs
            SET status = $2,
                completed_at = now(),
                exit_code = $3,
                stdout_tail = $4,
                stderr_tail = $5
          WHERE id = $1`,
        [id, result.status, result.exitCode, result.stdoutTail, result.stderrTail],
      );
    },
    async reconcileOrphanedJobs(olderThanMs) {
      // Parameterize the interval argument rather than interpolating
      // into the SQL string — `olderThanMs` is a number (not user
      // input today) but keeping the boundary clean is cheap insurance.
      // Postgres accepts `($1 || ' milliseconds')::interval` against a
      // numeric parameter.
      const result = await client.query(
        `UPDATE scheduler_jobs
            SET status = 'failed',
                completed_at = NOW(),
                stderr_tail = 'orphaned (worker restart)',
                exit_code = -1
          WHERE status = 'running'
            AND started_at < NOW() - ($1 || ' milliseconds')::interval`,
        [olderThanMs],
      );
      return result.rowCount ?? 0;
    },
  };
}
