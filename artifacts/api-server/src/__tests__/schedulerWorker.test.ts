/**
 * Scheduler worker — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/schedulerWorker.test.ts
 *
 * Exercises the worker logic with fake DB + fake spawn. No child processes
 * are launched, no interval is left running, and the DB is never touched.
 *
 * Scenarios:
 *  1. `tailLines` — happy path + short input + edge cases.
 *  2. `argsToFlags` — presence flags, false/null skipped, scalars stringified.
 *  3. Full cycle: 1 pending row + spawn exits 0 → status becomes `success`.
 *  4. Full cycle: spawn exits 1 → status becomes `failed`, stderr captured.
 *  5. Empty queue → no spawn, worker loops quietly.
 *  6. Timeout path: spawn never exits → killed + marked `failed`.
 *  7. Startup sweep — reconcileOrphanedJobs returns 3, count is logged
 *     and the poll loop still runs.
 *  8. Startup sweep — reconcileOrphanedJobs returns 0, no log emitted
 *     and the poll loop still runs.
 *  9. Startup sweep — reconcileOrphanedJobs throws, error is logged
 *     and the poll loop still runs (error is not propagated).
 */
import { EventEmitter } from "node:events";
import {
  tailLines,
  argsToFlags,
  executeJob,
  startSchedulerWorker,
  stopSchedulerWorker,
  type ClaimedJob,
  type JobResult,
  type SpawnFn,
  type WorkerDb,
} from "../scheduler/worker";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

function eq<T>(actual: T, expected: T, name: string, label: string) {
  if (actual !== expected) {
    failures.push({
      name,
      issue: `${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    });
  }
}

/**
 * Fake ChildProcess that exposes stdout/stderr as EventEmitters and lets
 * the test drive exit timing via `fakeExit(code)` or `fakeError(err)`.
 */
class FakeChild extends EventEmitter {
  stdout = new EventEmitter();
  stderr = new EventEmitter();
  killed = false;
  lastSignal: string | undefined;
  kill(signal?: string) {
    this.killed = true;
    this.lastSignal = signal;
    // Mirror real child-process semantics: SIGKILL surfaces via `close`
    // with a null exit code. We emit synchronously so the worker's timeout
    // branch can settle before the test assertion.
    queueMicrotask(() => this.emit("close", null));
    return true;
  }
  fakeStdout(chunk: string) {
    this.stdout.emit("data", chunk);
  }
  fakeStderr(chunk: string) {
    this.stderr.emit("data", chunk);
  }
  fakeExit(code: number | null) {
    this.emit("close", code);
  }
  fakeError(err: Error) {
    this.emit("error", err);
  }
}

function makeFakeSpawn(onSpawn: (child: FakeChild, command: string, args: readonly string[]) => void): {
  spawnFn: SpawnFn;
  calls: Array<{ command: string; args: readonly string[] }>;
} {
  const calls: Array<{ command: string; args: readonly string[] }> = [];
  const spawnFn: SpawnFn = (command, args) => {
    calls.push({ command, args });
    const child = new FakeChild();
    // Let the worker's listeners attach before any emit fires.
    queueMicrotask(() => onSpawn(child, command, args));
    // Cast to the real spawn return type — our FakeChild implements the
    // subset the worker actually uses (stdout/stderr/on/kill).
    return child as unknown as ReturnType<SpawnFn>;
  };
  return { spawnFn, calls };
}

async function run() {
  // --- 1. tailLines — happy path + short input + edge cases ---
  {
    eq(tailLines("a\nb\nc\nd\ne", 2), "d\ne", "tailLines-happy", "last 2 of 5");
    eq(tailLines("a\nb", 5), "a\nb", "tailLines-short", "input < n returns input");
    eq(tailLines("", 10), "", "tailLines-empty", "empty input → empty");
    eq(tailLines("one-line", 3), "one-line", "tailLines-single-line", "no newlines");
    eq(tailLines("a\nb\nc", 0), "", "tailLines-zero", "n=0 → empty");
    eq(tailLines("a\nb\nc", -1), "", "tailLines-negative", "negative n → empty");
    eq(tailLines("a\nb\nc\n", 2), "c\n", "tailLines-trailing-newline", "trailing newline preserved");
  }

  // --- 2. argsToFlags ---
  {
    const flags = argsToFlags({ "event-id": "123", "dry-run": true, verbose: false, nope: null });
    eq(
      JSON.stringify(flags),
      JSON.stringify(["--event-id", "123", "--dry-run"]),
      "argsToFlags-mixed",
      "string value + bool true + bool false + null",
    );
    eq(
      JSON.stringify(argsToFlags(null)),
      JSON.stringify([]),
      "argsToFlags-null",
      "null args → []",
    );
    eq(
      JSON.stringify(argsToFlags({ limit: 5 })),
      JSON.stringify(["--limit", "5"]),
      "argsToFlags-number",
      "number stringified",
    );
  }

  // --- 3. Full cycle: exit 0 → success ---
  {
    const pending: ClaimedJob = { id: 7, jobKey: "sincsports-events", args: { "dry-run": true } };
    let claimCalls = 0;
    const finishCalls: Array<{ id: number; result: JobResult }> = [];
    const db: WorkerDb = {
      async claimNextJob() {
        claimCalls += 1;
        return claimCalls === 1 ? pending : null;
      },
      async finishJob(id, result) {
        finishCalls.push({ id, result });
      },
      async reconcileOrphanedJobs() {
        return 0;
      },
    };
    const { spawnFn, calls } = makeFakeSpawn((child) => {
      child.fakeStdout("line1\nline2\n");
      child.fakeExit(0);
    });
    startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
    // Wait for the tick → spawn → exit → finishJob chain to resolve.
    await new Promise((r) => setTimeout(r, 40));
    stopSchedulerWorker();
    assert(calls.length >= 1, "success-spawn", "spawn should have been called");
    eq(calls[0]?.command, "python3", "success-spawn", "command is python3");
    assert(
      calls[0]?.args[0] === "scraper/run.py" &&
        calls[0]?.args[1] === "--source" &&
        calls[0]?.args[2] === "sincsports-events" &&
        calls[0]?.args[3] === "--dry-run",
      "success-spawn-args",
      `unexpected args: ${JSON.stringify(calls[0]?.args)}`,
    );
    assert(finishCalls.length === 1, "success-finish", `finishJob called ${finishCalls.length}x`);
    eq(finishCalls[0]?.id, 7, "success-finish", "job id propagated");
    eq(finishCalls[0]?.result.status, "success", "success-finish", "status should be success");
    eq(finishCalls[0]?.result.exitCode, 0, "success-finish", "exit code 0");
    assert(
      (finishCalls[0]?.result.stdoutTail ?? "").includes("line1"),
      "success-finish",
      "stdout_tail should include captured stdout",
    );
  }

  // --- 4. Exit 1 → failed, stderr captured ---
  {
    const pending: ClaimedJob = { id: 9, jobKey: "tryouts-wordpress", args: null };
    let claimed = false;
    const finishCalls: Array<{ id: number; result: JobResult }> = [];
    const db: WorkerDb = {
      async claimNextJob() {
        if (claimed) return null;
        claimed = true;
        return pending;
      },
      async finishJob(id, result) {
        finishCalls.push({ id, result });
      },
      async reconcileOrphanedJobs() {
        return 0;
      },
    };
    const { spawnFn } = makeFakeSpawn((child) => {
      child.fakeStderr("boom: something failed\n");
      child.fakeExit(1);
    });
    startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
    await new Promise((r) => setTimeout(r, 40));
    stopSchedulerWorker();
    assert(finishCalls.length === 1, "fail-finish", `finishJob called ${finishCalls.length}x`);
    eq(finishCalls[0]?.result.status, "failed", "fail-finish", "status should be failed");
    eq(finishCalls[0]?.result.exitCode, 1, "fail-finish", "exit code 1");
    assert(
      (finishCalls[0]?.result.stderrTail ?? "").includes("boom"),
      "fail-finish",
      "stderr_tail should include captured stderr",
    );
  }

  // --- 5. Empty queue → no spawn, worker loops quietly ---
  {
    let claimCalls = 0;
    const db: WorkerDb = {
      async claimNextJob() {
        claimCalls += 1;
        return null;
      },
      async finishJob() {
        failures.push({ name: "empty-finish", issue: "finishJob should not run on empty queue" });
      },
      async reconcileOrphanedJobs() {
        return 0;
      },
    };
    const { spawnFn, calls } = makeFakeSpawn(() => {
      failures.push({ name: "empty-spawn", issue: "spawn should not run on empty queue" });
    });
    startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
    await new Promise((r) => setTimeout(r, 30));
    stopSchedulerWorker();
    assert(claimCalls >= 1, "empty-claim", `claimNextJob should tick at least once (was ${claimCalls})`);
    assert(calls.length === 0, "empty-spawn-count", `spawn should not be called (was ${calls.length})`);
  }

  // --- 6. Timeout: spawn never exits → kill + failed ---
  //     Drive executeJob directly with a tiny timeout so we don't need to
  //     wait 30 minutes. This is the same code path the worker uses.
  {
    const { spawnFn } = makeFakeSpawn((child) => {
      child.fakeStdout("partial output\n");
      // Deliberately never call fakeExit — let the timeout fire.
    });
    const result = await executeJob(
      { id: 11, jobKey: "slow-scraper", args: null },
      { spawnFn, jobTimeoutMs: 25 },
    );
    eq(result.status, "failed", "timeout", "status should be failed");
    assert(
      result.stderrTail.includes("timeout"),
      "timeout",
      `stderr_tail should mention timeout (got: ${JSON.stringify(result.stderrTail)})`,
    );
    assert(
      result.stdoutTail.includes("partial"),
      "timeout",
      "captured stdout should still be in tail",
    );
  }

  // --- 7. Startup sweep: 3 orphans reconciled, logged, poll loop still runs ---
  {
    let reconcileCalls = 0;
    let reconcileOlderThanMs: number | null = null;
    let claimCalls = 0;
    const db: WorkerDb = {
      async claimNextJob() {
        claimCalls += 1;
        return null;
      },
      async finishJob() {
        /* no-op */
      },
      async reconcileOrphanedJobs(olderThanMs) {
        reconcileCalls += 1;
        reconcileOlderThanMs = olderThanMs;
        return 3;
      },
    };
    const logLines: string[] = [];
    const origLog = console.log;
    const origErr = console.error;
    console.log = (...args: unknown[]) => {
      logLines.push(args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" "));
    };
    console.error = (...args: unknown[]) => {
      logLines.push(
        "[ERR] " + args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" "),
      );
    };
    try {
      const { spawnFn } = makeFakeSpawn(() => {
        failures.push({
          name: "reconcile-3-spawn",
          issue: "spawn should not run on empty queue",
        });
      });
      startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
      // Allow the microtask-queued reconcile + at least one tick to complete.
      await new Promise((r) => setTimeout(r, 30));
      stopSchedulerWorker();
    } finally {
      console.log = origLog;
      console.error = origErr;
    }
    eq(reconcileCalls, 1, "reconcile-3", "reconcileOrphanedJobs called exactly once on startup");
    eq(
      reconcileOlderThanMs,
      10_000,
      "reconcile-3",
      "threshold should be the worker's jobTimeoutMs",
    );
    assert(
      logLines.some((l) => l.includes("reconciled 3 orphaned")),
      "reconcile-3",
      `expected a 'reconciled 3 orphaned' log line, saw: ${JSON.stringify(logLines)}`,
    );
    assert(
      claimCalls >= 1,
      "reconcile-3",
      `poll loop should still tick after sweep (claimCalls=${claimCalls})`,
    );
  }

  // --- 8. Startup sweep: 0 orphans, no log emitted, poll loop still runs ---
  {
    let claimCalls = 0;
    const db: WorkerDb = {
      async claimNextJob() {
        claimCalls += 1;
        return null;
      },
      async finishJob() {
        /* no-op */
      },
      async reconcileOrphanedJobs() {
        return 0;
      },
    };
    const logLines: string[] = [];
    const origLog = console.log;
    console.log = (...args: unknown[]) => {
      logLines.push(args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" "));
    };
    try {
      const { spawnFn } = makeFakeSpawn(() => {
        failures.push({
          name: "reconcile-0-spawn",
          issue: "spawn should not run on empty queue",
        });
      });
      startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
      await new Promise((r) => setTimeout(r, 30));
      stopSchedulerWorker();
    } finally {
      console.log = origLog;
    }
    assert(
      !logLines.some((l) => l.includes("reconciled")),
      "reconcile-0",
      `no 'reconciled' log line expected, saw: ${JSON.stringify(logLines)}`,
    );
    assert(
      claimCalls >= 1,
      "reconcile-0",
      `poll loop should still tick after sweep (claimCalls=${claimCalls})`,
    );
  }

  // --- 9. Startup sweep: reconciliation throws, error logged, poll loop still runs ---
  {
    let claimCalls = 0;
    const db: WorkerDb = {
      async claimNextJob() {
        claimCalls += 1;
        return null;
      },
      async finishJob() {
        /* no-op */
      },
      async reconcileOrphanedJobs() {
        throw new Error("db unavailable");
      },
    };
    const errLines: string[] = [];
    const origErr = console.error;
    console.error = (...args: unknown[]) => {
      errLines.push(args.map((a) => (typeof a === "string" ? a : String(a))).join(" "));
    };
    try {
      const { spawnFn } = makeFakeSpawn(() => {
        failures.push({
          name: "reconcile-throws-spawn",
          issue: "spawn should not run on empty queue",
        });
      });
      // If startup propagated the error, this next line would throw.
      startSchedulerWorker({ db, spawnFn, pollIntervalMs: 5, jobTimeoutMs: 10_000 });
      await new Promise((r) => setTimeout(r, 30));
      stopSchedulerWorker();
    } finally {
      console.error = origErr;
    }
    assert(
      errLines.some((l) => l.includes("orphan reconciliation failed")),
      "reconcile-throws",
      `expected error log to mention 'orphan reconciliation failed', saw: ${JSON.stringify(errLines)}`,
    );
    assert(
      claimCalls >= 1,
      "reconcile-throws",
      `poll loop should still tick even when sweep throws (claimCalls=${claimCalls})`,
    );
  }

  // Guard against interval leaking into the next test file when run in
  // sequence (api-server's `test` script is a long `&&` chain).
  stopSchedulerWorker();

  if (failures.length === 0) {
    console.log("[schedulerWorker-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[schedulerWorker-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
