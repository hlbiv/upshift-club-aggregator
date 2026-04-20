import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  jsonb,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { adminUsers } from "./admin";

/**
 * scheduler_jobs — persistence backing for the admin "Run now" scraper queue.
 *
 * Design doc: docs/design/scheduler-queue-decision.md (PR #132 on master).
 *
 * Decisions baked into the follow-up PRs (NOT this schema):
 *  - Worker runs inside the api-server process (no dedicated worker deploy).
 *  - "Run now" route is gated behind super_admin role at the route layer.
 *  - 90-day retention handled by a future sweep job; schema has no hard TTL
 *    but includes indexes on `status`, `requested_at`, `job_key` to support
 *    the queue worker's claim query and the eventual retention sweep.
 *
 * `requested_by` is nullable so future auto-runs (cron-triggered jobs) can
 * write rows without a human admin behind them. The FK is SET NULL on
 * admin deletion to preserve audit history.
 */
export const schedulerJobs = pgTable(
  "scheduler_jobs",
  {
    id: serial("id").primaryKey(),
    // scraper_key shelled out to python run.py (e.g. "sincsports-events").
    jobKey: text("job_key").notNull(),
    // Optional free-form kwargs forwarded to the scraper as CLI flags.
    args: jsonb("args"),
    status: text("status").notNull().default("pending"),
    requestedBy: integer("requested_by").references(() => adminUsers.id, {
      onDelete: "set null",
    }),
    requestedAt: timestamp("requested_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    startedAt: timestamp("started_at", { withTimezone: true }),
    completedAt: timestamp("completed_at", { withTimezone: true }),
    exitCode: integer("exit_code"),
    // Last N lines of child-process output, captured for admin UI display.
    stdoutTail: text("stdout_tail"),
    stderrTail: text("stderr_tail"),
  },
  (t) => [
    check(
      "scheduler_jobs_status_enum",
      sql`${t.status} IN ('pending', 'running', 'success', 'failed', 'canceled')`,
    ),
    index("scheduler_jobs_status_idx").on(t.status),
    index("scheduler_jobs_requested_at_idx").on(t.requestedAt),
    index("scheduler_jobs_job_key_idx").on(t.jobKey),
  ],
);

export type SchedulerJob = typeof schedulerJobs.$inferSelect;
export type InsertSchedulerJob = typeof schedulerJobs.$inferInsert;
