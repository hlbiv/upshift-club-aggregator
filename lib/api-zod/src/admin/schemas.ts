import { z } from "zod";

/** Admin login request body: email + password (min 8 chars). */
export const AdminLoginRequest = z.object({
  email: z.string().email(),
  password: z.string().min(8),
});
export type AdminLoginRequest = z.infer<typeof AdminLoginRequest>;

/** Admin login response body: identity fields (session token travels via Set-Cookie, not body). */
export const AdminLoginResponse = z.object({
  id: z.number().int(),
  email: z.string().email(),
  role: z.enum(["admin", "super_admin"]),
});
export type AdminLoginResponse = z.infer<typeof AdminLoginResponse>;

/** Current-admin identity response — same shape as AdminLoginResponse per contract. */
export const AdminMeResponse = AdminLoginResponse;
export type AdminMeResponse = z.infer<typeof AdminMeResponse>;

/** Admin logout acknowledgement: `{ ok: true }`. */
export const AdminLogoutResponse = z.object({
  ok: z.literal(true),
});
export type AdminLogoutResponse = z.infer<typeof AdminLogoutResponse>;

/** Single row from `scrape_run_logs`: one scraper invocation with status + counts. */
export const ScrapeRunLog = z.object({
  id: z.number().int(),
  scraperKey: z.string(),
  jobKey: z.string().nullable(),
  status: z.enum(["running", "ok", "partial", "failed"]),
  startedAt: z.string().datetime(),
  completedAt: z.string().datetime().nullable(),
  recordsTouched: z.number().int().nullable(),
  errorMessage: z.string().nullable(),
  metadata: z.record(z.unknown()).nullable(),
});
export type ScrapeRunLog = z.infer<typeof ScrapeRunLog>;

/** Paginated envelope of ScrapeRunLog rows. */
export const ScrapeRunLogList = z.object({
  runs: z.array(ScrapeRunLog),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type ScrapeRunLogList = z.infer<typeof ScrapeRunLogList>;

/** Single row from `scrape_health`: rolling scrape state per entity. */
export const ScrapeHealthRow = z.object({
  entityType: z.enum(["club", "event", "league", "college", "coach"]),
  entityId: z.number().int(),
  lastScrapedAt: z.string().datetime().nullable(),
  lastStatus: z.enum(["running", "ok", "partial", "failed"]).nullable(),
  consecutiveFailures: z.number().int(),
  nextScheduledAt: z.string().datetime().nullable(),
  metadata: z.record(z.unknown()).nullable(),
});
export type ScrapeHealthRow = z.infer<typeof ScrapeHealthRow>;

/** Envelope of ScrapeHealthRow rows with a total count. */
export const ScrapeHealthList = z.object({
  rows: z.array(ScrapeHealthRow),
  total: z.number().int(),
});
export type ScrapeHealthList = z.infer<typeof ScrapeHealthList>;

/** Coverage rollup: per-source successes/failures + totals across a time window. */
export const CoverageRollup = z.object({
  bySource: z.array(
    z.object({
      source: z.string(),
      successes: z.number().int(),
      failures: z.number().int(),
      lastRunAt: z.string().datetime().nullable(),
    }),
  ),
  totalRuns: z.number().int(),
  windowDays: z.number().int(),
});
export type CoverageRollup = z.infer<typeof CoverageRollup>;

/** Single club-duplicate pair record surfaced in the dedup review queue. */
export const ClubDuplicate = z.object({
  id: z.number().int(),
  leftClubId: z.number().int(),
  rightClubId: z.number().int(),
  score: z.number(),
  method: z.string(),
  status: z.enum(["pending", "merged", "rejected"]),
  createdAt: z.string().datetime(),
  reviewedAt: z.string().datetime().nullable(),
  reviewedBy: z.number().int().nullable(),
  leftSnapshot: z.record(z.unknown()),
  rightSnapshot: z.record(z.unknown()),
});
export type ClubDuplicate = z.infer<typeof ClubDuplicate>;

/** Paginated envelope of ClubDuplicate pairs. */
export const ClubDuplicateList = z.object({
  pairs: z.array(ClubDuplicate),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type ClubDuplicateList = z.infer<typeof ClubDuplicateList>;

/** ClubDuplicate extended with live side-by-side context for the detail view. */
export const ClubDuplicateDetail = ClubDuplicate.extend({
  leftCurrent: z.record(z.unknown()),
  rightCurrent: z.record(z.unknown()),
  affiliations: z.object({
    leftAffiliationCount: z.number().int(),
    rightAffiliationCount: z.number().int(),
  }),
  rosters: z.object({
    leftRosterSnapshotCount: z.number().int(),
    rightRosterSnapshotCount: z.number().int(),
  }),
});
export type ClubDuplicateDetail = z.infer<typeof ClubDuplicateDetail>;

/** Request body for POST /v1/admin/dedup/clubs/:id/merge. */
export const ClubDuplicateMergeRequest = z.object({
  winnerId: z.number().int(),
  loserId: z.number().int(),
  notes: z.string().optional(),
});
export type ClubDuplicateMergeRequest = z.infer<typeof ClubDuplicateMergeRequest>;

/** Response body for POST /v1/admin/dedup/clubs/:id/merge. */
export const ClubDuplicateMergeResponse = z.object({
  ok: z.literal(true),
  winnerId: z.number().int(),
  loserAliasesCreated: z.number().int(),
  affiliationsReparented: z.number().int(),
  rosterSnapshotsReparented: z.number().int(),
});
export type ClubDuplicateMergeResponse = z.infer<typeof ClubDuplicateMergeResponse>;

/**
 * Request body for POST /v1/admin/data-quality/ga-premier-orphans.
 *
 * `dryRun` defaults to true — callers must explicitly opt in to destructive
 * DELETEs. `limit` caps the scan/delete size per invocation (max 10k).
 */
export const GaPremierOrphanCleanupRequest = z.object({
  dryRun: z.boolean().default(true),
  limit: z.number().int().positive().max(10_000).default(500),
});
export type GaPremierOrphanCleanupRequest = z.infer<typeof GaPremierOrphanCleanupRequest>;

/**
 * Response body for POST /v1/admin/data-quality/ga-premier-orphans.
 *
 * `scanned` = rows inspected, `flagged` = rows matching the bad-token
 * patterns, `deleted` = rows actually removed (0 on dry-run). `sampleNames`
 * is capped at 20 representative `club_name_raw` values for operator review.
 */
export const GaPremierOrphanCleanupResponse = z.object({
  scanned: z.number().int(),
  flagged: z.number().int(),
  deleted: z.number().int(),
  sampleNames: z.array(z.string()).max(20),
});
export type GaPremierOrphanCleanupResponse = z.infer<typeof GaPremierOrphanCleanupResponse>;

/**
 * Growth dashboard — "records added since X" counts across the five
 * headline ingest tables. Timestamps used per table:
 *   canonical_clubs → last_scraped_at      (no first_seen column today)
 *   coaches → first_seen_at
 *   events → last_scraped_at               (no first_seen column today)
 *   club_roster_snapshots → snapshot_date
 *   matches → scraped_at
 */
export const ScrapedCountsDelta = z.object({
  since: z.string().datetime(),
  clubsAdded: z.number().int(),
  coachesAdded: z.number().int(),
  eventsAdded: z.number().int(),
  rosterSnapshotsAdded: z.number().int(),
  matchesAdded: z.number().int(),
});
export type ScrapedCountsDelta = z.infer<typeof ScrapedCountsDelta>;

/** One day of scrape-run health telemetry — aggregated from scrape_run_logs. */
export const CoverageTrendPoint = z.object({
  date: z.string(),
  runs: z.number().int(),
  successes: z.number().int(),
  failures: z.number().int(),
  rowsTouched: z.number().int(),
});
export type CoverageTrendPoint = z.infer<typeof CoverageTrendPoint>;

/** Coverage-trend response envelope: daily points over a rolling window. */
export const CoverageTrendResponse = z.object({
  points: z.array(CoverageTrendPoint),
  windowDays: z.number().int(),
});
export type CoverageTrendResponse = z.infer<typeof CoverageTrendResponse>;

/**
 * Scheduler queue row — one admin-triggered scraper invocation.
 *
 * Mirrors `scheduler_jobs` (lib/db/src/schema/scheduler-jobs.ts). Timestamps
 * are serialized as ISO-8601 strings across the wire. `stdout_tail` and
 * `stderr_tail` hold the last N lines of the child-process output (see
 * `tailLines` in artifacts/api-server/src/scheduler/worker.ts).
 */
export const SchedulerJob = z.object({
  id: z.number().int(),
  jobKey: z.string(),
  args: z.record(z.unknown()).nullable(),
  status: z.enum(["pending", "running", "success", "failed", "canceled"]),
  requestedBy: z.number().int().nullable(),
  requestedAt: z.string().datetime(),
  startedAt: z.string().datetime().nullable(),
  completedAt: z.string().datetime().nullable(),
  exitCode: z.number().int().nullable(),
  stdoutTail: z.string().nullable(),
  stderrTail: z.string().nullable(),
});
export type SchedulerJob = z.infer<typeof SchedulerJob>;

/** Paginated envelope of SchedulerJob rows. */
export const SchedulerJobList = z.object({
  jobs: z.array(SchedulerJob),
  total: z.number().int(),
});
export type SchedulerJobList = z.infer<typeof SchedulerJobList>;

/**
 * Request body for POST /v1/admin/scheduler/run-now — enqueue a scraper.
 *
 * `args` is forwarded as CLI flags by the worker: `{ 'event-id': '123',
 * 'dry-run': true }` → `['--event-id', '123', '--dry-run']`. Boolean values
 * become presence flags (no value).
 */
export const RunNowRequest = z.object({
  jobKey: z.string().min(1),
  args: z.record(z.unknown()).optional(),
});
export type RunNowRequest = z.infer<typeof RunNowRequest>;

/**
 * Response body for POST /v1/admin/scheduler/run-now — row inserted at
 * status=pending; the in-process worker will pick it up on its next tick.
 */
export const RunNowResponse = z.object({
  id: z.number().int(),
  jobKey: z.string(),
  status: z.literal("pending"),
  requestedAt: z.string().datetime(),
});
export type RunNowResponse = z.infer<typeof RunNowResponse>;
