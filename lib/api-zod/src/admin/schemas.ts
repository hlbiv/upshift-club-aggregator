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
 * Request for GET /v1/admin/data-quality/empty-staff-pages.
 *
 * `windowDays` bounds the "has this club been usefully scraped recently"
 * lookback on `coach_discoveries.last_seen_at`. Without it, a club scraped
 * once 90 days ago with zero results looks identical to one that just
 * legitimately has no coaches — the window separates genuinely-broken
 * extractors from clubs waiting on the next scheduled run. It also keeps
 * the panel disjoint from `stale-scrapes`: a row both empty AND older than
 * `windowDays` shows up on the staleness panel instead.
 *
 * Default of 30 days matches the backlog doc; `page_size` capped at the
 * repo's 100-row pagination ceiling.
 */
export const EmptyStaffPagesRequest = z.object({
  windowDays: z.number().int().positive().max(365).default(30),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
});
export type EmptyStaffPagesRequest = z.infer<typeof EmptyStaffPagesRequest>;

/**
 * One row of the empty-staff-pages panel — a canonical_clubs row with a
 * `staff_page_url` set AND zero distinct coach discoveries recorded inside
 * `windowDays`. `coachCountWindow` is almost always 0 by construction, but
 * exposing the count lets an operator spot-check the query after running
 * a fix by filtering `coach_count_window > 0`.
 */
export const EmptyStaffPagesRow = z.object({
  clubId: z.number().int(),
  clubNameCanonical: z.string(),
  staffPageUrl: z.string(),
  lastScrapedAt: z.string().datetime().nullable(),
  coachCountWindow: z.number().int(),
});
export type EmptyStaffPagesRow = z.infer<typeof EmptyStaffPagesRow>;

/** Paginated envelope for the empty-staff-pages panel. */
export const EmptyStaffPagesResponse = z.object({
  rows: z.array(EmptyStaffPagesRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
  windowDays: z.number().int(),
});
export type EmptyStaffPagesResponse = z.infer<typeof EmptyStaffPagesResponse>;

/**
 * Request for GET /v1/admin/data-quality/stale-scrapes.
 *
 * `thresholdDays` default of 14 matches the operational reality that most
 * scrapes run on a weekly cadence — 14 days = two missed scheduled runs,
 * the earliest point where staleness is meaningful rather than noise (a
 * single missed run could be a transient failure that recovers next cycle).
 */
export const StaleScrapesRequest = z.object({
  thresholdDays: z.number().int().positive().max(365).default(14),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
});
export type StaleScrapesRequest = z.infer<typeof StaleScrapesRequest>;

/**
 * One row of the stale-scrapes panel — an entity in `scrape_health` whose
 * `last_scraped_at` is older than `thresholdDays` or NULL. `entityName` is
 * a best-effort human label joined in from `canonical_clubs` /
 * `leagues_master` / `colleges` / `coaches` per `entity_type`. If the join
 * fails (the entity row was deleted, or the type isn't joinable in the
 * current schema), the field is null — we don't fabricate.
 */
export const StaleScrapesRow = z.object({
  entityType: z.string(),
  entityId: z.number().int(),
  entityName: z.string().nullable(),
  lastScrapedAt: z.string().datetime().nullable(),
  lastStatus: z.string().nullable(),
  consecutiveFailures: z.number().int(),
});
export type StaleScrapesRow = z.infer<typeof StaleScrapesRow>;

/** Paginated envelope for the stale-scrapes panel. */
export const StaleScrapesResponse = z.object({
  rows: z.array(StaleScrapesRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
  thresholdDays: z.number().int(),
});
export type StaleScrapesResponse = z.infer<typeof StaleScrapesResponse>;

/**
 * State filter for GET /v1/admin/data-quality/nav-leaked-names — replaces
 * the previous binary `includeResolved` boolean so reviewers can split the
 * resolved queue into "legitimate leak cleaned up" vs "false positive
 * dismissed" views without a second UI toggle. `open` (the default) is the
 * triage queue. `all` is escape-hatch for cross-state comparisons.
 */
export const NavLeakedNamesState = z.enum([
  "open",
  "resolved",
  "dismissed",
  "all",
]);
export type NavLeakedNamesState = z.infer<typeof NavLeakedNamesState>;

/**
 * Request for GET /v1/admin/data-quality/nav-leaked-names.
 *
 * Panel for `roster_quality_flags` rows whose `flag_type =
 * 'nav_leaked_name'`. Default `state='open'` surfaces only active flags —
 * operators almost always want to work the unresolved queue. `page_size`
 * capped at the repo-wide 100-row pagination ceiling.
 */
export const NavLeakedNamesRequest = z.object({
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
  state: NavLeakedNamesState.default("open"),
});
export type NavLeakedNamesRequest = z.infer<typeof NavLeakedNamesRequest>;

/**
 * One row of the nav-leaked-names panel — a `roster_quality_flags` row
 * joined to its `club_roster_snapshots` parent and the snapshot's
 * `canonical_clubs` resolution (if any).
 *
 * Typed fields are extracted from `roster_quality_flags.metadata` at the
 * API boundary — callers never see the raw jsonb payload. The shape is
 * fixed for `flag_type='nav_leaked_name'`:
 *   metadata.leaked_strings → leakedStrings
 *   metadata.snapshot_roster_size → snapshotRosterSize
 *
 * `resolvedByEmail` is joined from `admin_users.email` if the flag has
 * been resolved; null otherwise.
 */
export const NavLeakedNamesRow = z.object({
  id: z.number().int(),
  snapshotId: z.number().int(),
  clubId: z.number().int().nullable(),
  clubNameCanonical: z.string().nullable(),
  leakedStrings: z.array(z.string()),
  snapshotRosterSize: z.number().int(),
  flaggedAt: z.string().datetime(),
  resolvedAt: z.string().datetime().nullable(),
  resolvedByEmail: z.string().nullable(),
  // 'resolved' / 'dismissed' — null while the flag is still open. Matches
  // `roster_quality_flags.resolution_reason` and mirrors the CHECK list.
  resolutionReason: z.enum(["resolved", "dismissed"]).nullable(),
});
export type NavLeakedNamesRow = z.infer<typeof NavLeakedNamesRow>;

/**
 * Body for PATCH /v1/admin/data-quality/roster-quality-flags/:id/resolve.
 * `reason` captures operator intent: `resolved` = legitimate leak cleaned
 * up out of band; `dismissed` = false positive.
 */
export const ResolveRosterQualityFlagRequest = z.object({
  reason: z.enum(["resolved", "dismissed"]),
});
export type ResolveRosterQualityFlagRequest = z.infer<
  typeof ResolveRosterQualityFlagRequest
>;

/** Paginated envelope for the nav-leaked-names panel. */
export const NavLeakedNamesResponse = z.object({
  rows: z.array(NavLeakedNamesRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type NavLeakedNamesResponse = z.infer<typeof NavLeakedNamesResponse>;


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

/**
 * One known scraper schedule — metadata (jobKey + description +
 * cronExpression) plus the latest N `scheduler_jobs` rows for that jobKey.
 * Returned by GET /v1/admin/scraper-schedules.
 *
 * `cronExpression` is a curated display string ("0 3 * * *") or null when
 * the job only runs via admin-triggered "Run now" (no fixed cron). The
 * server is the source of truth for both `description` and
 * `cronExpression`; the dashboard just renders them.
 */
export const ScraperSchedule = z.object({
  jobKey: z.string(),
  description: z.string(),
  cronExpression: z.string().nullable(),
  recentRuns: z.array(SchedulerJob),
});
export type ScraperSchedule = z.infer<typeof ScraperSchedule>;

/** Envelope of all known scraper schedules in server-defined order. */
export const ScraperSchedulesResponse = z.object({
  schedules: z.array(ScraperSchedule),
});
export type ScraperSchedulesResponse = z.infer<typeof ScraperSchedulesResponse>;
