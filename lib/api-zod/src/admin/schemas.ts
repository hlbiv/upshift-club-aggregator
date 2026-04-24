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

// ---------------------------------------------------------------------------
// College dedup schemas
// ---------------------------------------------------------------------------

/** Single college-duplicate pair record surfaced in the dedup review queue. */
export const CollegeDuplicate = z.object({
  id: z.number().int(),
  leftCollegeId: z.number().int(),
  rightCollegeId: z.number().int(),
  score: z.number(),
  method: z.string(),
  status: z.enum(["pending", "merged", "rejected"]),
  createdAt: z.string().datetime(),
  reviewedAt: z.string().datetime().nullable(),
  reviewedBy: z.number().int().nullable(),
  leftSnapshot: z.record(z.unknown()),
  rightSnapshot: z.record(z.unknown()),
});
export type CollegeDuplicate = z.infer<typeof CollegeDuplicate>;

/** Paginated envelope of CollegeDuplicate pairs. */
export const CollegeDuplicateList = z.object({
  pairs: z.array(CollegeDuplicate),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type CollegeDuplicateList = z.infer<typeof CollegeDuplicateList>;

/** CollegeDuplicate extended with live side-by-side context for the detail view. */
export const CollegeDuplicateDetail = CollegeDuplicate.extend({
  leftCurrent: z.record(z.unknown()),
  rightCurrent: z.record(z.unknown()),
});
export type CollegeDuplicateDetail = z.infer<typeof CollegeDuplicateDetail>;

/** Request body for POST /v1/admin/dedup/colleges/:id/merge. */
export const CollegeDuplicateMergeRequest = z.object({
  winnerId: z.number().int(),
  loserId: z.number().int(),
  notes: z.string().optional(),
});
export type CollegeDuplicateMergeRequest = z.infer<typeof CollegeDuplicateMergeRequest>;

/** Response body for POST /v1/admin/dedup/colleges/:id/merge. */
export const CollegeDuplicateMergeResponse = z.object({
  ok: z.literal(true),
  winnerId: z.number().int(),
  loserAliasesCreated: z.number().int(),
  coachesReparented: z.number().int(),
  rosterRowsReparented: z.number().int(),
  tenuresReparented: z.number().int(),
});
export type CollegeDuplicateMergeResponse = z.infer<typeof CollegeDuplicateMergeResponse>;

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
 * Request for GET /v1/admin/data-quality/coach-misses.
 *
 * Lists colleges where the head-coach extractor found nothing on the
 * most recent run that recorded a miss for that program. Backed by the
 * `coach_misses` table, populated by the NCAA roster scraper when env
 * `COACH_MISSES_REPORT_ENABLED=true`.
 *
 * `division` and `gender` are optional narrowing filters so an operator
 * fixing D1 women's first can scope the queue.
 */
export const CoachMissesRequest = z.object({
  division: z.enum(["D1", "D2", "D3"]).optional(),
  gender: z.enum(["mens", "womens"]).optional(),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
});
export type CoachMissesRequest = z.infer<typeof CoachMissesRequest>;

/**
 * One row of the coach-misses panel — a college whose head coach the
 * scraper failed to extract from both the inline roster page and the
 * `/coaches` fallback. `probedUrls` is the newline-separated list of
 * URLs that the fallback tried before giving up; useful as input for
 * follow-up #55 (Playwright on fallback) and any manual triage.
 */
export const CoachMissesRow = z.object({
  collegeId: z.number().int(),
  collegeName: z.string(),
  division: z.string(),
  genderProgram: z.string(),
  rosterUrl: z.string().nullable(),
  probedUrls: z.array(z.string()),
  scrapeRunLogId: z.number().int().nullable(),
  recordedAt: z.string().datetime(),
});
export type CoachMissesRow = z.infer<typeof CoachMissesRow>;

/** Paginated envelope for the coach-misses panel. */
export const CoachMissesResponse = z.object({
  rows: z.array(CoachMissesRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type CoachMissesResponse = z.infer<typeof CoachMissesResponse>;

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

// ---------------------------------------------------------------------------
// coach_quality_flags — canary table for coach-pollution remediation.
// ---------------------------------------------------------------------------

/**
 * CHECK-list values mirrored from lib/db/src/schema/coach-quality-flags.ts.
 * Adding a new flag_type is a 3-place change: the Postgres CHECK, the
 * schema file comment, and this enum.
 */
export const CoachQualityFlagType = z.enum([
  "looks_like_name_reject",
  "role_label_as_name",
  "corrupt_email",
  "nav_leaked",
]);
export type CoachQualityFlagType = z.infer<typeof CoachQualityFlagType>;

/**
 * Request for GET /v1/admin/data-quality/coach-quality-flags.
 *
 * `resolved` is optional tri-state: true → resolved only, false → active
 * only, omitted → both. This differs from the nav-leaked-names panel's
 * boolean-with-default — the coach canary is read more often in forensic
 * "show everything ever flagged" mode during the active purge.
 */
export const CoachQualityFlagsRequest = z.object({
  flagType: CoachQualityFlagType.optional(),
  resolved: z.boolean().optional(),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
});
export type CoachQualityFlagsRequest = z.infer<typeof CoachQualityFlagsRequest>;

/**
 * One row of the coach-quality-flags panel — a `coach_quality_flags` row
 * joined to its `coach_discoveries` parent (for `coachName` / `coachEmail`
 * / `clubNameRaw`) and the discovery's `canonical_clubs` resolution (for
 * `clubDisplayName` when `club_id` is set). `metadata` is the raw jsonb
 * payload (shape varies by `flagType` — see the schema-file docstring for
 * the per-flag-type contract); callers can narrow on `flagType` to read
 * typed fields safely.
 */
export const CoachQualityFlag = z.object({
  id: z.number().int(),
  discoveryId: z.number().int(),
  flagType: CoachQualityFlagType,
  metadata: z.record(z.unknown()).nullable(),
  flaggedAt: z.string().datetime(),
  resolvedAt: z.string().datetime().nullable(),
  resolvedByEmail: z.string().nullable(),
  resolutionNote: z.string().nullable(),
  coachName: z.string(),
  coachEmail: z.string().nullable(),
  clubNameRaw: z.string().nullable(),
  clubId: z.number().int().nullable(),
  clubDisplayName: z.string().nullable(),
});
export type CoachQualityFlag = z.infer<typeof CoachQualityFlag>;

/** Paginated envelope for the coach-quality-flags panel. */
export const CoachQualityFlagsResponse = z.object({
  items: z.array(CoachQualityFlag),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type CoachQualityFlagsResponse = z.infer<typeof CoachQualityFlagsResponse>;

// ---------------------------------------------------------------------------
// numeric_only_name — roster_quality_flags flag_type for scraper bugs where
// jersey numbers or dates leak into player_name.
// ---------------------------------------------------------------------------

/**
 * State filter for GET /v1/admin/data-quality/numeric-only-names — same
 * open / resolved / dismissed / all semantics as nav-leaked-names. See
 * NavLeakedNamesState for the rationale.
 */
export const NumericOnlyNamesState = z.enum([
  "open",
  "resolved",
  "dismissed",
  "all",
]);
export type NumericOnlyNamesState = z.infer<typeof NumericOnlyNamesState>;

/**
 * Request for GET /v1/admin/data-quality/numeric-only-names.
 *
 * Panel for `roster_quality_flags` rows whose `flag_type =
 * 'numeric_only_name'`. Surfaces scraper bugs where jersey numbers or
 * dates leak into the `player_name` column instead of actual names.
 */
export const NumericOnlyNamesRequest = z.object({
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
  state: NumericOnlyNamesState.default("open"),
});
export type NumericOnlyNamesRequest = z.infer<typeof NumericOnlyNamesRequest>;

/**
 * One row of the numeric-only-names panel — identical shape to
 * NavLeakedNamesRow, with `numericStrings` in place of `leakedStrings`.
 *
 * Typed fields extracted from `roster_quality_flags.metadata` for
 * `flag_type='numeric_only_name'`:
 *   metadata.numeric_strings → numericStrings
 *   metadata.snapshot_roster_size → snapshotRosterSize
 */
export const NumericOnlyNamesRow = z.object({
  id: z.number().int(),
  snapshotId: z.number().int(),
  clubId: z.number().int().nullable(),
  clubNameCanonical: z.string().nullable(),
  numericStrings: z.array(z.string()),
  snapshotRosterSize: z.number().int(),
  flaggedAt: z.string().datetime(),
  resolvedAt: z.string().datetime().nullable(),
  resolvedByEmail: z.string().nullable(),
  resolutionReason: z.enum(["resolved", "dismissed"]).nullable(),
});
export type NumericOnlyNamesRow = z.infer<typeof NumericOnlyNamesRow>;

/** Paginated envelope for the numeric-only-names panel. */
export const NumericOnlyNamesResponse = z.object({
  rows: z.array(NumericOnlyNamesRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type NumericOnlyNamesResponse = z.infer<typeof NumericOnlyNamesResponse>;


// ---------------------------------------------------------------------------
// pro-academies — operator-edit surface for canonical_clubs.is_pro_academy.
// ---------------------------------------------------------------------------

/**
 * Tier values mirrored from `competitive_tier` Postgres enum (see
 * lib/db/src/schema/index.ts). Kept in lockstep with the OpenAPI enum on
 * ProAcademyRow / UpdateProAcademyResponse — adding a tier is a 3-place
 * change (DB enum + this file + openapi.yaml).
 */
export const CompetitiveTier = z.enum([
  "recreational",
  "recreational_plus",
  "competitive",
  "elite",
  "academy",
]);
export type CompetitiveTier = z.infer<typeof CompetitiveTier>;

/**
 * Filter mode for GET /v1/admin/pro-academies. `unflagged` surfaces the
 * borderline-candidate work queue (academy-family clubs not yet on the
 * curated allow-list). `flagged` is the inverse — currently-flagged pro
 * academies. `all` (default) returns the union, paginated.
 */
export const ProAcademiesFlagFilter = z.enum(["all", "flagged", "unflagged"]);
export type ProAcademiesFlagFilter = z.infer<typeof ProAcademiesFlagFilter>;

/** Request for GET /v1/admin/pro-academies. */
export const ProAcademiesRequest = z.object({
  flag: ProAcademiesFlagFilter.default("all"),
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(200).default(50),
});
export type ProAcademiesRequest = z.infer<typeof ProAcademiesRequest>;

/**
 * One tier-1 academy-family affiliation contributing to a ProAcademyRow.
 * `genderProgram` is included so co-ed clubs with mixed pro pipelines
 * (boys MLS NEXT + girls NWSL Academy) are legible at a glance.
 */
export const ProAcademyAffiliation = z.object({
  leagueId: z.number().int().nullable(),
  leagueName: z.string(),
  leagueFamily: z.string(),
  genderProgram: z.string().nullable(),
});
export type ProAcademyAffiliation = z.infer<typeof ProAcademyAffiliation>;

/**
 * One row of the pro-academies admin panel. `families` is the deduplicated
 * set of academy-family labels at tier 1; `affiliations` is the underlying
 * per-affiliation breakdown for tooltips / drilldown. `affiliationCount` is
 * a precomputed convenience for the table.
 */
export const ProAcademyRow = z.object({
  clubId: z.number().int(),
  clubNameCanonical: z.string(),
  city: z.string().nullable(),
  state: z.string().nullable(),
  isProAcademy: z.boolean(),
  competitiveTier: CompetitiveTier,
  families: z.array(z.string()),
  affiliations: z.array(ProAcademyAffiliation),
  affiliationCount: z.number().int(),
});
export type ProAcademyRow = z.infer<typeof ProAcademyRow>;

/** Paginated envelope for the pro-academies admin panel. */
export const ProAcademiesResponse = z.object({
  rows: z.array(ProAcademyRow),
  total: z.number().int(),
  // Total flagged irrespective of the current filter — lets the dashboard
  // show "X of Y flagged" without a second request.
  flaggedTotal: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type ProAcademiesResponse = z.infer<typeof ProAcademiesResponse>;

/**
 * Body for PATCH /v1/admin/pro-academies/{clubId}. A no-op flip (current
 * value already matches) is allowed and still re-runs the per-club rollup
 * — keeps the contract idempotent and gives operators a way to force a
 * recompute after upstream affiliation changes.
 */
export const UpdateProAcademyRequest = z.object({
  isProAcademy: z.boolean(),
});
export type UpdateProAcademyRequest = z.infer<typeof UpdateProAcademyRequest>;

/**
 * Response for PATCH /v1/admin/pro-academies/{clubId}. `previousCompetitiveTier`
 * is the value the row had before the rollup re-ran so the dashboard can
 * surface visible upgrade / downgrade transitions in the toast.
 */
export const UpdateProAcademyResponse = z.object({
  clubId: z.number().int(),
  isProAcademy: z.boolean(),
  competitiveTier: CompetitiveTier,
  previousCompetitiveTier: CompetitiveTier,
});
export type UpdateProAcademyResponse = z.infer<typeof UpdateProAcademyResponse>;

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

/**
 * Phase 3 coverage drilldown.
 *
 * `/v1/admin/coverage/leagues` aggregates per-league club coverage — which
 * clubs have a roster snapshot, a coach discovery, or are stale / never
 * scraped. Join path: `leagues_master.id` ↔ `club_affiliations.league_id`
 * — a stable id, so a `leagues_master.league_name` rename can't drop the
 * league out of the rollup. See docstring on the route for the caveat.
 *
 * `/v1/admin/coverage/leagues/:leagueId` drilldown surfaces the per-club
 * roster-snapshot / coach-discovery / stale-scrape state for one league.
 */

/** Request for GET /v1/admin/coverage/leagues. */
export const CoverageLeaguesRequest = z.object({
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
});
export type CoverageLeaguesRequest = z.infer<typeof CoverageLeaguesRequest>;

/**
 * One league with its aggregate coverage counts across its affiliated
 * canonical_clubs. `clubsNeverScraped` counts clubs with no `scrape_health`
 * row (or `last_scraped_at IS NULL` — both signal the canonical-club linker
 * has seen them but the club-level scraper has not). `clubsStale14d` counts
 * clubs with a `last_scraped_at` older than 14 days.
 */
export const CoverageLeagueRow = z.object({
  leagueId: z.number().int(),
  leagueName: z.string(),
  clubsTotal: z.number().int(),
  clubsWithRosterSnapshot: z.number().int(),
  clubsWithCoachDiscovery: z.number().int(),
  clubsNeverScraped: z.number().int(),
  clubsStale14d: z.number().int(),
});
export type CoverageLeagueRow = z.infer<typeof CoverageLeagueRow>;

/** Paginated envelope for /v1/admin/coverage/leagues. */
export const CoverageLeaguesResponse = z.object({
  rows: z.array(CoverageLeagueRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type CoverageLeaguesResponse = z.infer<typeof CoverageLeaguesResponse>;

/**
 * Aggregate coverage rollup across every league. Powers the Coverage
 * page's KpiStrip. Counts are deduplicated by canonical club so a
 * club affiliated with N leagues counts once.
 */
export const CoverageLeaguesSummaryResponse = z.object({
  leaguesTotal: z.number().int(),
  clubsTotal: z.number().int(),
  clubsWithRosterSnapshot: z.number().int(),
  clubsWithCoachDiscovery: z.number().int(),
  clubsNeverScraped: z.number().int(),
  clubsStale14d: z.number().int(),
});
export type CoverageLeaguesSummaryResponse = z.infer<
  typeof CoverageLeaguesSummaryResponse
>;

/** Request for GET /v1/admin/coverage/leagues/history. */
export const CoverageLeaguesHistoryRequest = z.object({
  days: z.number().int().positive().max(365).default(30),
});
export type CoverageLeaguesHistoryRequest = z.infer<
  typeof CoverageLeaguesHistoryRequest
>;

/**
 * One day's snapshot of the global coverage rollup. Same six counters as
 * CoverageLeaguesSummaryResponse so the trend series is drop-in
 * comparable to the live current snapshot.
 */
export const CoverageHistoryRow = z.object({
  snapshotDate: z.string(),
  leaguesTotal: z.number().int(),
  clubsTotal: z.number().int(),
  clubsWithRosterSnapshot: z.number().int(),
  clubsWithCoachDiscovery: z.number().int(),
  clubsNeverScraped: z.number().int(),
  clubsStale14d: z.number().int(),
});
export type CoverageHistoryRow = z.infer<typeof CoverageHistoryRow>;

/**
 * Daily timeseries of the global coverage rollup. Rows are ordered by
 * snapshotDate ascending so the dashboard can pass them straight into a
 * sparkline. May contain fewer than `days` entries on a fresh deploy
 * (history starts when the first summary call records its first row).
 */
export const CoverageLeaguesHistoryResponse = z.object({
  rows: z.array(CoverageHistoryRow),
});
export type CoverageLeaguesHistoryResponse = z.infer<
  typeof CoverageLeaguesHistoryResponse
>;

/** Request for GET /v1/admin/coverage/leagues/:leagueId/history. */
export const CoverageLeagueHistoryRequest = z.object({
  days: z.number().int().positive().max(365).default(30),
});
export type CoverageLeagueHistoryRequest = z.infer<
  typeof CoverageLeagueHistoryRequest
>;

/**
 * One day's snapshot of a single league's coverage rollup. Mirrors the
 * five per-league counters returned by `listLeagues` (no `leaguesTotal` —
 * that's a global field, not per-league).
 */
export const CoverageLeagueHistoryRow = z.object({
  snapshotDate: z.string(),
  clubsTotal: z.number().int(),
  clubsWithRosterSnapshot: z.number().int(),
  clubsWithCoachDiscovery: z.number().int(),
  clubsNeverScraped: z.number().int(),
  clubsStale14d: z.number().int(),
});
export type CoverageLeagueHistoryRow = z.infer<typeof CoverageLeagueHistoryRow>;

/**
 * Daily timeseries of one league's coverage rollup. Rows are ordered by
 * snapshotDate ascending so the dashboard can pass them straight into a
 * sparkline. May contain fewer than `days` entries on a fresh deploy
 * (history starts when the first summary call records its first row), and
 * the `league` field echoes the resolved (id, name) pair so the page can
 * render a header without a second request.
 */
export const CoverageLeagueHistoryResponse = z.object({
  league: z.object({
    id: z.number().int(),
    name: z.string(),
  }),
  rows: z.array(CoverageLeagueHistoryRow),
});
export type CoverageLeagueHistoryResponse = z.infer<
  typeof CoverageLeagueHistoryResponse
>;

/**
 * Status filter for the per-league drilldown. `all` (default) returns every
 * club affiliated with the league; `never_scraped` keeps only those with no
 * `scrape_health.last_scraped_at`; `stale` keeps only those with a non-null
 * `last_scraped_at` older than 14 days.
 */
export const CoverageLeagueDetailStatus = z.enum([
  "all",
  "never_scraped",
  "stale",
]);
export type CoverageLeagueDetailStatus = z.infer<typeof CoverageLeagueDetailStatus>;

/** Request for GET /v1/admin/coverage/leagues/:leagueId. */
export const CoverageLeagueDetailRequest = z.object({
  page: z.number().int().positive().default(1),
  pageSize: z.number().int().positive().max(100).default(20),
  status: CoverageLeagueDetailStatus.default("all"),
});
export type CoverageLeagueDetailRequest = z.infer<
  typeof CoverageLeagueDetailRequest
>;

/** Per-club coverage row inside the drilldown table. */
export const CoverageLeagueDetailRow = z.object({
  clubId: z.number().int(),
  clubNameCanonical: z.string(),
  lastScrapedAt: z.string().datetime().nullable(),
  consecutiveFailures: z.number().int(),
  coachCount: z.number().int(),
  hasRosterSnapshot: z.boolean(),
  staffPageUrl: z.string().nullable(),
  scrapeConfidence: z.number().nullable(),
});
export type CoverageLeagueDetailRow = z.infer<typeof CoverageLeagueDetailRow>;

/** Paginated envelope for /v1/admin/coverage/leagues/:leagueId. */
export const CoverageLeagueDetailResponse = z.object({
  league: z.object({
    id: z.number().int(),
    name: z.string(),
  }),
  rows: z.array(CoverageLeagueDetailRow),
  total: z.number().int(),
  page: z.number().int(),
  pageSize: z.number().int(),
});
export type CoverageLeagueDetailResponse = z.infer<
  typeof CoverageLeagueDetailResponse
>;
