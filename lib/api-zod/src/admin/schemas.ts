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
