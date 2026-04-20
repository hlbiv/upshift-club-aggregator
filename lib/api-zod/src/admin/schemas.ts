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
  source: z.string(),
  jobKey: z.string().nullable(),
  status: z.enum(["success", "failure", "running"]),
  startedAt: z.string().datetime(),
  finishedAt: z.string().datetime().nullable(),
  rowsIn: z.number().int().nullable(),
  rowsOut: z.number().int().nullable(),
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
  lastStatus: z.enum(["success", "failure", "running"]).nullable(),
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
