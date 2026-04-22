/**
 * Domain 8 — Scrape health.
 *
 * `scrape_run_logs` is the append-only log (one row per scraper invocation).
 * `scrape_health` is the polymorphic current-state rollup written by a
 *   post-run reconciler, not the scraper itself.
 *
 * The Python `FailureKind` enum in scraper/run.py feeds directly into
 * `failure_kind` — keep the enum values in sync.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  smallint,
  real,
  timestamp,
  date,
  unique,
  uniqueIndex,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

export const scrapeRunLogs = pgTable(
  "scrape_run_logs",
  {
    id: serial("id").primaryKey(),
    scraperKey: text("scraper_key").notNull(),
    leagueName: text("league_name"),
    startedAt: timestamp("started_at").defaultNow().notNull(),
    completedAt: timestamp("completed_at"),
    status: text("status").notNull().default("running"),
    failureKind: text("failure_kind"),
    recordsCreated: integer("records_created").default(0).notNull(),
    recordsUpdated: integer("records_updated").default(0).notNull(),
    recordsFailed: integer("records_failed").default(0).notNull(),
    // Generated column — avoids derived-value drift. Postgres only
    // supports STORED generated columns (VIRTUAL arrives in PG 17), so
    // no explicit mode option is needed. Drizzle 0.45 emits
    // `GENERATED ALWAYS AS (...) STORED`.
    recordsTouched: integer("records_touched").generatedAlwaysAs(
      sql`records_created + records_updated`,
    ),
    errorMessage: text("error_message"),
    sourceUrl: text("source_url"),
    // Set by the Python logger from the `SCRAPE_TRIGGERED_BY` env var
    // (see scraper/scrape_run_logger.py::_triggered_by). Typical
    // values: 'scheduler' for Replit Scheduled Deployments (wrapper
    // scripts in scraper/scheduled/*.sh) and 'manual' for
    // operator-invoked runs. NOT NULL with a default so the column is
    // safe to push against existing rows — every pre-existing row
    // gets backfilled to 'manual' by Postgres during ALTER TABLE.
    triggeredBy: text("triggered_by").notNull().default("manual"),
  },
  (t) => [
    check(
      "scrape_run_logs_status_enum",
      sql`${t.status} IN ('running','ok','partial','failed')`,
    ),
    check(
      "scrape_run_logs_failure_kind_enum",
      sql`${t.failureKind} IS NULL OR ${t.failureKind} IN ('timeout','network','parse_error','zero_results','unknown')`,
    ),
    index("scrape_run_logs_scraper_started_idx").on(
      t.scraperKey,
      t.startedAt.desc(),
    ),
    index("scrape_run_logs_status_started_idx").on(
      t.status,
      t.startedAt.desc(),
    ),
  ],
);

/**
 * Polymorphic roll-up: (entity_type, entity_id) — one row per tracked
 * entity. Who writes: the scraper writes scrape_run_logs; a post-run
 * reconciler writes scrape_health.
 */
export const scrapeHealth = pgTable(
  "scrape_health",
  {
    id: serial("id").primaryKey(),
    entityType: text("entity_type").notNull(),
    entityId: integer("entity_id").notNull(),
    lastScrapedAt: timestamp("last_scraped_at"),
    lastSuccessAt: timestamp("last_success_at"),
    status: text("status").notNull().default("never"),
    confidence: real("confidence"),
    consecutiveFailures: integer("consecutive_failures").default(0).notNull(),
    lastError: text("last_error"),
    nextScheduledAt: timestamp("next_scheduled_at"),
    priority: smallint("priority"),
  },
  (t) => [
    check(
      "scrape_health_entity_type_enum",
      sql`${t.entityType} IN ('club','league','college','coach','event','match','tryout')`,
    ),
    check(
      "scrape_health_status_enum",
      sql`${t.status} IN ('ok','stale','failed','never')`,
    ),
    check(
      "scrape_health_priority_range",
      sql`${t.priority} IS NULL OR (${t.priority} >= 1 AND ${t.priority} <= 4)`,
    ),
    unique("scrape_health_entity_uq").on(t.entityType, t.entityId),
    index("scrape_health_status_scraped_idx").on(t.status, t.lastScrapedAt),
  ],
);

/**
 * raw_html_archive — Point-in-time record of every successful HTML fetch.
 *
 * The scraper gzips the response body, hashes the uncompressed bytes with
 * sha256, and uploads the gzip blob to Replit Object Storage (bucket
 * `upshift-raw-html`, key layout `YYYY/MM/DD/<sha256>.html.gz`). One row
 * here lets downstream replay / re-parse tools find the blob without
 * needing to re-fetch the source URL — which is both slower and may be
 * blocked by the origin.
 *
 * `sha256` is the content-addressable identifier; collisions on the same
 * HTML payload are skipped via the unique index (same bytes → same sha →
 * no-op insert via ON CONFLICT DO NOTHING).
 *
 * `scrape_run_log_id` is a nullable FK to `scrape_run_logs.id`. It's
 * nullable because the hook sits beneath the per-league run lifecycle —
 * ad-hoc extractor calls that don't allocate a ScrapeRunLogger still
 * archive. When present it's the integer id of the owning
 * `scrape_run_logs` row, which lets KPI 4 (raw HTML archive gap) in
 * docs/design/data-quality-slas.md join archive rows to their run
 * directly. ON DELETE SET NULL preserves the archived blob's metadata
 * even if the run-log row is ever pruned; the bucket_path still
 * identifies the object.
 *
 * Writes happen through scraper/ingest/raw_html_archive_writer.py.
 * Archiving is gated on env `ARCHIVE_RAW_HTML_ENABLED=true`; with the
 * flag unset, the hook no-ops and this table stays empty.
 */
/**
 * coach_misses — One row per (run, college, season) where the head-coach
 * extractor (inline `extract_head_coach_from_html` + the
 * `coaches-page-fallback` probe) found nothing. Populated by the NCAA
 * roster scraper when env `COACH_MISSES_REPORT_ENABLED=true`.
 *
 * Surfaced by `GET /api/v1/admin/data-quality/coach-misses` so operators
 * can see exactly which schools still have no head coach captured and
 * what URLs were tried — the input list for follow-up #55 (Playwright on
 * fallback) and any manual lookups.
 *
 * `scrape_run_log_id` is a nullable FK to `scrape_run_logs.id` (per
 * existing convention in `raw_html_archive` — ad-hoc extractor calls
 * outside a run lifecycle still record a miss with NULL run_log_id).
 *
 * Current-state semantics: at most one row per (college_id,
 * gender_program). The scraper INSERT ... ON CONFLICT DO UPDATEs to
 * refresh the row each miss, and DELETEs the row on the next
 * successful extraction. This keeps the dashboard view honest — a
 * school listed here is one we *currently* have no head coach for, not
 * a school that ever missed at some point in history. If a per-run
 * audit log of misses is later needed, that's a separate
 * append-only table.
 */
export const coachMisses = pgTable(
  "coach_misses",
  {
    id: serial("id").primaryKey(),
    scrapeRunLogId: integer("scrape_run_log_id").references(
      () => scrapeRunLogs.id,
      { onDelete: "set null" },
    ),
    collegeId: integer("college_id").notNull(),
    division: text("division").notNull(),
    genderProgram: text("gender_program").notNull(),
    rosterUrl: text("roster_url"),
    // Newline-separated list of URLs that the fallback probed before
    // giving up. Stored as text rather than text[] for portability with
    // the existing Drizzle setup (no array import churn) and because
    // the dashboard renders the list as preformatted text either way.
    probedUrls: text("probed_urls"),
    recordedAt: timestamp("recorded_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    unique("coach_misses_college_gender_uq").on(
      t.collegeId,
      t.genderProgram,
    ),
    index("coach_misses_recorded_at_idx").on(t.recordedAt.desc()),
    index("coach_misses_college_id_idx").on(t.collegeId),
  ],
);

/**
 * coverage_history — daily snapshot of the global coverage rollup that
 * powers the Coverage page's KpiStrip.
 *
 * Columns mirror the live `summarizeLeagues` rollup so the history series
 * is drop-in comparable to the current snapshot. One row per UTC day,
 * keyed by `snapshot_date`. The summary endpoint upserts today's row on
 * each call (`ON CONFLICT (snapshot_date) DO UPDATE`); subsequent calls
 * within the same day cheaply rewrite the same six counters with the
 * latest values rather than re-aggregating into a separate timeseries
 * pipeline. Reads (the trend endpoint) just `SELECT ... ORDER BY
 * snapshot_date DESC LIMIT N` — index-only by the unique key.
 */
export const coverageHistory = pgTable(
  "coverage_history",
  {
    id: serial("id").primaryKey(),
    snapshotDate: date("snapshot_date").notNull(),
    leaguesTotal: integer("leagues_total").notNull(),
    clubsTotal: integer("clubs_total").notNull(),
    clubsWithRosterSnapshot: integer("clubs_with_roster_snapshot").notNull(),
    clubsWithCoachDiscovery: integer("clubs_with_coach_discovery").notNull(),
    clubsNeverScraped: integer("clubs_never_scraped").notNull(),
    clubsStale14d: integer("clubs_stale_14d").notNull(),
    recordedAt: timestamp("recorded_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("coverage_history_snapshot_date_uq").on(t.snapshotDate),
  ],
);

export const rawHtmlArchive = pgTable(
  "raw_html_archive",
  {
    id: serial("id").primaryKey(),
    scrapeRunLogId: integer("scrape_run_log_id").references(
      () => scrapeRunLogs.id,
      { onDelete: "set null" },
    ),
    sourceUrl: text("source_url").notNull(),
    sha256: text("sha256").notNull(),
    // Object Storage key, e.g. "upshift-raw-html/2026/04/18/<sha>.html.gz".
    bucketPath: text("bucket_path").notNull(),
    // Compressed (gzip) size in bytes. The uncompressed size is not
    // stored — if a consumer needs it they can GET the blob and check
    // the gunzipped length.
    contentBytes: integer("content_bytes").notNull(),
    archivedAt: timestamp("archived_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("raw_html_archive_sha256_uq").on(t.sha256),
    index("raw_html_archive_scrape_run_log_id_idx").on(t.scrapeRunLogId),
  ],
);
