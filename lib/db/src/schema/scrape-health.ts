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
  unique,
  uniqueIndex,
  check,
  index,
  uuid,
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
 * `run_id` is nullable because the hook sits beneath the per-league run
 * lifecycle — ad-hoc extractor calls that don't allocate a ScrapeRunLogger
 * still archive. When present it's a UUID tagging the archive row to a
 * logical scrape run (not an FK to scrape_run_logs.id, which is a serial).
 *
 * Writes happen through scraper/ingest/raw_html_archive_writer.py.
 * Archiving is gated on env `ARCHIVE_RAW_HTML_ENABLED=true`; with the
 * flag unset, the hook no-ops and this table stays empty.
 */
export const rawHtmlArchive = pgTable(
  "raw_html_archive",
  {
    id: serial("id").primaryKey(),
    runId: uuid("run_id"),
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
    index("raw_html_archive_run_id_idx").on(t.runId),
  ],
);
