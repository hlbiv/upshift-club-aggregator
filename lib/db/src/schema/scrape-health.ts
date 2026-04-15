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
