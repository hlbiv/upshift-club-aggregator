/**
 * Video sources — archive of club-associated video content from YouTube,
 * fan.hudl.com, MLS.com, and future platforms.
 *
 * Schema-only foundation. Unblocks:
 *   - Pipeline 1a (ECNL YouTube extractor)
 *   - Pipeline 3 Phase 1 (fan.hudl.com scraper)
 *
 * Canonical-club-linker pattern: `clubId` is nullable at archive time.
 * Scrapers write `clubNameRaw` and leave `clubId` NULL; a linker job
 * resolves the FK later.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  jsonb,
  unique,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { canonicalClubs } from "./index";

export const videoSources = pgTable(
  "video_sources",
  {
    id: serial("id").primaryKey(),
    // Which club does this video belong to? Nullable — link might be TBD at
    // archive time (same canonical-club-linker pattern other tables use).
    clubId: integer("club_id").references(() => canonicalClubs.id),
    clubNameRaw: text("club_name_raw"),
    // Source classification.
    sourcePlatform: text("source_platform").notNull(),
    // e.g., 'youtube', 'hudl_fan_recap', 'hudl_fan_full_game', 'hudl_broadcast', 'mls_com'
    videoType: text("video_type").notNull(),
    // e.g., 'highlight' | 'full_game' | 'documentary' | 'promo'
    externalId: text("external_id").notNull(),
    // YouTube videoId, Hudl broadcastId, etc. — platform-specific identifier
    sourceUrl: text("source_url").notNull(),
    // Canonical watch-URL or embed-URL.
    title: text("title"),
    publishedAt: timestamp("published_at", { withTimezone: true }),
    durationSeconds: integer("duration_seconds"),
    thumbnailUrl: text("thumbnail_url"),
    // Free-form metadata so platform-specific fields don't need schema
    // migrations for every new source.
    metadata: jsonb("metadata"),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    // Idempotency: a video is identified by (platform, external_id). Re-running
    // the scraper on the same channel doesn't duplicate rows.
    unique("video_sources_platform_external_id_uq").on(
      t.sourcePlatform,
      t.externalId,
    ),
    check(
      "video_sources_source_platform_enum",
      sql`${t.sourcePlatform} IN ('youtube', 'hudl_fan_recap', 'hudl_fan_full_game', 'hudl_broadcast', 'mls_com')`,
    ),
    check(
      "video_sources_video_type_enum",
      sql`${t.videoType} IN ('highlight', 'full_game', 'documentary', 'promo', 'unknown')`,
    ),
    index("video_sources_club_id_idx").on(t.clubId),
    index("video_sources_published_at_idx").on(t.publishedAt),
  ],
);

export type VideoSource = typeof videoSources.$inferSelect;
export type InsertVideoSource = typeof videoSources.$inferInsert;
