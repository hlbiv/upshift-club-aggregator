/**
 * Domain 6 — Roster diffs (per-player events, materialized from
 *            `club_roster_snapshots` season-over-season comparisons).
 * Domain 7 — Tryouts.
 *
 * Both tables follow the canonical-club-linker pattern: scrapers write
 * `clubNameRaw` and leave `clubId` NULL; `scraper/canonical_club_linker.py`
 * resolves the FK in a follow-up pass.
 *
 * `roster_diffs` was previously an aggregate-per-(clubId,seasonFrom,seasonTo,
 * ageGroup,gender) shape with jsonb arrays of joined/departed/retained
 * player names. The shape was reworked to a per-player event row so it can
 * (a) participate in the linker pattern with a natural key that doesn't
 * depend on clubId being pre-resolved and (b) carry richer diff detail
 * (jersey change, position change) without schema churn.
 *
 * Also defines `roster_quality_flags` — admin data-quality flags attached to
 * `club_roster_snapshots` rows. See the table doc below for the per-flag-type
 * metadata contract.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  jsonb,
  unique,
  uniqueIndex,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { canonicalClubs } from "./index";
import { clubSiteChanges, clubRosterSnapshots } from "./clubs-extended";
import { adminUsers } from "./admin";

export const rosterDiffs = pgTable(
  "roster_diffs",
  {
    id: serial("id").primaryKey(),
    // Nullable — populated by the canonical-club linker after the scraper
    // writes `clubNameRaw`.
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    clubNameRaw: text("club_name_raw").notNull(),
    season: text("season"),
    ageGroup: text("age_group"),
    gender: text("gender"),
    playerName: text("player_name").notNull(),
    diffType: text("diff_type").notNull(),
    fromJerseyNumber: text("from_jersey_number"),
    toJerseyNumber: text("to_jersey_number"),
    fromPosition: text("from_position"),
    toPosition: text("to_position"),
    detectedAt: timestamp("detected_at").defaultNow().notNull(),
  },
  (t) => [
    check(
      "roster_diffs_diff_type_enum",
      sql`${t.diffType} IN ('added','removed','jersey_changed','position_changed')`,
    ),
    // Named natural-key unique keyed on the RAW club name. COALESCE
    // nullable columns so pre-linker rows with NULL season/age/gender
    // don't silently re-insert every scrape.
    uniqueIndex("roster_diffs_name_season_age_gender_player_type_uq").on(
      t.clubNameRaw,
      sql`COALESCE(${t.season}, '')`,
      sql`COALESCE(${t.ageGroup}, '')`,
      sql`COALESCE(${t.gender}, '')`,
      t.playerName,
      t.diffType,
    ),
    index("roster_diffs_club_idx").on(t.clubId),
  ],
);

export const tryouts = pgTable(
  "tryouts",
  {
    id: serial("id").primaryKey(),
    // Nullable — populated by the canonical-club linker after the scraper
    // writes `clubNameRaw`.
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    clubNameRaw: text("club_name_raw").notNull(),
    ageGroup: text("age_group"),
    gender: text("gender"),
    division: text("division"),
    tryoutDate: timestamp("tryout_date"),
    registrationDeadline: timestamp("registration_deadline"),
    locationName: text("location_name"),
    locationAddress: text("location_address"),
    locationCity: text("location_city"),
    locationState: text("location_state"),
    cost: text("cost"),
    url: text("url"),
    notes: text("notes"),
    source: text("source").notNull().default("manual"),
    status: text("status").notNull().default("unknown"),
    detectedAt: timestamp("detected_at").defaultNow().notNull(),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
    expiresAt: timestamp("expires_at"),
    siteChangeId: integer("site_change_id").references(
      () => clubSiteChanges.id,
      { onDelete: "set null" },
    ),
  },
  (t) => [
    check(
      "tryouts_source_enum",
      sql`${t.source} IN ('site_monitor','gotsport','manual','other')`,
    ),
    check(
      "tryouts_status_enum",
      sql`${t.status} IN ('upcoming','active','expired','cancelled','unknown')`,
    ),
    // Named natural-key unique keyed on the RAW club name (not clubId),
    // so scrapers can upsert before the linker resolves the FK. COALESCE
    // nullable columns to sentinels — Postgres treats NULL as distinct
    // in unique indexes otherwise.
    uniqueIndex("tryouts_name_date_bracket_uq").on(
      t.clubNameRaw,
      sql`COALESCE(${t.tryoutDate}, 'epoch'::timestamp)`,
      sql`COALESCE(${t.ageGroup}, '')`,
      sql`COALESCE(${t.gender}, '')`,
    ),
    index("tryouts_club_expires_idx").on(t.clubId, t.expiresAt),
  ],
);

export const rosterDiffsRelations = relations(rosterDiffs, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [rosterDiffs.clubId],
    references: [canonicalClubs.id],
  }),
}));

export const tryoutsRelations = relations(tryouts, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [tryouts.clubId],
    references: [canonicalClubs.id],
  }),
  siteChange: one(clubSiteChanges, {
    fields: [tryouts.siteChangeId],
    references: [clubSiteChanges.id],
  }),
}));

// ---------------------------------------------------------------------------
// roster_quality_flags — admin data-quality flags on club_roster_snapshots.
// ---------------------------------------------------------------------------

/**
 * Per-snapshot data-quality flag for `club_roster_snapshots`.
 *
 * Phase 1 (this PR) ships the table + read-only API + dashboard panel. The
 * scraper-side detection heuristic that populates `roster_quality_flags` is
 * deliberately Phase 2 — the table will be empty at merge time.
 *
 * flag_type is a text column with a CHECK constraint (not a pgEnum), matching
 * the repo convention for extensible enum-like columns (see
 * `scrape_run_logs.status`, `scrape_health.status`, `canonical_clubs.website_status`).
 * Adding a new flag_type is a CHECK-list extension rather than an
 * ALTER TYPE / pg_catalog dance.
 *
 * roster_quality_flags.metadata shape by flag_type:
 *   nav_leaked_name: { leaked_strings: string[], snapshot_roster_size: number }
 *   <future flag_types>: <future shapes>
 *
 * Snapshot-supersession semantics: when a later snapshot replaces an earlier
 * one, existing flags on the earlier snapshot STAY FLAGGED (historical
 * record). They do not auto-resolve. Operators resolve flags explicitly via
 * the panel (Phase 3+).
 *
 * resolved_by is an FK to admin_users.id (not a string) so the panel can
 * join and show the resolver's real email in Phase 2+.
 *
 * Per-(snapshot_id, flag_type) uniqueness prevents the detector re-inserting
 * duplicates if it runs twice on the same snapshot — it should upsert into
 * the existing row instead.
 */
// Column types intentionally match the referenced PKs. `club_roster_snapshots.id`
// and `admin_users.id` are both `serial` (int4) today — using `bigint` for the
// FK columns would make Postgres reject the constraint on a type mismatch.
// If either PK is widened to bigserial in the future, widen these FKs to match
// in the same migration.
export const rosterQualityFlags = pgTable(
  "roster_quality_flags",
  {
    id: serial("id").primaryKey(),
    snapshotId: integer("snapshot_id")
      .notNull()
      .references(() => clubRosterSnapshots.id, { onDelete: "cascade" }),
    flagType: text("flag_type").notNull(),
    metadata: jsonb("metadata")
      .notNull()
      .default(sql`'{}'::jsonb`),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    resolvedAt: timestamp("resolved_at", { withTimezone: true }),
    resolvedBy: integer("resolved_by").references(() => adminUsers.id, {
      onDelete: "set null",
    }),
  },
  (t) => [
    check(
      "roster_quality_flags_flag_type_enum",
      sql`${t.flagType} IN ('nav_leaked_name')`,
    ),
    unique("roster_quality_flags_snapshot_type_uq").on(
      t.snapshotId,
      t.flagType,
    ),
    index("roster_quality_flags_snapshot_id_idx").on(t.snapshotId),
    // Partial index: "active flag" lookups — by-far the dominant access
    // pattern (admin panel filters resolved_at IS NULL by default).
    index("roster_quality_flags_flag_type_active_idx")
      .on(t.flagType)
      .where(sql`resolved_at IS NULL`),
  ],
);

export const rosterQualityFlagsRelations = relations(
  rosterQualityFlags,
  ({ one }) => ({
    snapshot: one(clubRosterSnapshots, {
      fields: [rosterQualityFlags.snapshotId],
      references: [clubRosterSnapshots.id],
    }),
    resolver: one(adminUsers, {
      fields: [rosterQualityFlags.resolvedBy],
      references: [adminUsers.id],
    }),
  }),
);

export type RosterQualityFlag = typeof rosterQualityFlags.$inferSelect;
export type InsertRosterQualityFlag = typeof rosterQualityFlags.$inferInsert;
