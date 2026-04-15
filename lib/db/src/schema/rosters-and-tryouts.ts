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
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  unique,
  uniqueIndex,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { canonicalClubs } from "./index";
import { clubSiteChanges } from "./clubs-extended";

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
