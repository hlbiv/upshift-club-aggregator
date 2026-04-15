/**
 * Domain 6 — Roster diffs (materialized from club_roster_snapshots).
 * Domain 7 — Tryouts.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  real,
  timestamp,
  jsonb,
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
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    seasonFrom: text("season_from").notNull(),
    seasonTo: text("season_to").notNull(),
    ageGroup: text("age_group").notNull(),
    gender: text("gender").notNull(),
    playersJoined: jsonb("players_joined").$type<string[]>().notNull(),
    playersDeparted: jsonb("players_departed").$type<string[]>().notNull(),
    playersRetained: jsonb("players_retained").$type<string[]>().notNull(),
    retentionRate: real("retention_rate"),
    calculatedAt: timestamp("calculated_at").defaultNow().notNull(),
  },
  (t) => [
    unique("roster_diffs_unique").on(
      t.clubId,
      t.seasonFrom,
      t.seasonTo,
      t.ageGroup,
      t.gender,
    ),
    index("roster_diffs_club_idx").on(t.clubId),
  ],
);

export const tryouts = pgTable(
  "tryouts",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
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
    // COALESCE nullable columns: NULL-distinct semantics would otherwise
    // let every re-scrape insert a duplicate tryout when date/age/gender
    // aren't known. Expression index requires uniqueIndex (not unique()).
    uniqueIndex("tryouts_club_date_bracket_uq").on(
      t.clubId,
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
