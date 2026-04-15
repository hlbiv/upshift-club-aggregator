/**
 * Club-domain additions (Domain 1).
 *
 * - `clubRosterSnapshots` — season-over-season roster captures per club
 * - `clubSiteChanges` — site-monitor diffs for change detection
 *
 * The 9 new columns on the existing `canonicalClubs` table (logo_url,
 * founded_year, socials, staff_page_url, website timestamps, scrape
 * confidence) and the `website_status` check-enum are declared inline
 * in ./index.ts — drizzle-kit push picks them up as a pure ALTER.
 * There is no separate SQL migration file.
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
import { sql, relations } from "drizzle-orm";
import { canonicalClubs } from "./index";
import { events } from "./events";

export const clubRosterSnapshots = pgTable(
  "club_roster_snapshots",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    season: text("season").notNull(),
    ageGroup: text("age_group").notNull(),
    gender: text("gender").notNull(),
    division: text("division"),
    // player_name is text, never an FK. Aggregator does not have player
    // identity — cross-roster attribution is a fuzzy derived job.
    playerName: text("player_name").notNull(),
    jerseyNumber: text("jersey_number"),
    position: text("position"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
    source: text("source"),
    eventId: integer("event_id").references(() => events.id, {
      onDelete: "set null",
    }),
  },
  (t) => [
    unique("club_roster_snapshots_unique").on(
      t.clubId,
      t.season,
      t.ageGroup,
      t.gender,
      t.playerName,
    ),
    index("club_roster_snapshots_club_season_idx").on(t.clubId, t.season),
  ],
);

export type ClubSiteChangeDetail = {
  added?: string[];
  removed?: string[];
  summary?: string;
  [k: string]: unknown;
};

export const clubSiteChanges = pgTable(
  "club_site_changes",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    changeType: text("change_type").notNull(),
    changeDetail: jsonb("change_detail").$type<ClubSiteChangeDetail>(),
    detectedAt: timestamp("detected_at").defaultNow().notNull(),
    pageUrl: text("page_url"),
    snapshotHashBefore: text("snapshot_hash_before"),
    snapshotHashAfter: text("snapshot_hash_after"),
  },
  (t) => [
    check(
      "club_site_changes_change_type_enum",
      sql`${t.changeType} IN ('staff_added','staff_removed','tryout_posted','announcement','site_redesign','page_404')`,
    ),
    unique("club_site_changes_unique").on(
      t.clubId,
      t.snapshotHashBefore,
      t.snapshotHashAfter,
      t.changeType,
    ),
    index("club_site_changes_club_detected_idx").on(t.clubId, t.detectedAt),
  ],
);

export const clubRosterSnapshotsRelations = relations(
  clubRosterSnapshots,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [clubRosterSnapshots.clubId],
      references: [canonicalClubs.id],
    }),
    event: one(events, {
      fields: [clubRosterSnapshots.eventId],
      references: [events.id],
    }),
  }),
);

export const clubSiteChangesRelations = relations(
  clubSiteChanges,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [clubSiteChanges.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);
