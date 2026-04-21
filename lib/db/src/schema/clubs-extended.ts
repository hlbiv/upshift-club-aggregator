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
import { events, eventsSourceEnum } from "./events";

export const clubRosterSnapshots = pgTable(
  "club_roster_snapshots",
  {
    id: serial("id").primaryKey(),
    // Nullable by design — scrapers write snapshots with the raw club name
    // before the canonical-club linker resolves the FK. See
    // `scraper/canonical_club_linker.py`.
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    // Raw scraped club name — what the source called the club. The linker
    // reads this column to resolve `club_id`.
    clubNameRaw: text("club_name_raw").notNull(),
    // URL the snapshot was captured from. Nullable — not every source has
    // a stable roster URL.
    sourceUrl: text("source_url"),
    // When the scrape captured this roster. Defaults to now() at the DB.
    snapshotDate: timestamp("snapshot_date").defaultNow(),
    season: text("season").notNull(),
    ageGroup: text("age_group").notNull(),
    gender: text("gender").notNull(),
    division: text("division"),
    // player_name is text, never an FK. Aggregator does not have player
    // identity — cross-roster attribution is a fuzzy derived job.
    playerName: text("player_name").notNull(),
    jerseyNumber: text("jersey_number"),
    position: text("position"),
    // --- Enrichment columns (Phase 1 scraper migration) ---
    // These fields enable Player to materialize shadow_players from
    // Data's roster API instead of running its own ~27 scrapers.
    gradYear: integer("grad_year"),
    hometown: text("hometown"),
    state: text("state"),
    country: text("country"),
    nationality: text("nationality"),
    collegeCommitment: text("college_commitment"),
    academicYear: text("academic_year"),
    prevClub: text("prev_club"),
    league: text("league"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
    source: eventsSourceEnum("source"),
    eventId: integer("event_id").references(() => events.id, {
      onDelete: "set null",
    }),
  },
  (t) => [
    // Named natural-key constraint keyed on the RAW club name so scrapers
    // can upsert before the linker runs. Named (not predicate-based) to
    // survive Drizzle expression-text drift — see PR #10 matches.ts.
    unique("club_roster_snapshots_name_season_age_gender_player_uq").on(
      t.clubNameRaw,
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
