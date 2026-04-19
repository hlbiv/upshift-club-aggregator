/**
 * Domain: Olympic Development Program (ODP) state rosters.
 *
 * Each US state soccer association publishes a per-season ODP roster
 * (or player-pool list) split by age group and gender. This table
 * captures individual player entries. Acts as a recruiting-pipeline
 * signal adjacent to the elite-club data we already collect — many
 * ODP players are not yet on an ECNL/MLS Next roster.
 *
 * Canonical-club linker pattern (see docs/path-a-data-model.md): the
 * scraper writes `clubNameRaw` (the club the player plays for, as the
 * ODP site prints it) and leaves `clubId` NULL. The linker fills it
 * in on a follow-up pass. Not every ODP site prints a club — when
 * the source is silent `clubNameRaw` is NULL too.
 *
 * Natural key: (playerName, state, programYear, ageGroup, gender).
 * Re-running the same scrape is a no-op; moving to a new program
 * year creates a new row by design (historical retention).
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { canonicalClubs } from "./index";

export const odpRosterEntries = pgTable(
  "odp_roster_entries",
  {
    id: serial("id").primaryKey(),
    playerName: text("player_name").notNull(),
    graduationYear: integer("graduation_year"),
    position: text("position"),
    // Two-letter US state association code (e.g. "CA", "TX", "NY-E").
    // State associations don't 1:1 map to states — "PA-E" vs "PA-W",
    // "NY-E" vs "NY-W", "CA-N" vs "CA-S" — so this is free-text text
    // rather than a USPS enum.
    state: text("state").notNull(),
    // Season identifier, e.g. "2025-26".
    programYear: text("program_year").notNull(),
    ageGroup: text("age_group").notNull(),
    gender: text("gender").notNull(),
    // Canonical-club linker pattern — NULL at scrape time.
    clubId: integer("club_id").references(() => canonicalClubs.id),
    clubNameRaw: text("club_name_raw"),
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("odp_roster_entries_natural_key_uq").on(
      t.playerName,
      t.state,
      t.programYear,
      t.ageGroup,
      t.gender,
    ),
    index("odp_state_year_idx").on(t.state, t.programYear),
  ],
);

export const odpRosterEntriesRelations = relations(
  odpRosterEntries,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [odpRosterEntries.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);
