/**
 * Domain 11 — Commitments.
 *
 * Public college commitment announcements scraped from TopDrawerSoccer
 * (and eventually other sources). This is the highest-value missing
 * dataset for the player-platform recruiting graph.
 *
 * Design notes
 * ------------
 * - Club side follows the canonical-club-linker pattern: scrapers write
 *   `club_name_raw` and leave `club_id` NULL. The 4-pass linker in
 *   `scraper/canonical_club_linker.py` resolves the FK in a later pass.
 * - College side does the opposite: scrapers should attempt an exact
 *   match into `colleges` at write time, populate `college_id` when
 *   they hit, and always keep `college_name_raw` as the authoritative
 *   raw form for audit / fuzzy re-linking later. `college_name_raw`
 *   is NOT NULL because the natural key depends on it.
 * - Natural key is `(player_name, graduation_year, college_name_raw)`.
 *   This tolerates the common case where a player commits and the
 *   same row gets re-scraped across days / pages. `last_seen_at` is
 *   bumped on every re-scrape.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  date,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { canonicalClubs } from "./index";
import { colleges } from "./colleges";

export const commitments = pgTable(
  "commitments",
  {
    id: serial("id").primaryKey(),
    playerName: text("player_name").notNull(),
    graduationYear: integer("graduation_year"),
    position: text("position"),
    // club side — canonical-club-linker pattern: raw now, FK later
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "set null",
    }),
    clubNameRaw: text("club_name_raw"),
    // college side — link directly if we have a match
    collegeId: integer("college_id").references(() => colleges.id, {
      onDelete: "set null",
    }),
    collegeNameRaw: text("college_name_raw").notNull(),
    commitmentDate: date("commitment_date"),
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("commitments_natural_key_uq").on(
      t.playerName,
      t.graduationYear,
      t.collegeNameRaw,
    ),
    index("commitments_grad_year_idx").on(t.graduationYear),
  ],
);

export const commitmentsRelations = relations(commitments, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [commitments.clubId],
    references: [canonicalClubs.id],
  }),
  college: one(colleges, {
    fields: [commitments.collegeId],
    references: [colleges.id],
  }),
}));

export type Commitment = typeof commitments.$inferSelect;
export type InsertCommitment = typeof commitments.$inferInsert;
