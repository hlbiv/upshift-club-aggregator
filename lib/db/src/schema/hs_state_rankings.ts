/**
 * Domain: High-school state-level rankings / polls.
 *
 * State associations (starting with CIF California) publish weekly
 * ranking / poll tables. Schools are ordered with records, and often
 * grouped by section (e.g. "CIF Northern California",
 * "CIF Southern Section — Division I").
 *
 * Same canonical-schools linker pattern as ``hs_rosters`` +
 * ``hs_matches`` — scrapers write ``school_name_raw`` and leave
 * ``school_id`` NULL; the state-scoped linker resolves the FK later.
 *
 * Natural key: ``(state, gender, season, school_name_raw, rank)``.
 * ``rank`` is part of the key because a school can hold #1 in boys
 * Division I and #3 in boys Open Division in the same season — those
 * are two separate rows on the CIF poll page (different ``section``
 * values) but share the same natural-key triple up to ``rank``.
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
import { canonicalSchools } from "./schools";

export const hsStateRankings = pgTable(
  "hs_state_rankings",
  {
    id: serial("id").primaryKey(),
    state: text("state").notNull(),         // 2-letter
    gender: text("gender").notNull(),       // "boys" | "girls"
    season: text("season").notNull(),       // "2025-26"
    rank: integer("rank").notNull(),
    // Linker-resolved FK.
    schoolId: integer("school_id").references(() => canonicalSchools.id, {
      onDelete: "set null",
    }),
    schoolNameRaw: text("school_name_raw").notNull(),
    record: text("record"),    // "18-2-1" — kept as text; interpretation varies
    points: integer("points"),
    section: text("section"),  // e.g. "CIF Northern California"
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => ({
    naturalKey: uniqueIndex("hs_state_rankings_natural_key_uq").on(
      t.state,
      t.gender,
      t.season,
      t.schoolNameRaw,
      t.rank,
    ),
    stateSeasonIdx: index("hs_state_rankings_state_season_idx").on(
      t.state,
      t.season,
    ),
    schoolIdx: index("hs_state_rankings_school_id_idx").on(t.schoolId),
  }),
);

export const hsStateRankingsRelations = relations(
  hsStateRankings,
  ({ one }) => ({
    school: one(canonicalSchools, {
      fields: [hsStateRankings.schoolId],
      references: [canonicalSchools.id],
    }),
  }),
);

export type HsStateRanking = typeof hsStateRankings.$inferSelect;
export type InsertHsStateRanking = typeof hsStateRankings.$inferInsert;
