/**
 * Domain: High-school match results — state-tournament edition.
 *
 * Complement to ``hs_rosters`` (MaxPreps). CIF (California
 * Interscholastic Federation, cifstate.org) publishes state-tournament
 * brackets, results, and team pages that MaxPreps does not expose.
 * This table captures the match-level rows emitted by the CIF parser
 * (bracket rows with empty scores before the games are played; the
 * same rows filled in once played).
 *
 * Canonical-schools linker pattern (see ./schools.ts):
 *   - Scrapers write ``school_name_raw`` + ``school_state`` and
 *     ``opponent_raw`` + ``school_state`` and leave BOTH ``school_id``
 *     and ``opponent_school_id`` NULL.
 *   - A follow-up linker pass
 *     (``python3 run.py --source link-canonical-schools``) resolves
 *     the FKs state-scoped. "Lincoln High" in NE and CA never collapse.
 *
 * Natural key: ``(school_name_raw, school_state, opponent_raw,
 * match_date, gender)``. ``match_date`` is part of the key because a
 * school can play the same opponent twice in a season (regional final
 * + state championship is common).
 */

import {
  pgTable,
  serial,
  text,
  integer,
  date,
  timestamp,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { canonicalSchools } from "./schools";

export const hsMatches = pgTable(
  "hs_matches",
  {
    id: serial("id").primaryKey(),
    // Nullable — populated by the canonical-school linker.
    schoolId: integer("school_id").references(() => canonicalSchools.id, {
      onDelete: "set null",
    }),
    schoolNameRaw: text("school_name_raw").notNull(),
    schoolState: text("school_state").notNull(), // 2-letter
    // Opponent side — same linker contract.
    opponentSchoolId: integer("opponent_school_id").references(
      () => canonicalSchools.id,
      { onDelete: "set null" },
    ),
    opponentRaw: text("opponent_raw").notNull(),
    matchDate: date("match_date"),
    gender: text("gender").notNull(), // "boys" | "girls"
    teamLevel: text("team_level"), // "Varsity" | "JV" (CIF is varsity-only in practice)
    // Result fields — all nullable because bracket pages publish future
    // fixtures with no score, and the same rows get filled in later.
    result: text("result"), // "W" | "L" | "T" | NULL
    scoreFor: integer("score_for"),
    scoreAgainst: integer("score_against"),
    tournament: text("tournament"), // e.g. "CIF State Championship"
    round: text("round"),           // e.g. "Regional Final", "State Final"
    season: text("season"),         // "2025-26"
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => ({
    naturalKey: uniqueIndex("hs_matches_natural_key_uq").on(
      t.schoolNameRaw,
      t.schoolState,
      t.opponentRaw,
      t.matchDate,
      t.gender,
    ),
    stateIdx: index("hs_matches_state_idx").on(t.schoolState),
    schoolIdx: index("hs_matches_school_id_idx").on(t.schoolId),
    opponentIdx: index("hs_matches_opponent_school_id_idx").on(
      t.opponentSchoolId,
    ),
    tournamentIdx: index("hs_matches_tournament_idx").on(t.tournament),
  }),
);

export const hsMatchesRelations = relations(hsMatches, ({ one }) => ({
  school: one(canonicalSchools, {
    fields: [hsMatches.schoolId],
    references: [canonicalSchools.id],
    relationName: "hsMatchesSchool",
  }),
  opponentSchool: one(canonicalSchools, {
    fields: [hsMatches.opponentSchoolId],
    references: [canonicalSchools.id],
    relationName: "hsMatchesOpponent",
  }),
}));

export type HsMatch = typeof hsMatches.$inferSelect;
export type InsertHsMatch = typeof hsMatches.$inferInsert;
