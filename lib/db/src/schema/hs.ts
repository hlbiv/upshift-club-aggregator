/**
 * Domain: High-school rosters (MaxPreps).
 *
 * MaxPreps has the widest HS sports coverage in the US. This framework
 * ships the schema + a parser + a small smoke runner. Live volume will
 * depend on proxy infrastructure (MaxPreps blocks aggressively).
 *
 * There is no HS-canonical-school linker yet: scrapers write
 * `school_name_raw` + `school_state` only. A linker pass against a
 * future `canonical_schools` table is a follow-up.
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

export const hsRosters = pgTable(
  "hs_rosters",
  {
    id: serial("id").primaryKey(),
    schoolNameRaw: text("school_name_raw").notNull(),
    schoolState: text("school_state").notNull(), // 2-letter
    schoolCity: text("school_city"),
    teamLevel: text("team_level"), // "Varsity" | "JV" | "Freshman"
    season: text("season"), // "2025-26"
    gender: text("gender").notNull(), // "boys" | "girls"
    playerName: text("player_name").notNull(),
    jerseyNumber: text("jersey_number"), // string — can be "10A" or non-numeric
    graduationYear: integer("graduation_year"),
    position: text("position"),
    height: text("height"), // e.g., "5'11\"" — stored as-is
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => ({
    naturalKey: uniqueIndex("hs_rosters_natural_key_uq").on(
      t.schoolNameRaw,
      t.schoolState,
      t.teamLevel,
      t.season,
      t.gender,
      t.playerName,
    ),
    stateIdx: index("hs_rosters_state_idx").on(t.schoolState),
    gradYearIdx: index("hs_rosters_grad_year_idx").on(t.graduationYear),
  }),
);

export type HsRoster = typeof hsRosters.$inferSelect;
export type InsertHsRoster = typeof hsRosters.$inferInsert;
