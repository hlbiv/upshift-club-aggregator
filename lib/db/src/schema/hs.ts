/**
 * Domain: High-school rosters (MaxPreps).
 *
 * MaxPreps has the widest HS sports coverage in the US. This framework
 * ships the schema + a parser + a small smoke runner. Live volume will
 * depend on proxy infrastructure (MaxPreps blocks aggressively).
 *
 * Canonical-schools linker pattern: scrapers write `school_name_raw` +
 * `school_state` and leave `school_id` NULL. The linker
 * (`scraper/canonical_school_linker.py`,
 * `python3 run.py --source link-canonical-schools`) resolves the FK in a
 * follow-up pass. The canonical table lives in ./schools.ts.
 *
 * Every pass of the linker is state-scoped — "Lincoln High" in NE and CA
 * are intentionally distinct canonical rows.
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

export const hsRosters = pgTable(
  "hs_rosters",
  {
    id: serial("id").primaryKey(),
    // Nullable — populated by the canonical-school linker after the
    // scraper writes `schoolNameRaw` + `schoolState`.
    schoolId: integer("school_id").references(() => canonicalSchools.id, {
      onDelete: "set null",
    }),
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
    schoolIdx: index("hs_rosters_school_id_idx").on(t.schoolId),
  }),
);

export const hsRostersRelations = relations(hsRosters, ({ one }) => ({
  school: one(canonicalSchools, {
    fields: [hsRosters.schoolId],
    references: [canonicalSchools.id],
  }),
}));

export type HsRoster = typeof hsRosters.$inferSelect;
export type InsertHsRoster = typeof hsRosters.$inferInsert;
