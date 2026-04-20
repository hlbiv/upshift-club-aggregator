/**
 * Domain: Canonical high-school directory + aliases.
 *
 * Mirrors the canonical_clubs / club_aliases pattern (see ./index.ts) for
 * the HS roster domain. Scrapers write `school_name_raw` + `school_state`
 * into hs_rosters and leave `hs_rosters.school_id` NULL; a linker pass
 * (`scraper/canonical_school_linker.py`,
 * `python3 run.py --source link-canonical-schools`) runs a 4-pass
 * resolver and fills the FK in.
 *
 * CRITICAL: state is part of every join key. "Lincoln High" in NE and
 * "Lincoln High" in CA must NEVER collapse. The `(name, state)` unique on
 * canonical_schools and the `(alias, state)` unique on school_aliases
 * enforce that at the DB layer; the linker enforces it at the query
 * layer by scoping every pass to the source row's `school_state`.
 *
 * Starts empty — no seed data. Rows are populated manually (operator
 * curation) or by the linker when a fuzzy hit surfaces a new alias.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  unique,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

export const canonicalSchools = pgTable(
  "canonical_schools",
  {
    id: serial("id").primaryKey(),
    schoolNameCanonical: text("school_name_canonical").notNull(),
    schoolState: text("school_state").notNull(), // 2-letter
    website: text("website"),
    mascot: text("mascot"),
    city: text("city"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    unique("canonical_schools_name_state_uq").on(
      t.schoolNameCanonical,
      t.schoolState,
    ),
  ],
);

export const schoolAliases = pgTable(
  "school_aliases",
  {
    id: serial("id").primaryKey(),
    schoolId: integer("school_id")
      .notNull()
      .references(() => canonicalSchools.id, { onDelete: "cascade" }),
    aliasName: text("alias_name").notNull(),
    schoolState: text("school_state").notNull(), // 2-letter, must match parent
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    unique("school_aliases_alias_state_uq").on(t.aliasName, t.schoolState),
  ],
);

export const canonicalSchoolsRelations = relations(
  canonicalSchools,
  ({ many }) => ({
    aliases: many(schoolAliases),
  }),
);

export const schoolAliasesRelations = relations(schoolAliases, ({ one }) => ({
  school: one(canonicalSchools, {
    fields: [schoolAliases.schoolId],
    references: [canonicalSchools.id],
  }),
}));

export const insertCanonicalSchoolSchema = createInsertSchema(
  canonicalSchools,
).omit({ id: true });
export const insertSchoolAliasSchema = createInsertSchema(schoolAliases).omit({
  id: true,
});

export type CanonicalSchool = typeof canonicalSchools.$inferSelect;
export type SchoolAlias = typeof schoolAliases.$inferSelect;
export type InsertCanonicalSchool = typeof canonicalSchools.$inferInsert;
export type InsertSchoolAlias = typeof schoolAliases.$inferInsert;
