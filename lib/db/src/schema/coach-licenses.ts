/**
 * USSF coaching license tiers per coach.
 *
 * Populated via two paths:
 *   Path A (immediate): scripts/src/import-coach-licenses.ts — idempotent
 *     CSV importer. Export CSV from the USSF directory while authenticated,
 *     then run the importer.
 *   Path B (automated, deferred): scraper/extractors/ussf_licenses.py —
 *     Playwright + Auth0 flow. Gated on TOS review.
 *
 * Natural key: (coach_id, license_tier, state). A coach can hold multiple
 * license tiers (e.g. B + Goalkeeping C) and can be licensed in multiple
 * states (less common). coach_id is nullable so rows survive coach master
 * table deletion — the license data is the source of truth here.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  uniqueIndex,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { coaches } from "./coaches";

export const coachLicenses = pgTable(
  "coach_licenses",
  {
    id: serial("id").primaryKey(),
    coachId: integer("coach_id").references(() => coaches.id, {
      onDelete: "set null",
    }),
    licenseTier: text("license_tier").notNull(),
    state: text("state"),
    issueDate: timestamp("issue_date"),
    expiresAt: timestamp("expires_at"),
    sourceUrl: text("source_url"),
    firstSeenAt: timestamp("first_seen_at").defaultNow().notNull(),
    lastSeenAt: timestamp("last_seen_at").defaultNow().notNull(),
  },
  (t) => [
    check(
      "coach_licenses_tier_enum",
      sql`${t.licenseTier} IN ('grassroots_online','grassroots_in_person','D','C','B','A','Pro')`,
    ),
    uniqueIndex("coach_licenses_coach_tier_state_uq")
      .on(t.coachId, t.licenseTier, sql`COALESCE(${t.state}, '')`)
      .where(sql`${t.coachId} IS NOT NULL`),
    index("coach_licenses_coach_id_idx").on(t.coachId),
    index("coach_licenses_tier_idx").on(t.licenseTier),
  ],
);

export const coachLicensesRelations = relations(coachLicenses, ({ one }) => ({
  coach: one(coaches, {
    fields: [coachLicenses.coachId],
    references: [coaches.id],
  }),
}));

export type CoachLicense = typeof coachLicenses.$inferSelect;
export type InsertCoachLicense = typeof coachLicenses.$inferInsert;
