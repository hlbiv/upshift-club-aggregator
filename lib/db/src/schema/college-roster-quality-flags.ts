/**
 * college_roster_quality_flags — admin data-quality flags on `colleges` rows.
 *
 * Tracks scraper failure modes so operators can triage URL gaps and parser
 * regressions. Three flag_types:
 *
 *   historical_no_data — URL works for the current season but not the
 *     historical season path (archive never existed or was removed).
 *
 *   partial_parse — URL fetched 200 OK but fewer than the small-roster
 *     threshold (typically 5) players were parsed. Parser selector may be
 *     broken or the program has a genuinely small roster.
 *
 *   url_needs_review — Catch-all for URL-level failures, classified further
 *     by metadata->>'reason' (see 6-value enum below). This is the workflow
 *     entrypoint for the kid worklist (operator sees a reason code and knows
 *     what to do next).
 *
 * url_needs_review reason values (stored in metadata.reason):
 *   no_url_at_all          — colleges.soccer_program_url IS NULL
 *   static_404             — URL exists but page 404'd (school migrated CMS)
 *   playwright_exhausted   — Static fail + Playwright fail (JS-only or auth wall)
 *   partial_parse          — URL works but returned < SMALL_ROSTER_THRESHOLD
 *   historical_no_data     — URL works for current but not historical season
 *   current_zero_parse     — Current-season URL returned 200 but parser found 0
 *
 * Per-(college_id, academic_year, flag_type) uniqueness prevents re-insertion
 * on repeat scrape runs — the writer upserts via ON CONFLICT.
 *
 * Resolution semantics mirror roster_quality_flags:
 *   'resolved'  — the issue was fixed out of band (URL updated, parser fixed)
 *   'dismissed' — false positive; the college was intentionally skipped
 *
 * See docs/multi-sport-schema-contract.md for the full design rationale and
 * the 6-value reason enum contract.
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
import { colleges } from "./colleges";
import { adminUsers } from "./admin";

export const collegeRosterQualityFlags = pgTable(
  "college_roster_quality_flags",
  {
    id: serial("id").primaryKey(),
    collegeId: integer("college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    academicYear: text("academic_year").notNull(),
    flagType: text("flag_type").notNull(),
    metadata: jsonb("metadata")
      .notNull()
      .default(sql`'{}'::jsonb`),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    resolvedAt: timestamp("resolved_at"),
    resolvedBy: integer("resolved_by").references(() => adminUsers.id, {
      onDelete: "set null",
    }),
    resolutionNote: text("resolution_note"),
  },
  (t) => [
    check(
      "college_roster_quality_flags_flag_type_enum",
      sql`${t.flagType} IN ('historical_no_data','partial_parse','url_needs_review')`,
    ),
    check(
      "college_roster_quality_flags_resolution_reason",
      sql`(
        ${t.resolvedAt} IS NULL AND ${t.resolutionNote} IS NULL
      ) OR (
        ${t.resolvedAt} IS NOT NULL
      )`,
    ),
    unique("college_roster_quality_flags_college_year_type_uq").on(
      t.collegeId,
      t.academicYear,
      t.flagType,
    ),
    index("college_roster_quality_flags_college_id_idx").on(t.collegeId),
    index("college_roster_quality_flags_active_idx")
      .on(t.flagType)
      .where(sql`resolved_at IS NULL`),
  ],
);

export const collegeRosterQualityFlagsRelations = relations(
  collegeRosterQualityFlags,
  ({ one }) => ({
    college: one(colleges, {
      fields: [collegeRosterQualityFlags.collegeId],
      references: [colleges.id],
    }),
    resolver: one(adminUsers, {
      fields: [collegeRosterQualityFlags.resolvedBy],
      references: [adminUsers.id],
    }),
  }),
);

export type CollegeRosterQualityFlag =
  typeof collegeRosterQualityFlags.$inferSelect;
export type InsertCollegeRosterQualityFlag =
  typeof collegeRosterQualityFlags.$inferInsert;
