/**
 * college_duplicates — queue of candidate duplicate colleges pairs for
 * admin review.
 *
 * Populated by `scraper/dedup/college_dedup.py --persist`. The fuzzy sweep
 * (threshold 0.85) writes pending rows; the admin API
 * (`/api/v1/admin/dedup/colleges`) provides the human-in-the-loop review
 * interface.
 *
 * Ordered-pair uniqueness:
 *   Indexed on LEAST(left, right), GREATEST(left, right) so a pair (a, b)
 *   and (b, a) collapse to the same row. ON CONFLICT DO NOTHING keeps
 *   re-runs of the sweep idempotent.
 *
 * college_aliases:
 *   Prevents re-creation of the loser row after merge. When a scraper runs
 *   again two weeks after a merge, the winner's aliases table routes the old
 *   name back to the surviving college. Alias rows are written atomically
 *   inside `mergeColleges`.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  real,
  jsonb,
  timestamp,
  uniqueIndex,
  index,
  check,
  unique,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

import { colleges } from "./colleges";
import { adminUsers } from "./admin";

export const collegeDuplicates = pgTable(
  "college_duplicates",
  {
    id: serial("id").primaryKey(),
    leftCollegeId: integer("left_college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    rightCollegeId: integer("right_college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    // 0..1 weighted similarity score. See scraper/dedup/college_dedup.py.
    score: real("score").notNull(),
    // "name_fuzzy_88" or future methods.
    method: text("method").notNull().default("name_fuzzy_88"),
    status: text("status").notNull().default("pending"),
    // Denormalized college fields as-of pair creation. Stored so the review
    // queue can render the pair even if one side is renamed before review.
    leftSnapshot: jsonb("left_snapshot").notNull(),
    rightSnapshot: jsonb("right_snapshot").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
    reviewedBy: integer("reviewed_by").references(() => adminUsers.id, {
      onDelete: "set null",
    }),
    notes: text("notes"),
  },
  (t) => [
    // Ordered-pair uniqueness — (a, b) and (b, a) collide on this index.
    // Writer uses `ON CONFLICT (LEAST(...), GREATEST(...)) DO NOTHING`.
    uniqueIndex("college_duplicates_ordered_pair_uq").on(
      sql`LEAST(${t.leftCollegeId}, ${t.rightCollegeId})`,
      sql`GREATEST(${t.leftCollegeId}, ${t.rightCollegeId})`,
    ),
    check(
      "college_duplicates_status_enum",
      sql`${t.status} IN ('pending','merged','rejected')`,
    ),
    check(
      "college_duplicates_score_range",
      sql`${t.score} >= 0 AND ${t.score} <= 1`,
    ),
    index("college_duplicates_status_idx").on(t.status),
  ],
);

export const insertCollegeDuplicateSchema = createInsertSchema(collegeDuplicates).omit({
  id: true,
  createdAt: true,
  reviewedAt: true,
  reviewedBy: true,
});

export type CollegeDuplicate = typeof collegeDuplicates.$inferSelect;
export type InsertCollegeDuplicate = typeof collegeDuplicates.$inferInsert;

// ---------------------------------------------------------------------------
// college_aliases — prevents re-creation of the loser row after merge.
// ---------------------------------------------------------------------------

export const collegeAliases = pgTable(
  "college_aliases",
  {
    id: serial("id").primaryKey(),
    collegeId: integer("college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    aliasName: text("alias_name").notNull(),
    // The old loser id — for audit/reversibility. Populated by mergeColleges.
    mergedFromCollegeId: integer("merged_from_college_id"),
    mergedAt: timestamp("merged_at"),
  },
  (t) => [
    unique("college_aliases_college_alias_uq").on(t.collegeId, t.aliasName),
  ],
);

export const insertCollegeAliasSchema = createInsertSchema(collegeAliases).omit({
  id: true,
});

export type CollegeAlias = typeof collegeAliases.$inferSelect;
export type InsertCollegeAlias = typeof collegeAliases.$inferInsert;
