/**
 * club_duplicates — queue of candidate duplicate canonical_clubs pairs for
 * admin review.
 *
 * Populated by `scraper/dedup/club_dedup.py --persist`. The RapidFuzz sweep
 * (threshold 88) writes pending rows; the admin UI (separate PR) will read
 * them, surface them in a review queue, and mark them merged or rejected.
 *
 * Ordered-pair uniqueness:
 *   Indexed on LEAST(left, right), GREATEST(left, right) so a pair (a, b)
 *   and (b, a) collapse to the same row. ON CONFLICT DO NOTHING keeps
 *   re-runs of the sweep idempotent.
 *
 * Scope:
 *   - Queue only. Merge logic (alias redirection, affiliation reassignment,
 *     roster rehoming) lives in `scraper/dedup/canonical_club_merger.py`
 *     and will be wired to the `merged` transition in a follow-up PR.
 *   - Admin endpoints (GET /api/admin/dedup, POST merge/reject) are not yet
 *     implemented — see upshift-data-admin-api-contract.md §"Endpoints —
 *     dedup review".
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
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

import { canonicalClubs } from "./index";
import { adminUsers } from "./admin";

export const clubDuplicates = pgTable(
  "club_duplicates",
  {
    id: serial("id").primaryKey(),
    leftClubId: integer("left_club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    rightClubId: integer("right_club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    // 0..1 RapidFuzz similarity (token_set_ratio / 100 at the default
    // threshold of 88). See scraper/dedup/club_dedup.py.
    score: real("score").notNull(),
    // "name_fuzzy_88", "alias_match", future methods.
    method: text("method").notNull(),
    status: text("status").notNull().default("pending"),
    // Denormalized club fields as-of pair creation. Stored so the review
    // queue can render the pair even if one side is renamed before
    // review.
    leftSnapshot: jsonb("left_snapshot").notNull(),
    rightSnapshot: jsonb("right_snapshot").notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    reviewedAt: timestamp("reviewed_at", { withTimezone: true }),
    reviewedBy: integer("reviewed_by").references(() => adminUsers.id),
    notes: text("notes"),
  },
  (t) => [
    // Ordered-pair uniqueness — (a, b) and (b, a) collide on this index.
    // Writer uses `ON CONFLICT (LEAST(...), GREATEST(...)) DO NOTHING`.
    uniqueIndex("club_duplicates_ordered_pair_uq").on(
      sql`LEAST(${t.leftClubId}, ${t.rightClubId})`,
      sql`GREATEST(${t.leftClubId}, ${t.rightClubId})`,
    ),
    check(
      "club_duplicates_status_enum",
      sql`${t.status} IN ('pending','merged','rejected')`,
    ),
    check(
      "club_duplicates_score_range",
      sql`${t.score} >= 0 AND ${t.score} <= 1`,
    ),
    index("club_duplicates_status_idx").on(t.status),
  ],
);

export const insertClubDuplicateSchema = createInsertSchema(clubDuplicates).omit({
  id: true,
  createdAt: true,
  reviewedAt: true,
  reviewedBy: true,
});

export type ClubDuplicate = typeof clubDuplicates.$inferSelect;
export type InsertClubDuplicate = typeof clubDuplicates.$inferInsert;
