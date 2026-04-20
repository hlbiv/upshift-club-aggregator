/**
 * Domain — Duplicate review decisions.
 *
 * Operator-facing record of which near-duplicate canonical-club pairs have
 * been reviewed and with what decision. Powers the admin UI's
 * "walk the duplicate queue" workflow: `GET /api/analytics/duplicates`
 * filters previously-reviewed pairs out of the default list, and
 * `POST /api/analytics/duplicates/review` upserts a decision row.
 *
 * Design notes
 * ------------
 * - Pair is normalized at write time so (a, b) and (b, a) collapse to a
 *   single row: the CHECK constraint and the unique index both assume
 *   `club_a_id < club_b_id`. The POST route swaps the ids before writing.
 * - `decided_by` is populated from `req.apiKey?.name` when the auth
 *   middleware has run; nullable so the endpoint is usable whether auth
 *   is on or off (a follow-up PR will gate writes with `requireScope`).
 * - This table only records the DECISION. The actual merge action
 *   (cascading affiliations, aliases, coach_discoveries, etc.) is a
 *   separate task pending design — see BACKLOG.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  uniqueIndex,
  check,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";
import { canonicalClubs } from "./index";

export const duplicateReviewDecisions = pgTable(
  "duplicate_review_decisions",
  {
    id: serial("id").primaryKey(),
    clubAId: integer("club_a_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    clubBId: integer("club_b_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    decision: text("decision").notNull(),
    decidedBy: text("decided_by"),
    decidedAt: timestamp("decided_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    notes: text("notes"),
  },
  (t) => [
    check(
      "duplicate_review_decisions_decision_enum",
      sql`${t.decision} IN ('pending','merged','rejected')`,
    ),
    // Normalized pair ordering — enforced so there is exactly one canonical
    // row per pair. The POST route swaps the ids before writing.
    check(
      "duplicate_review_decisions_pair_ordering",
      sql`${t.clubAId} < ${t.clubBId}`,
    ),
    uniqueIndex("duplicate_review_decisions_pair_uq").on(
      t.clubAId,
      t.clubBId,
    ),
  ],
);

export const duplicateReviewDecisionsRelations = relations(
  duplicateReviewDecisions,
  ({ one }) => ({
    clubA: one(canonicalClubs, {
      fields: [duplicateReviewDecisions.clubAId],
      references: [canonicalClubs.id],
      relationName: "duplicate_review_club_a",
    }),
    clubB: one(canonicalClubs, {
      fields: [duplicateReviewDecisions.clubBId],
      references: [canonicalClubs.id],
      relationName: "duplicate_review_club_b",
    }),
  }),
);

export const insertDuplicateReviewDecisionSchema = createInsertSchema(
  duplicateReviewDecisions,
).omit({ id: true, decidedAt: true });

export type DuplicateReviewDecision =
  typeof duplicateReviewDecisions.$inferSelect;
export type InsertDuplicateReviewDecision =
  typeof duplicateReviewDecisions.$inferInsert;
