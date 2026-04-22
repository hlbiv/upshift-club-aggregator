/**
 * coach_quality_flags — admin data-quality flags on `coach_discoveries`.
 *
 * Canary infrastructure for the coach-pollution remediation effort. A
 * standing investigation confirmed ~90% of email-having `coaches` rows are
 * garbage produced by over-eager staff-page parsers: nav text ("CONTACT",
 * "STAFF"), article titles, date strings ("Mon Apr 7 2026"), and section
 * headers mistakenly extracted as coach names.
 *
 * This table is the audit trail the upcoming purge script (PR 2 of the
 * 3-PR remediation sequence) writes into before deleting any
 * `coach_discoveries` row, plus the pre-purge scanning surface a shared
 * guard (PR 1) can use to flag suspicious rows as scrapers write them.
 *
 * Mirrors the shape of `roster_quality_flags` (see
 * `./rosters-and-tryouts.ts`) one-for-one so both tables can share admin
 * UI patterns and downstream "resolve" tooling.
 *
 * flag_type is a text column with a CHECK constraint (not a pgEnum),
 * matching the repo convention for extensible enum-like columns. Adding a
 * new flag_type is a CHECK-list extension rather than an ALTER TYPE dance.
 *
 * coach_quality_flags.metadata shape by flag_type:
 *
 *   looks_like_name_reject: {
 *     reject_reason: string,     // short machine-readable reason code
 *     raw_name:      string      // the name that failed the guard
 *   }
 *   role_label_as_name: {
 *     raw_name:        string,   // e.g. "Head Coach" extracted as a name
 *     matched_pattern: string    // the pattern that caught it
 *   }
 *   corrupt_email: {
 *     raw_email:       string,   // the email as written to coach_discoveries
 *     corruption_kind: string    // short code — "nav_token" / "date_string" / ...
 *   }
 *   nav_leaked: {
 *     leaked_strings:  string[], // nav-menu tokens found in the name/email
 *     raw_name:        string
 *   }
 *   ui_fragment_as_name: {
 *     matched_raw:      string,  // the display_name that matched the gazetteer
 *     matched_category: string,  // nav_label | marketing_tile | pricing_or_date | section_heading
 *     raw_email:        string|null
 *   }
 *
 * Per-(discovery_id, flag_type) uniqueness prevents the shared guard (or
 * repeated detector runs) from inserting duplicates — the flag is idempotent
 * and the detector should upsert into the existing row instead.
 *
 * Resolution semantics mirror roster_quality_flags: flags are resolved
 * explicitly by an operator; they do not auto-expire when the underlying
 * discovery is purged. ON DELETE CASCADE on `discovery_id` cleans up flag
 * rows when PR 2's purge script deletes the discovery (the flag has already
 * done its audit-trail work by then — see the resolution_note column for
 * optional per-purge context).
 *
 * resolved_by is an FK to admin_users.id (not a string) so the admin panel
 * can join and show the resolver's real email.
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
import { coachDiscoveries } from "./index";
import { adminUsers } from "./admin";

// Column types intentionally match the referenced PKs. `coach_discoveries.id`
// and `admin_users.id` are both `serial` (int4) today — using `bigint` for
// the FK columns would make Postgres reject the constraint on a type
// mismatch. If either PK is widened to bigserial, widen these FKs to match
// in the same migration.
export const coachQualityFlags = pgTable(
  "coach_quality_flags",
  {
    id: serial("id").primaryKey(),
    discoveryId: integer("discovery_id")
      .notNull()
      .references(() => coachDiscoveries.id, { onDelete: "cascade" }),
    flagType: text("flag_type").notNull(),
    metadata: jsonb("metadata"),
    flaggedAt: timestamp("flagged_at").defaultNow().notNull(),
    resolvedAt: timestamp("resolved_at"),
    resolvedBy: integer("resolved_by").references(() => adminUsers.id, {
      onDelete: "set null",
    }),
    resolutionNote: text("resolution_note"),
  },
  (t) => [
    check(
      "coach_quality_flags_flag_type_enum",
      sql`${t.flagType} IN ('looks_like_name_reject','role_label_as_name','corrupt_email','nav_leaked','ui_fragment_as_name')`,
    ),
    unique("coach_quality_flags_discovery_type_uq").on(
      t.discoveryId,
      t.flagType,
    ),
    index("coach_quality_flags_flagged_at_idx").on(t.flaggedAt),
  ],
);

export const coachQualityFlagsRelations = relations(
  coachQualityFlags,
  ({ one }) => ({
    discovery: one(coachDiscoveries, {
      fields: [coachQualityFlags.discoveryId],
      references: [coachDiscoveries.id],
    }),
    resolver: one(adminUsers, {
      fields: [coachQualityFlags.resolvedBy],
      references: [adminUsers.id],
    }),
  }),
);

export type CoachQualityFlag = typeof coachQualityFlags.$inferSelect;
export type InsertCoachQualityFlag = typeof coachQualityFlags.$inferInsert;
