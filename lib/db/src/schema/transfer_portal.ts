/**
 * Domain 12 — NCAA Transfer Portal.
 *
 * Public college-transfer announcements scraped from TopDrawerSoccer
 * transfer-tracker articles:
 *
 *   /college-soccer-articles/{year}-{mens|womens}-division-i-transfer-tracker_aidNNNNN
 *
 * Each tracker page publishes a single HTML <table> with three columns:
 *   Name | Outgoing College | Incoming College
 *
 * Position is embedded as a prefix on the Name cell (e.g. "D/F Chloe
 * Bryant", "M Reece Paget", "GK Some Name"). The parser splits the
 * prefix tokens off as `position` and leaves `player_name` clean.
 *
 * Both destination and origin are always present on tracker pages —
 * TDS only lists players whose transfer has been officially announced
 * (either by the player or the receiving school). So
 * `to_college_name_raw` is notNull, matching `from_college_name_raw`.
 *
 * Design notes
 * ------------
 * - From/To college sides follow the same pattern as `commitments.college_id`:
 *   scrapers attempt an exact match into `colleges` at write time and
 *   populate `from_college_id` / `to_college_id` when they hit. Raw
 *   names are preserved as the authoritative form for audit and fuzzy
 *   re-linking later.
 * - Natural key is `(player_name, from_college_name_raw, season_window)`.
 *   `season_window` encodes year + gender + division + tracker-half
 *   (e.g. "2026-womens-di-mid-year", "2026-mens-di-summer") so the
 *   same player entering the portal in successive windows produces
 *   distinct rows. `last_seen_at` is bumped on every re-scrape of the
 *   same row.
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
import { colleges } from "./colleges";

export const transferPortalEntries = pgTable(
  "transfer_portal_entries",
  {
    id: serial("id").primaryKey(),
    playerName: text("player_name").notNull(),
    position: text("position"),

    // Origin side — try exact match at write time; linker fills fuzzy.
    fromCollegeId: integer("from_college_id").references(() => colleges.id, {
      onDelete: "set null",
    }),
    fromCollegeNameRaw: text("from_college_name_raw").notNull(),

    // Destination side — same contract as origin. NotNull because TDS
    // tracker pages only list committed destinations.
    toCollegeId: integer("to_college_id").references(() => colleges.id, {
      onDelete: "set null",
    }),
    toCollegeNameRaw: text("to_college_name_raw").notNull(),

    // Composite label that encodes the TDS tracker article identity.
    // Examples: "2026-womens-di-mid-year", "2026-mens-di-summer".
    seasonWindow: text("season_window").notNull(),
    gender: text("gender").notNull(), // "mens" | "womens"
    division: text("division").notNull(), // "d1" | "d2" | "d3" (TDS currently only publishes D1)

    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("transfer_portal_entries_natural_key_uq").on(
      t.playerName,
      t.fromCollegeNameRaw,
      t.seasonWindow,
    ),
    index("transfer_portal_entries_season_window_idx").on(t.seasonWindow),
    index("transfer_portal_entries_to_college_id_idx").on(t.toCollegeId),
    index("transfer_portal_entries_from_college_id_idx").on(t.fromCollegeId),
  ],
);

export const transferPortalEntriesRelations = relations(
  transferPortalEntries,
  ({ one }) => ({
    fromCollege: one(colleges, {
      fields: [transferPortalEntries.fromCollegeId],
      references: [colleges.id],
      relationName: "fromCollege",
    }),
    toCollege: one(colleges, {
      fields: [transferPortalEntries.toCollegeId],
      references: [colleges.id],
      relationName: "toCollege",
    }),
  }),
);

export type TransferPortalEntry = typeof transferPortalEntries.$inferSelect;
export type InsertTransferPortalEntry =
  typeof transferPortalEntries.$inferInsert;
