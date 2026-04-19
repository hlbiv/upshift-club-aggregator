/**
 * Domain — US Soccer Youth National Team (YNT) call-ups.
 *
 * US Soccer publishes press releases on ussoccer.com/news announcing
 * player call-ups for training camps and tournaments at each age group
 * (U-14 through U-20, boys & girls). This table captures each player
 * appearance per camp event.
 *
 * Follows the canonical-club-linker pattern: the scraper writes
 * `clubNameRaw` and leaves `clubId` NULL. The canonical-club linker
 * resolves the FK in a follow-up pass.
 *
 * The natural key is (playerName, ageGroup, gender, campEvent) — a
 * player can appear in multiple camps, but should only show up once
 * per camp within an age group / gender.
 */
import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  date,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { canonicalClubs } from "./index";

export const yntCallUps = pgTable(
  "ynt_call_ups",
  {
    id: serial("id").primaryKey(),
    playerName: text("player_name").notNull(),
    graduationYear: integer("graduation_year"),
    position: text("position"),
    // Nullable — populated by the canonical-club linker after the
    // scraper writes `clubNameRaw`.
    clubId: integer("club_id").references(() => canonicalClubs.id),
    clubNameRaw: text("club_name_raw"),
    ageGroup: text("age_group").notNull(), // "U-14" … "U-20"
    gender: text("gender").notNull(), // "boys" | "girls"
    campEvent: text("camp_event"), // e.g. "January 2026 Training Camp"
    campStartDate: date("camp_start_date"),
    campEndDate: date("camp_end_date"),
    sourceUrl: text("source_url").notNull(),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
  },
  (t) => [
    uniqueIndex("ynt_call_ups_natural_key_uq").on(
      t.playerName,
      t.ageGroup,
      t.gender,
      t.campEvent,
    ),
    index("ynt_call_ups_age_group_gender_idx").on(t.ageGroup, t.gender),
  ],
);

export const yntCallUpsRelations = relations(yntCallUps, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [yntCallUps.clubId],
    references: [canonicalClubs.id],
  }),
}));
