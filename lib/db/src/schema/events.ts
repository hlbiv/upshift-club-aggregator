/**
 * Domain 4 — Events (tournaments + showcases) and event_teams participation.
 *
 * Replaces the empty, wrong-shaped `club_events` table.
 * Actual match records live in ./matches.ts.
 */

import {
  pgTable,
  pgEnum,
  serial,
  text,
  integer,
  timestamp,
  unique,
  index,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { canonicalClubs } from "./index";

/**
 * Shared `source` enum for the four tables that originate from the same
 * scraper-platform vocabulary: events, event_teams, matches,
 * club_roster_snapshots. Replaces the per-table CHECK constraints so the
 * value set is enforced at the type level.
 *
 * `tryouts.source` and the various coach/college/alias `source` columns
 * are intentionally kept as free-form text — different value sets.
 */
// Value order matches the on-disk enum after 0002_split_events_source_enum.sql
// applies its `ALTER TYPE ... ADD VALUE` statements (Postgres appends new
// values to the end of the type's value list). Keep the order in sync —
// drizzle-kit compares by order when checking for drift.
export const eventsSourceEnum = pgEnum("events_source_enum", [
  "gotsport",
  "sincsports",
  "manual",
  "other",
  "totalglobalsports",
  "usclub_sanctioned",
  "athleteone",
  "mlsnext",
]);

// Separate enum for `club_roster_snapshots.source` so roster-specific
// runners (maxpreps, ncaa, soccerwire, …) don't have to overload the
// events enum. Created and the snapshot column retyped by
// 0002_split_events_source_enum.sql. Keep this list in sync with the
// CREATE TYPE statement in that migration.
export const rosterSourceEnum = pgEnum("roster_source_enum", [
  "gotsport",
  "sincsports",
  "maxpreps",
  "ncaa",
  "naia",
  "njcaa",
  "odp",
  "soccerwire",
  "club_website",
  "duda_360player",
  "manual",
  "other",
]);

export const events = pgTable(
  "events",
  {
    id: serial("id").primaryKey(),
    name: text("name").notNull(),
    slug: text("slug").notNull().unique(),
    leagueName: text("league_name"),
    season: text("season"),
    // Nullable because multi-bracket events can't have a single value.
    // Scrapers populate for single-bracket events only; `event_teams`
    // carries the real per-team bracket.
    ageGroup: text("age_group"),
    gender: text("gender"),
    division: text("division"),
    locationCity: text("location_city"),
    locationState: text("location_state"),
    startDate: timestamp("start_date"),
    endDate: timestamp("end_date"),
    registrationUrl: text("registration_url"),
    sourceUrl: text("source_url"),
    source: eventsSourceEnum("source"),
    platformEventId: text("platform_event_id"),
    lastScrapedAt: timestamp("last_scraped_at"),
  },
  (t) => [
    unique("events_source_platform_id_uq").on(t.source, t.platformEventId),
    index("events_start_date_idx").on(t.startDate),
  ],
);

/**
 * Per-tournament team participation. `canonical_club_id` may be NULL when
 * the scraper hasn't resolved the raw team name to a club yet; a nightly
 * linker job populates it. Never resolve inline at scrape time — that
 * hides scraping bugs behind dedup logic.
 *
 * Race policy: best-effort uniqueness on (event_id, team_name_raw). If
 * two runs scrape different raw names for the same team, they'll create
 * two rows; the linker collapses them via canonical_club_id.
 */
export const eventTeams = pgTable(
  "event_teams",
  {
    id: serial("id").primaryKey(),
    eventId: integer("event_id")
      .notNull()
      .references(() => events.id, { onDelete: "cascade" }),
    canonicalClubId: integer("canonical_club_id").references(
      () => canonicalClubs.id,
      { onDelete: "set null" },
    ),
    teamNameRaw: text("team_name_raw").notNull(),
    teamNameCanonical: text("team_name_canonical"),
    ageGroup: text("age_group"),
    gender: text("gender"),
    divisionCode: text("division_code"),
    registeredAt: timestamp("registered_at"),
    sourceUrl: text("source_url"),
    source: eventsSourceEnum("source"),
  },
  (t) => [
    unique("event_teams_event_team_name_uq").on(t.eventId, t.teamNameRaw),
    index("event_teams_canonical_club_idx").on(t.canonicalClubId),
  ],
);

export const eventsRelations = relations(events, ({ many }) => ({
  teams: many(eventTeams),
}));

export const eventTeamsRelations = relations(eventTeams, ({ one }) => ({
  event: one(events, {
    fields: [eventTeams.eventId],
    references: [events.id],
  }),
  canonicalClub: one(canonicalClubs, {
    fields: [eventTeams.canonicalClubId],
    references: [canonicalClubs.id],
  }),
}));
