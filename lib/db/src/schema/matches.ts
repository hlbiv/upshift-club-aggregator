/**
 * Domain 5 — Matches + club_results.
 *
 * `matches` is the superset: covers league play AND event games.
 * `event_id` nullable: null = league match (not tournament).
 *
 * `club_results` is a materialized rollup; recomputed nightly from matches.
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
import { canonicalClubs } from "./index";
import { events } from "./events";

export const matches = pgTable(
  "matches",
  {
    id: serial("id").primaryKey(),
    eventId: integer("event_id").references(() => events.id, {
      onDelete: "set null",
    }),
    homeClubId: integer("home_club_id").references(() => canonicalClubs.id, {
      onDelete: "set null",
    }),
    awayClubId: integer("away_club_id").references(() => canonicalClubs.id, {
      onDelete: "set null",
    }),
    homeTeamName: text("home_team_name").notNull(),
    awayTeamName: text("away_team_name").notNull(),
    homeScore: integer("home_score"),
    awayScore: integer("away_score"),
    matchDate: timestamp("match_date"),
    ageGroup: text("age_group"),
    gender: text("gender"),
    division: text("division"),
    season: text("season"),
    league: text("league"),
    status: text("status").default("scheduled").notNull(),
    source: text("source"),
    sourceUrl: text("source_url"),
    platformMatchId: text("platform_match_id"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
  },
  (t) => [
    check(
      "matches_status_enum",
      sql`${t.status} IN ('scheduled','final','cancelled','forfeit','postponed')`,
    ),
    // Primary dedup key when the source provides a stable match ID.
    uniqueIndex("matches_source_platform_id_uq")
      .on(t.source, t.platformMatchId)
      .where(sql`${t.platformMatchId} IS NOT NULL`),
    // Fallback dedup for sources without stable IDs (schedule HTML scrapes).
    // Postgres treats NULL as distinct in unique indexes, so we COALESCE
    // the nullable key columns to sentinels. Without this, two schedule
    // rows with unknown match_date would never collide and would
    // re-insert every scrape.
    uniqueIndex("matches_natural_key_uq")
      .on(
        t.homeTeamName,
        t.awayTeamName,
        sql`COALESCE(${t.matchDate}, 'epoch'::timestamp)`,
        sql`COALESCE(${t.ageGroup}, '')`,
        sql`COALESCE(${t.gender}, '')`,
      )
      .where(sql`${t.platformMatchId} IS NULL`),
    index("matches_home_club_date_idx").on(t.homeClubId, t.matchDate),
    index("matches_away_club_date_idx").on(t.awayClubId, t.matchDate),
    index("matches_event_id_idx").on(t.eventId),
  ],
);

/** Materialized per-club standings. Recomputed nightly from matches. */
export const clubResults = pgTable(
  "club_results",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    season: text("season").notNull(),
    league: text("league"),
    division: text("division"),
    ageGroup: text("age_group"),
    gender: text("gender"),
    wins: integer("wins").default(0).notNull(),
    losses: integer("losses").default(0).notNull(),
    draws: integer("draws").default(0).notNull(),
    goalsFor: integer("goals_for").default(0).notNull(),
    goalsAgainst: integer("goals_against").default(0).notNull(),
    matchesPlayed: integer("matches_played").default(0).notNull(),
    lastCalculatedAt: timestamp("last_calculated_at").defaultNow().notNull(),
  },
  (t) => [
    uniqueIndex("club_results_unique").on(
      t.clubId,
      t.season,
      t.league,
      t.division,
      t.ageGroup,
      t.gender,
    ),
  ],
);

export const matchesRelations = relations(matches, ({ one }) => ({
  event: one(events, {
    fields: [matches.eventId],
    references: [events.id],
  }),
  homeClub: one(canonicalClubs, {
    fields: [matches.homeClubId],
    references: [canonicalClubs.id],
    relationName: "matches_home_club",
  }),
  awayClub: one(canonicalClubs, {
    fields: [matches.awayClubId],
    references: [canonicalClubs.id],
    relationName: "matches_away_club",
  }),
}));

export const clubResultsRelations = relations(clubResults, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [clubResults.clubId],
    references: [canonicalClubs.id],
  }),
}));
