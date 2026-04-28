/**
 * Tournament match results — showcase events, cups, invitationals.
 *
 * Kept separate from `matches` (league play) because tournament structure
 * has distinct fields: flight, group, bracket_round, match_type. Cards
 * UNION both tables when rendering a player or club's full match history.
 *
 * Linker pattern: scrapers write raw team names and leave home_club_id /
 * away_club_id NULL. The canonical-club linker resolves FKs the same way
 * it does for the league matches table.
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
import { events, eventsSourceEnum } from "./events";

export const tournamentMatches = pgTable(
  "tournament_matches",
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
    tournamentName: text("tournament_name"),
    flight: text("flight"),
    groupName: text("group_name"),
    bracketRound: text("bracket_round"),
    matchType: text("match_type"),
    status: text("status").default("scheduled").notNull(),
    source: eventsSourceEnum("source"),
    sourceUrl: text("source_url"),
    platformMatchId: text("platform_match_id"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
  },
  (t) => [
    check(
      "tournament_matches_status_enum",
      sql`${t.status} IN ('scheduled','final','cancelled','forfeit','postponed')`,
    ),
    check(
      "tournament_matches_match_type_enum",
      sql`${t.matchType} IS NULL OR ${t.matchType} IN ('group','knockout','placement','friendly')`,
    ),
    uniqueIndex("tournament_matches_source_platform_id_uq")
      .on(t.source, t.platformMatchId)
      .where(sql`${t.platformMatchId} IS NOT NULL`),
    uniqueIndex("tournament_matches_natural_key_uq")
      .on(
        t.homeTeamName,
        t.awayTeamName,
        sql`COALESCE(${t.matchDate}, 'epoch'::timestamp)`,
        sql`COALESCE(${t.ageGroup}, '')`,
        sql`COALESCE(${t.gender}, '')`,
        sql`COALESCE(${t.tournamentName}, '')`,
      )
      .where(sql`${t.platformMatchId} IS NULL`),
    index("tournament_matches_home_club_date_idx").on(t.homeClubId, t.matchDate),
    index("tournament_matches_away_club_date_idx").on(t.awayClubId, t.matchDate),
    index("tournament_matches_event_id_idx").on(t.eventId),
    index("tournament_matches_tournament_name_idx").on(t.tournamentName),
  ],
);

export const tournamentMatchesRelations = relations(tournamentMatches, ({ one }) => ({
  event: one(events, {
    fields: [tournamentMatches.eventId],
    references: [events.id],
  }),
  homeClub: one(canonicalClubs, {
    fields: [tournamentMatches.homeClubId],
    references: [canonicalClubs.id],
    relationName: "tournament_matches_home_club",
  }),
  awayClub: one(canonicalClubs, {
    fields: [tournamentMatches.awayClubId],
    references: [canonicalClubs.id],
    relationName: "tournament_matches_away_club",
  }),
}));

export type TournamentMatch = typeof tournamentMatches.$inferSelect;
export type InsertTournamentMatch = typeof tournamentMatches.$inferInsert;
