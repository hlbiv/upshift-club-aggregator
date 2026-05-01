/**
 * Domain: Club rankings.
 *
 * Stores periodic ranking snapshots scraped from external platforms
 * (SincSports USA Rank, GotSport, USARank). One row per
 * (platform, club_name_raw, age_group, gender, season, division) natural key.
 *
 * Canonical-club-linker pattern: scrapers write ``club_name_raw`` and leave
 * ``canonical_club_id`` NULL. The linker resolves the FK after the scraper
 * writes. Do NOT try to pre-resolve ``canonical_club_id`` inside the
 * extractor.
 *
 * ``rank_value`` is the numeric position (1 = best).
 * ``rating_value`` is the raw rating string from the platform — some use
 * decimal ratings, some use tier labels (Gold/Silver/Bronze), so it is
 * stored as text and left to callers to interpret.
 */

import {
  pgTable,
  serial,
  integer,
  text,
  timestamp,
  unique,
  index,
} from "drizzle-orm/pg-core";
import { canonicalClubs } from "./index";

export const clubRankings = pgTable(
  "club_rankings",
  {
    id: serial("id").primaryKey(),
    // FK resolved by canonical-club linker; NULL at scrape time.
    canonicalClubId: integer("canonical_club_id").references(
      () => canonicalClubs.id,
      { onDelete: "set null" },
    ),
    clubNameRaw: text("club_name_raw").notNull(),
    // Platform identifier: 'sincsports' | 'gotsport' | 'usarank'
    platform: text("platform").notNull(),
    // Numeric rank (1 = best); NULL if platform only provides a rating.
    rankValue: integer("rank_value"),
    // Raw rating string (some platforms use decimals or tier labels).
    ratingValue: text("rating_value"),
    ageGroup: text("age_group"),
    gender: text("gender"),
    season: text("season"),
    division: text("division"),
    sourceUrl: text("source_url"),
    scrapedAt: timestamp("scraped_at").defaultNow(),
  },
  (t) => [
    unique("club_rankings_natural_uq").on(
      t.platform,
      t.clubNameRaw,
      t.ageGroup,
      t.gender,
      t.season,
      t.division,
    ),
    index("club_rankings_canonical_club_idx").on(t.canonicalClubId),
    index("club_rankings_platform_idx").on(t.platform),
  ],
);

export type ClubRanking = typeof clubRankings.$inferSelect;
export type InsertClubRanking = typeof clubRankings.$inferInsert;
