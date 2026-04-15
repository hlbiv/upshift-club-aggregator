/**
 * Domain 2 — Colleges.
 *
 * Reference data for college programs (D1/D2/D3/NAIA/NJCAA),
 * their coaching staff, and season-over-season rosters.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  boolean,
  timestamp,
  real,
  unique,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { coaches } from "./coaches";

export const colleges = pgTable(
  "colleges",
  {
    id: serial("id").primaryKey(),
    name: text("name").notNull(),
    slug: text("slug").notNull().unique(),
    ncaaId: text("ncaa_id"),
    division: text("division").notNull(),
    conference: text("conference"),
    state: text("state"),
    city: text("city"),
    website: text("website"),
    soccerProgramUrl: text("soccer_program_url"),
    genderProgram: text("gender_program").notNull(),
    enrollment: integer("enrollment"),
    scholarshipAvailable: boolean("scholarship_available"),
    logoUrl: text("logo_url"),
    twitter: text("twitter"),
    lastScrapedAt: timestamp("last_scraped_at"),
    scrapeConfidence: real("scrape_confidence"),
  },
  (t) => [
    check(
      "colleges_division_enum",
      sql`${t.division} IN ('D1','D2','D3','NAIA','NJCAA')`,
    ),
    check(
      "colleges_gender_program_enum",
      sql`${t.genderProgram} IN ('mens','womens','both')`,
    ),
    unique("colleges_name_division_gender_uq").on(
      t.name,
      t.division,
      t.genderProgram,
    ),
  ],
);

export const collegeCoaches = pgTable(
  "college_coaches",
  {
    id: serial("id").primaryKey(),
    collegeId: integer("college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    coachId: integer("coach_id").references(() => coaches.id, {
      onDelete: "set null",
    }),
    name: text("name").notNull(),
    title: text("title"),
    email: text("email"),
    phone: text("phone"),
    twitter: text("twitter"),
    linkedin: text("linkedin"),
    isHeadCoach: boolean("is_head_coach").default(false).notNull(),
    source: text("source"),
    sourceUrl: text("source_url"),
    scrapedAt: timestamp("scraped_at").defaultNow(),
    confidence: real("confidence"),
    firstSeenAt: timestamp("first_seen_at").defaultNow().notNull(),
    lastSeenAt: timestamp("last_seen_at").defaultNow().notNull(),
  },
  (t) => [
    unique("college_coaches_college_name_title_uq").on(
      t.collegeId,
      t.name,
      t.title,
    ),
    index("college_coaches_coach_id_idx").on(t.coachId),
  ],
);

export const collegeRosterHistory = pgTable(
  "college_roster_history",
  {
    id: serial("id").primaryKey(),
    collegeId: integer("college_id")
      .notNull()
      .references(() => colleges.id, { onDelete: "cascade" }),
    playerName: text("player_name").notNull(),
    position: text("position"),
    year: text("year"),
    academicYear: text("academic_year").notNull(),
    hometown: text("hometown"),
    prevClub: text("prev_club"),
    jerseyNumber: text("jersey_number"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
  },
  (t) => [
    check(
      "college_roster_history_year_enum",
      sql`${t.year} IS NULL OR ${t.year} IN ('freshman','sophomore','junior','senior','grad')`,
    ),
    unique("college_roster_history_college_player_year_uq").on(
      t.collegeId,
      t.playerName,
      t.academicYear,
    ),
    index("college_roster_history_college_year_idx").on(
      t.collegeId,
      t.academicYear,
    ),
  ],
);

export const collegesRelations = relations(colleges, ({ many }) => ({
  coaches: many(collegeCoaches),
  rosters: many(collegeRosterHistory),
}));

export const collegeCoachesRelations = relations(collegeCoaches, ({ one }) => ({
  college: one(colleges, {
    fields: [collegeCoaches.collegeId],
    references: [colleges.id],
  }),
  coach: one(coaches, {
    fields: [collegeCoaches.coachId],
    references: [coaches.id],
  }),
}));

export const collegeRosterHistoryRelations = relations(
  collegeRosterHistory,
  ({ one }) => ({
    college: one(colleges, {
      fields: [collegeRosterHistory.collegeId],
      references: [colleges.id],
    }),
  }),
);
