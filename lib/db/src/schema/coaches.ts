/**
 * Domain 3 — Youth Coaches (master + derived tables).
 *
 * `coaches` is the master identity table. One real coach = one row,
 * regardless of how many clubs they've worked at. `person_hash` is
 * computed as sha256 of normalized name + lowercased email (if present).
 *
 * The 4 new columns on the existing `coach_discoveries` table
 * (`coach_id` FK → coaches SET NULL, phone, first_seen_at, last_seen_at)
 * are declared inline in ./index.ts — drizzle-kit push applies them as
 * a pure ALTER. There is no separate SQL migration file.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  smallint,
  boolean,
  timestamp,
  jsonb,
  real,
  unique,
  check,
  index,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { canonicalClubs } from "./index";

/** Master identity — one real coach, one row. */
export const coaches = pgTable(
  "coaches",
  {
    id: serial("id").primaryKey(),
    personHash: text("person_hash").notNull().unique(),
    displayName: text("display_name").notNull(),
    primaryEmail: text("primary_email"),
    firstSeenAt: timestamp("first_seen_at").defaultNow().notNull(),
    lastSeenAt: timestamp("last_seen_at").defaultNow().notNull(),
    manuallyMerged: boolean("manually_merged").default(false).notNull(),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (t) => [index("coaches_display_name_idx").on(t.displayName)],
);

/**
 * Polymorphic career history. `entity_type` + `entity_id` points to
 * either canonical_clubs.id or colleges.id. App-layer validation.
 */
export const coachCareerHistory = pgTable(
  "coach_career_history",
  {
    id: serial("id").primaryKey(),
    coachId: integer("coach_id")
      .notNull()
      .references(() => coaches.id, { onDelete: "cascade" }),
    entityType: text("entity_type").notNull(),
    entityId: integer("entity_id").notNull(),
    role: text("role").notNull(),
    startYear: integer("start_year"),
    endYear: integer("end_year"),
    isCurrent: boolean("is_current").default(false).notNull(),
    source: text("source"),
    sourceUrl: text("source_url"),
    confidence: real("confidence"),
  },
  (t) => [
    check(
      "coach_career_history_entity_type_enum",
      sql`${t.entityType} IN ('club','college')`,
    ),
    check(
      "coach_career_history_role_enum",
      sql`${t.role} IN ('head_coach','assistant','doc','gk_coach','fitness','club_director','other')`,
    ),
    unique("coach_career_history_unique").on(
      t.coachId,
      t.entityType,
      t.entityId,
      t.role,
      t.startYear,
    ),
    index("coach_career_history_entity_idx").on(t.entityType, t.entityId),
  ],
);

/**
 * Append-only movement feed. Derived from weekly diffs of
 * coach_career_history + coach_scrape_snapshots.
 */
export const coachMovementEvents = pgTable(
  "coach_movement_events",
  {
    id: serial("id").primaryKey(),
    coachId: integer("coach_id")
      .notNull()
      .references(() => coaches.id, { onDelete: "cascade" }),
    eventType: text("event_type").notNull(),
    fromEntityType: text("from_entity_type"),
    fromEntityId: integer("from_entity_id"),
    toEntityType: text("to_entity_type"),
    toEntityId: integer("to_entity_id"),
    fromRole: text("from_role"),
    toRole: text("to_role"),
    detectedAt: timestamp("detected_at").defaultNow().notNull(),
    scrapeRunLogId: integer("scrape_run_log_id"),
    confidence: real("confidence"),
  },
  (t) => [
    check(
      "coach_movement_events_event_type_enum",
      sql`${t.eventType} IN ('joined','departed','promoted','role_changed','vanished')`,
    ),
    check(
      "coach_movement_events_from_entity_type_enum",
      sql`${t.fromEntityType} IS NULL OR ${t.fromEntityType} IN ('club','college')`,
    ),
    check(
      "coach_movement_events_to_entity_type_enum",
      sql`${t.toEntityType} IS NULL OR ${t.toEntityType} IN ('club','college')`,
    ),
    unique("coach_movement_events_unique").on(
      t.coachId,
      t.eventType,
      t.detectedAt,
      t.fromEntityType,
      t.fromEntityId,
    ),
    index("coach_movement_events_detected_at_idx").on(t.detectedAt),
  ],
);

export type RawStaffEntry = {
  name: string;
  title?: string | null;
  email?: string | null;
  phone?: string | null;
  [k: string]: unknown;
};

/** Raw per-scrape blobs — source of truth for weekly diffs. */
export const coachScrapeSnapshots = pgTable(
  "coach_scrape_snapshots",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id")
      .notNull()
      .references(() => canonicalClubs.id, { onDelete: "cascade" }),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
    rawStaff: jsonb("raw_staff").$type<RawStaffEntry[]>().notNull(),
    parseConfidence: real("parse_confidence"),
    staffCount: integer("staff_count").notNull().default(0),
  },
  (t) => [
    unique("coach_scrape_snapshots_club_scraped_uq").on(t.clubId, t.scrapedAt),
  ],
);

/** Materialized effectiveness — recomputed nightly from derived joins. */
export const coachEffectiveness = pgTable(
  "coach_effectiveness",
  {
    id: serial("id").primaryKey(),
    coachId: integer("coach_id")
      .notNull()
      .references(() => coaches.id, { onDelete: "cascade" }),
    playersPlacedD1: integer("players_placed_d1").default(0).notNull(),
    playersPlacedD2: integer("players_placed_d2").default(0).notNull(),
    playersPlacedD3: integer("players_placed_d3").default(0).notNull(),
    playersPlacedNaia: integer("players_placed_naia").default(0).notNull(),
    playersPlacedNjcaa: integer("players_placed_njcaa").default(0).notNull(),
    playersPlacedTotal: integer("players_placed_total").default(0).notNull(),
    clubsCoached: integer("clubs_coached").default(0).notNull(),
    seasonsTracked: smallint("seasons_tracked").default(0).notNull(),
    lastCalculatedAt: timestamp("last_calculated_at").defaultNow().notNull(),
  },
  (t) => [unique("coach_effectiveness_coach_uq").on(t.coachId)],
);

export const coachesRelations = relations(coaches, ({ many }) => ({
  careerHistory: many(coachCareerHistory),
  movementEvents: many(coachMovementEvents),
  effectiveness: many(coachEffectiveness),
}));

export const coachCareerHistoryRelations = relations(
  coachCareerHistory,
  ({ one }) => ({
    coach: one(coaches, {
      fields: [coachCareerHistory.coachId],
      references: [coaches.id],
    }),
  }),
);

export const coachMovementEventsRelations = relations(
  coachMovementEvents,
  ({ one }) => ({
    coach: one(coaches, {
      fields: [coachMovementEvents.coachId],
      references: [coaches.id],
    }),
  }),
);

export const coachScrapeSnapshotsRelations = relations(
  coachScrapeSnapshots,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [coachScrapeSnapshots.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);

export const coachEffectivenessRelations = relations(
  coachEffectiveness,
  ({ one }) => ({
    coach: one(coaches, {
      fields: [coachEffectiveness.coachId],
      references: [coaches.id],
    }),
  }),
);
