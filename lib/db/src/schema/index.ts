import {
  pgTable,
  serial,
  text,
  boolean,
  integer,
  unique,
  timestamp,
  real,
  check,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";
// Forward reference for coach_discoveries.coach_id FK. Drizzle resolves
// the callback lazily, so the ESM import cycle (coaches.ts imports
// canonicalClubs from this file) is safe.
import { coaches } from "./coaches";

export const leaguesMaster = pgTable("leagues_master", {
  id: serial("id").primaryKey(),
  leagueName: text("league_name").notNull().unique(),
  leagueFamily: text("league_family").notNull(),
  governingBody: text("governing_body"),
  tierNumeric: integer("tier_numeric"),
  tierLabel: text("tier_label"),
  gender: text("gender"),
  geographicScope: text("geographic_scope"),
  hasPublicClubs: boolean("has_public_clubs").default(false),
  scrapePriority: text("scrape_priority"),
  sourceType: text("source_type"),
  officialUrl: text("official_url"),
  notes: text("notes"),
});

export const leagueSources = pgTable("league_sources", {
  id: serial("id").primaryKey(),
  platformName: text("platform_name").notNull(),
  sourceName: text("source_name").notNull(),
  sourceKind: text("source_kind"),
  sourceUrl: text("source_url").notNull(),
  verificationStatus: text("verification_status").default("verified"),
  notes: text("notes"),
});

export const canonicalClubs = pgTable(
  "canonical_clubs",
  {
    id: serial("id").primaryKey(),
    clubNameCanonical: text("club_name_canonical").unique().notNull(),
    clubSlug: text("club_slug").unique(),
    city: text("city"),
    state: text("state"),
    country: text("country").default("USA"),
    status: text("status").default("active"),
    website: text("website"),
    websiteDiscoveredAt: timestamp("website_discovered_at"),
    websiteStatus: text("website_status"),
    // Path A additions (see docs/path-a-data-model.md).
    logoUrl: text("logo_url"),
    foundedYear: integer("founded_year"),
    twitter: text("twitter"),
    instagram: text("instagram"),
    facebook: text("facebook"),
    staffPageUrl: text("staff_page_url"),
    websiteLastCheckedAt: timestamp("website_last_checked_at"),
    lastScrapedAt: timestamp("last_scraped_at"),
    scrapeConfidence: real("scrape_confidence"),
  },
  (t) => [
    // 'search' is included because enrich_websites.py writes it to mark
    // rows that were discovered via Brave Search API but not yet verified
    // (544 existing rows at time of migration).
    check(
      "canonical_clubs_website_status_enum",
      sql`${t.websiteStatus} IS NULL OR ${t.websiteStatus} IN ('active','dead','redirected','no_staff_page','search','unchecked')`,
    ),
  ],
);

export const clubAliases = pgTable(
  "club_aliases",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    aliasName: text("alias_name").notNull(),
    aliasSlug: text("alias_slug"),
    source: text("source"),
    isOfficial: boolean("is_official").default(false),
  },
  (t) => [unique("club_aliases_club_alias_uq").on(t.clubId, t.aliasName)],
);

export const clubAffiliations = pgTable(
  "club_affiliations",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    genderProgram: text("gender_program"),
    platformName: text("platform_name"),
    platformTier: text("platform_tier"),
    conferenceName: text("conference_name"),
    divisionName: text("division_name"),
    season: text("season"),
    sourceUrl: text("source_url"),
    sourceName: text("source_name"),
    verificationStatus: text("verification_status").default("verified"),
    notes: text("notes"),
  },
  (t) => [
    unique("club_affiliations_club_source_uq").on(t.clubId, t.sourceName),
  ],
);

// club_coaches — DROPPED April 2026.
//
// Prototype coach table that predated coach_discoveries. Absorbed into
// coach_discoveries via scripts/src/backfill-coaches-master.ts (step 1).
// On Replit the absorb step returned 0 rows because the table was already
// empty. API route (/api/coaches/search) was rewired to coach_discoveries
// in PR #3 before this drop.
//
// club_events — DROPPED April 2026.
//
// Legacy single-table events model. Replaced by `events` + `event_teams`
// (see ./events.ts). API route (/api/events/search) was rewired to the
// new two-table model in the same PR that dropped this table.

export const coachDiscoveries = pgTable(
  "coach_discoveries",
  {
    id: serial("id").primaryKey(),
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "cascade",
    }),
    name: text("name").notNull(),
    title: text("title").default("").notNull(),
    email: text("email"),
    sourceUrl: text("source_url"),
    scrapedAt: timestamp("scraped_at").defaultNow(),
    confidence: real("confidence").default(1.0),
    platformFamily: text("platform_family").default("unknown").notNull(),
    // Path A additions — coach master link, phone (absorbed from
    // club_coaches), and change-detection timestamps. `coach_id` is
    // SET NULL so merging the master row never deletes discovery history.
    coachId: integer("coach_id").references(() => coaches.id, {
      onDelete: "set null",
    }),
    phone: text("phone"),
    firstSeenAt: timestamp("first_seen_at").defaultNow(),
    lastSeenAt: timestamp("last_seen_at").defaultNow(),
  },
  (t) => [
    unique("coach_discoveries_club_name_title_uq").on(
      t.clubId,
      t.name,
      t.title,
    ),
    check(
      "coach_discoveries_confidence_range",
      sql`${t.confidence} >= 0.0 AND ${t.confidence} <= 1.0`,
    ),
    check(
      "coach_discoveries_platform_family_enum",
      sql`${t.platformFamily} IN ('sportsengine', 'leagueapps', 'wordpress', 'unknown')`,
    ),
  ],
);

export const canonicalClubsRelations = relations(
  canonicalClubs,
  ({ many }) => ({
    aliases: many(clubAliases),
    affiliations: many(clubAffiliations),
    coachDiscoveries: many(coachDiscoveries),
  }),
);

export const clubAliasesRelations = relations(clubAliases, ({ one }) => ({
  club: one(canonicalClubs, {
    fields: [clubAliases.clubId],
    references: [canonicalClubs.id],
  }),
}));

export const clubAffiliationsRelations = relations(
  clubAffiliations,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [clubAffiliations.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);

export const coachDiscoveriesRelations = relations(
  coachDiscoveries,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [coachDiscoveries.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);

export const insertLeagueMasterSchema = createInsertSchema(leaguesMaster).omit(
  { id: true },
);
export const insertCanonicalClubSchema = createInsertSchema(
  canonicalClubs,
).omit({ id: true });
export const insertClubAliasSchema = createInsertSchema(clubAliases).omit({
  id: true,
});
export const insertClubAffiliationSchema = createInsertSchema(
  clubAffiliations,
).omit({ id: true });
export const insertCoachDiscoverySchema = createInsertSchema(
  coachDiscoveries,
).omit({ id: true });

export type League = typeof leaguesMaster.$inferSelect;
export type CanonicalClub = typeof canonicalClubs.$inferSelect;
export type ClubAlias = typeof clubAliases.$inferSelect;
export type ClubAffiliation = typeof clubAffiliations.$inferSelect;
export type LeagueSource = typeof leagueSources.$inferSelect;
export type CoachDiscovery = typeof coachDiscoveries.$inferSelect;

export type InsertLeague = typeof leaguesMaster.$inferInsert;
export type InsertCanonicalClub = typeof canonicalClubs.$inferInsert;
export type InsertClubAlias = typeof clubAliases.$inferInsert;
export type InsertClubAffiliation = typeof clubAffiliations.$inferInsert;
export type InsertCoachDiscovery = typeof coachDiscoveries.$inferInsert;

// ---------------------------------------------------------------------------
// Path A additions — new domains. See docs/path-a-data-model.md.
//
// These files use `import { canonicalClubs } from "./index"` to reference
// the existing tables. Re-exports below preserve `@workspace/db/schema`
// as the single public entry point.
// ---------------------------------------------------------------------------

export * from "./clubs-extended";
export * from "./colleges";
export * from "./coaches";
export * from "./commitments";
export * from "./events";
export * from "./matches";
export * from "./odp";
export * from "./rosters-and-tryouts";
export * from "./scrape-health";
export * from "./api-keys";
export * from "./player-id-selections";
