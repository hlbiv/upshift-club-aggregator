import {
  pgTable,
  serial,
  text,
  boolean,
  integer,
  unique,
  timestamp,
} from "drizzle-orm/pg-core";
import { relations } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

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

export const canonicalClubs = pgTable("canonical_clubs", {
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
});

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

export const canonicalClubsRelations = relations(
  canonicalClubs,
  ({ many }) => ({
    aliases: many(clubAliases),
    affiliations: many(clubAffiliations),
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

export type League = typeof leaguesMaster.$inferSelect;
export type CanonicalClub = typeof canonicalClubs.$inferSelect;
export type ClubAlias = typeof clubAliases.$inferSelect;
export type ClubAffiliation = typeof clubAffiliations.$inferSelect;
export type LeagueSource = typeof leagueSources.$inferSelect;

export type InsertLeague = typeof leaguesMaster.$inferInsert;
export type InsertCanonicalClub = typeof canonicalClubs.$inferInsert;
export type InsertClubAlias = typeof clubAliases.$inferInsert;
export type InsertClubAffiliation = typeof clubAffiliations.$inferInsert;
