/**
 * Player iD selections (US Club Soccer).
 *
 * Captures individual player honors / call-ups to the US Club Soccer iD
 * National Pool, regional iD events, and Training Center selections.
 *
 * NOT a roster — an iD selection is a *player honor*, the selection event
 * is primary and the club is incidental. We deliberately do NOT reuse
 * `club_roster_snapshots`: that table's natural key
 * `(club_name_raw, season, age_group, gender, player_name)` assumes
 * "club roster" semantics, and per-snapshot diff materialization (added /
 * removed / jersey_changed / position_changed) does not apply to honors.
 *
 * Sources (Plan 8):
 *   - 'soccerwire'    — public SoccerWire iD pool / Training Center articles
 *   - 'usclubsoccer'  — login-gated members area (Option B; not yet wired)
 *
 * Natural key: `(player_name, selection_year, birth_year, gender, pool_tier)`.
 *
 * Why this key, and what it cannot do:
 *
 *   - There is no player-identity FK in this database (no `players` table by
 *     design — Player identity lives in `upshift-player-platform`). Two
 *     "John Smith" honors with the same selection year, birth year, gender,
 *     and pool tier will collapse into a single row.
 *   - `birth_year` is the only realistic per-honor disambiguator. Articles
 *     that omit it are unresolvable for "John Smith"-class collisions.
 *   - We capture aggressively: `region`, `state`, `club_name_raw`, and
 *     `position` are recorded when present so a downstream resolver has
 *     enough signal to attempt fuzzy person-resolution post-hoc.
 *
 * `club_id` is left NULL at insert. The canonical-club linker
 * (`scraper/canonical_club_linker.py`) resolves it on its next pass.
 */

import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  unique,
  index,
  check,
} from "drizzle-orm/pg-core";
import { sql, relations } from "drizzle-orm";
import { canonicalClubs } from "./index";

export const playerIdSelections = pgTable(
  "player_id_selections",
  {
    id: serial("id").primaryKey(),
    // Player name as printed in the source article. No FK — see module
    // docstring on the player-identity gap.
    playerName: text("player_name").notNull(),
    // Year the selection was announced (e.g. 2026). Together with
    // birth_year + pool_tier this gives the honor a stable natural key.
    selectionYear: integer("selection_year").notNull(),
    // Player's birth year (e.g. 2010). The only realistic disambiguator
    // for same-name honors; capture whenever the article lists it.
    birthYear: integer("birth_year"),
    // 'M' | 'F' (free text in case the article uses 'Boys' / 'Girls').
    gender: text("gender").notNull(),
    // 'national' | 'regional' | 'training-center'. Free text — not a
    // CHECK enum in this initial scaffold so new tiers (e.g. 'iD2')
    // don't require a schema push.
    poolTier: text("pool_tier").notNull(),
    // Optional regional bucket (e.g. 'West', 'Southeast').
    region: text("region"),
    // Raw club name as printed. The linker resolves club_id post-hoc.
    clubNameRaw: text("club_name_raw"),
    // Resolved by `scraper/canonical_club_linker.py` on its next pass.
    // Nullable by design — see module docstring.
    clubId: integer("club_id").references(() => canonicalClubs.id, {
      onDelete: "set null",
    }),
    state: text("state"),
    position: text("position"),
    sourceUrl: text("source_url"),
    // 'soccerwire' | 'usclubsoccer'. Free text in the scaffold; tighten
    // to a CHECK enum once Option B (members-area) ships.
    source: text("source").notNull(),
    // When the announcement was published (article date). Nullable
    // because some sources only carry "selection year" granularity.
    announcedAt: timestamp("announced_at"),
    scrapedAt: timestamp("scraped_at").defaultNow().notNull(),
  },
  (t) => [
    // Named natural-key constraint — mirror clubs-extended.ts:78 pattern
    // so the writer can use ON CONFLICT ON CONSTRAINT <name>.
    unique("player_id_selections_player_year_birth_gender_tier_uq").on(
      t.playerName,
      t.selectionYear,
      t.birthYear,
      t.gender,
      t.poolTier,
    ),
    index("player_id_selections_year_tier_idx").on(
      t.selectionYear,
      t.poolTier,
    ),
    index("player_id_selections_club_idx").on(t.clubId),
    check(
      "player_id_selections_source_enum",
      sql`${t.source} IN ('soccerwire','usclubsoccer')`,
    ),
  ],
);

export const playerIdSelectionsRelations = relations(
  playerIdSelections,
  ({ one }) => ({
    club: one(canonicalClubs, {
      fields: [playerIdSelections.clubId],
      references: [canonicalClubs.id],
    }),
  }),
);

export type PlayerIdSelection = typeof playerIdSelections.$inferSelect;
export type InsertPlayerIdSelection = typeof playerIdSelections.$inferInsert;
