import { pgTable, serial, text, integer, timestamp } from "drizzle-orm/pg-core";
import { adminUsers } from "./admin";

/**
 * linker_ignores — operator-curated set of raw team names to skip during
 * canonical-club resolution. When a raw_team_name is in this table the
 * linker will not attempt to match it against canonical_clubs, avoiding
 * wasted fuzzy passes and preventing false-positive links for strings that
 * are known non-club tokens (e.g. "BYE", "TBD", nav-menu leakage).
 *
 * Written via POST /api/v1/admin/linker/ignore.
 * Read by the Python linker (canonical_club_linker.py) at startup.
 */
export const linkerIgnores = pgTable("linker_ignores", {
  id: serial("id").primaryKey(),
  rawTeamName: text("raw_team_name").notNull().unique(),
  reason: text("reason"),
  createdBy: integer("created_by").references(() => adminUsers.id, {
    onDelete: "set null",
  }),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});
