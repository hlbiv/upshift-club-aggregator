import {
  pgTable,
  serial,
  text,
  timestamp,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

/**
 * api_keys — machine-to-machine credentials for the upshift-data API.
 *
 * Writes only happen via the admin CLI scripts (scripts/src/create-api-key.ts
 * and scripts/src/revoke-api-key.ts). The plaintext key is shown ONCE at
 * creation time; only the sha256 hash is persisted. `key_prefix` (first 8
 * chars of plaintext) is stored non-secretly so operators can identify a key
 * in logs / admin UIs without exposing the credential.
 */
export const apiKeys = pgTable("api_keys", {
  id: serial("id").primaryKey(),
  name: text("name").notNull(),
  keyHash: text("key_hash").notNull().unique(),
  keyPrefix: text("key_prefix").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  lastUsedAt: timestamp("last_used_at"),
  revokedAt: timestamp("revoked_at"),
  scopes: text("scopes")
    .array()
    .notNull()
    .default(sql`'{}'::text[]`),
});

export const insertApiKeySchema = createInsertSchema(apiKeys).omit({
  id: true,
  createdAt: true,
  lastUsedAt: true,
});

export type ApiKey = typeof apiKeys.$inferSelect;
export type InsertApiKey = typeof apiKeys.$inferInsert;
