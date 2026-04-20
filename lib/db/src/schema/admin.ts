import {
  pgTable,
  serial,
  text,
  integer,
  timestamp,
  check,
  index,
  unique,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";

/**
 * admin_users — human admin accounts for the Data admin UI.
 *
 * Bootstrapped via scripts/src/create-admin-user.ts (bcrypt 12 rounds, matches
 * the Player convention). Only the hash is persisted. Password reset is
 * CLI-only in v1 (scripts/src/reset-admin-password.ts). No MFA yet.
 *
 * Role is a CHECK-constrained text column rather than pgEnum — matches the
 * existing enum-as-CHECK pattern used throughout this schema (e.g.
 * coach_career_history_entity_type_enum).
 */
export const adminUsers = pgTable(
  "admin_users",
  {
    id: serial("id").primaryKey(),
    email: text("email").notNull(),
    passwordHash: text("password_hash").notNull(),
    role: text("role").notNull().default("admin"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    lastLoginAt: timestamp("last_login_at", { withTimezone: true }),
  },
  (t) => [
    unique("admin_users_email_uq").on(t.email),
    check(
      "admin_users_role_enum",
      sql`${t.role} IN ('admin','super_admin')`,
    ),
  ],
);

export const insertAdminUserSchema = createInsertSchema(adminUsers).omit({
  id: true,
  createdAt: true,
  lastLoginAt: true,
});

export type AdminUser = typeof adminUsers.$inferSelect;
export type InsertAdminUser = typeof adminUsers.$inferInsert;

/**
 * admin_sessions — cookie-backed session store for the admin UI.
 *
 * Session cookie carries the raw token; DB stores only the SHA256 hash, same
 * pattern as api_keys. The raw token is generated + shown ONCE at login and
 * set as upshift_admin_sid (httpOnly, secure, sameSite=lax). Expired rows are
 * swept periodically; the expires_at index supports that sweep query.
 *
 * On admin deletion, all sessions cascade-delete — no orphan auth state.
 */
export const adminSessions = pgTable(
  "admin_sessions",
  {
    id: serial("id").primaryKey(),
    adminUserId: integer("admin_user_id")
      .notNull()
      .references(() => adminUsers.id, { onDelete: "cascade" }),
    tokenHash: text("token_hash").notNull(),
    expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
    createdAt: timestamp("created_at", { withTimezone: true })
      .defaultNow()
      .notNull(),
    userAgent: text("user_agent"),
    ip: text("ip"),
  },
  (t) => [
    unique("admin_sessions_token_hash_uq").on(t.tokenHash),
    index("admin_sessions_admin_user_id_idx").on(t.adminUserId),
    index("admin_sessions_expires_at_idx").on(t.expiresAt),
  ],
);

export const insertAdminSessionSchema = createInsertSchema(adminSessions).omit({
  id: true,
  createdAt: true,
});

export type AdminSession = typeof adminSessions.$inferSelect;
export type InsertAdminSession = typeof adminSessions.$inferInsert;
