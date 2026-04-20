/**
 * Admin-session helpers. Cookie-backed session store for the Data admin UI.
 *
 * The raw session token is only ever held in memory at the moment of creation
 * (and in the caller's browser, as the `upshift_admin_sid` cookie). The DB
 * stores a SHA256 hash in `admin_sessions.token_hash` — same pattern as
 * `api_keys.key_hash`. A lost cookie cannot be recovered from the database.
 *
 * TTL: 12-hour rolling idle expiry. Each time `findAdminSessionByTokenHash`
 * returns a live row, the caller is expected to `bumpSessionExpiry(id)` to
 * slide the window forward. Absolute max session lifetime is not enforced
 * here — add a `max_lifetime` column + check if that becomes a requirement.
 */
import crypto from "node:crypto";
import { and, eq, gt } from "drizzle-orm";
import { db } from "./index";
import {
  adminSessions,
  type AdminSession,
  type InsertAdminSession,
} from "./schema/admin";

/** Rolling idle TTL applied to every session on create and on every use. */
export const ADMIN_SESSION_TTL_MS = 12 * 60 * 60 * 1000;

/**
 * Generate a cryptographically random session token. 32 random bytes encoded
 * as base64url (URL-safe, no padding) — 43 chars, roughly the same strength
 * as the 64-hex-char M2M API keys.
 */
export function generateSessionToken(): string {
  return crypto.randomBytes(32).toString("base64url");
}

/** SHA256 hex digest used to store and look up admin sessions. */
export function hashSessionToken(raw: string): string {
  return crypto.createHash("sha256").update(raw).digest("hex");
}

/**
 * Create a new session row for an admin user and return the plaintext token
 * plus its expiry. The caller MUST send the plaintext back to the browser as
 * the `upshift_admin_sid` cookie immediately — it is not recoverable later.
 *
 * `userAgent` / `ip` are best-effort audit fields.
 */
export async function createAdminSession(
  adminUserId: number,
  userAgent: string | null,
  ip: string | null,
): Promise<{ token: string; expiresAt: Date }> {
  const token = generateSessionToken();
  const tokenHash = hashSessionToken(token);
  const expiresAt = new Date(Date.now() + ADMIN_SESSION_TTL_MS);

  const insertValues: InsertAdminSession = {
    adminUserId,
    tokenHash,
    expiresAt,
    userAgent,
    ip,
  };

  await db.insert(adminSessions).values(insertValues);

  return { token, expiresAt };
}

/**
 * Look up a session row by its token hash. Returns null for a miss OR for
 * an expired row — same indistinguishable-failure posture as
 * `findApiKeyByHash`. Expiry is enforced in SQL (`expires_at > now()`) so a
 * stale row never leaks into application code.
 */
export async function findAdminSessionByTokenHash(
  hash: string,
): Promise<AdminSession | null> {
  const now = new Date();
  const rows = await db
    .select()
    .from(adminSessions)
    .where(and(eq(adminSessions.tokenHash, hash), gt(adminSessions.expiresAt, now)))
    .limit(1);

  return rows[0] ?? null;
}

/**
 * Delete a session row by its token hash. Idempotent — a miss is a no-op.
 * Called from the logout handler.
 */
export async function deleteAdminSession(hash: string): Promise<void> {
  await db.delete(adminSessions).where(eq(adminSessions.tokenHash, hash));
}

/**
 * Slide a session's `expires_at` forward to `now + ADMIN_SESSION_TTL_MS`.
 * Called after every successful `findAdminSessionByTokenHash` hit so the
 * 12-hour idle timer resets on each use.
 *
 * Not wrapped in the find-lookup because the caller may want to skip the
 * bump under certain conditions (e.g. read-only health probes) in the future.
 */
export async function bumpSessionExpiry(id: number): Promise<void> {
  const newExpiry = new Date(Date.now() + ADMIN_SESSION_TTL_MS);
  await db
    .update(adminSessions)
    .set({ expiresAt: newExpiry })
    .where(eq(adminSessions.id, id));
}
