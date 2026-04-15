/**
 * API-key helpers. Single source of truth for generating, hashing, and
 * looking up M2M API credentials. Imported by the api-server middleware and
 * by the create-/revoke- CLI scripts.
 */
import { eq } from "drizzle-orm";
import { db } from "./index";
import { apiKeys, type ApiKey } from "./schema/api-keys";

// hashApiKey / generateApiKey live in a pure-function module that doesn't
// import db — see ./api-keys-crypto.ts. We re-export them here so callers
// can `import { hashApiKey, generateApiKey, findApiKeyByHash } from "@workspace/db"`.
export { hashApiKey, generateApiKey } from "./api-keys-crypto";
export type { GeneratedApiKey } from "./api-keys-crypto";

/**
 * Look up a key by its sha256 hash. If the key exists and is not revoked,
 * update last_used_at in the same statement and return the row. Returns null
 * on miss OR on a revoked key — the middleware treats both as 401.
 */
export async function findApiKeyByHash(
  hash: string,
): Promise<ApiKey | null> {
  const rows = await db
    .update(apiKeys)
    .set({ lastUsedAt: new Date() })
    .where(eq(apiKeys.keyHash, hash))
    .returning();

  const row = rows[0];
  if (!row) return null;
  if (row.revokedAt) return null;
  return row;
}
