/**
 * Pure crypto helpers for API keys — no database imports, so tests can
 * require them without a DATABASE_URL.
 */
import crypto from "node:crypto";

export type GeneratedApiKey = {
  plaintext: string;
  hash: string;
  prefix: string;
};

/** sha256 hex digest used to store and look up API keys. */
export function hashApiKey(plaintext: string): string {
  return crypto.createHash("sha256").update(plaintext).digest("hex");
}

/**
 * Generate a fresh API key. 32 random bytes → 64-char hex. The caller is
 * responsible for showing the plaintext exactly once; we never persist it.
 */
export function generateApiKey(): GeneratedApiKey {
  const plaintext = crypto.randomBytes(32).toString("hex");
  return {
    plaintext,
    hash: hashApiKey(plaintext),
    prefix: plaintext.slice(0, 8),
  };
}
