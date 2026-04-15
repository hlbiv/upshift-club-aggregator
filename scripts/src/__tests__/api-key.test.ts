/**
 * API-key helpers (hashApiKey, generateApiKey) — pure-function tests.
 *
 * Run: pnpm --filter @workspace/scripts run test:api-key
 *
 * We don't test findApiKeyByHash here because it requires a live DB (the
 * smoke test at lib/db/src/schema/__tests__/smoke.ts covers the schema
 * shape; artifacts/api-server/src/__tests__/apiKeyAuth.test.ts covers the
 * middleware with an injected lookup).
 */
import { hashApiKey, generateApiKey } from "@workspace/db";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// 1. hashApiKey is deterministic + 64-char sha256 hex
{
  const a = hashApiKey("hello");
  const b = hashApiKey("hello");
  assert(a === b, "hash-deterministic", "same input must yield same hash");
  assert(
    /^[0-9a-f]{64}$/.test(a),
    "hash-format",
    `expected 64-char hex, got ${a}`,
  );
  assert(
    hashApiKey("hello") !== hashApiKey("world"),
    "hash-unique",
    "different inputs must yield different hashes",
  );
}

// 2. generateApiKey produces 64-char hex + 8-char prefix + matching hash
{
  const k = generateApiKey();
  assert(
    /^[0-9a-f]{64}$/.test(k.plaintext),
    "gen-plaintext",
    `expected 64-char hex plaintext, got ${k.plaintext}`,
  );
  assert(
    k.prefix === k.plaintext.slice(0, 8),
    "gen-prefix",
    "prefix must be first 8 chars of plaintext",
  );
  assert(
    k.hash === hashApiKey(k.plaintext),
    "gen-hash",
    "hash must equal hashApiKey(plaintext)",
  );

  // Randomness — two generated keys must differ.
  const k2 = generateApiKey();
  assert(
    k.plaintext !== k2.plaintext,
    "gen-random",
    "two generated keys must differ",
  );
}

if (failures.length === 0) {
  console.log("[api-key-helpers] OK — all assertions passed");
  process.exit(0);
} else {
  console.error(`[api-key-helpers] ${failures.length} failure(s):`);
  for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
  process.exit(1);
}
