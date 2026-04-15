/**
 * Revoke an API key by its 8-char prefix.
 *
 * Usage (on Replit):
 *   pnpm --filter @workspace/scripts run revoke-api-key -- --prefix abc12345
 *
 * Revocation is soft: sets revoked_at = now() on the row, leaving the audit
 * trail intact. The middleware treats any row with revoked_at != NULL as a
 * 401. Idempotent — revoking an already-revoked key is a no-op success.
 */
import { and, eq, isNull } from "drizzle-orm";
import { db, apiKeys } from "@workspace/db";

function parseArgs(argv: string[]): { prefix: string } {
  let prefix: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--prefix") {
      prefix = argv[++i];
    } else if (arg?.startsWith("--prefix=")) {
      prefix = arg.slice("--prefix=".length);
    }
  }
  if (!prefix || prefix.trim().length === 0) {
    console.error("Error: --prefix <8-char-prefix> is required");
    process.exit(1);
  }
  return { prefix: prefix.trim() };
}

async function main() {
  const { prefix } = parseArgs(process.argv.slice(2));

  const matches = await db
    .select({
      id: apiKeys.id,
      name: apiKeys.name,
      keyPrefix: apiKeys.keyPrefix,
      revokedAt: apiKeys.revokedAt,
    })
    .from(apiKeys)
    .where(eq(apiKeys.keyPrefix, prefix));

  if (matches.length === 0) {
    console.error(`No key found with prefix ${prefix}`);
    process.exit(1);
  }
  if (matches.length > 1) {
    console.error(
      `Ambiguous: ${matches.length} keys share prefix ${prefix}. Inspect manually.`,
    );
    for (const m of matches) {
      console.error(`  id=${m.id} name="${m.name}" revokedAt=${m.revokedAt}`);
    }
    process.exit(1);
  }

  const [match] = matches;
  if (match.revokedAt) {
    console.log(
      `Key ${match.keyPrefix} (id=${match.id}, "${match.name}") was already revoked at ${match.revokedAt.toISOString()}.`,
    );
    process.exit(0);
  }

  const [updated] = await db
    .update(apiKeys)
    .set({ revokedAt: new Date() })
    .where(and(eq(apiKeys.id, match.id), isNull(apiKeys.revokedAt)))
    .returning({ id: apiKeys.id, revokedAt: apiKeys.revokedAt });

  console.log(
    `Revoked key ${match.keyPrefix} (id=${updated.id}, "${match.name}") at ${updated.revokedAt?.toISOString()}.`,
  );
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
