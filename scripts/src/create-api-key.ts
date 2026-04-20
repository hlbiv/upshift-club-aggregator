/**
 * Create a new API key for a machine caller.
 *
 * Usage (on Replit):
 *   pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-player-platform prod"
 *
 *   # With one or more scopes (repeatable):
 *   pnpm --filter @workspace/scripts run create-api-key -- \
 *     --name "admin-ops" --scope admin:read --scope admin:write
 *
 * Prints the plaintext key ONCE. Copy it into the caller's env var
 * (e.g. UPSHIFT_DATA_API_KEY) immediately — only the hash is stored in the
 * database, so a lost key cannot be recovered.
 */
import { db, apiKeys, generateApiKey } from "@workspace/db";

function parseArgs(argv: string[]): { name: string; scopes: string[] } {
  let name: string | undefined;
  const scopes: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--name") {
      name = argv[++i];
    } else if (arg?.startsWith("--name=")) {
      name = arg.slice("--name=".length);
    } else if (arg === "--scope") {
      const value = argv[++i];
      if (value && value.trim().length > 0) scopes.push(value.trim());
    } else if (arg?.startsWith("--scope=")) {
      const value = arg.slice("--scope=".length);
      if (value.trim().length > 0) scopes.push(value.trim());
    }
  }
  if (!name || name.trim().length === 0) {
    console.error("Error: --name <label> is required");
    console.error(
      'Example: pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-player-platform prod"',
    );
    process.exit(1);
  }
  // De-dup scopes preserving insertion order.
  const uniqueScopes = Array.from(new Set(scopes));
  return { name: name.trim(), scopes: uniqueScopes };
}

async function main() {
  const { name, scopes } = parseArgs(process.argv.slice(2));
  const { plaintext, hash, prefix } = generateApiKey();

  const [row] = await db
    .insert(apiKeys)
    .values({ name, keyHash: hash, keyPrefix: prefix, scopes })
    .returning({
      id: apiKeys.id,
      name: apiKeys.name,
      keyPrefix: apiKeys.keyPrefix,
      scopes: apiKeys.scopes,
    });

  console.log("=".repeat(72));
  console.log("API key created.");
  console.log(`  id:     ${row.id}`);
  console.log(`  name:   ${row.name}`);
  console.log(`  prefix: ${row.keyPrefix}`);
  console.log(
    `  scopes: ${row.scopes.length === 0 ? "(none)" : row.scopes.join(", ")}`,
  );
  console.log("");
  console.log("  PLAINTEXT (save this NOW — it will not be shown again):");
  console.log("");
  console.log(`    ${plaintext}`);
  console.log("");
  console.log(
    "  Set it on the caller side (e.g. UPSHIFT_DATA_API_KEY) and send it as",
  );
  console.log("  the X-API-Key header on every request.");
  console.log("=".repeat(72));
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
