/**
 * Create a new admin user for the Data admin UI.
 *
 * Usage (on Replit):
 *   pnpm --filter @workspace/scripts run create-admin-user -- --email you@example.com
 *   pnpm --filter @workspace/scripts run create-admin-user -- --email you@example.com --role super_admin
 *
 * Password source (first match wins):
 *   1. --password <pw> flag
 *   2. ADMIN_PASSWORD environment variable  (preferred non-interactive flow)
 *   3. TTY prompt via readline (plaintext — we do not attempt to mute echo;
 *      see PASSWORD_PROMPT_NOTE below). If stdin is not a TTY we fail fast
 *      rather than hang.
 *
 * PASSWORD_PROMPT_NOTE: hiding stdin echo portably on Node without a native
 * dep (e.g. `read`, `inquirer`) is finicky and can silently fall back to echo
 * on non-POSIX terminals. Rather than pretend, we document ADMIN_PASSWORD as
 * the preferred path and accept plaintext stdin as a deliberate fallback.
 *
 * We hash with bcryptjs 12 rounds to match the Player convention documented
 * in lib/db/src/schema/admin.ts. bcryptjs is a pure-JS port of bcrypt; its
 * `$2b$` output is wire-compatible with the native `bcrypt` package that the
 * api-server login route (also bcryptjs) accepts. Only the hash is written
 * to admin_users — the plaintext password is never logged, printed, or
 * persisted.
 */
import { createInterface } from "node:readline/promises";
import bcrypt from "bcryptjs";
import { db, adminUsers } from "@workspace/db";

export const ADMIN_ROLES = ["admin", "super_admin"] as const;
export type AdminRole = (typeof ADMIN_ROLES)[number];

export const BCRYPT_ROUNDS = 12;

/**
 * Intentionally permissive — we only guard against trivially wrong inputs
 * (missing @, missing domain dot). Real deliverability is out of scope for a
 * bootstrap CLI.
 */
export const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

export function isValidEmail(email: string): boolean {
  return EMAIL_RE.test(email);
}

export function isValidRole(role: string): role is AdminRole {
  return (ADMIN_ROLES as readonly string[]).includes(role);
}

export type ParsedArgs = {
  email: string;
  role: AdminRole;
  password: string | undefined;
};

export function parseArgs(argv: string[]): ParsedArgs {
  let email: string | undefined;
  let role: string | undefined;
  let password: string | undefined;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--email") {
      email = argv[++i];
    } else if (arg?.startsWith("--email=")) {
      email = arg.slice("--email=".length);
    } else if (arg === "--role") {
      role = argv[++i];
    } else if (arg?.startsWith("--role=")) {
      role = arg.slice("--role=".length);
    } else if (arg === "--password") {
      password = argv[++i];
    } else if (arg?.startsWith("--password=")) {
      password = arg.slice("--password=".length);
    }
  }

  if (!email || email.trim().length === 0) {
    throw new Error("--email <addr> is required");
  }
  const trimmedEmail = email.trim().toLowerCase();
  if (!isValidEmail(trimmedEmail)) {
    throw new Error(`Invalid email address: ${trimmedEmail}`);
  }

  const rawRole = role?.trim() ?? "admin";
  if (!isValidRole(rawRole)) {
    throw new Error(
      `Invalid --role "${rawRole}". Allowed: ${ADMIN_ROLES.join(", ")}`,
    );
  }

  return {
    email: trimmedEmail,
    role: rawRole,
    password: password && password.length > 0 ? password : undefined,
  };
}

async function readPasswordFromTty(): Promise<string> {
  if (!process.stdin.isTTY) {
    throw new Error(
      "No --password flag, no ADMIN_PASSWORD env var, and stdin is not a TTY. Refusing to hang.",
    );
  }
  // We print the prompt to stderr so stdout stays clean for any callers
  // piping the output. Echo is NOT hidden — see PASSWORD_PROMPT_NOTE.
  const rl = createInterface({
    input: process.stdin,
    output: process.stderr,
    terminal: true,
  });
  try {
    const pw = await rl.question(
      "Password (visible as you type — prefer ADMIN_PASSWORD env var): ",
    );
    return pw;
  } finally {
    rl.close();
  }
}

export async function resolvePassword(
  cliPassword: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
  promptFn: () => Promise<string> = readPasswordFromTty,
): Promise<string> {
  if (cliPassword && cliPassword.length > 0) return cliPassword;
  const envPw = env.ADMIN_PASSWORD;
  if (envPw && envPw.length > 0) return envPw;
  const typed = await promptFn();
  if (!typed || typed.length === 0) {
    throw new Error("Password is required (none provided).");
  }
  return typed;
}

export async function hashPassword(plaintext: string): Promise<string> {
  return bcrypt.hash(plaintext, BCRYPT_ROUNDS);
}

/**
 * Best-effort detection of the Postgres unique-violation error (code 23505)
 * surfaced through node-postgres. Drizzle preserves the underlying pg error
 * on the thrown object's `cause` in some paths and directly on the error in
 * others, so we check both.
 */
export function isUniqueViolation(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const e = err as { code?: unknown; cause?: { code?: unknown } };
  if (e.code === "23505") return true;
  if (
    e.cause &&
    typeof e.cause === "object" &&
    (e.cause as { code?: unknown }).code === "23505"
  ) {
    return true;
  }
  return false;
}

async function main() {
  let parsed: ParsedArgs;
  try {
    parsed = parseArgs(process.argv.slice(2));
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`Error: ${msg}`);
    console.error(
      'Example: pnpm --filter @workspace/scripts run create-admin-user -- --email you@example.com --role admin',
    );
    process.exit(1);
  }

  const password = await resolvePassword(parsed.password);
  const passwordHash = await hashPassword(password);

  let row: { id: number; email: string; role: string; createdAt: Date };
  try {
    const [inserted] = await db
      .insert(adminUsers)
      .values({
        email: parsed.email,
        passwordHash,
        role: parsed.role,
      })
      .returning({
        id: adminUsers.id,
        email: adminUsers.email,
        role: adminUsers.role,
        createdAt: adminUsers.createdAt,
      });
    row = inserted;
  } catch (err) {
    if (isUniqueViolation(err)) {
      console.error(
        `Error: an admin_users row with email "${parsed.email}" already exists.`,
      );
      console.error(
        "  Pick a different email, or rotate the password via the reset CLI (v1: manual SQL).",
      );
      process.exit(1);
    }
    throw err;
  }

  console.log("=".repeat(72));
  console.log("Admin user created.");
  console.log(`  id:         ${row.id}`);
  console.log(`  email:      ${row.email}`);
  console.log(`  role:       ${row.role}`);
  console.log(`  created_at: ${row.createdAt.toISOString()}`);
  console.log("");
  console.log(
    "  Log in at the admin UI with this email + the password you just set.",
  );
  console.log("  The password hash is stored; the plaintext is not.");
  console.log("=".repeat(72));
}

// Only run main() when invoked as a script — this lets the test file import
// the pure helpers without triggering the DB insert.
const invokedAsScript =
  import.meta.url === `file://${process.argv[1]}` ||
  import.meta.url.endsWith(process.argv[1] ?? "");

if (invokedAsScript) {
  main()
    .then(() => process.exit(0))
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
