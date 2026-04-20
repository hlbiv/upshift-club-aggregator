/**
 * create-admin-user CLI — pure-helper tests.
 *
 * Run: pnpm --filter @workspace/scripts run test:create-admin-user
 *
 * We cover:
 *   1. parseArgs — happy path, default role, --role=val style, --email=val style
 *   2. parseArgs — missing email rejected, invalid email rejected, invalid
 *      role rejected
 *   3. isValidEmail — basic regex sanity
 *   4. hashPassword — produces a valid bcrypt hash that verifies the input
 *   5. resolvePassword — flag > env var > prompt precedence
 *   6. isUniqueViolation — matches pg error code 23505 both directly and
 *      via a `cause` chain
 *
 * We do NOT exercise the db.insert(...) path. That lives behind @workspace/db
 * and would need a live Postgres; per the scripts-pattern it's verified via
 * the CLI smoke test on Replit. DATABASE_URL is set to a bogus value in the
 * package.json test script so that importing @workspace/db doesn't throw on
 * load — the Pool is never actually queried.
 */
import bcrypt from "bcrypt";
import {
  parseArgs,
  isValidEmail,
  isValidRole,
  resolvePassword,
  hashPassword,
  isUniqueViolation,
  ADMIN_ROLES,
  BCRYPT_ROUNDS,
} from "../create-admin-user.js";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

function expectThrows(
  fn: () => unknown,
  name: string,
  match?: string,
): void {
  try {
    fn();
    failures.push({ name, issue: "expected throw, got resolve" });
  } catch (err) {
    if (match) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes(match)) {
        failures.push({
          name,
          issue: `expected error containing "${match}", got "${msg}"`,
        });
      }
    }
  }
}

// 1. parseArgs — happy path
{
  const p = parseArgs(["--email", "jane@example.com"]);
  assert(p.email === "jane@example.com", "parse-happy-email", `got ${p.email}`);
  assert(p.role === "admin", "parse-happy-default-role", `got ${p.role}`);
  assert(p.password === undefined, "parse-happy-no-password", `got ${p.password}`);
}

// 1b. parseArgs — --role + --password
{
  const p = parseArgs([
    "--email",
    "Jane@Example.COM",
    "--role",
    "super_admin",
    "--password",
    "s3cret!",
  ]);
  assert(
    p.email === "jane@example.com",
    "parse-lowercases-email",
    `got ${p.email}`,
  );
  assert(p.role === "super_admin", "parse-role-super", `got ${p.role}`);
  assert(p.password === "s3cret!", "parse-password", `got ${p.password}`);
}

// 1c. parseArgs — =style args
{
  const p = parseArgs(["--email=foo@bar.co", "--role=admin"]);
  assert(p.email === "foo@bar.co", "parse-eq-email", `got ${p.email}`);
  assert(p.role === "admin", "parse-eq-role", `got ${p.role}`);
}

// 2. parseArgs — missing email
expectThrows(() => parseArgs([]), "parse-missing-email", "--email");

// 2b. parseArgs — invalid email (no @)
expectThrows(
  () => parseArgs(["--email", "not-an-email"]),
  "parse-invalid-email-no-at",
  "Invalid email",
);

// 2c. parseArgs — invalid email (no domain)
expectThrows(
  () => parseArgs(["--email", "foo@bar"]),
  "parse-invalid-email-no-tld",
  "Invalid email",
);

// 2d. parseArgs — invalid role
expectThrows(
  () => parseArgs(["--email", "a@b.co", "--role", "root"]),
  "parse-invalid-role",
  'Invalid --role',
);

// 3. isValidEmail
assert(isValidEmail("a@b.co"), "email-basic", "a@b.co should pass");
assert(
  isValidEmail("henry.beaty4+admin@gmail.com"),
  "email-plus-tag",
  "plus-tag address should pass",
);
assert(!isValidEmail(""), "email-empty", "empty should fail");
assert(!isValidEmail("no-at"), "email-no-at", "no-at should fail");
assert(!isValidEmail("a@b"), "email-no-tld", "no tld should fail");
assert(!isValidEmail("a @b.co"), "email-space", "space should fail");

// 3b. isValidRole + ADMIN_ROLES constant
assert(isValidRole("admin"), "role-admin", "admin is valid");
assert(isValidRole("super_admin"), "role-super", "super_admin is valid");
assert(!isValidRole("root"), "role-root", "root is not valid");
assert(
  ADMIN_ROLES.length === 2,
  "role-enum-size",
  `expected 2 roles, got ${ADMIN_ROLES.length}`,
);

// 4. hashPassword — produces a bcrypt-recognizable hash, verifies the input,
//    and uses BCRYPT_ROUNDS = 12.
{
  const pw = "correct horse battery staple";
  const hash = await hashPassword(pw);
  assert(
    hash.startsWith("$2") && hash.length >= 59,
    "hash-bcrypt-prefix",
    `expected $2*$ bcrypt hash, got "${hash}"`,
  );
  assert(
    hash !== pw,
    "hash-not-plaintext",
    "hash must differ from plaintext",
  );
  assert(
    await bcrypt.compare(pw, hash),
    "hash-verifies",
    "bcrypt.compare(pw, hash) should return true",
  );
  assert(
    !(await bcrypt.compare("wrong", hash)),
    "hash-wrong-pw",
    "bcrypt.compare(wrong, hash) should return false",
  );
  assert(BCRYPT_ROUNDS === 12, "bcrypt-rounds-12", `got ${BCRYPT_ROUNDS}`);
}

// 5. resolvePassword — precedence
{
  // 5a. flag wins
  const pw1 = await resolvePassword("from-flag", { ADMIN_PASSWORD: "from-env" }, async () => "from-prompt");
  assert(pw1 === "from-flag", "resolve-flag-wins", `got ${pw1}`);

  // 5b. env wins over prompt when no flag
  const pw2 = await resolvePassword(undefined, { ADMIN_PASSWORD: "from-env" }, async () => "from-prompt");
  assert(pw2 === "from-env", "resolve-env-wins", `got ${pw2}`);

  // 5c. prompt fires when neither flag nor env is set
  let promptCalls = 0;
  const pw3 = await resolvePassword(undefined, {}, async () => {
    promptCalls++;
    return "from-prompt";
  });
  assert(pw3 === "from-prompt", "resolve-prompt-fires", `got ${pw3}`);
  assert(promptCalls === 1, "resolve-prompt-once", `called ${promptCalls}x`);

  // 5d. empty string from prompt is rejected
  try {
    await resolvePassword(undefined, {}, async () => "");
    failures.push({
      name: "resolve-empty-prompt",
      issue: "expected throw for empty prompt",
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (!msg.includes("required")) {
      failures.push({
        name: "resolve-empty-prompt",
        issue: `expected "required" in error, got "${msg}"`,
      });
    }
  }
}

// 6. isUniqueViolation — matches code 23505 on the error directly and via cause
{
  const direct = Object.assign(new Error("duplicate"), { code: "23505" });
  assert(isUniqueViolation(direct), "unique-direct", "direct 23505 should match");

  const chained = Object.assign(new Error("wrapped"), {
    cause: { code: "23505" },
  });
  assert(isUniqueViolation(chained), "unique-cause", "cause 23505 should match");

  const unrelated = Object.assign(new Error("other"), { code: "42703" });
  assert(
    !isUniqueViolation(unrelated),
    "unique-other-code",
    "non-23505 should not match",
  );

  assert(!isUniqueViolation(null), "unique-null", "null should not match");
  assert(
    !isUniqueViolation("string"),
    "unique-string",
    "string should not match",
  );
}

if (failures.length === 0) {
  console.log("[create-admin-user] OK — all assertions passed");
  process.exit(0);
} else {
  console.error(`[create-admin-user] ${failures.length} failure(s):`);
  for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
  process.exit(1);
}
