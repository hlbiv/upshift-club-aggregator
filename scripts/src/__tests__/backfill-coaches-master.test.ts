/**
 * backfill-coaches-master CLI — pure-helper tests.
 *
 * Run: pnpm --filter @workspace/scripts run test:backfill-coaches-master
 *
 * Covers:
 *   1. normalizeName — case-folds, strips punctuation outside the
 *      letter / digit / whitespace / hyphen / apostrophe set.
 *   2. personHash — three branches (email, no-email default, no-email
 *      with --allow-rehash).
 *   3. The cutover invariant: without `--allow-rehash` the same
 *      email-less coach across two clubs hashes to TWO distinct
 *      person_hash values; with `--allow-rehash` they collapse to ONE.
 *   4. buildRehashAuditPath — filesystem-safe stamp.
 *   5. formatRehashAuditRecord — stable shape.
 *
 * DB-mutating cutover behavior (the SELECT / UPDATE / DELETE inside
 * rehashCutover()) is NOT covered here — those live behind
 * @workspace/db and are validated via the Replit smoke run documented
 * in CLAUDE.md. The DATABASE_URL set in the package.json test script
 * is bogus; importing the module is safe because the script body only
 * touches the DB when invoked as a process entry point.
 */
import {
  normalizeName,
  personHash,
  buildRehashAuditPath,
  formatRehashAuditRecord,
} from "../backfill-coaches-master.js";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// 1a. normalizeName — case + collapse
{
  assert(
    normalizeName("Mike  Smith") === "mike smith",
    "norm-collapse-spaces",
    `got ${JSON.stringify(normalizeName("Mike  Smith"))}`,
  );
}

// 1b. normalizeName — strips parens / commas
{
  const got = normalizeName("Mike (Coach) Smith, Jr.");
  // The pattern `[^\p{L}\p{N}\s'-]` keeps letters/digits/space/'/-; commas,
  // parens, periods become spaces and then collapse.
  assert(got === "mike coach smith jr", "norm-punct", `got ${got}`);
}

// 1c. normalizeName — keeps hyphen + apostrophe
{
  assert(
    normalizeName("O'Brien-Smith") === "o'brien-smith",
    "norm-keep-apostrophe-hyphen",
    `got ${normalizeName("O'Brien-Smith")}`,
  );
}

// 2a. personHash — email branch ignores clubId / allowRehash
{
  const a = personHash("Sam Carter", "sam@x.com", 1, false);
  const b = personHash("Sam Carter", "sam@x.com", 999, true);
  assert(
    a === b,
    "hash-email-stable-across-clubid-and-flag",
    `a=${a} b=${b}`,
  );
}

// 2b. personHash — no-email default differs across clubIds
{
  const a = personHash("Sam Carter", null, 1, false);
  const b = personHash("Sam Carter", null, 2, false);
  assert(
    a !== b,
    "hash-no-email-default-splits-by-club",
    `a==b unexpectedly: ${a}`,
  );
}

// 2c. CRITICAL — without --allow-rehash, same email-less coach across
// two clubs hashes to TWO DIFFERENT values. This is the bug the
// cutover fixes; it must remain true before the cutover so existing
// production rows are not silently rehashed by a routine backfill.
{
  const a = personHash("Mike Smith", null, 11, false);
  const b = personHash("Mike Smith", null, 22, false);
  assert(
    a !== b,
    "cutover-bug-pre-state-still-splits",
    `expected different hashes, got ${a} === ${b}`,
  );
}

// 2d. CRITICAL — with --allow-rehash, the same coach across two
// clubs collapses to a SINGLE hash. This is the cutover semantics.
{
  const a = personHash("Mike Smith", null, 11, true);
  const b = personHash("Mike Smith", null, 22, true);
  assert(
    a === b,
    "cutover-allow-rehash-collapses",
    `expected same hash, got ${a} vs ${b}`,
  );
}

// 2e. --allow-rehash also collapses null-clubId vs numeric-clubId
{
  const a = personHash("Mike Smith", null, null, true);
  const b = personHash("Mike Smith", null, 11, true);
  assert(
    a === b,
    "cutover-allow-rehash-null-and-numeric",
    `expected same hash, got ${a} vs ${b}`,
  );
}

// 2f. Hash output shape — sha256 hex = 64 chars, lower-case hex
{
  const h = personHash("Sam Carter", null, null, true);
  assert(
    /^[0-9a-f]{64}$/.test(h),
    "hash-shape-sha256-hex",
    `got ${h}`,
  );
}

// 2g. Different normalized names yield different hashes
{
  const a = personHash("Sam Carter", null, null, true);
  const b = personHash("Pam Carter", null, null, true);
  assert(a !== b, "hash-name-differentiates", `a=${a} b=${b}`);
}

// 3a. buildRehashAuditPath — filesystem-safe stamp
{
  const p = buildRehashAuditPath(
    "/tmp",
    new Date("2026-04-21T20:21:22.345Z"),
  );
  assert(
    p === "/tmp/coach-rehash-cutover-2026-04-21T20-21-22-345Z.jsonl",
    "audit-path-stamp",
    `got ${p}`,
  );
  assert(!p.includes(":"), "audit-path-no-colon", `got ${p}`);
  const stem = p.slice(0, -".jsonl".length);
  assert(!stem.includes("."), "audit-path-no-dot-in-stem", `got ${p}`);
}

// 3b. buildRehashAuditPath — respects custom directory
{
  const p = buildRehashAuditPath(
    "/work/audit",
    new Date("2026-01-02T03:04:05.006Z"),
  );
  assert(
    p === "/work/audit/coach-rehash-cutover-2026-01-02T03-04-05-006Z.jsonl",
    "audit-path-custom-dir",
    `got ${p}`,
  );
}

// 4a. formatRehashAuditRecord — stable key order, newline-terminated
{
  const line = formatRehashAuditRecord(
    "abc",
    { id: 1, display_name: "Sam Carter" },
    [{ id: 2, display_name: "Sam Carter" }],
  );
  assert(line.endsWith("\n"), "audit-rec-newline", `got ${JSON.stringify(line)}`);
  const parsed = JSON.parse(line);
  assert(
    JSON.stringify(Object.keys(parsed)) === '["newHash","winner","losers"]',
    "audit-rec-key-order",
    `got ${Object.keys(parsed).join(",")}`,
  );
  assert(parsed.newHash === "abc", "audit-rec-new-hash", parsed.newHash);
  assert(
    parsed.winner.id === 1,
    "audit-rec-winner-id",
    String(parsed.winner.id),
  );
  assert(
    Array.isArray(parsed.losers) && parsed.losers.length === 1,
    "audit-rec-losers-array",
    JSON.stringify(parsed.losers),
  );
}

// 4b. formatRehashAuditRecord — empty losers serialized as []
{
  const line = formatRehashAuditRecord("xyz", { id: 5 }, []);
  const parsed = JSON.parse(line);
  assert(
    Array.isArray(parsed.losers) && parsed.losers.length === 0,
    "audit-rec-empty-losers",
    JSON.stringify(parsed.losers),
  );
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if (failures.length === 0) {
  console.log("[backfill-coaches-master.test] all pure-helper tests passed");
  process.exit(0);
} else {
  console.error(
    `[backfill-coaches-master.test] ${failures.length} failure(s):`,
  );
  for (const f of failures) {
    console.error(`  - ${f.name}: ${f.issue}`);
  }
  process.exit(1);
}
