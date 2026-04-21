/**
 * purge-polluted-coach-discoveries CLI — pure-helper tests.
 *
 * Run: pnpm --filter @workspace/scripts run test:purge-polluted-coach-discoveries
 *
 * Covers the four pure helpers exported from the script so we can
 * unit-test without a live Postgres:
 *   1. parseArgs — dry-run default, --commit flips it, --audit-dir
 *      space-separated + `=`-separated, --flag-type override, empty
 *      rejection for both flags.
 *   2. buildAuditPath — filesystem-safe stamp (no `:` or `.`), slash
 *      separator honored.
 *   3. chunk — partition, remainder, zero-size rejection, empty input.
 *   4. groupFlagsByDiscoveryId — group + preserve order, drop
 *      non-numeric ids.
 *   5. formatAuditRecord — top-level key set is fixed; JSON round-trips.
 *
 * DB-mutating behavior (SELECT targets, DELETE, CASCADE verification,
 * transaction rollback on dry-run) is deliberately NOT covered here —
 * those live behind @workspace/db and are validated via the Replit
 * smoke run documented in CLAUDE.md. DATABASE_URL is set to a bogus
 * value in the package.json test script so importing the module
 * doesn't blow up on load; the Pool is never queried.
 */
import {
  parseArgs,
  buildAuditPath,
  chunk,
  groupFlagsByDiscoveryId,
  formatAuditRecord,
  DEFAULT_FLAG_TYPE,
  DEFAULT_AUDIT_DIR,
} from "../purge-polluted-coach-discoveries.js";

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

// 1a. parseArgs — empty argv = dry-run default + default dirs
{
  const p = parseArgs([]);
  assert(p.commit === false, "parse-default-commit", `got ${p.commit}`);
  assert(
    p.auditDir === DEFAULT_AUDIT_DIR,
    "parse-default-audit-dir",
    `got ${p.auditDir}`,
  );
  assert(
    p.flagType === DEFAULT_FLAG_TYPE,
    "parse-default-flag-type",
    `got ${p.flagType}`,
  );
}

// 1b. parseArgs — --commit flips to true
{
  const p = parseArgs(["--commit"]);
  assert(p.commit === true, "parse-commit-flag", `got ${p.commit}`);
}

// 1c. parseArgs — --audit-dir space-separated
{
  const p = parseArgs(["--audit-dir", "/var/log/purge"]);
  assert(
    p.auditDir === "/var/log/purge",
    "parse-audit-dir-space",
    `got ${p.auditDir}`,
  );
}

// 1d. parseArgs — --audit-dir=val equals-separated
{
  const p = parseArgs(["--audit-dir=/var/log/purge"]);
  assert(
    p.auditDir === "/var/log/purge",
    "parse-audit-dir-equals",
    `got ${p.auditDir}`,
  );
}

// 1e. parseArgs — --flag-type override, space- and equals-separated
{
  const p1 = parseArgs(["--flag-type", "role_label_as_name"]);
  assert(
    p1.flagType === "role_label_as_name",
    "parse-flag-type-space",
    `got ${p1.flagType}`,
  );
  const p2 = parseArgs(["--flag-type=corrupt_email"]);
  assert(
    p2.flagType === "corrupt_email",
    "parse-flag-type-equals",
    `got ${p2.flagType}`,
  );
}

// 1f. parseArgs — all three flags combined
{
  const p = parseArgs([
    "--commit",
    "--audit-dir",
    "/tmp/x",
    "--flag-type",
    "nav_leaked",
  ]);
  assert(p.commit === true, "parse-combo-commit", `got ${p.commit}`);
  assert(
    p.auditDir === "/tmp/x",
    "parse-combo-audit-dir",
    `got ${p.auditDir}`,
  );
  assert(
    p.flagType === "nav_leaked",
    "parse-combo-flag-type",
    `got ${p.flagType}`,
  );
}

// 1g. parseArgs — unknown tokens ignored (forward-compat)
{
  const p = parseArgs(["--commit", "--something-future", "value"]);
  assert(p.commit === true, "parse-unknown-ignored-commit", `got ${p.commit}`);
  assert(
    p.auditDir === DEFAULT_AUDIT_DIR,
    "parse-unknown-ignored-audit",
    `got ${p.auditDir}`,
  );
}

// 1h. parseArgs — empty --flag-type rejected
expectThrows(
  () => parseArgs(["--flag-type="]),
  "parse-empty-flag-type-rejected",
  "--flag-type must not be empty",
);

// 1i. parseArgs — empty --audit-dir rejected
expectThrows(
  () => parseArgs(["--audit-dir="]),
  "parse-empty-audit-dir-rejected",
  "--audit-dir must not be empty",
);

// 2a. buildAuditPath — filesystem-safe stamp
{
  const p = buildAuditPath("/tmp", new Date("2026-04-21T20:21:22.345Z"));
  assert(
    p === "/tmp/coach-discoveries-purge-2026-04-21T20-21-22-345Z.jsonl",
    "build-audit-path-stamp",
    `got ${p}`,
  );
  assert(!p.includes(":"), "build-audit-path-no-colon", `got ${p}`);
  // One "." in the extension is fine; none elsewhere in the stamp.
  const stem = p.slice(0, -".jsonl".length);
  assert(
    !stem.includes("."),
    "build-audit-path-no-dot-in-stem",
    `got ${p}`,
  );
}

// 2b. buildAuditPath — respects custom directory
{
  const p = buildAuditPath(
    "/home/runner/workspace/artifacts/purge",
    new Date("2026-01-02T03:04:05.006Z"),
  );
  assert(
    p ===
      "/home/runner/workspace/artifacts/purge/coach-discoveries-purge-2026-01-02T03-04-05-006Z.jsonl",
    "build-audit-path-custom-dir",
    `got ${p}`,
  );
}

// 3a. chunk — even partition
{
  const c = chunk([1, 2, 3, 4], 2);
  assert(c.length === 2, "chunk-even-length", `got ${c.length}`);
  assert(
    JSON.stringify(c) === "[[1,2],[3,4]]",
    "chunk-even-content",
    `got ${JSON.stringify(c)}`,
  );
}

// 3b. chunk — remainder
{
  const c = chunk([1, 2, 3, 4, 5], 2);
  assert(
    JSON.stringify(c) === "[[1,2],[3,4],[5]]",
    "chunk-remainder",
    `got ${JSON.stringify(c)}`,
  );
}

// 3c. chunk — empty input
{
  const c = chunk<number>([], 10);
  assert(c.length === 0, "chunk-empty", `got ${c.length}`);
}

// 3d. chunk — rejects non-positive size
expectThrows(() => chunk([1, 2], 0), "chunk-zero-rejected", "chunk size");
expectThrows(() => chunk([1, 2], -1), "chunk-negative-rejected", "chunk size");

// 4a. groupFlagsByDiscoveryId — groups + preserves insertion order
{
  const flags = [
    { discovery_id: 10, kind: "a" },
    { discovery_id: 20, kind: "b" },
    { discovery_id: 10, kind: "c" },
  ];
  const g = groupFlagsByDiscoveryId(flags);
  assert(g.size === 2, "group-size", `got ${g.size}`);
  const got10 = g.get(10)?.map((f) => f.kind);
  assert(
    JSON.stringify(got10) === '["a","c"]',
    "group-preserves-order",
    `got ${JSON.stringify(got10)}`,
  );
  const got20 = g.get(20)?.map((f) => f.kind);
  assert(
    JSON.stringify(got20) === '["b"]',
    "group-singleton",
    `got ${JSON.stringify(got20)}`,
  );
}

// 4b. groupFlagsByDiscoveryId — drops rows with non-numeric id
{
  const flags = [
    { discovery_id: 10, kind: "a" },
    { discovery_id: "bad", kind: "b" },
    { discovery_id: null, kind: "c" },
    { discovery_id: NaN, kind: "d" },
    { discovery_id: 30, kind: "e" },
  ];
  const g = groupFlagsByDiscoveryId(flags);
  assert(g.size === 2, "group-drops-non-numeric-size", `got ${g.size}`);
  assert(g.has(10) && g.has(30), "group-drops-non-numeric-keys", `got ${[...g.keys()].join(",")}`);
}

// 5a. formatAuditRecord — shape + newline-terminated
{
  const line = formatAuditRecord(
    { id: 1, name: "CONTACT" },
    [{ flag_type: "looks_like_name_reject" }],
    { id: 99, person_hash: "abc" },
  );
  assert(line.endsWith("\n"), "audit-newline", `got ${JSON.stringify(line)}`);
  const parsed = JSON.parse(line);
  assert(
    JSON.stringify(Object.keys(parsed)) === '["discovery","flags","coach"]',
    "audit-key-order",
    `got ${Object.keys(parsed).join(",")}`,
  );
  assert(
    parsed.discovery.name === "CONTACT",
    "audit-discovery-pass-through",
    `got ${parsed.discovery.name}`,
  );
  assert(
    Array.isArray(parsed.flags) && parsed.flags.length === 1,
    "audit-flags-array",
    `got ${JSON.stringify(parsed.flags)}`,
  );
  assert(
    parsed.coach && parsed.coach.id === 99,
    "audit-coach-present",
    `got ${JSON.stringify(parsed.coach)}`,
  );
}

// 5b. formatAuditRecord — null coach serialized as null (not omitted)
{
  const line = formatAuditRecord({ id: 1 }, [], null);
  const parsed = JSON.parse(line);
  assert(parsed.coach === null, "audit-coach-null", `got ${parsed.coach}`);
  assert(
    Array.isArray(parsed.flags) && parsed.flags.length === 0,
    "audit-empty-flags",
    `got ${JSON.stringify(parsed.flags)}`,
  );
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if (failures.length === 0) {
  console.log(
    "[purge-polluted-coach-discoveries.test] all pure-helper tests passed",
  );
  process.exit(0);
} else {
  console.error(
    `[purge-polluted-coach-discoveries.test] ${failures.length} failure(s):`,
  );
  for (const f of failures) {
    console.error(`  - ${f.name}: ${f.issue}`);
  }
  process.exit(1);
}
