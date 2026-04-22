/**
 * sweep-orphan-coaches CLI — pure-helper tests.
 *
 * Run: pnpm --filter @workspace/scripts run test:sweep-orphan-coaches
 *
 * Covers the five pure helpers exported from the script:
 *   1. parseArgs — dry-run default, --commit flips it, --audit-dir
 *      space-separated + `=`-separated, empty rejection.
 *   2. buildAuditPath — filesystem-safe stamp (no `:` or `.`), slash
 *      separator honored.
 *   3. chunk — partition, remainder, zero-size rejection, empty input.
 *   4. groupByCoachId — group + preserve order, drop non-numeric ids.
 *   5. formatAuditRecord — top-level key set is fixed; JSON round-trips;
 *      arrays preserved for each of the three cascade-child fields.
 *
 * DB-mutating behavior (SELECT targets, DELETE with manually_merged
 * filter, CASCADE verification, transaction rollback on dry-run) is NOT
 * covered here — those live behind @workspace/db and are validated via
 * the Replit smoke run documented in CLAUDE.md. DATABASE_URL is set to
 * a bogus value in the package.json test script so importing the
 * module doesn't blow up on load; the Pool is never queried.
 */
import {
  parseArgs,
  buildAuditPath,
  chunk,
  groupByCoachId,
  formatAuditRecord,
  DEFAULT_AUDIT_DIR,
} from "../sweep-orphan-coaches.js";

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

// 1a. parseArgs — empty argv = dry-run default + default dir
{
  const p = parseArgs([]);
  assert(p.commit === false, "parse-default-commit", `got ${p.commit}`);
  assert(
    p.auditDir === DEFAULT_AUDIT_DIR,
    "parse-default-audit-dir",
    `got ${p.auditDir}`,
  );
}

// 1b. parseArgs — --commit flips to true
{
  const p = parseArgs(["--commit"]);
  assert(p.commit === true, "parse-commit-flag", `got ${p.commit}`);
}

// 1c. parseArgs — --audit-dir space-separated
{
  const p = parseArgs(["--audit-dir", "/var/log/sweep"]);
  assert(
    p.auditDir === "/var/log/sweep",
    "parse-audit-dir-space",
    `got ${p.auditDir}`,
  );
}

// 1d. parseArgs — --audit-dir=val equals-separated
{
  const p = parseArgs(["--audit-dir=/var/log/sweep"]);
  assert(
    p.auditDir === "/var/log/sweep",
    "parse-audit-dir-equals",
    `got ${p.auditDir}`,
  );
}

// 1e. parseArgs — both flags combined
{
  const p = parseArgs(["--commit", "--audit-dir", "/tmp/x"]);
  assert(p.commit === true, "parse-combo-commit", `got ${p.commit}`);
  assert(
    p.auditDir === "/tmp/x",
    "parse-combo-audit-dir",
    `got ${p.auditDir}`,
  );
}

// 1f. parseArgs — unknown tokens ignored (forward-compat)
{
  const p = parseArgs(["--commit", "--something-future", "value"]);
  assert(p.commit === true, "parse-unknown-ignored-commit", `got ${p.commit}`);
  assert(
    p.auditDir === DEFAULT_AUDIT_DIR,
    "parse-unknown-ignored-audit",
    `got ${p.auditDir}`,
  );
}

// 1g. parseArgs — empty --audit-dir rejected
expectThrows(
  () => parseArgs(["--audit-dir="]),
  "parse-empty-audit-dir-rejected",
  "--audit-dir must not be empty",
);

// 2a. buildAuditPath — filesystem-safe stamp
{
  const p = buildAuditPath("/tmp", new Date("2026-04-21T20:21:22.345Z"));
  assert(
    p === "/tmp/orphan-coaches-sweep-2026-04-21T20-21-22-345Z.jsonl",
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
    "/home/runner/workspace/artifacts/sweep",
    new Date("2026-01-02T03:04:05.006Z"),
  );
  assert(
    p ===
      "/home/runner/workspace/artifacts/sweep/orphan-coaches-sweep-2026-01-02T03-04-05-006Z.jsonl",
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

// 4a. groupByCoachId — groups + preserves insertion order
{
  const rows = [
    { coach_id: 10, kind: "a" },
    { coach_id: 20, kind: "b" },
    { coach_id: 10, kind: "c" },
  ];
  const g = groupByCoachId(rows);
  assert(g.size === 2, "group-size", `got ${g.size}`);
  const got10 = g.get(10)?.map((r) => r.kind);
  assert(
    JSON.stringify(got10) === '["a","c"]',
    "group-preserves-order",
    `got ${JSON.stringify(got10)}`,
  );
  const got20 = g.get(20)?.map((r) => r.kind);
  assert(
    JSON.stringify(got20) === '["b"]',
    "group-singleton",
    `got ${JSON.stringify(got20)}`,
  );
}

// 4b. groupByCoachId — drops rows with non-numeric coach_id
{
  const rows = [
    { coach_id: 10, kind: "a" },
    { coach_id: "bad", kind: "b" },
    { coach_id: null, kind: "c" },
    { coach_id: NaN, kind: "d" },
    { coach_id: 30, kind: "e" },
  ];
  const g = groupByCoachId(rows);
  assert(g.size === 2, "group-drops-non-numeric-size", `got ${g.size}`);
  assert(
    g.has(10) && g.has(30),
    "group-drops-non-numeric-keys",
    `got ${[...g.keys()].join(",")}`,
  );
}

// 5a. formatAuditRecord — shape + newline-terminated
{
  const line = formatAuditRecord(
    { id: 1, display_name: "CONTACT" },
    [{ entity_type: "club", role: "head_coach" }],
    [{ event_type: "joined" }],
    [{ seasons_tracked: 2 }],
  );
  assert(line.endsWith("\n"), "audit-newline", `got ${JSON.stringify(line)}`);
  const parsed = JSON.parse(line);
  assert(
    JSON.stringify(Object.keys(parsed)) ===
      '["coach","careerHistory","movementEvents","effectiveness"]',
    "audit-key-order",
    `got ${Object.keys(parsed).join(",")}`,
  );
  assert(
    parsed.coach.display_name === "CONTACT",
    "audit-coach-pass-through",
    `got ${parsed.coach.display_name}`,
  );
  assert(
    Array.isArray(parsed.careerHistory) && parsed.careerHistory.length === 1,
    "audit-career-array",
    `got ${JSON.stringify(parsed.careerHistory)}`,
  );
  assert(
    Array.isArray(parsed.movementEvents) && parsed.movementEvents.length === 1,
    "audit-movement-array",
    `got ${JSON.stringify(parsed.movementEvents)}`,
  );
  assert(
    Array.isArray(parsed.effectiveness) && parsed.effectiveness.length === 1,
    "audit-effectiveness-array",
    `got ${JSON.stringify(parsed.effectiveness)}`,
  );
}

// 5b. formatAuditRecord — empty children arrays serialized as [] (not omitted)
{
  const line = formatAuditRecord({ id: 1 }, [], [], []);
  const parsed = JSON.parse(line);
  assert(
    Array.isArray(parsed.careerHistory) && parsed.careerHistory.length === 0,
    "audit-empty-career",
    `got ${JSON.stringify(parsed.careerHistory)}`,
  );
  assert(
    Array.isArray(parsed.movementEvents) && parsed.movementEvents.length === 0,
    "audit-empty-movement",
    `got ${JSON.stringify(parsed.movementEvents)}`,
  );
  assert(
    Array.isArray(parsed.effectiveness) && parsed.effectiveness.length === 0,
    "audit-empty-effectiveness",
    `got ${JSON.stringify(parsed.effectiveness)}`,
  );
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if (failures.length === 0) {
  console.log("[sweep-orphan-coaches.test] all pure-helper tests passed");
  process.exit(0);
} else {
  console.error(
    `[sweep-orphan-coaches.test] ${failures.length} failure(s):`,
  );
  for (const f of failures) {
    console.error(`  - ${f.name}: ${f.issue}`);
  }
  process.exit(1);
}
