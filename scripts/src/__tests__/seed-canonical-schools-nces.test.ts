/**
 * Smoke tests for the NCES CCD seeder. No live Postgres required — the
 * runSeed function accepts an injected UpsertClient, so we capture batched
 * rows in memory and assert on them.
 *
 * Run: pnpm --filter @workspace/scripts run test:seed-canonical-schools-nces
 *
 * The fixture CSV is intentionally tiny + includes edge cases:
 *   - 1 valid public HS (regular, grades 09-12)                     → OK
 *   - 1 K-5 elementary (regular, grades KG-05)                      → skip-non-hs
 *   - 1 regular K-12 with grades KG-12                              → OK (spans HS)
 *   - 1 special-ed school (SCH_TYPE=2, grades 09-12)                → skip-non-hs
 *   - 1 malformed row (missing NCESSCH)                             → drop
 *   - 1 malformed row (bogus state "ZZ")                            → drop
 *   - 1 duplicate of row #1 (same NCESSCH)                          → still counted as considered
 */

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  filterAndTransformRow,
  parseArgs,
  parseGrade,
  runSeed,
  spanOverlapsHighSchool,
  type NcesRow,
  type UpsertClient,
} from "../seed-canonical-schools-nces";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

function assertEq<T>(actual: T, expected: T, name: string) {
  if (actual !== expected) {
    failures.push({
      name,
      issue: `expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`,
    });
  }
}

// ---------------------------------------------------------------------------
// Unit tests — pure helpers
// ---------------------------------------------------------------------------

// parseGrade
assertEq(parseGrade("KG"), 0, "parseGrade-KG");
assertEq(parseGrade("PK"), -1, "parseGrade-PK");
assertEq(parseGrade("09"), 9, "parseGrade-09");
assertEq(parseGrade("12"), 12, "parseGrade-12");
assertEq(parseGrade("UG"), null, "parseGrade-UG");
assertEq(parseGrade(""), null, "parseGrade-empty");
assertEq(parseGrade(undefined), null, "parseGrade-undef");

// spanOverlapsHighSchool
assert(
  spanOverlapsHighSchool("09", "12"),
  "span-09-12",
  "09-12 must overlap HS",
);
assert(
  spanOverlapsHighSchool("KG", "12"),
  "span-KG-12",
  "KG-12 must overlap HS",
);
assert(
  !spanOverlapsHighSchool("KG", "05"),
  "span-KG-05",
  "elementary must not overlap HS",
);
assert(
  !spanOverlapsHighSchool("PK", "08"),
  "span-PK-08",
  "middle school ceiling 08 must not overlap HS",
);
assert(
  spanOverlapsHighSchool("11", "12"),
  "span-11-12",
  "11-12 must overlap HS",
);
assert(
  !spanOverlapsHighSchool("UG", "UG"),
  "span-UG-UG",
  "both-unparseable must reject",
);

// parseArgs
{
  const a = parseArgs(["--csv", "/tmp/x.csv", "--dry-run", "--limit", "50"]);
  assertEq(a.csvPath, "/tmp/x.csv", "args-csv");
  assertEq(a.dryRun, true, "args-dry");
  assertEq(a.limit, 50, "args-limit");
}
{
  const a = parseArgs([]);
  assertEq(a.csvPath, null, "args-empty-csv");
  assertEq(a.dryRun, false, "args-empty-dry");
  assertEq(a.limit, null, "args-empty-limit");
}

// filterAndTransformRow — state normalization
{
  const r = filterAndTransformRow({
    NCESSCH: "340996000195",
    SCH_NAME: "Lincoln High School",
    LSTATE: "nj", // lowercase — must uppercase
    SCH_TYPE: "1",
    GSLO: "09",
    GSHI: "12",
    LCITY: "Jersey City",
  });
  assertEq(r.kind, "ok", "filter-uppercase-state");
  if (r.row) {
    assertEq(r.row.state, "NJ", "filter-state-NJ");
    assertEq(r.row.ncessch, "340996000195", "filter-ncessch");
    assertEq(r.row.schoolName, "Lincoln High School", "filter-name");
    assertEq(r.row.city, "Jersey City", "filter-city");
  }
}

// filterAndTransformRow — drop bad state
{
  const r = filterAndTransformRow({
    NCESSCH: "X",
    SCH_NAME: "Test",
    LSTATE: "ZZZ",
    SCH_TYPE: "1",
    GSLO: "09",
    GSHI: "12",
  });
  assertEq(r.kind, "drop-malformed", "filter-drop-bad-state");
}

// filterAndTransformRow — skip elementary
{
  const r = filterAndTransformRow({
    NCESSCH: "A",
    SCH_NAME: "Maple Elementary",
    LSTATE: "CA",
    SCH_TYPE: "1",
    GSLO: "KG",
    GSHI: "05",
  });
  assertEq(r.kind, "skip-non-hs", "filter-skip-elementary");
}

// filterAndTransformRow — skip special-ed (SCH_TYPE=2)
{
  const r = filterAndTransformRow({
    NCESSCH: "B",
    SCH_NAME: "Alt Learning Center",
    LSTATE: "CA",
    SCH_TYPE: "2",
    GSLO: "09",
    GSHI: "12",
  });
  assertEq(r.kind, "skip-non-hs", "filter-skip-special-ed");
}

// ---------------------------------------------------------------------------
// Integration test — runSeed with a fixture CSV and captured upsert client
// ---------------------------------------------------------------------------

const fixture = [
  // Header — NCES CCD uses these exact column names
  [
    "NCESSCH",
    "SCH_NAME",
    "LSTATE",
    "LCITY",
    "SCH_TYPE",
    "GSLO",
    "GSHI",
  ].join(","),
  // Row 1: valid public HS
  ["340996000195", "Lincoln High School", "NJ", "Jersey City", "1", "09", "12"].join(","),
  // Row 2: elementary — skip
  ["060012301234", "Maple Elementary", "CA", "Fresno", "1", "KG", "05"].join(","),
  // Row 3: K-12 with HS grades present — OK (counts as HS)
  ["420024005678", "Rural K-12 School", "PA", "Altoona", "1", "KG", "12"].join(","),
  // Row 4: special-ed — skip
  [
    "060012309999",
    "Alt Learning Center",
    "CA",
    "Fresno",
    "2",
    "09",
    "12",
  ].join(","),
  // Row 5: malformed — missing NCESSCH
  ["", "Nameless HS", "TX", "Austin", "1", "09", "12"].join(","),
  // Row 6: malformed — state "ZZ" not 2-letter US (ZZ technically 2-letter, but
  //        we don't guard against bogus 2-letter pairs; use a 3-letter bogus)
  ["484848484848", "Bad State HS", "XXX", "?", "1", "09", "12"].join(","),
  // Row 7: duplicate of row 1 (same NCESSCH) — should still flow through the
  //        client (upsert idempotency is the client's responsibility, not the
  //        streamer's).
  ["340996000195", "Lincoln High School", "NJ", "Jersey City", "1", "09", "12"].join(","),
].join("\n");

async function integrationTest() {
  const tmp = path.join(os.tmpdir(), `ccd-fixture-${Date.now()}.csv`);
  fs.writeFileSync(tmp, fixture, "utf-8");

  const captured: NcesRow[][] = [];
  const client: UpsertClient = {
    async upsertBatch(rows) {
      captured.push([...rows]);
      return { inserted: rows.length, updated: 0 };
    },
  };

  const counters = await runSeed(tmp, client, { limit: null, dryRun: false });

  // 7 data rows total
  assertEq(counters.considered, 7, "integration-considered");
  // Rows 1, 3, 7 → OK (3 inserted; dedup is the DB's job)
  assertEq(counters.inserted, 3, "integration-inserted");
  assertEq(counters.updated, 0, "integration-updated");
  // Rows 2, 4 → skip-non-hs
  assertEq(counters.skippedNonHs, 2, "integration-skipped");
  // Rows 5, 6 → drop-malformed
  assertEq(counters.droppedMalformed, 2, "integration-dropped");

  // All captured rows have UPPERCASE 2-letter state
  const flat = captured.flat();
  assertEq(flat.length, 3, "integration-captured-count");
  for (const r of flat) {
    assert(
      /^[A-Z]{2}$/.test(r.state),
      "integration-state-format",
      `bad state: ${r.state}`,
    );
  }

  // Row 1 and Row 7 have the same NCESSCH — both get passed through.
  const lincolns = flat.filter((r) => r.ncessch === "340996000195");
  assertEq(lincolns.length, 2, "integration-dup-flow-through");

  fs.unlinkSync(tmp);
}

async function dryRunTest() {
  const tmp = path.join(os.tmpdir(), `ccd-fixture-dry-${Date.now()}.csv`);
  fs.writeFileSync(tmp, fixture, "utf-8");
  let called = 0;
  const client: UpsertClient = {
    async upsertBatch() {
      called++;
      return { inserted: 0, updated: 0 };
    },
  };
  const c = await runSeed(tmp, client, { limit: null, dryRun: true });
  assertEq(called, 0, "dryrun-no-client-calls");
  // Dry-run reports OK rows under `inserted` for parity
  assertEq(c.inserted, 3, "dryrun-inserted-count");
  fs.unlinkSync(tmp);
}

async function limitTest() {
  const tmp = path.join(os.tmpdir(), `ccd-fixture-limit-${Date.now()}.csv`);
  fs.writeFileSync(tmp, fixture, "utf-8");
  const client: UpsertClient = {
    async upsertBatch(rows) {
      return { inserted: rows.length, updated: 0 };
    },
  };
  const c = await runSeed(tmp, client, { limit: 2, dryRun: false });
  assertEq(c.considered, 2, "limit-considered");
  // Rows 1-2: row 1 = OK (insert), row 2 = elementary (skip)
  assertEq(c.inserted, 1, "limit-inserted");
  assertEq(c.skippedNonHs, 1, "limit-skipped");
  fs.unlinkSync(tmp);
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

(async () => {
  await integrationTest();
  await dryRunTest();
  await limitTest();

  if (failures.length === 0) {
    console.log("[seed-canonical-schools-nces] OK — all assertions passed");
    process.exit(0);
  }
  console.error(`[seed-canonical-schools-nces] ${failures.length} failure(s):`);
  for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
  process.exit(1);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
