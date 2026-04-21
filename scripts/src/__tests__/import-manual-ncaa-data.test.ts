/**
 * Unit tests for import-manual-ncaa-data.ts validators + CSV type detection.
 *
 * Run:
 *   pnpm --filter @workspace/scripts run test:import-manual-ncaa-data
 *
 * DATABASE_URL must be set (even to a fake value) so the @workspace/db
 * pool factory doesn't crash at import time. Tests never actually hit
 * the DB.
 */
import assert from "node:assert/strict";
import {
  detectCsvType,
  validateCoachRow,
  validateUrlRow,
  validateRosterRow,
} from "../import-manual-ncaa-data.ts";

const failures: string[] = [];

function test(name: string, fn: () => void) {
  try {
    fn();
    console.log(`  ok ${name}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    failures.push(`  not ok ${name}: ${msg}`);
    console.error(`  not ok ${name}: ${msg}`);
  }
}

// ---------------------------------------------------------------------------
// detectCsvType
// ---------------------------------------------------------------------------

console.log("detectCsvType");

test("coaches shape", () => {
  assert.equal(
    detectCsvType(["college_id", "name", "academic_year", "title"]),
    "coaches",
  );
});

test("urls shape (no academic_year)", () => {
  assert.equal(
    detectCsvType(["college_id", "college_name", "soccer_program_url"]),
    "urls",
  );
});

test("rosters shape", () => {
  assert.equal(
    detectCsvType(["college_id", "academic_year", "player_name", "position"]),
    "rosters",
  );
});

test("roster detection wins over coach even if 'name' present", () => {
  // If both player_name and name are in headers, rosters wins (player_name
  // is more specific)
  assert.equal(
    detectCsvType(["college_id", "name", "player_name", "academic_year"]),
    "rosters",
  );
});

test("unknown shape throws", () => {
  assert.throws(
    () => detectCsvType(["foo", "bar", "baz"]),
    /Cannot detect CSV type/,
  );
});

// ---------------------------------------------------------------------------
// validateCoachRow
// ---------------------------------------------------------------------------

console.log("validateCoachRow");

test("happy path full row", () => {
  const row = validateCoachRow(
    {
      college_id: "42",
      academic_year: "2023-24",
      name: "Jane Doe",
      title: "Head Coach",
      email: "JDOE@EXAMPLE.EDU",
      phone: "(555) 123-4567",
      source_url: "https://example.edu/roster",
      is_head_coach: "true",
    },
    1,
  );
  assert.ok(!("error" in row), "row should be valid");
  assert.equal((row as any).college_id, 42);
  assert.equal((row as any).academic_year, "2023-24");
  assert.equal((row as any).name, "Jane Doe");
  assert.equal((row as any).email, "jdoe@example.edu"); // lowercased
  assert.equal((row as any).is_head_coach, true);
});

test("blank email and phone kept as null", () => {
  const row = validateCoachRow(
    { college_id: "1", academic_year: "2025-26", name: "X", title: "Head Coach", email: "", phone: "" },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).email, null);
  assert.equal((row as any).phone, null);
});

test("missing college_id rejects", () => {
  const row = validateCoachRow(
    { college_id: "", academic_year: "2023-24", name: "X", title: "Head Coach" },
    7,
  );
  assert.ok("error" in row);
  assert.match((row as any).error, /college_id/);
});

test("bad academic_year format rejects", () => {
  const row = validateCoachRow(
    { college_id: "42", academic_year: "2023", name: "X", title: "Head Coach" },
    7,
  );
  assert.ok("error" in row);
  assert.match((row as any).error, /academic_year/);
});

test("missing name rejects", () => {
  const row = validateCoachRow(
    { college_id: "42", academic_year: "2023-24", name: "", title: "Head Coach" },
    7,
  );
  assert.ok("error" in row);
  assert.match((row as any).error, /missing name/);
});

test("is_head_coach defaults true when blank", () => {
  const row = validateCoachRow(
    { college_id: "1", academic_year: "2025-26", name: "X", title: "Head Coach", is_head_coach: "" },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).is_head_coach, true);
});

// ---------------------------------------------------------------------------
// validateUrlRow
// ---------------------------------------------------------------------------

console.log("validateUrlRow");

test("happy path", () => {
  const row = validateUrlRow(
    { college_id: "5", soccer_program_url: "https://example.edu/sports/mens-soccer/roster" },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).college_id, 5);
});

test("non-http URL rejects", () => {
  const row = validateUrlRow(
    { college_id: "5", soccer_program_url: "example.edu/sports" },
    1,
  );
  assert.ok("error" in row);
  assert.match((row as any).error, /http/);
});

test("missing college_id rejects", () => {
  const row = validateUrlRow(
    { college_id: "", soccer_program_url: "https://example.edu/x" },
    7,
  );
  assert.ok("error" in row);
});

// ---------------------------------------------------------------------------
// validateRosterRow
// ---------------------------------------------------------------------------

console.log("validateRosterRow");

test("happy path", () => {
  const row = validateRosterRow(
    {
      college_id: "5",
      academic_year: "2023-24",
      player_name: "Kim Lee",
      position: "GK",
      year: "senior",
      hometown: "Seattle, WA",
      prev_club: "Seattle HS",
      jersey_number: "1",
    },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).year, "senior");
});

test("blank template row (all empty) returns __BLANK__ marker", () => {
  const row = validateRosterRow(
    { college_id: "", academic_year: "", player_name: "" },
    1,
  );
  assert.ok("error" in row);
  assert.equal((row as any).error, "__BLANK__");
});

test("unknown year enum → null (not rejection)", () => {
  const row = validateRosterRow(
    {
      college_id: "5",
      academic_year: "2023-24",
      player_name: "X",
      year: "transfer", // not in the enum
    },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).year, null);
});

test("year enum accepts case-insensitive", () => {
  const row = validateRosterRow(
    { college_id: "5", academic_year: "2023-24", player_name: "X", year: "SENIOR" },
    1,
  );
  assert.ok(!("error" in row));
  assert.equal((row as any).year, "senior");
});

test("missing player_name rejects (non-blank row)", () => {
  const row = validateRosterRow(
    { college_id: "5", academic_year: "2023-24", player_name: "" },
    7,
  );
  assert.ok("error" in row);
  assert.notEqual((row as any).error, "__BLANK__");
  assert.match((row as any).error, /player_name/);
});

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

if (failures.length > 0) {
  console.error(`\n${failures.length} FAILED`);
  process.exit(1);
}
console.log("\nAll tests passed");
