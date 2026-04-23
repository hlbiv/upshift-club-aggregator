import assert from "node:assert/strict";
import { test } from "node:test";

import {
  isJunkName,
  isSafeToDelete,
  JUNK_NAME_EXACT,
  MAX_CLUB_NAME_LEN,
  parseArgs,
} from "../purge-junk-canonical-clubs.ts";

test("parseArgs defaults to dry-run with /tmp audit dir", () => {
  const a = parseArgs([]);
  assert.equal(a.commit, false);
  assert.equal(a.auditDir, "/tmp");
});

test("parseArgs --commit and --audit-dir override defaults", () => {
  const a = parseArgs(["--commit", "--audit-dir", "/work/x"]);
  assert.equal(a.commit, true);
  assert.equal(a.auditDir, "/work/x");
});

test("isJunkName flags every documented offender", () => {
  for (const bad of [
    "SINC Content Manager",
    "Merge Tourneys",
    "USYS",
    "US Club",
    "Us",
    "Display Settings",
  ]) {
    assert.equal(isJunkName(bad), true, `expected ${bad!} to be junk`);
  }
});

test("isJunkName flags overly long names regardless of content", () => {
  const blob = "X".repeat(MAX_CLUB_NAME_LEN + 1);
  assert.equal(isJunkName(blob), true);
});

test("isJunkName accepts real club names", () => {
  for (const good of [
    "FC Dallas",
    "Solar SC",
    "Hoover Soccer Club",
    "Mississippi Rush",
  ]) {
    assert.equal(isJunkName(good), false, `expected ${good} to pass`);
  }
});

test("JUNK_NAME_EXACT entries are all lower-cased and trimmed", () => {
  for (const k of JUNK_NAME_EXACT) {
    assert.equal(k, k.trim().toLowerCase(), `${k} must be normalized`);
  }
});

test("isSafeToDelete only allows affs/aliases references", () => {
  const zero = {
    affs: 0,
    aliases: 0,
    coaches: 0,
    tryouts: 0,
    commitments: 0,
    results: 0,
    rosters: 0,
    site_changes: 0,
    coach_snaps: 0,
    odp: 0,
    pid: 0,
    rdiffs: 0,
    videos: 0,
    ynt: 0,
    event_teams: 0,
  };
  assert.equal(isSafeToDelete(zero), true);
  assert.equal(isSafeToDelete({ ...zero, affs: 9, aliases: 3 }), true);
  assert.equal(isSafeToDelete({ ...zero, coaches: 1 }), false);
  assert.equal(isSafeToDelete({ ...zero, event_teams: 1 }), false);
  assert.equal(isSafeToDelete({ ...zero, tryouts: 1 }), false);
});
