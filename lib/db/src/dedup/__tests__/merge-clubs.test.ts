/**
 * mergeClubs — unit tests with a mocked Drizzle transaction.
 *
 * Run: pnpm --filter @workspace/db run test:merge-clubs
 *
 * We exercise the helper's SQL emission and result shape against an
 * in-memory fake that records each query and returns pre-configured
 * responses. No live DB. The integration-pg smoke test (when a real
 * Postgres is available) will catch constraint-level issues; here we
 * verify the helper's contract:
 *
 *   - every reparent table gets a single UPDATE keyed on loser id
 *   - matches gets TWO updates (home + away)
 *   - the audit alias is inserted
 *   - canonical_clubs winner is flipped manually_merged=true
 *   - canonical_clubs loser is DELETEd last
 *   - winner == loser throws
 *   - non-existent winnerId throws
 */

import { runMerge, type MergeClubsResult } from "../merge-clubs.js";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string): void {
  if (!cond) failures.push({ name, issue });
}

/** Minimal fake of the subset of the Drizzle tx API mergeClubs uses. */
type QueryCapture = {
  sqlText: string;
  params: unknown[];
};

type Responder = (capture: QueryCapture) => {
  rows?: Array<Record<string, unknown>>;
  rowCount?: number | null;
};

/**
 * Extract a readable-ish sqlText from the Drizzle SQL object. Drizzle's
 * `sql` template produces a wrapper with a `.queryChunks` array; chunks
 * are either:
 *   - StringChunk — literal SQL fragments; `.value` is a `string[]`
 *   - Param — template interpolation; `.value` is the runtime value
 *   - SQL (nested) — reuse via `sql.raw` etc; has its own `.queryChunks`
 *   - bare string — early-version shape
 * We flatten to a best-effort string + flat params list for substring
 * matching in tests.
 */
function stringifySql(q: unknown): { sqlText: string; params: unknown[] } {
  const anyQ = q as { queryChunks?: Array<unknown> };
  const chunks = anyQ.queryChunks ?? [];
  const parts: string[] = [];
  const params: unknown[] = [];
  for (const c of chunks) {
    if (typeof c === "string") {
      parts.push(c);
      continue;
    }
    // Drizzle inlines primitive params (numbers, bigints, strings when not
    // template-tagged) directly as chunk entries. Capture them as params.
    if (typeof c === "number" || typeof c === "bigint") {
      params.push(c);
      parts.push("?");
      continue;
    }
    if (c === null || c === undefined) {
      params.push(c);
      parts.push("?");
      continue;
    }
    if (typeof c !== "object") {
      parts.push(String(c));
      continue;
    }
    const obj = c as {
      value?: unknown;
      queryChunks?: unknown[];
    };
    // Nested SQL
    if (Array.isArray(obj.queryChunks)) {
      const sub = stringifySql(obj);
      parts.push(sub.sqlText);
      params.push(...sub.params);
      continue;
    }
    // StringChunk: .value is string[]
    if (Array.isArray(obj.value)) {
      parts.push(obj.value.join(""));
      continue;
    }
    // StringChunk (single string)
    if (typeof obj.value === "string") {
      parts.push(obj.value);
      continue;
    }
    // Param: primitive value
    if ("value" in obj) {
      params.push(obj.value);
      parts.push("?");
      continue;
    }
    parts.push(String(c));
  }
  return { sqlText: parts.join("").replace(/\s+/g, " ").trim(), params };
}

function makeTx(responder: Responder) {
  const captured: QueryCapture[] = [];
  const tx = {
    async execute(q: unknown): Promise<{
      rows?: Array<Record<string, unknown>>;
      rowCount?: number | null;
    }> {
      const cap = stringifySql(q);
      captured.push(cap);
      return responder(cap);
    },
  };
  return { tx, captured };
}

/**
 * Default responder for the happy-path test. Mirrors what Postgres would
 * return for each UPDATE/INSERT/DELETE we emit. Counts are arbitrary but
 * distinct per-table so we can verify the result shape wires them
 * correctly.
 */
function happyPathResponder(opts: {
  winnerId: number;
  loserId: number;
  counts: Record<string, number>;
}): Responder {
  const { winnerId, loserId, counts } = opts;
  return (cap) => {
    const s = cap.sqlText.toLowerCase();
    if (s.includes("select id, club_name_canonical")) {
      return {
        rows: [
          { id: winnerId, club_name_canonical: "Winner FC" },
          { id: loserId, club_name_canonical: "Loser SC" },
        ],
      };
    }
    if (s.includes("insert into club_aliases")) {
      return { rowCount: counts.loserAliasesCreated ?? 1 };
    }
    if (s.includes("update club_aliases set club_id")) {
      return { rowCount: 0 };
    }
    if (s.includes("update club_affiliations")) {
      return { rowCount: counts.affiliationsReparented ?? 0 };
    }
    if (s.includes("update coach_discoveries")) {
      return { rowCount: counts.coachDiscoveriesReparented ?? 0 };
    }
    if (s.includes("update club_roster_snapshots")) {
      return { rowCount: counts.rosterSnapshotsReparented ?? 0 };
    }
    if (s.includes("update roster_diffs")) {
      return { rowCount: counts.rosterDiffsReparented ?? 0 };
    }
    if (s.includes("update event_teams")) {
      return { rowCount: counts.eventTeamsReparented ?? 0 };
    }
    if (s.includes("update matches set home_club_id")) {
      return { rowCount: counts.matchesHome ?? 0 };
    }
    if (s.includes("update matches set away_club_id")) {
      return { rowCount: counts.matchesAway ?? 0 };
    }
    if (s.includes("update club_results")) {
      return { rowCount: counts.clubResultsReparented ?? 0 };
    }
    if (s.includes("update commitments")) {
      return { rowCount: counts.commitmentsReparented ?? 0 };
    }
    if (s.includes("update ynt_call_ups")) {
      return { rowCount: counts.yntReparented ?? 0 };
    }
    if (s.includes("update odp_roster_entries")) {
      return { rowCount: counts.odpReparented ?? 0 };
    }
    if (s.includes("update coach_career_history")) {
      return { rowCount: counts.coachCareerReparented ?? 0 };
    }
    if (s.includes("update tryouts")) {
      return { rowCount: counts.tryoutsReparented ?? 0 };
    }
    if (s.includes("update club_site_changes")) {
      return { rowCount: counts.siteChangesReparented ?? 0 };
    }
    if (s.includes("update coach_scrape_snapshots")) {
      return { rowCount: 0 };
    }
    if (s.includes("update player_id_selections")) {
      return { rowCount: 0 };
    }
    if (s.includes("update club_duplicates")) {
      return { rowCount: counts.clubDuplicatesMarked ?? 0 };
    }
    if (s.includes("update canonical_clubs set manually_merged")) {
      return { rowCount: 1 };
    }
    if (s.includes("delete from canonical_clubs")) {
      return { rowCount: 1 };
    }
    throw new Error(`happyPathResponder: unmatched sql: ${cap.sqlText}`);
  };
}

// ---------------------------------------------------------------------------
// Test 1 — Happy path. Each reparent table gets exactly one UPDATE keyed on
// loserId; matches gets two (home + away); counts flow through the result.
// ---------------------------------------------------------------------------
{
  const counts = {
    loserAliasesCreated: 1,
    affiliationsReparented: 3,
    coachDiscoveriesReparented: 4,
    rosterSnapshotsReparented: 5,
    rosterDiffsReparented: 6,
    eventTeamsReparented: 7,
    matchesHome: 8,
    matchesAway: 9,
    clubResultsReparented: 10,
    commitmentsReparented: 11,
    yntReparented: 12,
    odpReparented: 13,
    coachCareerReparented: 14,
    tryoutsReparented: 15,
    siteChangesReparented: 16,
    clubDuplicatesMarked: 17,
  };
  const { tx, captured } = makeTx(
    happyPathResponder({ winnerId: 100, loserId: 200, counts }),
  );

  let result: MergeClubsResult | undefined;
  let threw: unknown;
  try {
    result = await runMerge(tx, {
      winnerId: 100,
      loserId: 200,
      reviewedBy: 42,
      notes: "operator said so",
    });
  } catch (err) {
    threw = err;
  }

  assert(!threw, "happy-no-throw", `unexpected error: ${threw}`);
  assert(result !== undefined, "happy-result-defined", "result should be set");
  if (result) {
    assert(result.ok === true, "happy-ok", `got ${result.ok}`);
    assert(result.winnerId === 100, "happy-winnerId", `got ${result.winnerId}`);
    assert(
      result.loserAliasesCreated === 1,
      "happy-alias-count",
      `got ${result.loserAliasesCreated}`,
    );
    assert(
      result.affiliationsReparented === 3,
      "happy-affiliations",
      `got ${result.affiliationsReparented}`,
    );
    assert(
      result.coachDiscoveriesReparented === 4,
      "happy-coach-disc",
      `got ${result.coachDiscoveriesReparented}`,
    );
    assert(
      result.rosterSnapshotsReparented === 5,
      "happy-roster-snap",
      `got ${result.rosterSnapshotsReparented}`,
    );
    assert(
      result.rosterDiffsReparented === 6,
      "happy-roster-diffs",
      `got ${result.rosterDiffsReparented}`,
    );
    assert(
      result.eventTeamsReparented === 7,
      "happy-event-teams",
      `got ${result.eventTeamsReparented}`,
    );
    assert(
      result.matchesReparented === 17, // 8 home + 9 away
      "happy-matches",
      `got ${result.matchesReparented}`,
    );
    assert(
      result.clubResultsReparented === 10,
      "happy-club-results",
      `got ${result.clubResultsReparented}`,
    );
    assert(
      result.commitmentsReparented === 11,
      "happy-commitments",
      `got ${result.commitmentsReparented}`,
    );
    assert(
      result.yntReparented === 12,
      "happy-ynt",
      `got ${result.yntReparented}`,
    );
    assert(
      result.odpReparented === 13,
      "happy-odp",
      `got ${result.odpReparented}`,
    );
    assert(
      result.coachCareerReparented === 14,
      "happy-career",
      `got ${result.coachCareerReparented}`,
    );
    assert(
      result.tryoutsReparented === 15,
      "happy-tryouts",
      `got ${result.tryoutsReparented}`,
    );
    assert(
      result.siteChangesReparented === 16,
      "happy-site-changes",
      `got ${result.siteChangesReparented}`,
    );
    assert(
      result.clubDuplicatesMarked === 17,
      "happy-dup-marked",
      `got ${result.clubDuplicatesMarked}`,
    );
  }

  // Every expected FK table should appear in the captured SQL stream.
  const joined = captured.map((c) => c.sqlText.toLowerCase()).join("\n");
  const mustAppear = [
    "insert into club_aliases",
    "update club_aliases set club_id",
    "update club_affiliations set club_id",
    "update coach_discoveries set club_id",
    "update club_roster_snapshots set club_id",
    "update roster_diffs set club_id",
    "update event_teams set canonical_club_id",
    "update matches set home_club_id",
    "update matches set away_club_id",
    "update club_results set club_id",
    "update commitments set club_id",
    "update ynt_call_ups set club_id",
    "update odp_roster_entries set club_id",
    "update coach_career_history set entity_id",
    "update tryouts set club_id",
    "update club_site_changes set club_id",
    "update coach_scrape_snapshots set club_id",
    "update player_id_selections set club_id",
    "update club_duplicates set status",
    "update canonical_clubs set manually_merged",
    "delete from canonical_clubs",
  ];
  for (const needle of mustAppear) {
    assert(
      joined.includes(needle),
      `happy-emits-${needle.replace(/\s+/g, "-")}`,
      `expected sql containing "${needle}" to be emitted; captured: ${joined}`,
    );
  }

  // DELETE must be the last statement — otherwise FK violations on
  // subsequent UPDATEs would surface.
  const lastSql = captured[captured.length - 1]?.sqlText.toLowerCase() ?? "";
  assert(
    lastSql.startsWith("delete from canonical_clubs"),
    "happy-delete-last",
    `expected final sql to be DELETE, got: ${lastSql}`,
  );

  // Winner flip must precede the delete.
  const flipIdx = captured.findIndex((c) =>
    c.sqlText.toLowerCase().includes("update canonical_clubs set manually_merged"),
  );
  const deleteIdx = captured.findIndex((c) =>
    c.sqlText.toLowerCase().startsWith("delete from canonical_clubs"),
  );
  assert(
    flipIdx >= 0 && deleteIdx >= 0 && flipIdx < deleteIdx,
    "happy-flip-before-delete",
    `flip=${flipIdx} delete=${deleteIdx}`,
  );

  // Coach career-history UPDATE must scope to entity_type = 'club'.
  const careerSql = captured.find((c) =>
    c.sqlText.toLowerCase().includes("update coach_career_history"),
  )?.sqlText.toLowerCase() ?? "";
  assert(
    careerSql.includes("entity_type = 'club'"),
    "happy-career-scoped",
    `expected entity_type = 'club' guard; got: ${careerSql}`,
  );

  // Sanity — the loserId must be referenced as a parameter in every
  // reparent UPDATE. We find the loserId param by scanning every
  // captured query's params list for the value 200.
  const loserSeen = captured.filter((c) =>
    c.params.includes(200),
  ).length;
  assert(
    loserSeen >= 20,
    "happy-loser-param-reach",
    `expected loserId=200 in >= 20 queries, saw it in ${loserSeen}`,
  );
}

// ---------------------------------------------------------------------------
// Test 2 — winnerId == loserId throws with a descriptive message.
// ---------------------------------------------------------------------------
{
  const { tx } = makeTx(() => ({ rowCount: 0 }));
  let msg = "";
  try {
    await runMerge(tx, {
      winnerId: 7,
      loserId: 7,
      reviewedBy: null,
    });
    failures.push({ name: "same-id-throws", issue: "expected throw, got resolve" });
  } catch (err) {
    msg = err instanceof Error ? err.message : String(err);
  }
  assert(
    msg.includes("must differ"),
    "same-id-throws",
    `expected 'must differ' in error, got: ${msg}`,
  );
}

// ---------------------------------------------------------------------------
// Test 3 — non-existent winnerId throws. The existence check returns an
// empty (or partial) row set; the helper must raise before emitting any
// reparent SQL.
// ---------------------------------------------------------------------------
{
  const captured: QueryCapture[] = [];
  const tx = {
    async execute(q: unknown): Promise<{
      rows?: Array<Record<string, unknown>>;
      rowCount?: number | null;
    }> {
      const cap = stringifySql(q);
      captured.push(cap);
      const s = cap.sqlText.toLowerCase();
      if (s.includes("select id, club_name_canonical")) {
        // Only the loser row is present — winnerId 999 is missing.
        return { rows: [{ id: 200, club_name_canonical: "Loser SC" }] };
      }
      throw new Error(`unexpected sql: ${cap.sqlText}`);
    },
  };

  let msg = "";
  try {
    await runMerge(tx, {
      winnerId: 999,
      loserId: 200,
      reviewedBy: null,
    });
    failures.push({
      name: "missing-winner-throws",
      issue: "expected throw, got resolve",
    });
  } catch (err) {
    msg = err instanceof Error ? err.message : String(err);
  }
  assert(
    msg.includes("winnerId 999 not found"),
    "missing-winner-throws",
    `expected not-found message, got: ${msg}`,
  );
  assert(
    captured.length === 1,
    "missing-winner-no-side-effects",
    `expected exactly 1 query (the existence SELECT), got ${captured.length}`,
  );
}

// ---------------------------------------------------------------------------
// Test 4 — non-existent loserId throws.
// ---------------------------------------------------------------------------
{
  const tx = {
    async execute(q: unknown): Promise<{
      rows?: Array<Record<string, unknown>>;
      rowCount?: number | null;
    }> {
      const cap = stringifySql(q);
      const s = cap.sqlText.toLowerCase();
      if (s.includes("select id, club_name_canonical")) {
        return { rows: [{ id: 100, club_name_canonical: "Winner FC" }] };
      }
      throw new Error(`unexpected sql: ${cap.sqlText}`);
    },
  };

  let msg = "";
  try {
    await runMerge(tx, {
      winnerId: 100,
      loserId: 888,
      reviewedBy: null,
    });
    failures.push({
      name: "missing-loser-throws",
      issue: "expected throw, got resolve",
    });
  } catch (err) {
    msg = err instanceof Error ? err.message : String(err);
  }
  assert(
    msg.includes("loserId 888 not found"),
    "missing-loser-throws",
    `expected not-found message, got: ${msg}`,
  );
}

// ---------------------------------------------------------------------------
// Test 5 — DELETE returning 0 rows throws (defense in depth for the
// "cascade fired unexpectedly" scenario).
// ---------------------------------------------------------------------------
{
  const { tx } = makeTx((cap) => {
    const s = cap.sqlText.toLowerCase();
    if (s.includes("select id, club_name_canonical")) {
      return {
        rows: [
          { id: 100, club_name_canonical: "Winner FC" },
          { id: 200, club_name_canonical: "Loser SC" },
        ],
      };
    }
    if (s.includes("delete from canonical_clubs")) {
      return { rowCount: 0 };
    }
    // All other statements succeed with 0 rows touched.
    return { rowCount: 0 };
  });

  let msg = "";
  try {
    await runMerge(tx, {
      winnerId: 100,
      loserId: 200,
      reviewedBy: null,
    });
    failures.push({
      name: "delete-0-rows-throws",
      issue: "expected throw, got resolve",
    });
  } catch (err) {
    msg = err instanceof Error ? err.message : String(err);
  }
  assert(
    msg.includes("expected DELETE"),
    "delete-0-rows-throws",
    `expected DELETE-count guard message, got: ${msg}`,
  );
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if (failures.length === 0) {
  console.log("[merge-clubs] OK — all assertions passed");
  process.exit(0);
} else {
  console.error(`[merge-clubs] ${failures.length} failure(s):`);
  for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
  process.exit(1);
}
