/**
 * analytics/duplicates + analytics/duplicates/review — integration tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx \
 *        src/__tests__/analyticsDuplicateReview.test.ts
 *
 * No real database: we replace the router's query executor
 * (`__setExecRowsForTests`) with a tiny in-memory emulator that knows
 * just enough about the two SQL shapes this route family produces:
 *
 *   1. GET /analytics/duplicates — expands clusters into pairs and left-
 *      joins `duplicate_review_decisions`. The emulator pattern-matches
 *      the two CTE queries (count, data) and returns rows from a
 *      `FakeDb` seeded at test setup.
 *   2. POST /analytics/duplicates/review — pre-existence check + upsert.
 *      The emulator recognizes the two shapes and mutates the FakeDb.
 *
 * The tests cover the scenarios listed in the PR brief:
 *   (a) POST then GET `status=pending` excludes the reviewed pair
 *   (b) GET `status=all` returns review state attached to the pair
 *   (c) second POST upserts (no duplicate row, decision/notes updated)
 *   (d) POST with (b > a) resolves to the same row as (a, b)
 *   (e) POST with a == b returns 400 self_pair_not_allowed
 *   (f) POST with an unknown club id returns 400 unknown_club_id
 */
import http from "node:http";
import express from "express";
import type { SQL } from "drizzle-orm";
import analyticsRouter, {
  __setExecRowsForTests,
} from "../routes/analytics";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// ---------------------------------------------------------------------------
// Fake DB — just enough state for the SQL shapes the router emits.
// ---------------------------------------------------------------------------

type CanonicalClub = {
  id: number;
  name: string;
  normalized: string;
  state: string | null;
};

type ReviewRow = {
  id: number;
  clubA: number;
  clubB: number;
  decision: "pending" | "merged" | "rejected";
  decidedBy: string | null;
  decidedAt: Date;
  notes: string | null;
};

class FakeDb {
  clubs: CanonicalClub[] = [];
  reviews: ReviewRow[] = [];
  nextReviewId = 1;
  // The emulator doesn't parse the status filter out of the SQL — the
  // test harness pokes it in here before each GET call.
  status: "pending" | "all" | "rejected" | "merged" = "pending";

  seedCluster(
    normalizedName: string,
    clubs: { id: number; name: string; state: string | null }[],
  ): void {
    for (const c of clubs) {
      this.clubs.push({
        id: c.id,
        name: c.name,
        normalized: normalizedName,
        state: c.state,
      });
    }
  }

  findReview(a: number, b: number): ReviewRow | undefined {
    return this.reviews.find((r) => r.clubA === a && r.clubB === b);
  }

  buildPairs(): Array<{
    normalized: string;
    state: string | null;
    clubCount: number;
    clubIds: number[];
    clubNames: string[];
    clubAId: number;
    clubBId: number;
    clubAName: string;
    clubBName: string;
    review: ReviewRow | null;
  }> {
    const clusters = new Map<
      string,
      { normalized: string; state: string | null; clubs: CanonicalClub[] }
    >();
    for (const c of this.clubs) {
      const key = `${c.normalized}|${c.state ?? ""}`;
      if (!clusters.has(key)) {
        clusters.set(key, { normalized: c.normalized, state: c.state, clubs: [] });
      }
      clusters.get(key)!.clubs.push(c);
    }

    const pairs: Array<{
      normalized: string;
      state: string | null;
      clubCount: number;
      clubIds: number[];
      clubNames: string[];
      clubAId: number;
      clubBId: number;
      clubAName: string;
      clubBName: string;
      review: ReviewRow | null;
    }> = [];

    for (const cl of clusters.values()) {
      if (cl.clubs.length < 2) continue;
      const sorted = [...cl.clubs].sort((a, b) => a.id - b.id);
      const clubIds = sorted.map((c) => c.id);
      const clubNames = sorted.map((c) => c.name);
      for (let i = 0; i < sorted.length; i++) {
        for (let j = i + 1; j < sorted.length; j++) {
          const a = sorted[i]!;
          const b = sorted[j]!;
          const review = this.findReview(a.id, b.id) ?? null;
          const include = (() => {
            if (this.status === "all") return true;
            if (this.status === "pending") {
              return !review || review.decision === "pending";
            }
            return review?.decision === this.status;
          })();
          if (!include) continue;
          pairs.push({
            normalized: cl.normalized,
            state: cl.state,
            clubCount: cl.clubs.length,
            clubIds,
            clubNames,
            clubAId: a.id,
            clubBId: b.id,
            clubAName: a.name,
            clubBName: b.name,
            review,
          });
        }
      }
    }
    return pairs;
  }
}

// Flatten the literal SQL text from a drizzle SQL query, ignoring
// interpolated param values. Enough disambiguation for pattern matching
// across the handful of query shapes this route family emits.
function sqlText(q: SQL): string {
  const out: string[] = [];
  function walk(chunks: unknown[]): void {
    for (const c of chunks) {
      if (c == null || typeof c !== "object") continue;
      const asObj = c as Record<string, unknown>;
      if (Array.isArray(asObj.value)) {
        for (const s of asObj.value) {
          if (typeof s === "string") out.push(s);
        }
      }
      if (Array.isArray(asObj.queryChunks)) walk(asObj.queryChunks);
    }
  }
  const root = q as unknown as { queryChunks?: unknown[] };
  if (Array.isArray(root.queryChunks)) walk(root.queryChunks);
  return out.join("");
}

function extractParams(q: SQL): unknown[] {
  // Walk the drizzle-orm SQL queryChunks array. Chunks come in two kinds:
  //   1. StringChunk: `{ value: [<sql text>] }` — the literal SQL between
  //      interpolations. Skipped.
  //   2. Raw interpolated values (numbers, strings, null, nested SQL).
  //      Returned as params in order.
  // Nested SQLs (produced by `sql`` ` inside `${}`) recursively have their
  // own queryChunks — we walk them to flatten all params in source order.
  const out: unknown[] = [];
  const root = q as unknown as { queryChunks?: unknown[] };
  function walk(chunks: unknown[]): void {
    for (const c of chunks) {
      if (c == null) {
        out.push(null);
        continue;
      }
      if (typeof c === "object") {
        const asObj = c as Record<string, unknown>;
        if (
          "value" in asObj &&
          Array.isArray(asObj.value) &&
          !("queryChunks" in asObj)
        ) {
          // StringChunk — pure literal SQL, not a param.
          continue;
        }
        if (Array.isArray(asObj.queryChunks)) {
          walk(asObj.queryChunks);
          continue;
        }
        // Some other object (e.g. a Param wrapper) — take .value if present.
        if ("value" in asObj) {
          out.push(asObj.value);
          continue;
        }
        continue;
      }
      // Primitive — string / number / boolean.
      out.push(c);
    }
  }
  if (Array.isArray(root.queryChunks)) walk(root.queryChunks);
  return out;
}

function buildExec(
  fake: FakeDb,
): (q: SQL) => Promise<Record<string, unknown>[]> {
  return async function exec(q: SQL): Promise<Record<string, unknown>[]> {
    const text = sqlText(q);

    // --- POST /review pre-existence check ---
    if (
      text.includes("canonical_clubs") &&
      text.includes("WHERE id IN") &&
      !text.includes("normalized")
    ) {
      const params = extractParams(q);
      const ids = params.filter((p): p is number => typeof p === "number");
      const found = fake.clubs.filter((c) => ids.includes(c.id));
      return found.map((c) => ({ id: c.id }));
    }

    // --- POST /review upsert ---
    if (text.includes("INSERT INTO duplicate_review_decisions")) {
      const params = extractParams(q);
      // params order from the writer: clubA, clubB, decision, decidedBy, notes
      const numericIds = params.filter((p): p is number => typeof p === "number");
      const clubA = numericIds[0]!;
      const clubB = numericIds[1]!;
      const stringParams = params.filter(
        (p) => typeof p === "string" || p === null,
      ) as (string | null)[];
      // Decision is the first string; decidedBy and notes follow and may be null.
      const decision = stringParams[0] as ReviewRow["decision"];
      const decidedBy = stringParams[1] ?? null;
      const notes = stringParams[2] ?? null;

      const existing = fake.findReview(clubA, clubB);
      const writeRow = (row: ReviewRow): Record<string, unknown> => ({
        id: row.id,
        club_a_id: row.clubA,
        club_b_id: row.clubB,
        decision: row.decision,
        decided_by: row.decidedBy,
        decided_at: row.decidedAt,
        notes: row.notes,
      });
      if (existing) {
        existing.decision = decision;
        existing.decidedBy = decidedBy;
        existing.decidedAt = new Date();
        existing.notes = notes;
        return [writeRow(existing)];
      }
      const row: ReviewRow = {
        id: fake.nextReviewId++,
        clubA,
        clubB,
        decision,
        decidedBy,
        decidedAt: new Date(),
        notes,
      };
      fake.reviews.push(row);
      return [writeRow(row)];
    }

    // --- GET /duplicates — count query ---
    if (
      text.includes("pairs_with_review") &&
      text.includes("COUNT(*)") &&
      text.includes("total")
    ) {
      return [{ total: fake.buildPairs().length }];
    }

    // --- GET /duplicates — data query ---
    if (text.includes("pairs_with_review") && text.includes("sources")) {
      return fake.buildPairs().map((p) => ({
        normalized_name: p.normalized,
        state: p.state,
        club_count: p.clubCount,
        club_ids: p.clubIds,
        club_names: p.clubNames,
        club_a_id: p.clubAId,
        club_b_id: p.clubBId,
        club_a_name: p.clubAName,
        club_b_name: p.clubBName,
        decision: p.review?.decision ?? null,
        decided_by: p.review?.decidedBy ?? null,
        decided_at: p.review?.decidedAt ?? null,
        notes: p.review?.notes ?? null,
        sources: [],
      }));
    }

    throw new Error(
      `[FakeDb] unhandled SQL shape: ${text.slice(0, 200)}`,
    );
  };
}

// ---------------------------------------------------------------------------
// Tiny HTTP client helpers (mirrors docs.test.ts).
// ---------------------------------------------------------------------------

interface HitResult {
  status: number;
  body: unknown;
}

function hit(
  port: number,
  path: string,
  init: { method?: string; body?: unknown } = {},
): Promise<HitResult> {
  return new Promise((resolve, reject) => {
    const payload = init.body != null ? JSON.stringify(init.body) : undefined;
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path,
        method: init.method ?? "GET",
        headers: payload
          ? {
              "content-type": "application/json",
              "content-length": Buffer.byteLength(payload).toString(),
            }
          : {},
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          const text = Buffer.concat(chunks).toString("utf8");
          let parsed: unknown = text;
          try {
            parsed = text.length > 0 ? JSON.parse(text) : null;
          } catch {
            // leave as text
          }
          resolve({ status: res.statusCode ?? 0, body: parsed });
        });
      },
    );
    req.on("error", reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function startServer(): Promise<{ port: number; close: () => Promise<void> }> {
  const app = express();
  app.use(express.json());
  app.use("/api", analyticsRouter);
  return new Promise((resolve, reject) => {
    const server = app.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        reject(new Error("no server address"));
        return;
      }
      resolve({
        port: addr.port,
        close: () =>
          new Promise<void>((r, j) =>
            server.close((err) => (err ? j(err) : r())),
          ),
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function run() {
  const fake = new FakeDb();
  // Seed one cluster of two clubs so we have exactly one pair (1, 2).
  fake.seedCluster("phoenix rising", [
    { id: 1, name: "Phoenix Rising FC", state: "AZ" },
    { id: 2, name: "Phoenix Rising SC", state: "AZ" },
  ]);

  __setExecRowsForTests(buildExec(fake));
  const { port, close } = await startServer();

  try {
    // --- Baseline: GET pending returns the one pair, no review attached
    {
      fake.status = "pending";
      const res = await hit(port, "/api/analytics/duplicates?status=pending");
      const body = res.body as {
        duplicates: Record<string, unknown>[];
        total: number;
      };
      assert(res.status === 200, "baseline-pending", `expected 200 got ${res.status}`);
      assert(
        body.total === 1 && body.duplicates.length === 1,
        "baseline-pending",
        `expected 1 pair, got ${body.total}/${body.duplicates.length}`,
      );
      const pair = body.duplicates[0]!;
      assert(
        pair.club_a_id === 1 && pair.club_b_id === 2,
        "baseline-pending",
        `expected pair (1,2), got (${pair.club_a_id},${pair.club_b_id})`,
      );
      assert(
        pair.review === null,
        "baseline-pending",
        `expected review=null, got ${JSON.stringify(pair.review)}`,
      );
    }

    // --- (e) self-pair → 400
    {
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: { club_a_id: 1, club_b_id: 1, decision: "merged" },
      });
      assert(res.status === 400, "self-pair", `expected 400 got ${res.status}`);
      assert(
        (res.body as { error?: string }).error === "self_pair_not_allowed",
        "self-pair",
        `body should be self_pair_not_allowed, got ${JSON.stringify(res.body)}`,
      );
    }

    // --- (f) unknown club id → 400
    {
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: { club_a_id: 99, club_b_id: 100, decision: "merged" },
      });
      assert(res.status === 400, "unknown-club", `expected 400 got ${res.status}`);
      assert(
        (res.body as { error?: string }).error === "unknown_club_id",
        "unknown-club",
        `body should be unknown_club_id, got ${JSON.stringify(res.body)}`,
      );
    }

    // --- First POST: decision = merged, notes set
    {
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: {
          club_a_id: 1,
          club_b_id: 2,
          decision: "merged",
          notes: "obvious dup",
        },
      });
      assert(res.status === 200, "first-post", `expected 200 got ${res.status}`);
      const body = res.body as {
        id: number;
        club_a_id: number;
        club_b_id: number;
        decision: string;
        notes: string | null;
      };
      assert(
        body.club_a_id === 1 && body.club_b_id === 2,
        "first-post",
        `expected (1,2), got (${body.club_a_id},${body.club_b_id})`,
      );
      assert(body.decision === "merged", "first-post", `decision wrong: ${body.decision}`);
      assert(body.notes === "obvious dup", "first-post", `notes wrong: ${body.notes}`);
    }

    // --- (a) GET pending now excludes the reviewed pair
    {
      fake.status = "pending";
      const res = await hit(port, "/api/analytics/duplicates?status=pending");
      const body = res.body as { total: number };
      assert(body.total === 0, "pending-excludes", `expected 0, got ${body.total}`);
    }

    // --- (b) GET all returns the pair with review attached
    {
      fake.status = "all";
      const res = await hit(port, "/api/analytics/duplicates?status=all");
      const body = res.body as { duplicates: Record<string, unknown>[] };
      assert(
        body.duplicates.length === 1,
        "all-returns-one",
        `expected 1 pair, got ${body.duplicates.length}`,
      );
      const review = body.duplicates[0]!.review as Record<string, unknown> | null;
      assert(
        review !== null && review.decision === "merged",
        "all-review-attached",
        `review should be merged, got ${JSON.stringify(review)}`,
      );
      assert(
        review !== null && review.notes === "obvious dup",
        "all-notes-attached",
        `notes should be 'obvious dup', got ${JSON.stringify(review?.notes)}`,
      );
    }

    // --- (c) second POST upserts (not duplicates)
    {
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: {
          club_a_id: 1,
          club_b_id: 2,
          decision: "rejected",
          notes: "oh wait, different age group",
        },
      });
      assert(res.status === 200, "upsert-status", `expected 200 got ${res.status}`);
      assert(
        fake.reviews.length === 1,
        "upsert-no-dup",
        `expected 1 review row, got ${fake.reviews.length}`,
      );
      assert(
        fake.reviews[0]!.decision === "rejected",
        "upsert-decision",
        `decision should be rejected, got ${fake.reviews[0]!.decision}`,
      );
      assert(
        fake.reviews[0]!.notes === "oh wait, different age group",
        "upsert-notes",
        `notes should be updated, got ${fake.reviews[0]!.notes}`,
      );
    }

    // --- (d) normalization: POST (2, 1) resolves to same row
    {
      const before = fake.reviews.length;
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: { club_a_id: 2, club_b_id: 1, decision: "pending", notes: "revisit" },
      });
      assert(res.status === 200, "normalize-status", `expected 200 got ${res.status}`);
      assert(
        fake.reviews.length === before,
        "normalize-no-new-row",
        `POST (2,1) should upsert the (1,2) row, got new rows`,
      );
      assert(
        fake.reviews[0]!.decision === "pending" && fake.reviews[0]!.notes === "revisit",
        "normalize-updated",
        `row content should reflect the new POST`,
      );
      const body = res.body as { club_a_id: number; club_b_id: number };
      assert(
        body.club_a_id === 1 && body.club_b_id === 2,
        "normalize-response",
        `response pair should be (1,2), got (${body.club_a_id},${body.club_b_id})`,
      );
    }

    // --- Zod validation: missing decision → 400
    {
      const res = await hit(port, "/api/analytics/duplicates/review", {
        method: "POST",
        body: { club_a_id: 1, club_b_id: 2 },
      });
      assert(res.status === 400, "zod-missing", `expected 400, got ${res.status}`);
      assert(
        (res.body as { error?: string }).error === "invalid_body",
        "zod-missing",
        `body error should be invalid_body, got ${JSON.stringify(res.body)}`,
      );
    }
  } finally {
    await close();
    __setExecRowsForTests(null);
  }

  if (failures.length === 0) {
    console.log("[analytics-duplicate-review-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(
      `[analytics-duplicate-review-test] ${failures.length} failure(s):`,
    );
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
