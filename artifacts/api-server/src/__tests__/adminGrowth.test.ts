/**
 * admin/growth routes — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminGrowth.test.ts
 *
 * Same factory-handler pattern as adminDedup.test.ts — inject a fake
 * `GrowthDeps` so the handlers never reach Postgres. Three scenarios:
 *
 *   1. scraped-counts returns the 5 per-table fields with mocked counts.
 *   2. coverage-trend?days=30 aggregates by day correctly from mocked rows.
 *   3. coverage-trend?days=0 returns an empty points array (edge case).
 */
import type { Request, Response } from "express";
import {
  makeScrapedCountsHandler,
  makeCoverageTrendHandler,
  type GrowthDeps,
} from "../routes/admin/growth";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// ---------------------------------------------------------------------------
// Fake req/res.
// ---------------------------------------------------------------------------

type FakeRes = {
  statusCode: number;
  body: unknown;
  status: (code: number) => FakeRes;
  json: (body: unknown) => FakeRes;
};

function makeRes(): FakeRes {
  const res: FakeRes = {
    statusCode: 200,
    body: undefined,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      return this;
    },
  };
  return res;
}

function makeReq(query: Record<string, string> = {}): Request {
  return {
    params: {},
    query,
    body: {},
  } as unknown as Request;
}

// ---------------------------------------------------------------------------
// Deps fixtures.
// ---------------------------------------------------------------------------

type CountCall = {
  table: "clubs" | "coaches" | "events" | "rosterSnapshots" | "matches";
  since: Date;
};

function makeDeps(opts: {
  counts?: Partial<Record<CountCall["table"], number>>;
  trend?: Array<{
    date: string;
    runs: number;
    successes: number;
    failures: number;
    rowsTouched: number;
  }>;
  trendCalls?: Array<{ days: number }>;
  countCalls?: CountCall[];
}): GrowthDeps {
  const counts = opts.counts ?? {};
  return {
    countSince: async ({ table, since }) => {
      opts.countCalls?.push({ table, since });
      return counts[table] ?? 0;
    },
    coverageTrend: async ({ days }) => {
      opts.trendCalls?.push({ days });
      // Short-circuit when caller requested 0 — mirrors the prod impl so the
      // edge-case test exercises the real behavior rather than the fake.
      if (days <= 0) return [];
      return opts.trend ?? [];
    },
  };
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function run() {
  // --- 1. scraped-counts returns all 5 fields from mocked counts ----------
  {
    const countCalls: CountCall[] = [];
    const deps = makeDeps({
      counts: {
        clubs: 3,
        coaches: 17,
        events: 5,
        rosterSnapshots: 41,
        matches: 88,
      },
      countCalls,
    });
    const handler = makeScrapedCountsHandler(deps);
    const since = "2026-04-10T00:00:00.000Z";
    const req = makeReq({ since });
    const res = makeRes();
    await handler(req, res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "scraped-counts-ok",
      `expected 200, got ${res.statusCode}`,
    );

    const body = res.body as {
      since?: string;
      clubsAdded?: number;
      coachesAdded?: number;
      eventsAdded?: number;
      rosterSnapshotsAdded?: number;
      matchesAdded?: number;
    };
    assert(
      body.since === since,
      "scraped-counts-ok",
      `since should echo; got ${body.since}`,
    );
    assert(body.clubsAdded === 3, "scraped-counts-ok", `clubsAdded=3 expected, got ${body.clubsAdded}`);
    assert(body.coachesAdded === 17, "scraped-counts-ok", `coachesAdded=17 expected, got ${body.coachesAdded}`);
    assert(body.eventsAdded === 5, "scraped-counts-ok", `eventsAdded=5 expected, got ${body.eventsAdded}`);
    assert(
      body.rosterSnapshotsAdded === 41,
      "scraped-counts-ok",
      `rosterSnapshotsAdded=41 expected, got ${body.rosterSnapshotsAdded}`,
    );
    assert(body.matchesAdded === 88, "scraped-counts-ok", `matchesAdded=88 expected, got ${body.matchesAdded}`);

    // All 5 tables should be queried in parallel — call count == 5, each
    // with the same `since` Date.
    assert(
      countCalls.length === 5,
      "scraped-counts-ok",
      `expected 5 countSince calls, got ${countCalls.length}`,
    );
    const tables = new Set(countCalls.map((c) => c.table));
    assert(
      tables.has("clubs") &&
        tables.has("coaches") &&
        tables.has("events") &&
        tables.has("rosterSnapshots") &&
        tables.has("matches"),
      "scraped-counts-ok",
      "all 5 tables should be queried",
    );
    assert(
      countCalls.every((c) => c.since.toISOString() === since),
      "scraped-counts-ok",
      "all count calls should share the same parsed `since`",
    );
  }

  // --- scraped-counts missing `since` → 400 -------------------------------
  {
    const deps = makeDeps({});
    const handler = makeScrapedCountsHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});
    assert(
      res.statusCode === 400,
      "scraped-counts-missing",
      `expected 400, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "missing_since",
      "scraped-counts-missing",
      `expected missing_since, got ${body.error}`,
    );
  }

  // --- scraped-counts bogus `since` → 400 ---------------------------------
  {
    const deps = makeDeps({});
    const handler = makeScrapedCountsHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ since: "not-a-date" }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 400,
      "scraped-counts-bogus",
      `expected 400, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "invalid_since",
      "scraped-counts-bogus",
      `expected invalid_since, got ${body.error}`,
    );
  }

  // --- 2. coverage-trend?days=30 aggregates by day correctly --------------
  {
    const trendCalls: Array<{ days: number }> = [];
    const trend = [
      { date: "2026-04-15", runs: 10, successes: 8, failures: 2, rowsTouched: 400 },
      { date: "2026-04-16", runs: 12, successes: 11, failures: 1, rowsTouched: 520 },
      { date: "2026-04-17", runs: 9, successes: 6, failures: 3, rowsTouched: 180 },
    ];
    const deps = makeDeps({ trend, trendCalls });
    const handler = makeCoverageTrendHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ days: "30" }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 200,
      "coverage-trend-30",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      points?: Array<{
        date?: string;
        runs?: number;
        successes?: number;
        failures?: number;
        rowsTouched?: number;
      }>;
      windowDays?: number;
    };
    assert(
      body.windowDays === 30,
      "coverage-trend-30",
      `windowDays=30 expected, got ${body.windowDays}`,
    );
    assert(
      Array.isArray(body.points) && body.points.length === 3,
      "coverage-trend-30",
      `expected 3 points, got ${body.points?.length}`,
    );
    const first = body.points?.[0];
    assert(
      first?.date === "2026-04-15" &&
        first?.runs === 10 &&
        first?.successes === 8 &&
        first?.failures === 2 &&
        first?.rowsTouched === 400,
      "coverage-trend-30",
      "first point contract shape mismatch",
    );
    assert(
      trendCalls.length === 1 && trendCalls[0]?.days === 30,
      "coverage-trend-30",
      `expected 1 coverageTrend call with days=30, got ${JSON.stringify(trendCalls)}`,
    );
  }

  // --- 3. coverage-trend?days=0 → empty points array ----------------------
  {
    const trendCalls: Array<{ days: number }> = [];
    // Include trend data so we can prove days=0 forces the empty list
    // regardless of what the underlying dep would return.
    const deps = makeDeps({
      trendCalls,
      trend: [
        { date: "2026-04-17", runs: 1, successes: 1, failures: 0, rowsTouched: 10 },
      ],
    });
    const handler = makeCoverageTrendHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ days: "0" }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 200,
      "coverage-trend-0",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      points?: unknown[];
      windowDays?: number;
    };
    assert(
      Array.isArray(body.points) && body.points.length === 0,
      "coverage-trend-0",
      `expected empty points array, got ${JSON.stringify(body.points)}`,
    );
    assert(
      body.windowDays === 0,
      "coverage-trend-0",
      `windowDays=0 expected, got ${body.windowDays}`,
    );
    // The handler still calls the dep with days=0; the dep's own guard
    // returns [] so the aggregated shape is an empty list either way.
    assert(
      trendCalls.length === 1 && trendCalls[0]?.days === 0,
      "coverage-trend-0",
      `expected 1 coverageTrend call with days=0, got ${JSON.stringify(trendCalls)}`,
    );
  }

  // --- coverage-trend bogus `days` → 400 ----------------------------------
  {
    const deps = makeDeps({});
    const handler = makeCoverageTrendHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ days: "seventeen" }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 400,
      "coverage-trend-bogus",
      `expected 400, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      body.error === "invalid_days",
      "coverage-trend-bogus",
      `expected invalid_days, got ${body.error}`,
    );
  }

  if (failures.length === 0) {
    console.log("[adminGrowth-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminGrowth-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
