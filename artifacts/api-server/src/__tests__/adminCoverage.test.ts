/**
 * admin/coverage routes — unit tests.
 *
 * Run: DATABASE_URL=postgres://unused@localhost/test tsx src/__tests__/adminCoverage.test.ts
 *
 * Factory-handler pattern (same as adminGrowth / adminDedup / adminDataQuality)
 * — inject a fake `CoverageDeps` so the handlers never reach Postgres.
 *
 * Scenarios:
 *   1. list leagues (default pagination) — 200 + row shape + page echo
 *   2. list leagues (empty) — 200 + empty rows + total 0
 *   3. list leagues (bogus page) — 400
 *   4. league detail (stale filter) — 200 + status echoed to dep call
 *   5. league detail (unknown id) — 404
 *   6. league detail (bogus leagueId) — 400
 */
import type { Request, Response } from "express";
import {
  makeListLeaguesHandler,
  makeLeagueDetailHandler,
  makeSummaryHandler,
  makeHistoryHandler,
  type CoverageDeps,
  type CoverageLeagueAggRow,
  type CoverageLeagueDetailAggRow,
  type CoverageLeaguesSummary,
  type CoverageHistoryRow,
} from "../routes/admin/coverage";
import type { CoverageLeagueDetailStatus } from "@hlbiv/api-zod/admin";

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

function makeReq(opts: {
  query?: Record<string, string>;
  params?: Record<string, string>;
}): Request {
  return {
    query: opts.query ?? {},
    params: opts.params ?? {},
    body: {},
  } as unknown as Request;
}

// ---------------------------------------------------------------------------
// Deps fixture.
// ---------------------------------------------------------------------------

type ListLeaguesCall = { page: number; pageSize: number };
type DetailCall = {
  leagueId: number;
  leagueName: string;
  status: CoverageLeagueDetailStatus;
  page: number;
  pageSize: number;
};

function makeDeps(opts: {
  leagues?: { rows: CoverageLeagueAggRow[]; total: number };
  summary?: CoverageLeaguesSummary;
  knownLeagues?: Record<number, { id: number; name: string }>;
  detail?: { rows: CoverageLeagueDetailAggRow[]; total: number };
  history?: CoverageHistoryRow[];
  listLeaguesCalls?: ListLeaguesCall[];
  summaryCalls?: number[];
  detailCalls?: DetailCall[];
  historyCalls?: Array<{ days: number }>;
}): CoverageDeps {
  return {
    listLeagues: async ({ page, pageSize }) => {
      opts.listLeaguesCalls?.push({ page, pageSize });
      return opts.leagues ?? { rows: [], total: 0 };
    },
    summarizeLeagues: async () => {
      opts.summaryCalls?.push(Date.now());
      return (
        opts.summary ?? {
          leaguesTotal: 0,
          clubsTotal: 0,
          clubsWithRosterSnapshot: 0,
          clubsWithCoachDiscovery: 0,
          clubsNeverScraped: 0,
          clubsStale14d: 0,
        }
      );
    },
    getCoverageHistory: async ({ days }) => {
      opts.historyCalls?.push({ days });
      return opts.history ?? [];
    },
    findLeague: async ({ leagueId }) => {
      return opts.knownLeagues?.[leagueId] ?? null;
    },
    listClubsInLeague: async ({ leagueId, leagueName, status, page, pageSize }) => {
      opts.detailCalls?.push({ leagueId, leagueName, status, page, pageSize });
      return opts.detail ?? { rows: [], total: 0 };
    },
  };
}

// ---------------------------------------------------------------------------
// Scenarios.
// ---------------------------------------------------------------------------

async function run() {
  // --- 1. list leagues — default pagination + row shape -------------------
  {
    const listLeaguesCalls: ListLeaguesCall[] = [];
    const leagues: CoverageLeagueAggRow[] = [
      {
        leagueId: 1,
        leagueName: "ECNL",
        clubsTotal: 120,
        clubsWithRosterSnapshot: 95,
        clubsWithCoachDiscovery: 80,
        clubsNeverScraped: 10,
        clubsStale14d: 12,
      },
      {
        leagueId: 2,
        leagueName: "MLS NEXT",
        clubsTotal: 60,
        clubsWithRosterSnapshot: 50,
        clubsWithCoachDiscovery: 45,
        clubsNeverScraped: 5,
        clubsStale14d: 4,
      },
    ];
    const deps = makeDeps({
      leagues: { rows: leagues, total: 2 },
      listLeaguesCalls,
    });
    const handler = makeListLeaguesHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});

    assert(
      res.statusCode === 200,
      "leagues-default",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      rows?: CoverageLeagueAggRow[];
      total?: number;
      page?: number;
      pageSize?: number;
    };
    assert(
      body.total === 2,
      "leagues-default",
      `expected total=2, got ${body.total}`,
    );
    assert(
      body.page === 1 && body.pageSize === 20,
      "leagues-default",
      `expected default page=1, pageSize=20, got page=${body.page} pageSize=${body.pageSize}`,
    );
    assert(
      Array.isArray(body.rows) && body.rows.length === 2,
      "leagues-default",
      `expected 2 rows, got ${body.rows?.length}`,
    );
    const first = body.rows?.[0];
    assert(
      first?.leagueName === "ECNL" &&
        first?.clubsNeverScraped === 10 &&
        first?.clubsStale14d === 12,
      "leagues-default",
      "row shape mismatch",
    );
    assert(
      listLeaguesCalls.length === 1 &&
        listLeaguesCalls[0]?.page === 1 &&
        listLeaguesCalls[0]?.pageSize === 20,
      "leagues-default",
      `expected listLeagues call with page=1, pageSize=20, got ${JSON.stringify(listLeaguesCalls)}`,
    );
  }

  // --- 2. list leagues — empty result -------------------------------------
  {
    const deps = makeDeps({ leagues: { rows: [], total: 0 } });
    const handler = makeListLeaguesHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "leagues-empty",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as { rows?: unknown[]; total?: number };
    assert(
      Array.isArray(body.rows) && body.rows.length === 0 && body.total === 0,
      "leagues-empty",
      `expected empty rows + total=0, got ${JSON.stringify(body)}`,
    );
  }

  // --- 3. list leagues — bogus page → 400 ---------------------------------
  {
    const deps = makeDeps({});
    const handler = makeListLeaguesHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ query: { page: "-3" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 400,
      "leagues-bogus-page",
      `expected 400, got ${res.statusCode}`,
    );
  }

  // --- 4. league detail — stale filter ------------------------------------
  {
    const detailCalls: DetailCall[] = [];
    const detail: CoverageLeagueDetailAggRow[] = [
      {
        clubId: 42,
        clubNameCanonical: "FC Anywhere",
        lastScrapedAt: new Date("2026-03-01T00:00:00Z"),
        consecutiveFailures: 2,
        coachCount: 3,
        hasRosterSnapshot: true,
        staffPageUrl: "https://example.com/staff",
        scrapeConfidence: 0.92,
      },
    ];
    const deps = makeDeps({
      knownLeagues: { 7: { id: 7, name: "State Cup" } },
      detail: { rows: detail, total: 1 },
      detailCalls,
    });
    const handler = makeLeagueDetailHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({
        params: { leagueId: "7" },
        query: { status: "stale", page: "1", page_size: "20" },
      }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 200,
      "detail-stale",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as {
      league?: { id: number; name: string };
      rows?: Array<{
        clubId: number;
        clubNameCanonical: string;
        lastScrapedAt: string | null;
        consecutiveFailures: number;
        coachCount: number;
        hasRosterSnapshot: boolean;
        staffPageUrl: string | null;
        scrapeConfidence: number | null;
      }>;
      total?: number;
    };
    assert(
      body.league?.id === 7 && body.league?.name === "State Cup",
      "detail-stale",
      `expected league {id:7,name:'State Cup'}, got ${JSON.stringify(body.league)}`,
    );
    const row = body.rows?.[0];
    assert(
      row?.clubId === 42 &&
        row?.clubNameCanonical === "FC Anywhere" &&
        row?.lastScrapedAt === "2026-03-01T00:00:00.000Z" &&
        row?.consecutiveFailures === 2 &&
        row?.coachCount === 3 &&
        row?.hasRosterSnapshot === true &&
        row?.staffPageUrl === "https://example.com/staff" &&
        row?.scrapeConfidence === 0.92,
      "detail-stale",
      `row shape mismatch: ${JSON.stringify(row)}`,
    );
    // The handler must forward the parsed `status` and the resolved league
    // identity to the dep — the fake echoes them into detailCalls.
    assert(
      detailCalls.length === 1 &&
        detailCalls[0]?.status === "stale" &&
        detailCalls[0]?.leagueId === 7 &&
        detailCalls[0]?.leagueName === "State Cup" &&
        detailCalls[0]?.page === 1 &&
        detailCalls[0]?.pageSize === 20,
      "detail-stale",
      `detail call mismatch: ${JSON.stringify(detailCalls)}`,
    );
  }

  // --- 5. league detail — unknown id → 404 --------------------------------
  {
    const deps = makeDeps({ knownLeagues: {} });
    const handler = makeLeagueDetailHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ params: { leagueId: "999" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 404,
      "detail-notfound",
      `expected 404, got ${res.statusCode}`,
    );
    const body = res.body as { error?: string };
    assert(
      typeof body.error === "string" && body.error.length > 0,
      "detail-notfound",
      "expected error message on 404",
    );
  }

  // --- 6. league detail — bogus leagueId → 400 ----------------------------
  {
    const deps = makeDeps({});
    const handler = makeLeagueDetailHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ params: { leagueId: "abc" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 400,
      "detail-bogus-id",
      `expected 400, got ${res.statusCode}`,
    );
  }

  // --- 7. league detail — defaults apply when query omitted ---------------
  {
    const detailCalls: DetailCall[] = [];
    const deps = makeDeps({
      knownLeagues: { 3: { id: 3, name: "ECNL Regional" } },
      detail: { rows: [], total: 0 },
      detailCalls,
    });
    const handler = makeLeagueDetailHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ params: { leagueId: "3" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 200,
      "detail-defaults",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      detailCalls.length === 1 &&
        detailCalls[0]?.status === "all" &&
        detailCalls[0]?.page === 1 &&
        detailCalls[0]?.pageSize === 20,
      "detail-defaults",
      `expected default status=all, page=1, pageSize=20 — got ${JSON.stringify(detailCalls)}`,
    );
  }

  // --- 8. summary — returns aggregate rollup -----------------------------
  {
    const summaryCalls: number[] = [];
    const summary: CoverageLeaguesSummary = {
      leaguesTotal: 127,
      clubsTotal: 4321,
      clubsWithRosterSnapshot: 3100,
      clubsWithCoachDiscovery: 2400,
      clubsNeverScraped: 220,
      clubsStale14d: 510,
    };
    const deps = makeDeps({ summary, summaryCalls });
    const handler = makeSummaryHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "summary-ok",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as Partial<CoverageLeaguesSummary>;
    assert(
      body.leaguesTotal === 127 &&
        body.clubsTotal === 4321 &&
        body.clubsWithRosterSnapshot === 3100 &&
        body.clubsWithCoachDiscovery === 2400 &&
        body.clubsNeverScraped === 220 &&
        body.clubsStale14d === 510,
      "summary-ok",
      `summary shape mismatch: ${JSON.stringify(body)}`,
    );
    assert(
      summaryCalls.length === 1,
      "summary-ok",
      `expected exactly 1 summary call, got ${summaryCalls.length}`,
    );
  }

  // --- 9. summary — zeros are valid (empty DB / fresh deploy) -------------
  {
    const deps = makeDeps({});
    const handler = makeSummaryHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "summary-zeros",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as Partial<CoverageLeaguesSummary>;
    assert(
      body.leaguesTotal === 0 &&
        body.clubsTotal === 0 &&
        body.clubsWithRosterSnapshot === 0 &&
        body.clubsWithCoachDiscovery === 0 &&
        body.clubsNeverScraped === 0 &&
        body.clubsStale14d === 0,
      "summary-zeros",
      `expected all-zero summary, got ${JSON.stringify(body)}`,
    );
  }

  // --- 10. history — returns rows with default days ----------------------
  {
    const historyCalls: Array<{ days: number }> = [];
    const history: CoverageHistoryRow[] = [
      {
        snapshotDate: "2026-04-20",
        leaguesTotal: 100,
        clubsTotal: 1000,
        clubsWithRosterSnapshot: 800,
        clubsWithCoachDiscovery: 700,
        clubsNeverScraped: 50,
        clubsStale14d: 30,
      },
      {
        snapshotDate: "2026-04-21",
        leaguesTotal: 100,
        clubsTotal: 1010,
        clubsWithRosterSnapshot: 820,
        clubsWithCoachDiscovery: 720,
        clubsNeverScraped: 45,
        clubsStale14d: 25,
      },
    ];
    const deps = makeDeps({ history, historyCalls });
    const handler = makeHistoryHandler(deps);
    const res = makeRes();
    await handler(makeReq({}), res as unknown as Response, () => {});
    assert(
      res.statusCode === 200,
      "history-default",
      `expected 200, got ${res.statusCode}`,
    );
    const body = res.body as { rows?: CoverageHistoryRow[] };
    assert(
      Array.isArray(body.rows) && body.rows.length === 2,
      "history-default",
      `expected 2 rows, got ${body.rows?.length}`,
    );
    assert(
      body.rows?.[0]?.snapshotDate === "2026-04-20" &&
        body.rows?.[1]?.snapshotDate === "2026-04-21",
      "history-default",
      "expected oldest-first ordering",
    );
    assert(
      historyCalls.length === 1 && historyCalls[0]?.days === 30,
      "history-default",
      `expected default days=30, got ${JSON.stringify(historyCalls)}`,
    );
  }

  // --- 11. history — custom days param ------------------------------------
  {
    const historyCalls: Array<{ days: number }> = [];
    const deps = makeDeps({ historyCalls });
    const handler = makeHistoryHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ query: { days: "7" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 200,
      "history-custom-days",
      `expected 200, got ${res.statusCode}`,
    );
    assert(
      historyCalls.length === 1 && historyCalls[0]?.days === 7,
      "history-custom-days",
      `expected days=7, got ${JSON.stringify(historyCalls)}`,
    );
  }

  // --- 12. history — bogus days → 400 -------------------------------------
  {
    const deps = makeDeps({});
    const handler = makeHistoryHandler(deps);
    const res = makeRes();
    await handler(
      makeReq({ query: { days: "-5" } }),
      res as unknown as Response,
      () => {},
    );
    assert(
      res.statusCode === 400,
      "history-bogus-days",
      `expected 400, got ${res.statusCode}`,
    );
  }

  if (failures.length === 0) {
    console.log("[adminCoverage-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[adminCoverage-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
