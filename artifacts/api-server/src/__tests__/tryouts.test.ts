/**
 * tryouts consumer endpoints — unit tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx src/__tests__/tryouts.test.ts
 *
 * Verifies the Player Platform read contract for /api/tryouts/search
 * and /api/tryouts/upcoming:
 *   - date_from / date_to ISO bounds (and rejection of malformed input)
 *   - upcoming-only floor: past-dated rows excluded even when status='upcoming'
 *   - NULL tryout_date excluded
 *   - all existing filters still combine
 *   - response items use the public shape (no internal columns)
 *   - page_size capped at 100
 *
 * Uses an in-memory fake `TryoutsDeps` so the handlers never reach
 * Postgres. The fake mirrors the SQL semantics of `prodTryoutsDeps`.
 */
import http from "node:http";
import express from "express";
import {
  makeTryoutsRouter,
  type TryoutsDeps,
  type TryoutSearchFilters,
} from "../routes/tryouts";
import { tryouts } from "@workspace/db/schema";

type Row = typeof tryouts.$inferSelect;
type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// ---------------------------------------------------------------------------
// In-memory fake.
// ---------------------------------------------------------------------------

function mkRow(overrides: Partial<Row>): Row {
  return {
    id: overrides.id ?? 1,
    clubId: null,
    clubNameRaw: "Test Club",
    ageGroup: null,
    gender: null,
    division: null,
    tryoutDate: null,
    registrationDeadline: null,
    locationName: null,
    locationAddress: null,
    locationCity: null,
    locationState: null,
    cost: null,
    url: null,
    notes: null,
    source: "site_monitor",
    status: "upcoming",
    detectedAt: new Date("2026-01-01T00:00:00Z"),
    scrapedAt: new Date("2026-01-01T00:00:00Z"),
    expiresAt: null,
    siteChangeId: null,
    ...overrides,
  };
}

interface FakeState {
  rows: Row[];
  lastFilters?: TryoutSearchFilters;
}

interface PublicItem {
  id: number;
  club_name_raw: string;
  tryout_date: string | null;
  source: string;
  status: string;
  [key: string]: unknown;
}

interface SearchResponse {
  items?: PublicItem[];
  total?: number;
  page?: number;
  page_size?: number;
  error?: string;
}

function makeFakeDeps(state: FakeState): TryoutsDeps {
  return {
    async searchTryouts(filters) {
      state.lastFilters = filters;
      let out = state.rows.filter((r) => {
        // Mirror the SQL contract one-for-one.
        if (r.tryoutDate === null) return false;
        if (r.tryoutDate < filters.upcomingFloor) return false;
        if (filters.dateFrom && r.tryoutDate < filters.dateFrom) return false;
        if (filters.dateTo && r.tryoutDate > filters.dateTo) return false;
        if (filters.status) {
          if (r.status !== filters.status) return false;
        } else if (!["upcoming", "active"].includes(r.status)) {
          return false;
        }
        if (
          filters.clubName &&
          !r.clubNameRaw
            .toLowerCase()
            .includes(filters.clubName.toLowerCase())
        )
          return false;
        if (
          filters.ageGroup &&
          !(r.ageGroup ?? "")
            .toLowerCase()
            .includes(filters.ageGroup.toLowerCase())
        )
          return false;
        if (filters.gender && r.gender !== filters.gender) return false;
        if (
          filters.state &&
          !(r.locationState ?? "")
            .toLowerCase()
            .includes(filters.state.toLowerCase())
        )
          return false;
        if (filters.source && r.source !== filters.source) return false;
        return true;
      });
      out = out.sort((a, b) => {
        const at = a.tryoutDate?.getTime() ?? 0;
        const bt = b.tryoutDate?.getTime() ?? 0;
        return at - bt;
      });
      const total = out.length;
      const start = (filters.page - 1) * filters.pageSize;
      return { rows: out.slice(start, start + filters.pageSize), total };
    },
  };
}

// ---------------------------------------------------------------------------
// HTTP harness.
// ---------------------------------------------------------------------------

interface HitResult {
  status: number;
  json: SearchResponse;
}

async function hit(port: number, path: string): Promise<HitResult> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: "127.0.0.1", port, path, method: "GET" },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          const body = Buffer.concat(chunks).toString("utf8");
          let parsed: SearchResponse = {};
          try {
            parsed = body ? (JSON.parse(body) as SearchResponse) : {};
          } catch {
            parsed = { error: body };
          }
          resolve({ status: res.statusCode ?? 0, json: parsed });
        });
      },
    );
    req.on("error", reject);
    req.end();
  });
}

function startServer(deps: TryoutsDeps): Promise<{
  port: number;
  close: () => Promise<void>;
}> {
  return new Promise((resolve, reject) => {
    const app = express();
    app.use("/api", makeTryoutsRouter(deps));
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
// Test data.
// ---------------------------------------------------------------------------

const past = new Date("2020-01-01T00:00:00Z");
const soon = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000); // +7d
const later = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000); // +30d
const muchLater = new Date(Date.now() + 90 * 24 * 60 * 60 * 1000); // +90d

const fixtures: Row[] = [
  // 1. Past-dated but still status='upcoming' (status updater stale).
  //    MUST be excluded from consumer responses.
  mkRow({
    id: 1,
    clubNameRaw: "Stale Club",
    tryoutDate: past,
    status: "upcoming",
    locationState: "CA",
  }),
  // 2. NULL tryout_date — MUST be excluded.
  mkRow({
    id: 2,
    clubNameRaw: "No Date Club",
    tryoutDate: null,
    status: "upcoming",
  }),
  // 3. Soon, upcoming, CA.
  mkRow({
    id: 3,
    clubNameRaw: "Alpha SC",
    tryoutDate: soon,
    status: "upcoming",
    ageGroup: "U12",
    gender: "M",
    locationState: "CA",
    source: "site_monitor",
    siteChangeId: 999, // internal column — must NOT appear in response.
  }),
  // 4. Later, active, NY.
  mkRow({
    id: 4,
    clubNameRaw: "Beta FC",
    tryoutDate: later,
    status: "active",
    ageGroup: "U14",
    gender: "F",
    locationState: "NY",
    source: "gotsport",
  }),
  // 5. Much later, upcoming, TX.
  mkRow({
    id: 5,
    clubNameRaw: "Gamma United",
    tryoutDate: muchLater,
    status: "upcoming",
    locationState: "TX",
  }),
  // 6. Soon but cancelled — excluded by default status filter (upcoming|active).
  mkRow({
    id: 6,
    clubNameRaw: "Cancelled Club",
    tryoutDate: soon,
    status: "cancelled",
  }),
];

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

async function run() {
  // Default: search returns only future-dated upcoming|active rows.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search");
      assert(res.status === 200, "default-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id).sort();
      assert(
        JSON.stringify(ids) === JSON.stringify([3, 4, 5]),
        "default-excludes-past-null-cancelled",
        `expected [3,4,5], got ${JSON.stringify(ids)}`,
      );
      assert(
        res.json.total === 3,
        "default-total",
        `expected total=3, got ${res.json.total}`,
      );
      // Sorted ascending by tryout_date — soon (3) < later (4) < muchLater (5).
      const orderedIds = (res.json.items ?? []).map((r: PublicItem) => r.id);
      assert(
        JSON.stringify(orderedIds) === JSON.stringify([3, 4, 5]),
        "default-sort-asc",
        `expected ascending [3,4,5], got ${JSON.stringify(orderedIds)}`,
      );
    } finally {
      await close();
    }
  }

  // Past-date exclusion even when status='upcoming' is stale.
  {
    const state: FakeState = { rows: [fixtures[0]!] }; // only the stale row
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search?status=upcoming");
      assert(res.status === 200, "stale-status", `got ${res.status}`);
      assert(
        Array.isArray(res.json.items) && res.json.items.length === 0,
        "stale-upcoming-excluded",
        `stale upcoming row leaked: ${JSON.stringify(res.json.items)}`,
      );
    } finally {
      await close();
    }
  }

  // /tryouts/upcoming endpoint also enforces the floor.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/upcoming");
      assert(res.status === 200, "upcoming-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id).sort();
      // status='upcoming' filter excludes #4 (active); floor excludes #1 (past)
      // and #2 (null); status filter excludes #6 (cancelled).
      assert(
        JSON.stringify(ids) === JSON.stringify([3, 5]),
        "upcoming-shape",
        `expected [3,5], got ${JSON.stringify(ids)}`,
      );
    } finally {
      await close();
    }
  }

  // date_from / date_to inclusive bounds.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      // Bracket only the "later" row (~+30d).
      const from = new Date(Date.now() + 14 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);
      const to = new Date(Date.now() + 60 * 24 * 60 * 60 * 1000)
        .toISOString()
        .slice(0, 10);
      const res = await hit(
        port,
        `/api/tryouts/search?date_from=${from}&date_to=${to}`,
      );
      assert(res.status === 200, "date-range-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id).sort();
      assert(
        JSON.stringify(ids) === JSON.stringify([4]),
        "date-range",
        `expected [4], got ${JSON.stringify(ids)}`,
      );
    } finally {
      await close();
    }
  }

  // date_to is inclusive end-of-day: a tryout at 18:00 UTC on the
  // boundary day must be returned when date_to=YYYY-MM-DD of that day.
  {
    const boundaryDay = new Date(Date.now() + 21 * 24 * 60 * 60 * 1000);
    const boundaryDayIso = boundaryDay.toISOString().slice(0, 10);
    const eveningOfBoundary = new Date(`${boundaryDayIso}T18:00:00Z`);
    const rows: Row[] = [
      mkRow({
        id: 100,
        clubNameRaw: "Boundary Evening Tryout",
        tryoutDate: eveningOfBoundary,
        status: "upcoming",
      }),
    ];
    const deps = makeFakeDeps({ rows });
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(
        port,
        `/api/tryouts/search?date_to=${boundaryDayIso}`,
      );
      assert(res.status === 200, "boundary-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id);
      assert(
        JSON.stringify(ids) === JSON.stringify([100]),
        "date_to-inclusive-end-of-day",
        `evening-of-day row excluded by date_to: ${JSON.stringify(ids)}`,
      );
    } finally {
      await close();
    }
  }

  // Malformed date_from rejected with 400.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search?date_from=not-a-date");
      assert(
        res.status === 400,
        "bad-date-from",
        `expected 400, got ${res.status}`,
      );
    } finally {
      await close();
    }
  }

  // Combined filters: state=CA + age_group=U12 + gender=M.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(
        port,
        "/api/tryouts/search?state=CA&age_group=U12&gender=M",
      );
      assert(res.status === 200, "combined-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id).sort();
      assert(
        JSON.stringify(ids) === JSON.stringify([3]),
        "combined-filters",
        `expected [3], got ${JSON.stringify(ids)}`,
      );
    } finally {
      await close();
    }
  }

  // Public shape: internal columns must NOT appear.
  {
    const state: FakeState = { rows: [fixtures[2]!] }; // Alpha SC w/ siteChangeId=999
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search");
      assert(res.status === 200, "shape-status", `got ${res.status}`);
      const item = res.json.items?.[0];
      assert(item, "shape-item-present", `no item: ${JSON.stringify(res.json)}`);
      if (item) {
        for (const banned of [
          "site_change_id",
          "siteChangeId",
          "scraped_at",
          "scrapedAt",
          "detected_at",
          "detectedAt",
          "expires_at",
          "expiresAt",
        ]) {
          assert(
            !(banned in item),
            `shape-no-${banned}`,
            `internal column "${banned}" leaked: ${JSON.stringify(item)}`,
          );
        }
        // Confirm the expected public fields are present.
        for (const required of [
          "id",
          "club_name_raw",
          "tryout_date",
          "source",
          "status",
        ]) {
          assert(
            required in item,
            `shape-has-${required}`,
            `missing public field "${required}"`,
          );
        }
      }
    } finally {
      await close();
    }
  }

  // page_size capped at 100.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search?page_size=9999");
      assert(res.status === 200, "page-cap-status", `got ${res.status}`);
      assert(
        res.json.page_size === 100,
        "page-cap",
        `expected page_size capped to 100, got ${res.json.page_size}`,
      );
    } finally {
      await close();
    }
  }

  // Source filter: exact match on `source`.
  {
    const state: FakeState = { rows: fixtures };
    const deps = makeFakeDeps(state);
    const { port, close } = await startServer(deps);
    try {
      const res = await hit(port, "/api/tryouts/search?source=gotsport");
      assert(res.status === 200, "source-status", `got ${res.status}`);
      const ids = (res.json.items ?? []).map((r: PublicItem) => r.id).sort();
      assert(
        JSON.stringify(ids) === JSON.stringify([4]),
        "source-filter",
        `expected [4], got ${JSON.stringify(ids)}`,
      );
    } finally {
      await close();
    }
  }

  // Report.
  if (failures.length === 0) {
    console.log(`\nAll tryouts consumer tests passed.`);
    process.exit(0);
  } else {
    console.error(`\n${failures.length} failure(s):`);
    for (const f of failures) {
      console.error(`  - ${f.name}: ${f.issue}`);
    }
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
