import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import DataQualityPage from "../pages/DataQuality";

/**
 * DataQualityPage was migrated from `adminFetch()` to the Orval-generated
 * React Query hooks (`useGaPremierOrphanCleanup`, `useGetEmptyStaffPages`,
 * `useGetStaleScrapes`). Those hooks still bottom out at `globalThis.fetch`
 * via the `customFetch` mutator, so stubbing `fetch` per-test still works —
 * we just need a per-test `QueryClient` wrapper (retries disabled so error
 * tests don't hang on React Query's default 3x retry behaviour).
 */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function renderWithProviders(ui: React.ReactElement) {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>{ui}</MemoryRouter>
    </QueryClientProvider>,
  );
}

/**
 * Fetch dispatcher keyed by URL substring. Each panel loads its own
 * endpoint, and the GA Premier tab posts on demand.
 */
function makeFetchMock(
  routes: Record<string, (init: RequestInit) => Response | Promise<Response>>,
) {
  return vi.fn((url: RequestInfo | URL, init: RequestInit = {}) => {
    const u = typeof url === "string" ? url : url.toString();
    for (const [needle, handler] of Object.entries(routes)) {
      if (u.includes(needle)) return Promise.resolve(handler(init));
    }
    return Promise.resolve(
      new Response(JSON.stringify({ error: "no route" }), { status: 500 }),
    );
  });
}

describe("DataQualityPage — GA Premier tab", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("submits defaults (dryRun=true, limit=500) and renders counts + sample names", async () => {
    const fetchMock = makeFetchMock({
      "ga-premier-orphans": () =>
        jsonResponse({
          scanned: 1000,
          flagged: 12,
          deleted: 0,
          sampleNames: ["GA Premier 2011B", "GA Premier garbage row"],
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(screen.getByRole("button", { name: /run sweep/i }));

    await waitFor(() => {
      expect(screen.getByText("1,000")).toBeInTheDocument();
    });

    expect(screen.getByText("Scanned")).toBeInTheDocument();
    expect(screen.getByText("Flagged")).toBeInTheDocument();
    expect(screen.getByText("Deleted")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();

    expect(screen.getByText("GA Premier 2011B")).toBeInTheDocument();
    expect(screen.getByText("GA Premier garbage row")).toBeInTheDocument();

    // Request body used the defaults.
    const gaCall = fetchMock.mock.calls.find((c) =>
      String(c[0]).includes("ga-premier-orphans"),
    );
    expect(gaCall).toBeDefined();
    const [, init] = gaCall as [string, RequestInit];
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body as string)).toEqual({
      dryRun: true,
      limit: 500,
    });
  });

  it("opens confirmation dialog and commits deletion with dryRun=false", async () => {
    const ga = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          scanned: 500,
          flagged: 7,
          deleted: 0,
          sampleNames: ["bad-row-1", "bad-row-2"],
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          scanned: 500,
          flagged: 7,
          deleted: 7,
          sampleNames: [],
        }),
      );
    const fetchMock = vi.fn(
      (url: RequestInfo | URL, init: RequestInit = {}) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("ga-premier-orphans")) {
          return ga(u, init);
        }
        return Promise.resolve(
          new Response(JSON.stringify({ error: "no route" }), { status: 500 }),
        );
      },
    );
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(screen.getByRole("button", { name: /run sweep/i }));

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /commit deletion/i }),
      ).toBeInTheDocument();
    });

    await user.click(
      screen.getByRole("button", { name: /commit deletion/i }),
    );

    await waitFor(() => {
      expect(
        screen.getByText(/delete 7 roster snapshot rows\?/i),
      ).toBeInTheDocument();
    });
    expect(screen.getByText(/cannot be undone/i)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: /^delete$/i }));

    await waitFor(() => {
      expect(ga).toHaveBeenCalledTimes(2);
    });

    const secondCall = ga.mock.calls[1] as [string, RequestInit];
    expect(secondCall[1].method).toBe("POST");
    expect(JSON.parse(secondCall[1].body as string)).toEqual({
      dryRun: false,
      limit: 500,
    });

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(/deleted 7 rows/i);
    });
  });
});

describe("DataQualityPage — Empty staff pages tab", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("loads and renders rows with window_days=30 default", async () => {
    const fetchMock = makeFetchMock({
      "empty-staff-pages": () =>
        jsonResponse({
          rows: [
            {
              clubId: 42,
              clubNameCanonical: "Cactus Soccer Club",
              staffPageUrl: "https://cactussoccer.example/staff",
              lastScrapedAt: "2026-03-01T00:00:00Z",
              coachCountWindow: 0,
            },
            {
              clubId: 99,
              clubNameCanonical: "Granite FC",
              staffPageUrl: "https://granitefc.example/about/coaches",
              lastScrapedAt: null,
              coachCountWindow: 0,
            },
          ],
          total: 2,
          page: 1,
          pageSize: 20,
          windowDays: 30,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /empty staff pages/i }),
    );

    await waitFor(() => {
      expect(screen.getByText("Cactus Soccer Club")).toBeInTheDocument();
    });
    expect(screen.getByText("Granite FC")).toBeInTheDocument();

    // Request used the default window_days=30.
    const call = fetchMock.mock.calls.find((c) =>
      String(c[0]).includes("empty-staff-pages"),
    );
    expect(call).toBeDefined();
    expect(String(call?.[0])).toContain("window_days=30");
    expect(String(call?.[0])).toContain("page=1");
  });

  it("renders empty state when rows is empty", async () => {
    const fetchMock = makeFetchMock({
      "empty-staff-pages": () =>
        jsonResponse({
          rows: [],
          total: 0,
          page: 1,
          pageSize: 20,
          windowDays: 30,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /empty staff pages/i }),
    );

    await waitFor(() => {
      expect(screen.getByText(/no clubs matched/i)).toBeInTheDocument();
    });
  });

  it("renders error banner on HTTP 500", async () => {
    const fetchMock = makeFetchMock({
      "empty-staff-pages": () =>
        new Response(JSON.stringify({ error: "boom" }), { status: 500 }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /empty staff pages/i }),
    );

    await waitFor(() => {
      const alert = screen.getByRole("alert");
      expect(within(alert).getByText(/500/)).toBeInTheDocument();
    });
  });
});

describe("DataQualityPage — Nav-leaked names tab", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("loads and renders rows with default include_resolved=false", async () => {
    const fetchMock = makeFetchMock({
      "nav-leaked-names": () =>
        jsonResponse({
          rows: [
            {
              id: 1,
              snapshotId: 1001,
              clubId: 42,
              clubNameCanonical: "Cactus Soccer Club",
              leakedStrings: ["HOME", "CONTACT"],
              snapshotRosterSize: 24,
              flaggedAt: "2026-04-10T12:00:00Z",
              resolvedAt: null,
              resolvedByEmail: null,
            },
            {
              id: 2,
              snapshotId: 2002,
              // Unlinked snapshot — linker hasn't resolved the club yet.
              clubId: null,
              clubNameCanonical: null,
              leakedStrings: ["Register"],
              snapshotRosterSize: 1,
              flaggedAt: "2026-04-11T09:00:00Z",
              resolvedAt: null,
              resolvedByEmail: null,
            },
          ],
          total: 2,
          page: 1,
          pageSize: 20,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /nav-leaked names/i }),
    );

    await waitFor(() => {
      expect(screen.getByText("Cactus Soccer Club")).toBeInTheDocument();
    });

    // Leaked strings render as chips.
    expect(screen.getByText("HOME")).toBeInTheDocument();
    expect(screen.getByText("CONTACT")).toBeInTheDocument();
    expect(screen.getByText("Register")).toBeInTheDocument();

    // Unlinked snapshot renders the fallback marker.
    expect(screen.getByText(/unlinked snapshot #2002/i)).toBeInTheDocument();

    // Snapshot roster size surfaced.
    expect(screen.getByText("24")).toBeInTheDocument();

    // Active status badge on unresolved rows (there are 2).
    expect(screen.getAllByText("Active").length).toBe(2);

    // Request used the default include_resolved=false.
    const call = fetchMock.mock.calls.find((c) =>
      String(c[0]).includes("nav-leaked-names"),
    );
    expect(call).toBeDefined();
    expect(String(call?.[0])).toContain("include_resolved=false");
    expect(String(call?.[0])).toContain("page=1");
    expect(String(call?.[0])).toContain("page_size=20");
  });

  it("renders empty state when rows is empty (default state at Phase 1 merge)", async () => {
    const fetchMock = makeFetchMock({
      "nav-leaked-names": () =>
        jsonResponse({
          rows: [],
          total: 0,
          page: 1,
          pageSize: 20,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /nav-leaked names/i }),
    );

    await waitFor(() => {
      expect(screen.getByText(/no flagged snapshots/i)).toBeInTheDocument();
    });
  });

  it("renders error banner on HTTP 500", async () => {
    const fetchMock = makeFetchMock({
      "nav-leaked-names": () =>
        new Response(JSON.stringify({ error: "boom" }), { status: 500 }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /nav-leaked names/i }),
    );

    await waitFor(() => {
      const alert = screen.getByRole("alert");
      expect(within(alert).getByText(/500/)).toBeInTheDocument();
    });
  });

  it("re-queries with include_resolved=true when checkbox is toggled and refreshed", async () => {
    const fetchMock = makeFetchMock({
      "nav-leaked-names": () =>
        jsonResponse({ rows: [], total: 0, page: 1, pageSize: 20 }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /nav-leaked names/i }),
    );

    await waitFor(() => {
      expect(
        screen.getByText(/no flagged snapshots/i),
      ).toBeInTheDocument();
    });

    await user.click(
      screen.getByRole("checkbox", { name: /include resolved flags/i }),
    );
    await user.click(screen.getByRole("button", { name: /refresh/i }));

    await waitFor(() => {
      const withResolved = fetchMock.mock.calls.filter((c) =>
        String(c[0]).includes("include_resolved=true"),
      );
      expect(withResolved.length).toBeGreaterThanOrEqual(1);
    });
  });

  it("renders resolved status with resolver email when resolvedAt is set", async () => {
    const fetchMock = makeFetchMock({
      "nav-leaked-names": () =>
        jsonResponse({
          rows: [
            {
              id: 99,
              snapshotId: 9000,
              clubId: 7,
              clubNameCanonical: "Resolved FC",
              leakedStrings: ["ABOUT"],
              snapshotRosterSize: 15,
              flaggedAt: "2026-04-01T00:00:00Z",
              resolvedAt: "2026-04-05T00:00:00Z",
              resolvedByEmail: "ops@upshift.test",
            },
          ],
          total: 1,
          page: 1,
          pageSize: 20,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(
      screen.getByRole("tab", { name: /nav-leaked names/i }),
    );

    await waitFor(() => {
      expect(screen.getByText("Resolved FC")).toBeInTheDocument();
    });
    // The resolver email appears verbatim in the resolved badge copy.
    expect(screen.getByText(/ops@upshift\.test/)).toBeInTheDocument();
    // "Active" badge should NOT be present on a resolved row.
    expect(screen.queryByText("Active")).not.toBeInTheDocument();
  });
});

describe("DataQualityPage — Stale scrapes tab", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("loads and renders rows with threshold_days=14 default", async () => {
    const fetchMock = makeFetchMock({
      "stale-scrapes": () =>
        jsonResponse({
          rows: [
            {
              entityType: "club",
              entityId: 101,
              entityName: "Oakwood United",
              lastScrapedAt: "2026-02-15T12:00:00Z",
              lastStatus: "failed",
              consecutiveFailures: 4,
            },
            {
              entityType: "league",
              entityId: 7,
              entityName: null,
              lastScrapedAt: null,
              lastStatus: "never",
              consecutiveFailures: 0,
            },
          ],
          total: 2,
          page: 1,
          pageSize: 20,
          thresholdDays: 14,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(screen.getByRole("tab", { name: /stale scrapes/i }));

    await waitFor(() => {
      expect(screen.getByText("Oakwood United")).toBeInTheDocument();
    });

    // null entityName renders the fallback "(id N)" marker.
    expect(screen.getByText(/\(id 7\)/)).toBeInTheDocument();

    // Request used the default threshold_days=14.
    const call = fetchMock.mock.calls.find((c) =>
      String(c[0]).includes("stale-scrapes"),
    );
    expect(call).toBeDefined();
    expect(String(call?.[0])).toContain("threshold_days=14");
    expect(String(call?.[0])).toContain("page=1");
  });

  it("renders empty state when rows is empty", async () => {
    const fetchMock = makeFetchMock({
      "stale-scrapes": () =>
        jsonResponse({
          rows: [],
          total: 0,
          page: 1,
          pageSize: 20,
          thresholdDays: 14,
        }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(screen.getByRole("tab", { name: /stale scrapes/i }));

    await waitFor(() => {
      expect(screen.getByText(/no stale entities/i)).toBeInTheDocument();
    });
  });

  it("renders error banner on HTTP 500", async () => {
    const fetchMock = makeFetchMock({
      "stale-scrapes": () =>
        new Response(JSON.stringify({ error: "boom" }), { status: 500 }),
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    renderWithProviders(<DataQualityPage />);

    await user.click(screen.getByRole("tab", { name: /stale scrapes/i }));

    await waitFor(() => {
      const alert = screen.getByRole("alert");
      expect(within(alert).getByText(/500/)).toBeInTheDocument();
    });
  });
});
