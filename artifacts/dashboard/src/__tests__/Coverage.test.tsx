import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import CoveragePage from "../pages/Coverage";
import CoverageLeaguePage from "../pages/CoverageLeague";

/**
 * Coverage dashboard tests.
 *
 * The pages drive off Orval-generated React Query hooks
 * (`useGetCoverageLeagues` / `useGetCoverageLeagueDetail`), which bottom
 * out at `globalThis.fetch`. Stub that per-test with scenario-specific
 * mocks — same pattern as Growth.test.tsx.
 */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function renderWithProviders(ui: React.ReactElement, path = "/coverage") {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>{ui}</MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("CoveragePage (list)", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders the leagues table with aggregate counts", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/coverage/leagues")) {
          return Promise.resolve(
            jsonResponse({
              rows: [
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
                  clubsNeverScraped: 2,
                  clubsStale14d: 4,
                },
              ],
              total: 2,
              page: 1,
              pageSize: 20,
            }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderWithProviders(
      <Routes>
        <Route path="/coverage" element={<CoveragePage />} />
      </Routes>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("coverage-row-1")).toBeInTheDocument();
    });

    expect(screen.getByTestId("coverage-row-1")).toHaveTextContent("ECNL");
    expect(screen.getByTestId("coverage-row-1")).toHaveTextContent("120");
    expect(screen.getByTestId("coverage-row-2")).toHaveTextContent("MLS NEXT");
    expect(screen.getByText(/2 leagues/i)).toBeInTheDocument();
  });

  it("shows the empty state when no leagues come back", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(() =>
      Promise.resolve(
        jsonResponse({ rows: [], total: 0, page: 1, pageSize: 20 }),
      ),
    );

    renderWithProviders(
      <Routes>
        <Route path="/coverage" element={<CoveragePage />} />
      </Routes>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("coverage-empty")).toBeInTheDocument();
    });
  });

  it("renders the error panel when the request fails", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(() =>
      Promise.resolve(
        new Response("kaboom", {
          status: 500,
          headers: { "Content-Type": "text/plain" },
        }),
      ),
    );

    renderWithProviders(
      <Routes>
        <Route path="/coverage" element={<CoveragePage />} />
      </Routes>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("coverage-error")).toBeInTheDocument();
    });
  });

  it("links to the per-league drilldown", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/coverage/leagues/7")) {
          return Promise.resolve(
            jsonResponse({
              league: { id: 7, name: "State Cup" },
              rows: [],
              total: 0,
              page: 1,
              pageSize: 20,
            }),
          );
        }
        if (u.includes("/api/v1/admin/coverage/leagues")) {
          return Promise.resolve(
            jsonResponse({
              rows: [
                {
                  leagueId: 7,
                  leagueName: "State Cup",
                  clubsTotal: 20,
                  clubsWithRosterSnapshot: 10,
                  clubsWithCoachDiscovery: 8,
                  clubsNeverScraped: 4,
                  clubsStale14d: 3,
                },
              ],
              total: 1,
              page: 1,
              pageSize: 20,
            }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderWithProviders(
      <Routes>
        <Route path="/coverage" element={<CoveragePage />} />
        <Route path="/coverage/:leagueId" element={<CoverageLeaguePage />} />
      </Routes>,
    );

    const drill = await screen.findByRole("link", { name: /drill down/i });
    expect(drill).toHaveAttribute("href", "/coverage/7");
    await userEvent.click(drill);

    await waitFor(() => {
      // Breadcrumb "Coverage / League #7" lands on the drilldown page.
      expect(
        screen.getByRole("heading", { level: 1, name: /state cup/i }),
      ).toBeInTheDocument();
    });
  });
});

describe("CoverageLeaguePage (drilldown)", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders rows + refetches with status=stale when filter clicked", async () => {
    const fetchMock = (globalThis.fetch as ReturnType<typeof vi.fn>);

    fetchMock.mockImplementation((url: RequestInfo | URL) => {
      const u = typeof url === "string" ? url : url.toString();
      if (u.includes("status=stale")) {
        return Promise.resolve(
          jsonResponse({
            league: { id: 7, name: "State Cup" },
            rows: [
              {
                clubId: 99,
                clubNameCanonical: "Stale FC",
                lastScrapedAt: "2026-03-01T00:00:00.000Z",
                consecutiveFailures: 3,
                coachCount: 2,
                hasRosterSnapshot: true,
                staffPageUrl: null,
                scrapeConfidence: 0.7,
              },
            ],
            total: 1,
            page: 1,
            pageSize: 20,
          }),
        );
      }
      return Promise.resolve(
        jsonResponse({
          league: { id: 7, name: "State Cup" },
          rows: [
            {
              clubId: 10,
              clubNameCanonical: "Fresh FC",
              lastScrapedAt: "2026-04-20T00:00:00.000Z",
              consecutiveFailures: 0,
              coachCount: 5,
              hasRosterSnapshot: true,
              staffPageUrl: "https://example.com",
              scrapeConfidence: 0.95,
            },
            {
              clubId: 99,
              clubNameCanonical: "Stale FC",
              lastScrapedAt: "2026-03-01T00:00:00.000Z",
              consecutiveFailures: 3,
              coachCount: 2,
              hasRosterSnapshot: true,
              staffPageUrl: null,
              scrapeConfidence: 0.7,
            },
          ],
          total: 2,
          page: 1,
          pageSize: 20,
        }),
      );
    });

    renderWithProviders(
      <Routes>
        <Route path="/coverage/:leagueId" element={<CoverageLeaguePage />} />
      </Routes>,
      "/coverage/7",
    );

    await waitFor(() => {
      expect(screen.getByTestId("coverage-detail-row-10")).toBeInTheDocument();
    });
    expect(screen.getByTestId("coverage-detail-row-99")).toBeInTheDocument();

    // Click "Stale 14d" filter; the query key changes and the hook refetches.
    const staleBtn = screen.getByTestId("status-filter-stale");
    await userEvent.click(staleBtn);

    await waitFor(() => {
      // Fresh FC disappears — only Stale FC remains.
      expect(screen.queryByTestId("coverage-detail-row-10")).toBeNull();
    });
    expect(screen.getByTestId("coverage-detail-row-99")).toBeInTheDocument();

    const calls = fetchMock.mock.calls;
    const staleCalls = calls.filter((c) => String(c[0]).includes("status=stale"));
    expect(staleCalls.length).toBeGreaterThanOrEqual(1);
  });

  it("renders the league-not-found banner on 404", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(() =>
      Promise.resolve(
        new Response(JSON.stringify({ error: "League not found" }), {
          status: 404,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );

    renderWithProviders(
      <Routes>
        <Route path="/coverage/:leagueId" element={<CoverageLeaguePage />} />
      </Routes>,
      "/coverage/999",
    );

    await waitFor(() => {
      expect(screen.getByTestId("coverage-detail-notfound")).toBeInTheDocument();
    });
  });
});
