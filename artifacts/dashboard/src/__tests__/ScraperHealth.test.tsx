import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ScraperHealthPage from "../pages/ScraperHealth";

/**
 * This page migrated from `adminFetch()` to the Orval-generated
 * `useListScrapeRuns` / `useListScrapeHealth` hooks (Workstream A POC).
 *
 * The hooks route through the shared `customFetch` mutator, which ultimately
 * calls `globalThis.fetch` — so stubbing `fetch` per-test still works. We
 * just need a QueryClientProvider wrapper around the render.
 *
 * Per-test QueryClient (retries disabled) keeps error tests from hanging
 * on React Query's default 3x retry behavior.
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
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>{ui}</MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ScraperHealthPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders rollup + recent-runs tables from mocked fetch responses", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/scrape-health")) {
          return Promise.resolve(
            jsonResponse({
              rows: [
                {
                  entityType: "club",
                  entityId: 42,
                  lastScrapedAt: "2026-04-01T10:00:00.000Z",
                  lastStatus: "ok",
                  consecutiveFailures: 0,
                  nextScheduledAt: null,
                  metadata: null,
                },
                {
                  entityType: "league",
                  entityId: 7,
                  lastScrapedAt: "2026-04-01T09:00:00.000Z",
                  lastStatus: "failed",
                  consecutiveFailures: 4,
                  nextScheduledAt: null,
                  metadata: null,
                },
              ],
              total: 2,
            }),
          );
        }
        if (u.includes("/api/v1/admin/scrape-runs")) {
          return Promise.resolve(
            jsonResponse({
              runs: [
                {
                  id: 1001,
                  scraperKey: "gotsport-matches",
                  jobKey: "event-12345",
                  status: "ok",
                  startedAt: "2026-04-18T15:00:00.000Z",
                  completedAt: "2026-04-18T15:00:42.000Z",
                  recordsTouched: 118,
                  errorMessage: null,
                  metadata: null,
                },
                {
                  id: 1002,
                  scraperKey: "sincsports-events",
                  jobKey: null,
                  status: "running",
                  startedAt: "2026-04-18T15:02:00.000Z",
                  completedAt: null,
                  recordsTouched: null,
                  errorMessage: null,
                  metadata: null,
                },
              ],
              total: 2,
              page: 1,
              pageSize: 50,
            }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderWithProviders(<ScraperHealthPage />);

    await waitFor(() => {
      expect(screen.getByText("club")).toBeInTheDocument();
    });

    // Rollup table
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("league")).toBeInTheDocument();
    // "4" is the consecutive-failures count for the failing row.
    expect(screen.getByText("4")).toBeInTheDocument();

    // Recent runs table
    expect(screen.getByText("gotsport-matches")).toBeInTheDocument();
    expect(screen.getByText("event-12345")).toBeInTheDocument();
    expect(screen.getByText("sincsports-events")).toBeInTheDocument();

    // Status badges render for ok / failed / running under the new enum.
    const okBadges = screen.getAllByText("ok");
    expect(okBadges.length).toBeGreaterThanOrEqual(1);
    const failedBadges = screen.getAllByText("failed");
    expect(failedBadges.length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText("running")).toBeInTheDocument();
  });

  it("shows an error placeholder when the rollup request fails", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/scrape-health")) {
          return Promise.resolve(jsonResponse({ error: "boom" }, 500));
        }
        return Promise.resolve(
          jsonResponse({ runs: [], total: 0, page: 1, pageSize: 50 }),
        );
      },
    );

    renderWithProviders(<ScraperHealthPage />);

    await waitFor(() => {
      expect(screen.getByText(/failed to load: http 500/i)).toBeInTheDocument();
    });
  });

  it("sends same-origin admin fetches with credentials via the custom-fetch mutator", async () => {
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    fetchMock.mockImplementation(() =>
      Promise.resolve(
        jsonResponse({ rows: [], total: 0, runs: [], page: 1, pageSize: 50 }),
      ),
    );

    renderWithProviders(<ScraperHealthPage />);

    await waitFor(() => {
      // Both hooks should have issued at least one fetch by now.
      expect(fetchMock).toHaveBeenCalled();
    });

    // Every call to a relative `/api/...` path must carry credentials:
    // 'include' so the admin session cookie travels cross-origin in dev.
    for (const call of fetchMock.mock.calls) {
      const [input, init] = call as [RequestInfo | URL, RequestInit | undefined];
      const url = typeof input === "string" ? input : input.toString();
      if (!url.startsWith("/api/")) continue;
      expect(init?.credentials).toBe("include");
    }
  });
});
