import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import GrowthPage from "../pages/Growth";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function buildFetchMock(options?: {
  countsByDays?: Record<number, unknown>;
  trendByDays?: Record<number, unknown>;
}) {
  const defaults = {
    countsByDays: {
      30: {
        since: "2026-03-19T00:00:00.000Z",
        clubsAdded: 12,
        coachesAdded: 345,
        eventsAdded: 6,
        rosterSnapshotsAdded: 789,
        matchesAdded: 1011,
      },
      7: {
        since: "2026-04-11T00:00:00.000Z",
        clubsAdded: 3,
        coachesAdded: 40,
        eventsAdded: 1,
        rosterSnapshotsAdded: 80,
        matchesAdded: 90,
      },
    },
    trendByDays: {
      30: {
        windowDays: 30,
        points: [
          {
            date: "2026-04-17",
            runs: 10,
            successes: 9,
            failures: 1,
            rowsTouched: 500,
          },
          {
            date: "2026-04-18",
            runs: 12,
            successes: 11,
            failures: 1,
            rowsTouched: 600,
          },
        ],
      },
      7: {
        windowDays: 7,
        points: [
          {
            date: "2026-04-18",
            runs: 5,
            successes: 4,
            failures: 1,
            rowsTouched: 250,
          },
        ],
      },
    },
  };

  const countsByDays = options?.countsByDays ?? defaults.countsByDays;
  const trendByDays = options?.trendByDays ?? defaults.trendByDays;

  return (url: RequestInfo | URL) => {
    const u = typeof url === "string" ? url : url.toString();
    if (u.includes("/api/v1/admin/growth/scraped-counts")) {
      // The UI passes `since=<iso>` from now - days*86400000; derive days
      // back from the request so we can return the matching fixture.
      const match = u.match(/since=([^&]+)/);
      const iso = match ? decodeURIComponent(match[1]) : "";
      const sinceMs = new Date(iso).getTime();
      const days = Math.round((Date.now() - sinceMs) / 86400000);
      const body =
        (countsByDays as Record<number, unknown>)[days] ??
        (countsByDays as Record<number, unknown>)[30];
      return Promise.resolve(jsonResponse(body));
    }
    if (u.includes("/api/v1/admin/growth/coverage-trend")) {
      const match = u.match(/days=(\d+)/);
      const days = match ? Number(match[1]) : 30;
      const body =
        (trendByDays as Record<number, unknown>)[days] ??
        (trendByDays as Record<number, unknown>)[30];
      return Promise.resolve(jsonResponse(body));
    }
    return Promise.reject(new Error(`unexpected url: ${u}`));
  };
}

describe("GrowthPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders 5 stat cards + trend chart from mocked responses", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      buildFetchMock(),
    );

    render(
      <MemoryRouter>
        <GrowthPage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("stat-clubs-added")).toHaveTextContent("12");
    });

    expect(screen.getByTestId("stat-clubs-added")).toHaveTextContent("12");
    expect(screen.getByTestId("stat-coaches-added")).toHaveTextContent("345");
    expect(screen.getByTestId("stat-events-added")).toHaveTextContent("6");
    expect(screen.getByTestId("stat-roster-snapshots-added")).toHaveTextContent(
      "789",
    );
    expect(screen.getByTestId("stat-matches-added")).toHaveTextContent("1,011");

    // Chart renders with the accessible role we set.
    const chart = await screen.findByRole("img", { name: /line chart/i });
    expect(chart).toBeInTheDocument();

    // Default window is 30d — there should be exactly one fetch for each
    // endpoint so far.
    const calls = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls;
    const countsCalls = calls.filter((c) =>
      String(c[0]).includes("/growth/scraped-counts"),
    );
    const trendCalls = calls.filter((c) =>
      String(c[0]).includes("/growth/coverage-trend"),
    );
    expect(countsCalls).toHaveLength(1);
    expect(trendCalls).toHaveLength(1);
    expect(String(trendCalls[0][0])).toContain("days=30");
  });

  it("refetches with updated days when window selector changes", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      buildFetchMock(),
    );

    render(
      <MemoryRouter>
        <GrowthPage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByTestId("stat-clubs-added")).toHaveTextContent("12");
    });

    // Click the 7d window.
    const sevenDay = screen.getByRole("radio", { name: /^7d$/i });
    await userEvent.click(sevenDay);

    await waitFor(() => {
      expect(screen.getByTestId("stat-clubs-added")).toHaveTextContent("3");
    });

    const calls = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls;
    const trendCalls = calls.filter((c) =>
      String(c[0]).includes("/growth/coverage-trend"),
    );
    // One mount call + one refetch = 2.
    expect(trendCalls.length).toBe(2);
    expect(String(trendCalls[trendCalls.length - 1][0])).toContain("days=7");

    // scraped-counts should have been refetched as well.
    const countsCalls = calls.filter((c) =>
      String(c[0]).includes("/growth/scraped-counts"),
    );
    expect(countsCalls.length).toBe(2);
  });

  it("shows the empty state when the trend has no points", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      buildFetchMock({
        trendByDays: { 30: { windowDays: 30, points: [] } },
      }),
    );

    render(
      <MemoryRouter>
        <GrowthPage />
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/no runs in the last 30 days/i)).toBeInTheDocument();
    });
  });
});
