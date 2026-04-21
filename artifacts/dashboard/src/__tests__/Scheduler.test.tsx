import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import SchedulerPage from "../pages/Scheduler";

/**
 * SchedulerPage was migrated from `adminFetch()` to the Orval-generated
 * `useListScraperScheduleRuns` / `useRunScraperScheduleNow` /
 * `useGetSchedulerJob` hooks. Those hooks still bottom out at
 * `globalThis.fetch` via the `customFetch` mutator, so stubbing `fetch`
 * per-test still works — we just need a per-test `QueryClient` wrapper
 * (retries disabled).
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

function emptyRunsFor(): unknown {
  return { jobs: [], total: 0 };
}

function sampleRunsFor(jobKey: string): unknown {
  return {
    jobs: [
      {
        id: 42,
        jobKey,
        args: null,
        status: "success",
        requestedBy: 1,
        requestedAt: "2026-04-18T12:00:00.000Z",
        startedAt: "2026-04-18T12:00:01.000Z",
        completedAt: "2026-04-18T12:02:00.000Z",
        exitCode: 0,
        stdoutTail: "ok",
        stderrTail: null,
      },
    ],
    total: 1,
  };
}

/**
 * Mock `fetch` with a handler that dispatches based on (url, method).
 * The three on-mount GET requests (runs list per jobKey) resolve first; POSTs
 * are handled per-test.
 */
function installFetch(
  handler: (
    url: string,
    init: RequestInit | undefined,
  ) => Response | Promise<Response>,
): ReturnType<typeof vi.fn> {
  const fetchMock = vi
    .fn()
    .mockImplementation((url: RequestInfo | URL, init?: RequestInit) => {
      const u = typeof url === "string" ? url : url.toString();
      return Promise.resolve(handler(u, init));
    });
  (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(fetchMock);
  return fetchMock;
}

describe("SchedulerPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders 3 job cards with runs tables fetched in parallel on mount", async () => {
    const fetchMock = installFetch((url) => {
      if (url.includes("/scraper-schedules/nightly_tier1/runs")) {
        return jsonResponse(sampleRunsFor("nightly_tier1"));
      }
      if (url.includes("/scraper-schedules/weekly_state/runs")) {
        return jsonResponse(emptyRunsFor());
      }
      if (url.includes("/scraper-schedules/hourly_linker/runs")) {
        return jsonResponse(emptyRunsFor());
      }
      throw new Error(`unexpected URL: ${url}`);
    });

    renderWithProviders(<SchedulerPage />);

    // All three job-key cards present.
    await waitFor(() => {
      expect(screen.getByText("nightly_tier1")).toBeInTheDocument();
    });
    expect(screen.getByText("weekly_state")).toBeInTheDocument();
    expect(screen.getByText("hourly_linker")).toBeInTheDocument();

    // Descriptions present.
    expect(
      screen.getByText("Nightly Tier 1 league scraper"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Weekly state associations sweep"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Hourly canonical-club linker"),
    ).toBeInTheDocument();

    // Sample run row from nightly_tier1.
    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });

    // Two "No runs yet." placeholders (weekly_state + hourly_linker).
    expect(screen.getAllByText("No runs yet.")).toHaveLength(2);

    // 3 GETs went out on mount.
    const getCalls = fetchMock.mock.calls.filter(([, init]) => {
      return !init || (init as RequestInit).method !== "POST";
    });
    expect(getCalls.length).toBe(3);
  });

  it("shows inline 403 error banner when plain admin clicks Run now", async () => {
    const fetchMock = installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && url.includes("/runs")) {
        return jsonResponse(emptyRunsFor());
      }
      if (
        method === "POST" &&
        url.includes("/scraper-schedules/nightly_tier1/run")
      ) {
        return jsonResponse({ error: "super_admin required" }, 403);
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("nightly_tier1")).toBeInTheDocument();
    });

    // Each card has its own "Run now" button. Grab the first one (nightly_tier1
    // is rendered first).
    const runButtons = screen.getAllByRole("button", { name: /run now/i });
    await user.click(runButtons[0]!);

    // Confirm dialog appears with a "Run now" action.
    await waitFor(() => {
      expect(screen.getByText(/queue run for/i)).toBeInTheDocument();
    });
    const dialogRunButton = screen
      .getAllByRole("button", { name: /run now/i })
      .find(
        (btn) =>
          btn.textContent === "Run now" && btn.closest("[role=alertdialog]"),
      );
    expect(dialogRunButton).toBeDefined();
    await user.click(dialogRunButton!);

    // Inline 403 banner appears on the nightly_tier1 card.
    await waitFor(() => {
      const alerts = screen.getAllByRole("alert");
      expect(alerts.length).toBeGreaterThan(0);
      expect(
        alerts.some((el) =>
          /super_admin role required/i.test(el.textContent ?? ""),
        ),
      ).toBe(true);
    });

    // No success toast.
    expect(screen.queryByRole("status")).toBeNull();
    expect(fetchMock).toHaveBeenCalled();
  });

  it("shows success toast + refetches runs when super_admin clicks Run now", async () => {
    let nightlyRunsCallCount = 0;
    const fetchMock = installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (
        method === "GET" &&
        url.includes("/scraper-schedules/nightly_tier1/runs")
      ) {
        nightlyRunsCallCount += 1;
        // First call: empty. Subsequent calls (after POST refetch): one row.
        if (nightlyRunsCallCount === 1) {
          return jsonResponse(emptyRunsFor());
        }
        return jsonResponse(sampleRunsFor("nightly_tier1"));
      }
      if (method === "GET" && url.includes("/runs")) {
        return jsonResponse(emptyRunsFor());
      }
      if (
        method === "POST" &&
        url.includes("/scraper-schedules/nightly_tier1/run")
      ) {
        return jsonResponse(
          {
            id: 101,
            jobKey: "nightly_tier1",
            status: "pending",
            requestedAt: "2026-04-18T12:00:00.000Z",
          },
          201,
        );
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    // Wait for initial mount fetches to settle.
    await waitFor(() => {
      expect(screen.getByText("nightly_tier1")).toBeInTheDocument();
    });

    const runButtons = screen.getAllByRole("button", { name: /run now/i });
    await user.click(runButtons[0]!);

    // Click the confirm action inside the dialog.
    const dialog = await screen.findByRole("alertdialog");
    const dialogConfirm = within(dialog).getByRole("button", {
      name: /run now/i,
    });
    await user.click(dialogConfirm);

    // Success toast.
    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(/job queued: #101/i);
    });

    // POST body structure matches contract: { jobKey, args: {} }.
    const postCall = fetchMock.mock.calls.find(([, init]) => {
      return init && (init as RequestInit).method === "POST";
    });
    expect(postCall).toBeDefined();
    const [, postInit] = postCall as [string, RequestInit];
    expect(JSON.parse(postInit.body as string)).toEqual({
      jobKey: "nightly_tier1",
      args: {},
    });

    // Runs list refetched — the #42 row should now render.
    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });
    expect(nightlyRunsCallCount).toBeGreaterThanOrEqual(2);
  });

  it("clicking a run row opens the detail dialog and renders data from GET /scheduler-jobs/:id (not the list row)", async () => {
    // List row carries a placeholder stdout tail; the detail endpoint returns
    // different stdout/stderr tails. The dialog must show the detail payload.
    const listRow = {
      id: 42,
      jobKey: "nightly_tier1",
      args: null,
      status: "success",
      requestedBy: 1,
      requestedAt: "2026-04-18T12:00:00.000Z",
      startedAt: "2026-04-18T12:00:01.000Z",
      completedAt: "2026-04-18T12:02:00.000Z",
      exitCode: 0,
      stdoutTail: "LIST_STDOUT_NOT_SHOWN",
      stderrTail: null,
    };
    const detailPayload = {
      ...listRow,
      stdoutTail: "DETAIL_STDOUT_OK",
      stderrTail: "DETAIL_STDERR_OK",
    };

    let detailCallCount = 0;
    installFetch((url) => {
      if (url.includes("/scraper-schedules/nightly_tier1/runs")) {
        return jsonResponse({ jobs: [listRow], total: 1 });
      }
      if (url.includes("/scraper-schedules/")) {
        return jsonResponse(emptyRunsFor());
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        detailCallCount += 1;
        return jsonResponse(detailPayload);
      }
      throw new Error(`unexpected URL: ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    // Wait for the list row to appear.
    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });

    // Click the row — row-click sets selectedJobId which enables the detail
    // query. The dialog opens and fires GET /scheduler-jobs/42.
    await user.click(screen.getByText("#42"));

    // Dialog populated from the detail fetch (DETAIL_*, never LIST_*).
    await waitFor(() => {
      expect(screen.getByText("DETAIL_STDOUT_OK")).toBeInTheDocument();
    });
    expect(screen.getByText("DETAIL_STDERR_OK")).toBeInTheDocument();
    expect(screen.queryByText("LIST_STDOUT_NOT_SHOWN")).toBeNull();
    expect(detailCallCount).toBe(1);
  });

  it("detail dialog shows a loading indicator while the detail fetch is in flight", async () => {
    let resolveDetail: ((r: Response) => void) | null = null;
    installFetch((url) => {
      if (url.includes("/scraper-schedules/nightly_tier1/runs")) {
        return jsonResponse(sampleRunsFor("nightly_tier1"));
      }
      if (url.includes("/scraper-schedules/")) {
        return jsonResponse(emptyRunsFor());
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        return new Promise<Response>((resolve) => {
          resolveDetail = resolve;
        });
      }
      throw new Error(`unexpected URL: ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });
    await user.click(screen.getByText("#42"));

    // Loading indicator visible inside the dialog body.
    await waitFor(() => {
      expect(screen.getByText(/loading job detail/i)).toBeInTheDocument();
    });

    // Now resolve the detail fetch and verify the loading state clears.
    resolveDetail!(
      jsonResponse({
        id: 42,
        jobKey: "nightly_tier1",
        args: null,
        status: "success",
        requestedBy: 1,
        requestedAt: "2026-04-18T12:00:00.000Z",
        startedAt: "2026-04-18T12:00:01.000Z",
        completedAt: "2026-04-18T12:02:00.000Z",
        exitCode: 0,
        stdoutTail: "done",
        stderrTail: null,
      }),
    );
    await waitFor(() => {
      expect(screen.getByText("done")).toBeInTheDocument();
    });
  });

  it("detail dialog shows an inline error message when the detail fetch fails", async () => {
    installFetch((url) => {
      if (url.includes("/scraper-schedules/nightly_tier1/runs")) {
        return jsonResponse(sampleRunsFor("nightly_tier1"));
      }
      if (url.includes("/scraper-schedules/")) {
        return jsonResponse(emptyRunsFor());
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        return jsonResponse({ error: "not found" }, 404);
      }
      throw new Error(`unexpected URL: ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });
    await user.click(screen.getByText("#42"));

    await waitFor(() => {
      expect(
        screen.getByText(/failed to load job detail/i),
      ).toBeInTheDocument();
    });
  });
});
