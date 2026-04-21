import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import SchedulerPage from "../pages/Scheduler";

/**
 * SchedulerPage is driven by a single GET /scraper-schedules that returns
 * every known jobKey + its recent runs in one payload. Clicking a row fires
 * GET /scheduler-jobs/:id for the detail dialog. Run Now fires POST
 * /scraper-schedules/:jobKey/run. Orval hooks bottom out at globalThis.fetch
 * via the customFetch mutator, so we stub fetch per-test.
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

type SchedulerJobPayload = {
  id: number;
  jobKey: string;
  args: Record<string, unknown> | null;
  status: "pending" | "running" | "success" | "failed" | "canceled";
  requestedBy: number | null;
  requestedAt: string;
  startedAt: string | null;
  completedAt: string | null;
  exitCode: number | null;
  stdoutTail: string | null;
  stderrTail: string | null;
};

type SchedulePayload = {
  jobKey: string;
  description: string;
  cronExpression: string | null;
  recentRuns: SchedulerJobPayload[];
};

function sampleRun(jobKey: string, overrides: Partial<SchedulerJobPayload> = {}): SchedulerJobPayload {
  return {
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
    ...overrides,
  };
}

function defaultSchedules(): { schedules: SchedulePayload[] } {
  return {
    schedules: [
      {
        jobKey: "nightly_tier1",
        description: "Nightly Tier 1 league scraper",
        cronExpression: null,
        recentRuns: [sampleRun("nightly_tier1")],
      },
      {
        jobKey: "weekly_state",
        description: "Weekly state associations sweep",
        cronExpression: null,
        recentRuns: [],
      },
      {
        jobKey: "hourly_linker",
        description: "Hourly canonical-club linker",
        cronExpression: null,
        recentRuns: [],
      },
    ],
  };
}

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

function isListSchedulesUrl(url: string): boolean {
  const idx = url.indexOf("/scraper-schedules");
  if (idx < 0) return false;
  const rest = url.slice(idx + "/scraper-schedules".length);
  // Match `/scraper-schedules` or `/scraper-schedules?limit=10`, but not
  // `/scraper-schedules/:jobKey/runs` or `/scraper-schedules/:jobKey/run`.
  return rest.length === 0 || rest.startsWith("?");
}

describe("SchedulerPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders job cards dynamically from GET /scraper-schedules", async () => {
    const fetchMock = installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse(defaultSchedules());
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("nightly_tier1")).toBeInTheDocument();
    });
    expect(screen.getByText("weekly_state")).toBeInTheDocument();
    expect(screen.getByText("hourly_linker")).toBeInTheDocument();

    expect(
      screen.getByText("Nightly Tier 1 league scraper"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Weekly state associations sweep"),
    ).toBeInTheDocument();
    expect(
      screen.getByText("Hourly canonical-club linker"),
    ).toBeInTheDocument();

    // Run row from nightly_tier1's recentRuns.
    expect(screen.getByText("#42")).toBeInTheDocument();
    // Two "No runs yet." placeholders (weekly_state + hourly_linker).
    expect(screen.getAllByText("No runs yet.")).toHaveLength(2);

    // Exactly one GET /scraper-schedules on mount (not three per-jobKey GETs).
    const getCalls = fetchMock.mock.calls.filter(([, init]) => {
      return !init || (init as RequestInit).method !== "POST";
    });
    expect(getCalls.length).toBe(1);
  });

  it("renders the empty-state when the server returns zero schedules", async () => {
    installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse({ schedules: [] });
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(
        screen.getByText("No scheduled jobs configured."),
      ).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: /run now/i })).toBeNull();
  });

  it("renders cronExpression hint when the server provides one", async () => {
    installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse({
          schedules: [
            {
              jobKey: "future_job",
              description: "A hypothetical nightly job",
              cronExpression: "0 3 * * *",
              recentRuns: [],
            },
          ],
        });
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("future_job")).toBeInTheDocument();
    });
    expect(screen.getByText("0 3 * * *")).toBeInTheDocument();
  });

  it("shows inline 403 error banner when plain admin clicks Run now", async () => {
    const fetchMock = installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse(defaultSchedules());
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

    const runButtons = screen.getAllByRole("button", { name: /run now/i });
    await user.click(runButtons[0]!);

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

    await waitFor(() => {
      const alerts = screen.getAllByRole("alert");
      expect(
        alerts.some((el) =>
          /super_admin role required/i.test(el.textContent ?? ""),
        ),
      ).toBe(true);
    });

    expect(screen.queryByRole("status")).toBeNull();
    expect(fetchMock).toHaveBeenCalled();
  });

  it("shows success toast + refetches schedules when super_admin clicks Run now", async () => {
    let listCallCount = 0;
    const fetchMock = installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        listCallCount += 1;
        if (listCallCount === 1) {
          // First call: nightly_tier1 has no runs yet.
          const payload = defaultSchedules();
          payload.schedules[0]!.recentRuns = [];
          return jsonResponse(payload);
        }
        // Refetched after Run Now succeeds: one run row now present.
        return jsonResponse(defaultSchedules());
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

    await waitFor(() => {
      expect(screen.getByText("nightly_tier1")).toBeInTheDocument();
    });

    const runButtons = screen.getAllByRole("button", { name: /run now/i });
    await user.click(runButtons[0]!);

    const dialog = await screen.findByRole("alertdialog");
    const dialogConfirm = within(dialog).getByRole("button", {
      name: /run now/i,
    });
    await user.click(dialogConfirm);

    await waitFor(() => {
      expect(screen.getByRole("status")).toHaveTextContent(/job queued: #101/i);
    });

    const postCall = fetchMock.mock.calls.find(([, init]) => {
      return init && (init as RequestInit).method === "POST";
    });
    expect(postCall).toBeDefined();
    const [, postInit] = postCall as [string, RequestInit];
    expect(JSON.parse(postInit.body as string)).toEqual({
      jobKey: "nightly_tier1",
      args: {},
    });

    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });
    expect(listCallCount).toBeGreaterThanOrEqual(2);
  });

  it("clicking a run row opens the detail dialog and renders data from GET /scheduler-jobs/:id", async () => {
    const listRow = sampleRun("nightly_tier1", {
      stdoutTail: "LIST_STDOUT_NOT_SHOWN",
    });
    const detailPayload = {
      ...listRow,
      stdoutTail: "DETAIL_STDOUT_OK",
      stderrTail: "DETAIL_STDERR_OK",
    };

    let detailCallCount = 0;
    installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        const payload = defaultSchedules();
        payload.schedules[0]!.recentRuns = [listRow];
        return jsonResponse(payload);
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        detailCallCount += 1;
        return jsonResponse(detailPayload);
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });

    await user.click(screen.getByText("#42"));

    await waitFor(() => {
      expect(screen.getByText("DETAIL_STDOUT_OK")).toBeInTheDocument();
    });
    expect(screen.getByText("DETAIL_STDERR_OK")).toBeInTheDocument();
    expect(screen.queryByText("LIST_STDOUT_NOT_SHOWN")).toBeNull();
    expect(detailCallCount).toBe(1);
  });

  it("detail dialog shows a loading indicator while the detail fetch is in flight", async () => {
    let resolveDetail: ((r: Response) => void) | null = null;
    installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse(defaultSchedules());
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        return new Promise<Response>((resolve) => {
          resolveDetail = resolve;
        });
      }
      throw new Error(`unexpected: ${method} ${url}`);
    });

    const user = userEvent.setup();
    renderWithProviders(<SchedulerPage />);

    await waitFor(() => {
      expect(screen.getByText("#42")).toBeInTheDocument();
    });
    await user.click(screen.getByText("#42"));

    await waitFor(() => {
      expect(screen.getByText(/loading job detail/i)).toBeInTheDocument();
    });

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
    installFetch((url, init) => {
      const method = init?.method ?? "GET";
      if (method === "GET" && isListSchedulesUrl(url)) {
        return jsonResponse(defaultSchedules());
      }
      if (url.match(/\/scheduler-jobs\/42(\?|$)/)) {
        return jsonResponse({ error: "not found" }, 404);
      }
      throw new Error(`unexpected: ${method} ${url}`);
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
