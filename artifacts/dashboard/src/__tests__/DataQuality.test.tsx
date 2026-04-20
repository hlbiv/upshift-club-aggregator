import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import DataQualityPage from "../pages/DataQuality";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

/**
 * Fetch dispatcher keyed by URL substring. Each panel loads its own
 * endpoint, and the GA Premier tab posts on demand. Using a dispatcher
 * keeps tab-specific tests independent of the default-tab eager-load
 * behaviour from other panels.
 */
function makeFetchMock(
  routes: Record<string, (init: RequestInit) => Response | Promise<Response>>,
) {
  return vi.fn((url: string, init: RequestInit = {}) => {
    for (const [needle, handler] of Object.entries(routes)) {
      if (url.includes(needle)) return Promise.resolve(handler(init));
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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    const fetchMock = vi.fn((url: string, init: RequestInit = {}) => {
      if (String(url).includes("ga-premier-orphans")) {
        return ga(url, init);
      }
      return Promise.resolve(
        new Response(JSON.stringify({ error: "no route" }), { status: 500 }),
      );
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      fetchMock,
    );

    const user = userEvent.setup();
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

    await user.click(
      screen.getByRole("tab", { name: /empty staff pages/i }),
    );

    await waitFor(() => {
      const alert = screen.getByRole("alert");
      expect(within(alert).getByText(/500/)).toBeInTheDocument();
    });
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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

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
    render(
      <MemoryRouter>
        <DataQualityPage />
      </MemoryRouter>,
    );

    await user.click(screen.getByRole("tab", { name: /stale scrapes/i }));

    await waitFor(() => {
      const alert = screen.getByRole("alert");
      expect(within(alert).getByText(/500/)).toBeInTheDocument();
    });
  });
});
