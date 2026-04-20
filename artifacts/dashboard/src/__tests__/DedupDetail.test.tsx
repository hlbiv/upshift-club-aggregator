import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import DedupDetailPage from "../pages/DedupDetail";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function fixtureDetail(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 501,
    leftClubId: 1001,
    rightClubId: 1002,
    score: 0.931,
    method: "rapidfuzz-token-set",
    status: "pending" as const,
    createdAt: "2026-04-17T12:00:00.000Z",
    reviewedAt: null,
    reviewedBy: null,
    leftSnapshot: { clubNameCanonical: "Foo SC (stale)" },
    rightSnapshot: { clubNameCanonical: "Foo Soccer Club (stale)" },
    leftCurrent: {
      clubNameCanonical: "Foo SC",
      city: "Austin",
      state: "TX",
      websiteUrl: "https://foosc.example",
      foundedYear: 1999,
      aliases: ["Foo Soccer"],
    },
    rightCurrent: {
      clubNameCanonical: "Foo Soccer Club",
      city: "Austin",
      state: "TX",
      websiteUrl: "https://foosoccerclub.example",
      foundedYear: 2001,
      aliases: [],
    },
    affiliations: {
      leftAffiliationCount: 2,
      rightAffiliationCount: 1,
    },
    rosters: {
      leftRosterSnapshotCount: 5,
      rightRosterSnapshotCount: 3,
    },
    ...overrides,
  };
}

/** Route probe component that reports the current pathname + router state. */
function RouteProbe() {
  const loc = useLocation();
  const state = loc.state as { flash?: string } | null;
  return (
    <div data-testid="route-probe">
      <span data-testid="route-pathname">{loc.pathname}</span>
      <span data-testid="route-flash">{state?.flash ?? ""}</span>
    </div>
  );
}

function renderDetailAt(id: string) {
  return render(
    <MemoryRouter initialEntries={[`/dedup/${id}`]}>
      <Routes>
        <Route path="/dedup/:id" element={<DedupDetailPage />} />
        <Route path="/dedup" element={<RouteProbe />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("DedupDetailPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders both clubs from leftCurrent / rightCurrent, not snapshots", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.match(/\/api\/v1\/admin\/dedup\/clubs\/\d+$/)) {
          return Promise.resolve(jsonResponse(fixtureDetail()));
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderDetailAt("501");

    await waitFor(() => {
      expect(screen.getByText(/Left: Foo SC$/)).toBeInTheDocument();
    });
    expect(screen.getByText(/Right: Foo Soccer Club$/)).toBeInTheDocument();

    // Stale snapshot strings should NOT render (current fields win).
    expect(screen.queryByText(/stale/i)).not.toBeInTheDocument();

    // Affiliation + roster counts visible on both panels.
    expect(screen.getByText("5")).toBeInTheDocument(); // left roster count
    expect(screen.getByText("3")).toBeInTheDocument(); // right roster count

    // Action bar shown for pending pair.
    expect(
      screen.getByRole("button", { name: /pick left as winner/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /pick right as winner/i }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /reject pair/i }),
    ).toBeInTheDocument();
  });

  it("confirms merge via dialog, POSTs, and redirects with flash", async () => {
    const mergeUrl = "/api/v1/admin/dedup/clubs/501/merge";
    const calls: string[] = [];

    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL, init?: RequestInit) => {
        const u = typeof url === "string" ? url : url.toString();
        calls.push(`${init?.method ?? "GET"} ${u}`);
        if (u.endsWith("/api/v1/admin/dedup/clubs/501")) {
          return Promise.resolve(jsonResponse(fixtureDetail()));
        }
        if (u.endsWith(mergeUrl)) {
          return Promise.resolve(
            jsonResponse({
              ok: true,
              winnerId: 1001,
              loserAliasesCreated: 1,
              affiliationsReparented: 1,
              rosterSnapshotsReparented: 3,
            }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderDetailAt("501");

    await waitFor(() => {
      expect(screen.getByText(/Left: Foo SC$/)).toBeInTheDocument();
    });

    await userEvent.click(
      screen.getByRole("button", { name: /pick left as winner/i }),
    );

    // Dialog shows confirmation message.
    await waitFor(() => {
      expect(
        screen.getByRole("heading", { name: /confirm merge/i }),
      ).toBeInTheDocument();
    });

    await userEvent.click(
      screen.getByRole("button", { name: /^confirm merge$/i }),
    );

    // Redirects to /dedup with flash in route state.
    await waitFor(() => {
      expect(screen.getByTestId("route-pathname").textContent).toBe("/dedup");
    });
    expect(screen.getByTestId("route-flash").textContent).toMatch(
      /Merged into #1001/,
    );

    // POST hit the merge endpoint.
    expect(calls.some((c) => c.startsWith(`POST ${mergeUrl}`))).toBe(true);
  });

  it("reject button POSTs and redirects", async () => {
    const rejectUrl = "/api/v1/admin/dedup/clubs/501/reject";
    const calls: string[] = [];

    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL, init?: RequestInit) => {
        const u = typeof url === "string" ? url : url.toString();
        calls.push(`${init?.method ?? "GET"} ${u}`);
        if (u.endsWith("/api/v1/admin/dedup/clubs/501")) {
          return Promise.resolve(jsonResponse(fixtureDetail()));
        }
        if (u.endsWith(rejectUrl)) {
          return Promise.resolve(jsonResponse({ ok: true }));
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderDetailAt("501");

    await waitFor(() => {
      expect(screen.getByText(/Left: Foo SC$/)).toBeInTheDocument();
    });

    await userEvent.click(
      screen.getByRole("button", { name: /reject pair/i }),
    );

    await waitFor(() => {
      expect(screen.getByTestId("route-pathname").textContent).toBe("/dedup");
    });
    expect(screen.getByTestId("route-flash").textContent).toMatch(
      /Pair #501 rejected/,
    );
    expect(calls.some((c) => c.startsWith(`POST ${rejectUrl}`))).toBe(true);
  });
});
