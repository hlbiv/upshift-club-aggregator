import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProtectedRoute from "../components/ProtectedRoute";

/**
 * ProtectedRoute migrated from `adminFetch()` to the Orval-generated
 * `useAdminMe` hook. The hook routes through the shared customFetch
 * mutator, which ultimately calls `globalThis.fetch` — so stubbing
 * `fetch` per-test still works. We just need a QueryClientProvider
 * wrapper around the render.
 */
function renderWithRouter(initialPath = "/scraper-health") {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path="/login" element={<div>login page</div>} />
          <Route
            path="/scraper-health"
            element={
              <ProtectedRoute>
                <div>protected content</div>
              </ProtectedRoute>
            }
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("ProtectedRoute", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders children when /api/v1/admin/me returns 200", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(
        JSON.stringify({ id: 1, email: "admin@example.com", role: "admin" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );

    renderWithRouter();

    await waitFor(() => {
      expect(screen.getByText("protected content")).toBeInTheDocument();
    });
    const [url, init] = (globalThis.fetch as ReturnType<typeof vi.fn>).mock
      .calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/admin/me");
    // customFetch defaults `credentials: 'include'` for relative-path requests
    // so the admin session cookie travels in dev.
    expect(init.credentials).toBe("include");
  });

  it("redirects to /login when /api/v1/admin/me returns 401", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 }),
    );

    renderWithRouter();

    await waitFor(() => {
      expect(screen.getByText("login page")).toBeInTheDocument();
    });
    expect(screen.queryByText("protected content")).not.toBeInTheDocument();
  });

  it("redirects to /login when /api/v1/admin/me network-errors", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockRejectedValue(
      new TypeError("Failed to fetch"),
    );

    renderWithRouter();

    await waitFor(() => {
      expect(screen.getByText("login page")).toBeInTheDocument();
    });
  });

  it("shows a loading indicator while the check is in flight", () => {
    // Never-resolving fetch keeps the component in its loading state.
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockReturnValue(
      new Promise(() => {}),
    );

    renderWithRouter();

    expect(screen.getByRole("status")).toBeInTheDocument();
  });
});
