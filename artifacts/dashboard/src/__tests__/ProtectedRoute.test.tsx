import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import ProtectedRoute from "../components/ProtectedRoute";

function renderWithRouter(initialPath = "/scraper-health") {
  return render(
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
    </MemoryRouter>,
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

  it("renders children when /v1/admin/me returns 200", async () => {
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
    expect(globalThis.fetch).toHaveBeenCalledWith(
      expect.stringContaining("/v1/admin/me"),
      expect.objectContaining({ credentials: "include" }),
    );
  });

  it("redirects to /login when /v1/admin/me returns 401", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ error: "unauthorized" }), { status: 401 }),
    );

    renderWithRouter();

    await waitFor(() => {
      expect(screen.getByText("login page")).toBeInTheDocument();
    });
    expect(screen.queryByText("protected content")).not.toBeInTheDocument();
  });

  it("redirects to /login when /v1/admin/me network-errors", async () => {
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
