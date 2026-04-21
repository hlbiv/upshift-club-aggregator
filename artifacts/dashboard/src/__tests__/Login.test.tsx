import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import LoginPage from "../pages/Login";

/**
 * LoginPage migrated from `adminFetch()` to the Orval-generated
 * `useAdminLogin` mutation hook. The hook routes through the shared
 * customFetch mutator, which ultimately calls `globalThis.fetch` — so
 * stubbing `fetch` per-test still works. We just need a QueryClientProvider
 * wrapper around the render.
 *
 * Per-test QueryClient (retries disabled) keeps error tests from hanging
 * on React Query's default retry behavior.
 */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function renderLogin() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={["/login"]}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/scraper-health"
            element={<div>scraper health page</div>}
          />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("LoginPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("submits the form and navigates to /scraper-health on success", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse({ id: 1, email: "admin@example.com", role: "admin" }),
    );
    const user = userEvent.setup();

    renderLogin();

    await user.type(screen.getByLabelText(/email/i), "admin@example.com");
    await user.type(screen.getByLabelText(/password/i), "hunter22hunter");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    await waitFor(() => {
      expect(screen.getByText("scraper health page")).toBeInTheDocument();
    });

    const [url, init] = (globalThis.fetch as ReturnType<typeof vi.fn>).mock
      .calls[0] as [string, RequestInit];
    expect(url).toContain("/api/v1/admin/auth/login");
    expect(init.method).toBe("POST");
    // customFetch defaults `credentials: 'include'` for relative-path requests
    // so the admin session cookie travels in dev.
    expect(init.credentials).toBe("include");
    expect(JSON.parse(init.body as string)).toEqual({
      email: "admin@example.com",
      password: "hunter22hunter",
    });
  });

  it("shows the server error message on 401", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      jsonResponse({ error: "invalid credentials" }, 401),
    );
    const user = userEvent.setup();

    renderLogin();

    await user.type(screen.getByLabelText(/email/i), "admin@example.com");
    await user.type(screen.getByLabelText(/password/i), "wrongpassword");
    await user.click(screen.getByRole("button", { name: /sign in/i }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent(
        "invalid credentials",
      );
    });
    expect(screen.queryByText("scraper health page")).not.toBeInTheDocument();
  });
});
