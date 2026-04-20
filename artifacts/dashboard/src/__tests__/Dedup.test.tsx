import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import DedupPage from "../pages/Dedup";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function fixturePair(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    id: 501,
    leftClubId: 1001,
    rightClubId: 1002,
    score: 0.923,
    method: "rapidfuzz-token-set",
    status: "pending" as const,
    createdAt: "2026-04-17T12:00:00.000Z",
    reviewedAt: null,
    reviewedBy: null,
    leftSnapshot: { clubNameCanonical: "Foo SC" },
    rightSnapshot: { clubNameCanonical: "Foo Soccer Club" },
    ...overrides,
  };
}

describe("DedupPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders rows from mocked fetch and navigates on Review click", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/dedup/clubs")) {
          return Promise.resolve(
            jsonResponse({
              pairs: [
                fixturePair(),
                fixturePair({
                  id: 502,
                  leftClubId: 2001,
                  rightClubId: 2002,
                  score: 0.881,
                  leftSnapshot: { clubNameCanonical: "Bar FC" },
                  rightSnapshot: { clubNameCanonical: "Bar Futbol Club" },
                }),
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

    render(
      <MemoryRouter initialEntries={["/dedup"]}>
        <Routes>
          <Route path="/dedup" element={<DedupPage />} />
          <Route
            path="/dedup/:id"
            element={<div data-testid="detail-stub">detail</div>}
          />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText("Foo SC")).toBeInTheDocument();
    });

    // Both pairs render.
    expect(screen.getByText("Foo Soccer Club")).toBeInTheDocument();
    expect(screen.getByText("Bar FC")).toBeInTheDocument();
    expect(screen.getByText("Bar Futbol Club")).toBeInTheDocument();

    // Pending badge rendered.
    expect(screen.getAllByText("pending").length).toBeGreaterThan(0);

    // Click the first Review button.
    const reviewButtons = screen.getAllByRole("button", { name: /review/i });
    expect(reviewButtons.length).toBe(2);
    await userEvent.click(reviewButtons[0]);

    await waitFor(() => {
      expect(screen.getByTestId("detail-stub")).toBeInTheDocument();
    });
  });

  it("shows empty state when the queue is empty", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(() =>
      Promise.resolve(
        jsonResponse({ pairs: [], total: 0, page: 1, pageSize: 50 }),
      ),
    );

    render(
      <MemoryRouter initialEntries={["/dedup"]}>
        <Routes>
          <Route path="/dedup" element={<DedupPage />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText(/no pending dedup pairs/i)).toBeInTheDocument();
    });
    expect(
      screen.getByText(/python3 scraper\/run\.py --source club-dedup/i),
    ).toBeInTheDocument();
  });

  it("refetches when the status filter changes", async () => {
    const fetchMock = vi.fn((url: RequestInfo | URL) => {
      const u = typeof url === "string" ? url : url.toString();
      if (u.includes("status=merged")) {
        return Promise.resolve(
          jsonResponse({
            pairs: [
              fixturePair({ id: 900, status: "merged" }),
            ],
            total: 1,
            page: 1,
            pageSize: 50,
          }),
        );
      }
      return Promise.resolve(
        jsonResponse({
          pairs: [fixturePair()],
          total: 1,
          page: 1,
          pageSize: 50,
        }),
      );
    });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(fetchMock);

    render(
      <MemoryRouter initialEntries={["/dedup"]}>
        <Routes>
          <Route path="/dedup" element={<DedupPage />} />
        </Routes>
      </MemoryRouter>,
    );

    await waitFor(() => {
      expect(screen.getByText("Foo SC")).toBeInTheDocument();
    });

    const select = screen.getByLabelText("Status");
    await userEvent.selectOptions(select, "merged");

    await waitFor(() => {
      expect(screen.getByText("merged")).toBeInTheDocument();
    });
    // Pending fetch + merged fetch.
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(2);
  });
});
