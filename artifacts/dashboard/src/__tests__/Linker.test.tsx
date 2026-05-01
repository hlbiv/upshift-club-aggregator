import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import LinkerPage from "../pages/Linker";

/**
 * LinkerPage tests — follows the DedupDetail.test.tsx harness exactly:
 *   QueryClientProvider (retries disabled) + MemoryRouter + vi.stubGlobal fetch.
 *
 * useSearchClubs is mocked at the module level so the per-row typeahead
 * doesn't need the full Orval/customFetch chain in tests.
 */

vi.mock("@workspace/api-client-react", async (importActual) => {
  const actual = await importActual<typeof import("@workspace/api-client-react")>();
  return {
    ...actual,
    useSearchClubs: vi.fn(() => ({
      data: { query: "cf", results: [] },
      isFetching: false,
    })),
  };
});

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function renderLinker() {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <LinkerPage />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("LinkerPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders unmatched names from the API", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/linker/unmatched")) {
          return Promise.resolve(
            jsonResponse({
              items: [
                { raw_name: "CF Academy", total_count: 12 },
                { raw_name: "Semi Finals Winner", total_count: 3 },
              ],
              total: 2,
              page: 1,
              page_size: 50,
            }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderLinker();

    await waitFor(() => {
      expect(screen.getByText("CF Academy")).toBeInTheDocument();
    });
    expect(screen.getByText("Semi Finals Winner")).toBeInTheDocument();
  });

  it("Ignore button removes the row", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/linker/unmatched")) {
          return Promise.resolve(
            jsonResponse({
              items: [{ raw_name: "CF Academy", total_count: 12 }],
              total: 1,
              page: 1,
              page_size: 50,
            }),
          );
        }
        if (u.includes("/api/v1/admin/linker/ignore")) {
          return Promise.resolve(jsonResponse({ ok: true }));
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    const user = userEvent.setup();
    renderLinker();

    await waitFor(() => {
      expect(screen.getByText("CF Academy")).toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: /ignore/i }));

    await waitFor(() => {
      expect(screen.queryByText("CF Academy")).not.toBeInTheDocument();
    });
  });

  it("Resolve flow: select a club then Save removes the row", async () => {
    const { useSearchClubs } = await import("@workspace/api-client-react");
    const mockUseSearchClubs = useSearchClubs as ReturnType<typeof vi.fn>;

    // Start with no results; will update to return results after input
    mockUseSearchClubs.mockReturnValue({
      data: { query: "", results: [] },
      isFetching: false,
    });

    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.includes("/api/v1/admin/linker/unmatched")) {
          return Promise.resolve(
            jsonResponse({
              items: [{ raw_name: "CF Academy", total_count: 12 }],
              total: 1,
              page: 1,
              page_size: 50,
            }),
          );
        }
        if (u.includes("/api/v1/admin/linker/resolve")) {
          return Promise.resolve(
            jsonResponse({ ok: true, alias_id: 999, already_existed: false }),
          );
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    const user = userEvent.setup();
    renderLinker();

    await waitFor(() => {
      expect(screen.getByText("CF Academy")).toBeInTheDocument();
    });

    // Type into the search input — after typing, mock returns a result
    mockUseSearchClubs.mockReturnValue({
      data: {
        query: "concorde",
        results: [
          {
            id: 42,
            club_name_canonical: "Concorde Fire",
            club_slug: "concorde-fire",
            city: "Atlanta",
            state: "GA",
            country: "US",
            status: "active",
          },
        ],
      },
      isFetching: false,
    });

    // There are two "search clubs" inputs: the global search header input and
    // the per-row linker input. The linker row input has placeholder "Search clubs…"
    // (with ellipsis), while the global search has "Search clubs, coaches, leagues…".
    // Use getAllByPlaceholderText and pick the row input by the exact placeholder.
    const searchInputs = screen.getAllByPlaceholderText(/search clubs/i);
    // The linker row input is the one with placeholder "Search clubs…" (shorter)
    const searchInput =
      searchInputs.find((el) =>
        el.getAttribute("placeholder") === "Search clubs…",
      ) ?? searchInputs[searchInputs.length - 1];
    await user.type(searchInput, "concorde");

    // Dropdown should appear; click the club result
    await waitFor(() => {
      expect(screen.getByText("Concorde Fire")).toBeInTheDocument();
    });
    await user.click(screen.getByText("Concorde Fire"));

    // Club chip should show; Save button should be enabled
    await waitFor(() => {
      // The chip shows the club name
      expect(screen.getAllByText("Concorde Fire").length).toBeGreaterThan(0);
    });

    await user.click(screen.getByRole("button", { name: /^save$/i }));

    await waitFor(() => {
      expect(screen.queryByText("CF Academy")).not.toBeInTheDocument();
    });
  });
});
