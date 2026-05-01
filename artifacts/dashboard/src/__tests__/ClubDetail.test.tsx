import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ClubDetailPage from "../pages/ClubDetail";

/**
 * ClubDetailPage uses the Orval-generated `useGetClub`, `useGetClubResults`,
 * and `useGetClubStaff` hooks which route through `customFetch` → globalThis.fetch.
 * Stubbing `fetch` per-test is sufficient; each test gets a fresh QueryClient
 * with retries disabled so error cases don't hang.
 */
function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function renderAt(id: string) {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false, gcTime: 0 },
      mutations: { retry: false },
    },
  });
  return render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[`/clubs/${id}`]}>
        <Routes>
          <Route path="/clubs/:id" element={<ClubDetailPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

const FIXTURE_CLUB = {
  id: 482,
  club_name_canonical: "Concorde Fire SC",
  club_slug: "concorde-fire-sc",
  city: "Peachtree City",
  state: "GA",
  country: "US",
  status: "active",
  website: "https://concordefire.org",
  website_status: "ok",
  competitive_tier: "elite",
  aliases: [
    { id: 1, alias_name: "Concorde Fire", alias_slug: "concorde-fire", source: "gotsport", is_official: false },
    { id: 2, alias_name: "CFC", alias_slug: "cfc", source: "manual", is_official: false },
  ],
  affiliations: [
    {
      id: 10,
      gender_program: "boys",
      platform_name: "GotSport",
      platform_tier: "elite",
      conference_name: "",
      division_name: "Premier",
      season: "2025-26",
      source_url: "https://gotsport.com",
      source_name: "ECNL Boys",
      verification_status: "verified",
      notes: "",
    },
  ],
};

const FIXTURE_RESULTS = {
  club_id: 482,
  results: [
    {
      id: 1,
      season: "2024-25",
      league: "ECNL Boys",
      division: "Premier",
      age_group: "U17",
      gender: "M",
      wins: 10,
      losses: 3,
      draws: 2,
      goals_for: 35,
      goals_against: 18,
      matches_played: 15,
      last_calculated_at: "2026-04-01T00:00:00.000Z",
    },
  ],
};

const FIXTURE_STAFF = {
  club_id: 482,
  staff: [
    {
      id: 100,
      club_id: 482,
      name: "John Smith",
      title: "Head Coach",
      email: "john@concordefire.org",
      source_url: null,
      scraped_at: null,
      confidence: 0.92,
      platform_family: null,
    },
  ],
};

describe("ClubDetailPage", () => {
  beforeEach(() => {
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("renders club name, W-L-D numbers, affiliations, and staff from mocked fetch", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.match(/\/api\/clubs\/482\/results/)) {
          return Promise.resolve(jsonResponse(FIXTURE_RESULTS));
        }
        if (u.match(/\/api\/clubs\/482\/staff/)) {
          return Promise.resolve(jsonResponse(FIXTURE_STAFF));
        }
        if (u.match(/\/api\/clubs\/482$/)) {
          return Promise.resolve(jsonResponse(FIXTURE_CLUB));
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderAt("482");

    // Club name appears in the header
    await waitFor(() => {
      expect(screen.getByText("Concorde Fire SC")).toBeInTheDocument();
    });

    // W-L-D numbers rendered in the record table
    expect(screen.getByText("10")).toBeInTheDocument(); // wins
    expect(screen.getByText("3")).toBeInTheDocument();  // losses
    expect(screen.getByText("2")).toBeInTheDocument();  // draws

    // Season visible
    expect(screen.getByText("2024-25")).toBeInTheDocument();

    // Affiliations
    expect(screen.getByText("ECNL Boys")).toBeInTheDocument();

    // Staff
    expect(screen.getByText("John Smith")).toBeInTheDocument();
    expect(screen.getByText("92%")).toBeInTheDocument();
  });

  it("shows 'No results linked yet' when results array is empty", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockImplementation(
      (url: RequestInfo | URL) => {
        const u = typeof url === "string" ? url : url.toString();
        if (u.match(/\/api\/clubs\/482\/results/)) {
          return Promise.resolve(jsonResponse({ club_id: 482, results: [] }));
        }
        if (u.match(/\/api\/clubs\/482\/staff/)) {
          return Promise.resolve(jsonResponse({ club_id: 482, staff: [] }));
        }
        if (u.match(/\/api\/clubs\/482$/)) {
          return Promise.resolve(jsonResponse({ ...FIXTURE_CLUB, aliases: [], affiliations: [] }));
        }
        return Promise.reject(new Error(`unexpected url: ${u}`));
      },
    );

    renderAt("482");

    await waitFor(() => {
      expect(screen.getByText("Concorde Fire SC")).toBeInTheDocument();
    });

    expect(screen.getByText("No results linked yet.")).toBeInTheDocument();
  });
});
