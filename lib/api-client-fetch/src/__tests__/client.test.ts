/**
 * Client type-surface smoke test — no runtime harness, just a compile-time
 * check that generated functions exist, accept the expected params, and
 * return the expected shape. Runs as part of `tsc --build`.
 *
 * Run: pnpm --filter @hlbiv/api-client-fetch exec tsc -p tsconfig.build.json
 * (or simply `pnpm typecheck` from the monorepo root).
 */

import {
  listClubs,
  searchEvents,
  searchCoaches,
  listLeagues,
  healthCheck,
  UpshiftDataError,
  setBaseUrl,
  setApiKey,
  customFetch,
} from "../index.js";
import type {
  ClubListResponse,
  EventSearchResponse,
  CoachSearchResponse,
  LeagueListResponse,
  HealthStatus,
  ListClubsParams,
  SearchEventsParams,
  SearchCoachesParams,
} from "../index.js";

// --- Generated functions are callable with the expected param/return types ---

function assertCallSignatures() {
  // listClubs: optional params, returns ClubListResponse
  const p1: Promise<ClubListResponse> = listClubs();
  const p2: Promise<ClubListResponse> = listClubs({ page: 1, page_size: 20 } as ListClubsParams);

  // searchEvents
  const p3: Promise<EventSearchResponse> = searchEvents();
  const p4: Promise<EventSearchResponse> = searchEvents({
    season: "2025-26",
  } as SearchEventsParams);

  // searchCoaches
  const p5: Promise<CoachSearchResponse> = searchCoaches({} as SearchCoachesParams);

  // listLeagues (no params)
  const p6: Promise<LeagueListResponse> = listLeagues();

  // healthCheck
  const p7: Promise<HealthStatus> = healthCheck();

  // Reference the promises so TS doesn't flag them unused.
  void [p1, p2, p3, p4, p5, p6, p7];
}

// --- Error class surface ---

function assertErrorShape() {
  const err = new UpshiftDataError(500, "boom", { code: "x", details: { a: 1 } });
  const status: number = err.status;
  const message: string = err.message;
  const code: string | undefined = err.code;
  const details: unknown = err.details;
  void [status, message, code, details];
}

// --- Mutator configuration surface ---

function assertMutatorConfigurable() {
  setBaseUrl("https://upshiftdata.com");
  setApiKey("test-key");
  // customFetch should accept a URL + RequestInit-like options
  const p: Promise<unknown> = customFetch<unknown>("/api/clubs", { method: "GET" });
  void p;
}

// Export the no-op asserters so the file is a module and its body is checked
// by tsc's project-references build.
export const _assertions = {
  assertCallSignatures,
  assertErrorShape,
  assertMutatorConfigurable,
};
