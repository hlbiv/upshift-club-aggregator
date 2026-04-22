import {
  getGetEmptyStaffPagesQueryKey,
  getGetNavLeakedNamesQueryKey,
  getGetNumericOnlyNamesQueryKey,
  getGetStaleScrapesQueryKey,
  getListClubDuplicatesQueryKey,
  getListScrapeRunsQueryKey,
  useGetEmptyStaffPages,
  useGetNavLeakedNames,
  useGetNumericOnlyNames,
  useGetStaleScrapes,
  useListClubDuplicates,
  useListScrapeRuns,
} from "@workspace/api-client-react";

/**
 * Lightweight count hooks used by the Sidebar (badge counts) and the
 * Overview page (KPI strip + "needs attention" list). Each hits the
 * existing list endpoint with a 1-row page so we get the `total` field
 * without paying the cost of fetching all rows.
 *
 * Counts use a 5-minute staleTime: the sidebar mounts on every route, and
 * we don't want six list endpoints firing each time an operator clicks a
 * nav link. Operators who need a fresher count can hard-refresh.
 *
 * Each call passes `queryKey` explicitly via the matching `get…QueryKey`
 * helper. The orval-generated wrapper would default the same key when it's
 * omitted, but v5's `UseQueryOptions` types `queryKey` as required —
 * supplying it explicitly keeps the call sites strictly typed without
 * resorting to `any` casts.
 */

const ONE_ROW = { page: 1, page_size: 1 } as const;
const STALE_5MIN = 5 * 60_000;

export function usePendingDedupCount(): number | null {
  const params = { status: "pending", limit: 1, page: 1 } as const;
  const q = useListClubDuplicates(params, {
    query: {
      queryKey: getListClubDuplicatesQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  return q.data?.total ?? null;
}

export function useOpenNavLeakedCount(): number | null {
  const params = { ...ONE_ROW, state: "open" } as const;
  const q = useGetNavLeakedNames(params, {
    query: {
      queryKey: getGetNavLeakedNamesQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  return q.data?.total ?? null;
}

export function useOpenNumericOnlyCount(): number | null {
  const params = { ...ONE_ROW, state: "open" } as const;
  const q = useGetNumericOnlyNames(params, {
    query: {
      queryKey: getGetNumericOnlyNamesQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  return q.data?.total ?? null;
}

export function useEmptyStaffCount(): number | null {
  const params = { ...ONE_ROW, window_days: 30 } as const;
  const q = useGetEmptyStaffPages(params, {
    query: {
      queryKey: getGetEmptyStaffPagesQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  return q.data?.total ?? null;
}

export function useStaleScrapesCount(): number | null {
  const params = { ...ONE_ROW, threshold_days: 14 } as const;
  const q = useGetStaleScrapes(params, {
    query: {
      queryKey: getGetStaleScrapesQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  return q.data?.total ?? null;
}

/**
 * Failing scrape runs in the most recent N — used by the Overview KPI strip.
 * The list endpoint doesn't currently expose a "status=failed" filter, so we
 * fetch the recent window and tally client-side. 100 rows is plenty for the
 * "is anything on fire" question; if more are failing simultaneously we want
 * to surface that anyway.
 */
export function useFailingScrapeRunsCount(): number | null {
  const params = { limit: 100 } as const;
  const q = useListScrapeRuns(params, {
    query: {
      queryKey: getListScrapeRunsQueryKey(params),
      staleTime: STALE_5MIN,
    },
  });
  if (!q.data) return null;
  return q.data.runs.filter((r) => r.status === "failed").length;
}
