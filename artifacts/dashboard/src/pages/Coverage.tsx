import { useState } from "react";
import { Link } from "react-router-dom";
import {
  useGetCoverageLeagues,
  type CoverageLeaguesResponse,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";

/**
 * Coverage overview.
 *
 *   GET /api/v1/admin/coverage/leagues?page=N&page_size=M
 *
 * Paginated table of leagues alongside aggregate coverage counts (total
 * clubs, clubs with a roster snapshot, clubs with a coach discovery,
 * clubs never scraped, clubs stale 14d). Ordered worst-covered first so
 * the top of page 1 always surfaces the leagues in need of attention.
 *
 * Click a row's "Drill down" link to navigate to
 * `/coverage/:leagueId` for per-club detail.
 */

const PAGE_SIZE = 20;

export default function CoveragePage() {
  const [page, setPage] = useState(1);
  const query = useGetCoverageLeagues({ page, page_size: PAGE_SIZE });

  return (
    <AppShell>
      <PageHeader
        title="Coverage"
        description="Per-league rollup of club coverage. Worst-covered leagues first. Click through for per-club drilldown."
      />

      <LeaguesTable
        data={query.data}
        isLoading={query.isLoading}
        error={query.error}
        page={page}
        onPageChange={setPage}
      />
    </AppShell>
  );
}

function LeaguesTable({
  data,
  isLoading,
  error,
  page,
  onPageChange,
}: {
  data: CoverageLeaguesResponse | undefined;
  isLoading: boolean;
  error: unknown;
  page: number;
  onPageChange: (next: number) => void;
}) {
  if (isLoading) {
    return (
      <div
        className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-16 text-center text-sm text-neutral-500"
        data-testid="coverage-loading"
      >
        Loading…
      </div>
    );
  }
  if (error) {
    return (
      <div
        className="rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-sm text-red-700"
        data-testid="coverage-error"
      >
        Failed to load: {formatError(error)}
      </div>
    );
  }
  const rows = data?.rows ?? [];
  if (rows.length === 0) {
    return (
      <div
        className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-16 text-center text-sm text-neutral-500"
        data-testid="coverage-empty"
      >
        No leagues returned.
      </div>
    );
  }

  const total = data?.total ?? 0;
  const pageSize = data?.pageSize ?? PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  return (
    <>
      <div className="overflow-x-auto rounded-lg border border-neutral-200 bg-white">
        <table className="w-full text-sm">
          <thead className="border-b border-neutral-200 bg-neutral-50 text-left text-xs font-medium uppercase tracking-wide text-neutral-500">
            <tr>
              <th scope="col" className="px-4 py-3">League</th>
              <th scope="col" className="px-4 py-3 text-right">Clubs</th>
              <th scope="col" className="px-4 py-3 text-right">w/ Roster</th>
              <th scope="col" className="px-4 py-3 text-right">w/ Coach</th>
              <th scope="col" className="px-4 py-3 text-right">Never scraped</th>
              <th scope="col" className="px-4 py-3 text-right">Stale 14d</th>
              <th scope="col" className="px-4 py-3" />
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.leagueId}
                className="border-b border-neutral-100 last:border-b-0"
                data-testid={`coverage-row-${row.leagueId}`}
              >
                <td className="px-4 py-3 font-medium text-neutral-900">
                  {row.leagueName}
                </td>
                <td className="px-4 py-3 text-right text-neutral-700">
                  {row.clubsTotal.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-right text-neutral-700">
                  {row.clubsWithRosterSnapshot.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-right text-neutral-700">
                  {row.clubsWithCoachDiscovery.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-right">
                  <span
                    className={
                      row.clubsNeverScraped > 0
                        ? "font-medium text-red-700"
                        : "text-neutral-500"
                    }
                  >
                    {row.clubsNeverScraped.toLocaleString()}
                  </span>
                </td>
                <td className="px-4 py-3 text-right">
                  <span
                    className={
                      row.clubsStale14d > 0
                        ? "font-medium text-amber-700"
                        : "text-neutral-500"
                    }
                  >
                    {row.clubsStale14d.toLocaleString()}
                  </span>
                </td>
                <td className="px-4 py-3 text-right">
                  <Link
                    to={`/coverage/${row.leagueId}`}
                    className="text-sm font-medium text-blue-700 hover:underline"
                  >
                    Drill down
                  </Link>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <nav
        aria-label="Pagination"
        className="mt-4 flex items-center justify-between text-sm"
      >
        <div className="text-neutral-500">
          Page {page} of {totalPages} · {total.toLocaleString()} leagues
        </div>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => onPageChange(Math.max(1, page - 1))}
            disabled={page <= 1}
            className="rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-neutral-700 hover:bg-neutral-50 disabled:opacity-50"
          >
            Previous
          </button>
          <button
            type="button"
            onClick={() => onPageChange(Math.min(totalPages, page + 1))}
            disabled={page >= totalPages}
            className="rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-neutral-700 hover:bg-neutral-50 disabled:opacity-50"
          >
            Next
          </button>
        </div>
      </nav>
    </>
  );
}

function formatError(err: unknown): string {
  if (!err) return "Network error";
  if (err instanceof Error) {
    const status = (err as unknown as { status?: unknown }).status;
    if (typeof status === "number") return `HTTP ${status}`;
    return err.message;
  }
  return String(err);
}
