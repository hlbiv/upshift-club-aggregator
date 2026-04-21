import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  useGetCoverageLeagueDetail,
  type CoverageLeagueDetailResponse,
  type GetCoverageLeagueDetailStatus,
} from "@workspace/api-client-react";
import AdminNav from "../components/AdminNav";

/**
 * Coverage drilldown — per-club detail for one league.
 *
 *   GET /api/v1/admin/coverage/leagues/:leagueId?status=<all|never_scraped|stale>
 *
 * The `status` filter narrows the table to clubs with no scrape history
 * or a stale (>14d) last-scraped timestamp. Ordered oldest-first so the
 * most-neglected clubs surface on page 1.
 */

const PAGE_SIZE = 20;

const STATUS_OPTIONS: Array<{
  value: GetCoverageLeagueDetailStatus;
  label: string;
}> = [
  { value: "all", label: "All" },
  { value: "never_scraped", label: "Never scraped" },
  { value: "stale", label: "Stale 14d" },
];

export default function CoverageLeaguePage() {
  const { leagueId: leagueIdRaw } = useParams<{ leagueId: string }>();
  const leagueId = Number(leagueIdRaw);
  const [page, setPage] = useState(1);
  const [status, setStatus] = useState<GetCoverageLeagueDetailStatus>("all");

  const query = useGetCoverageLeagueDetail(leagueId, {
    page,
    page_size: PAGE_SIZE,
    status,
  });

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-6">
        <nav aria-label="Breadcrumb" className="mb-2 text-sm text-neutral-500">
          <Link to="/coverage" className="hover:underline">
            Coverage
          </Link>{" "}
          / <span className="text-neutral-700">League #{leagueId}</span>
        </nav>
        <h1 className="text-2xl font-semibold text-neutral-900">
          {query.data?.league.name ?? `League #${leagueId}`}
        </h1>
        <p className="text-sm text-neutral-500">
          Per-club coverage within this league. Oldest last-scraped first.
        </p>
      </header>

      <div
        role="radiogroup"
        aria-label="Status filter"
        className="mb-4 inline-flex overflow-hidden rounded-md border border-neutral-200 text-sm"
      >
        {STATUS_OPTIONS.map((opt) => {
          const active = opt.value === status;
          return (
            <button
              key={opt.value}
              type="button"
              role="radio"
              aria-checked={active}
              onClick={() => {
                setStatus(opt.value);
                setPage(1);
              }}
              className={
                active
                  ? "bg-neutral-900 px-3 py-1.5 font-medium text-white"
                  : "bg-white px-3 py-1.5 text-neutral-600 hover:bg-neutral-50"
              }
              data-testid={`status-filter-${opt.value}`}
            >
              {opt.label}
            </button>
          );
        })}
      </div>

      <DetailTable
        data={query.data}
        isLoading={query.isLoading}
        error={query.error}
        page={page}
        onPageChange={setPage}
      />
    </main>
  );
}

function DetailTable({
  data,
  isLoading,
  error,
  page,
  onPageChange,
}: {
  data: CoverageLeagueDetailResponse | undefined;
  isLoading: boolean;
  error: unknown;
  page: number;
  onPageChange: (next: number) => void;
}) {
  if (isLoading) {
    return (
      <div
        className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-16 text-center text-sm text-neutral-500"
        data-testid="coverage-detail-loading"
      >
        Loading…
      </div>
    );
  }
  if (error) {
    const status = (error as { status?: unknown }).status;
    if (typeof status === "number" && status === 404) {
      return (
        <div
          className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-8 text-sm text-amber-800"
          data-testid="coverage-detail-notfound"
        >
          League not found.
        </div>
      );
    }
    return (
      <div
        className="rounded-lg border border-red-200 bg-red-50 px-4 py-8 text-sm text-red-700"
        data-testid="coverage-detail-error"
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
        data-testid="coverage-detail-empty"
      >
        No clubs match the selected filter.
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
              <th scope="col" className="px-4 py-3">Club</th>
              <th scope="col" className="px-4 py-3">Last scraped</th>
              <th scope="col" className="px-4 py-3 text-right">Failures</th>
              <th scope="col" className="px-4 py-3 text-right">Coaches</th>
              <th scope="col" className="px-4 py-3">Roster?</th>
              <th scope="col" className="px-4 py-3">Staff page</th>
              <th scope="col" className="px-4 py-3 text-right">Confidence</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.clubId}
                className="border-b border-neutral-100 last:border-b-0"
                data-testid={`coverage-detail-row-${row.clubId}`}
              >
                <td className="px-4 py-3 font-medium text-neutral-900">
                  {row.clubNameCanonical}
                </td>
                <td className="px-4 py-3 text-neutral-700">
                  {row.lastScrapedAt === null ? (
                    <span className="font-medium text-red-700">Never</span>
                  ) : (
                    formatDate(row.lastScrapedAt)
                  )}
                </td>
                <td className="px-4 py-3 text-right">
                  <span
                    className={
                      row.consecutiveFailures > 0
                        ? "font-medium text-amber-700"
                        : "text-neutral-500"
                    }
                  >
                    {row.consecutiveFailures.toLocaleString()}
                  </span>
                </td>
                <td className="px-4 py-3 text-right text-neutral-700">
                  {row.coachCount.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-neutral-700">
                  {row.hasRosterSnapshot ? "Yes" : "—"}
                </td>
                <td className="px-4 py-3 text-neutral-700">
                  {row.staffPageUrl ? (
                    <a
                      href={row.staffPageUrl}
                      target="_blank"
                      rel="noreferrer noopener"
                      className="text-blue-700 hover:underline"
                    >
                      Link
                    </a>
                  ) : (
                    <span className="text-neutral-400">—</span>
                  )}
                </td>
                <td className="px-4 py-3 text-right text-neutral-700">
                  {row.scrapeConfidence === null
                    ? "—"
                    : row.scrapeConfidence.toFixed(2)}
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
          Page {page} of {totalPages} · {total.toLocaleString()} clubs
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

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(0, 10);
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
