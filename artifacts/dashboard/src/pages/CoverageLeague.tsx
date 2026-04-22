import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { Building2, Users, UserCheck, Ban, Clock3 } from "lucide-react";
import {
  useGetCoverageLeagueDetail,
  useGetCoverageLeagueHistory,
  type CoverageLeagueDetailResponse,
  type CoverageLeagueHistoryResponse,
  type GetCoverageLeagueDetailStatus,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";
import { KpiStrip } from "../components/primitives/KpiStrip";
import { KpiCard, type KpiTrend } from "../components/primitives/KpiCard";

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
  const historyQuery = useGetCoverageLeagueHistory(leagueId, { days: 30 });

  return (
    <AppShell>
      <nav aria-label="Breadcrumb" className="mb-2 text-sm text-slate-500">
        <Link to="/coverage" className="hover:underline">
          Coverage
        </Link>{" "}
        / <span className="text-slate-700">League #{leagueId}</span>
      </nav>
      <PageHeader
        title={query.data?.league.name ?? `League #${leagueId}`}
        description="Per-club coverage within this league. Oldest last-scraped first."
      />

      <TrendsStrip
        data={historyQuery.data}
        isLoading={historyQuery.isLoading}
        isError={historyQuery.isError}
      />

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
    </AppShell>
  );
}

/**
 * Per-league sparkline strip — same KpiStrip shape as the global Coverage
 * page, but driven by `/v1/admin/coverage/leagues/:leagueId/history`. The
 * five counters mirror the per-league rollup row exactly so this strip
 * always agrees with what the parent table shows for this league.
 *
 * History rows accumulate one per UTC day starting the first time the
 * Coverage summary endpoint is hit on a deployed instance, so a fresh
 * deploy renders the strip with KpiCards that simply omit the trend
 * (the cards still render their current value once the data lands).
 */
function TrendsStrip({
  data,
  isLoading,
  isError,
}: {
  data: CoverageLeagueHistoryResponse | undefined;
  isLoading: boolean;
  isError: boolean;
}) {
  const rows = data?.rows;
  const latest = rows && rows.length > 0 ? rows[rows.length - 1] : undefined;
  const trends = buildLeagueTrends(rows);

  return (
    <div className="mb-6">
      <KpiStrip cols={5}>
        <KpiCard
          label="Clubs"
          icon={Building2}
          tone="neutral"
          value={(latest?.clubsTotal ?? 0).toLocaleString()}
          isLoading={isLoading}
          isError={isError}
          trend={trends.clubsTotal}
        />
        <KpiCard
          label="With roster"
          icon={Users}
          tone="ok"
          value={(latest?.clubsWithRosterSnapshot ?? 0).toLocaleString()}
          isLoading={isLoading}
          isError={isError}
          trend={trends.clubsWithRosterSnapshot}
        />
        <KpiCard
          label="With coach"
          icon={UserCheck}
          tone="ok"
          value={(latest?.clubsWithCoachDiscovery ?? 0).toLocaleString()}
          isLoading={isLoading}
          isError={isError}
          trend={trends.clubsWithCoachDiscovery}
        />
        <KpiCard
          label="Never scraped"
          icon={Ban}
          tone={(latest?.clubsNeverScraped ?? 0) > 0 ? "fail" : "neutral"}
          value={(latest?.clubsNeverScraped ?? 0).toLocaleString()}
          isLoading={isLoading}
          isError={isError}
          trend={trends.clubsNeverScraped}
        />
        <KpiCard
          label="Stale 14d"
          icon={Clock3}
          tone={(latest?.clubsStale14d ?? 0) > 0 ? "warn" : "neutral"}
          value={(latest?.clubsStale14d ?? 0).toLocaleString()}
          isLoading={isLoading}
          isError={isError}
          trend={trends.clubsStale14d}
        />
      </KpiStrip>
    </div>
  );
}

/**
 * Slice the per-league history series into per-counter trends. Returns
 * `undefined` per counter when the series is empty so KpiCard simply omits
 * the trend slot rather than rendering an empty stub on first render.
 */
function buildLeagueTrends(
  rows: CoverageLeagueHistoryResponse["rows"] | undefined,
): {
  clubsTotal?: KpiTrend;
  clubsWithRosterSnapshot?: KpiTrend;
  clubsWithCoachDiscovery?: KpiTrend;
  clubsNeverScraped?: KpiTrend;
  clubsStale14d?: KpiTrend;
} {
  if (!rows || rows.length === 0) return {};
  const pick = (
    key: keyof CoverageLeagueHistoryResponse["rows"][number],
    deltaDirection: "higher_better" | "lower_better",
  ): KpiTrend => ({
    history: rows.map((r) => Number(r[key] ?? 0)),
    deltaDirection,
    windowDays: 7,
  });
  return {
    clubsTotal: pick("clubsTotal", "higher_better"),
    clubsWithRosterSnapshot: pick("clubsWithRosterSnapshot", "higher_better"),
    clubsWithCoachDiscovery: pick("clubsWithCoachDiscovery", "higher_better"),
    clubsNeverScraped: pick("clubsNeverScraped", "lower_better"),
    clubsStale14d: pick("clubsStale14d", "lower_better"),
  };
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
