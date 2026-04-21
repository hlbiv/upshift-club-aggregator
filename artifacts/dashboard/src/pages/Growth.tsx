import { lazy, Suspense, useMemo, useState } from "react";
import {
  useGetGrowthCoverageTrend,
  useGetGrowthScrapedCounts,
  type CoverageTrendResponse,
  type ScrapedCountsDelta,
} from "@workspace/api-client-react";
import type { CoverageTrendPoint } from "@hlbiv/api-zod/admin";
import AdminNav from "../components/AdminNav";

// Recharts is ~400KB minified — lazy-load so it doesn't inflate the
// initial bundle for pages that don't render the chart (Scraper health,
// Dedup, Login). Vite splits this into its own chunk automatically.
const LazyCoverageChart = lazy(() => import("../components/CoverageChart"));

/**
 * Growth dashboard.
 *
 *   GET /api/v1/admin/growth/scraped-counts?since=<iso>  → ScrapedCountsDelta
 *   GET /api/v1/admin/growth/coverage-trend?days=<n>     → CoverageTrendResponse
 *
 * Five stat cards across the top (clubs / coaches / events / roster
 * snapshots / matches added) + a daily successes-vs-failures line chart
 * underneath. Window selector chooses the rolling horizon (7 / 30 / 90d).
 *
 * Both requests re-fire on window change via Orval-generated React Query
 * hooks (`useGetGrowthScrapedCounts` / `useGetGrowthCoverageTrend`). No
 * debounce — selector is three hard-coded choices. Empty state triggers
 * when the chart has zero points.
 */

type Window = 7 | 30 | 90;

type CountsCell =
  | { kind: "loading" }
  | { kind: "error" }
  | { kind: "ok"; n: number };

export default function GrowthPage() {
  const [days, setDays] = useState<Window>(30);

  // `since` is memoized off the chosen window so React Query's queryKey
  // (which includes params) stays stable across re-renders until `days`
  // changes. Recomputing on every render would invalidate the cache on
  // every mouse twitch.
  const since = useMemo(
    () => new Date(Date.now() - days * 86400000).toISOString(),
    [days],
  );

  const countsQuery = useGetGrowthScrapedCounts({ since });
  const trendQuery = useGetGrowthCoverageTrend({ days });

  const caption = `last ${days}d`;

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold text-neutral-900">Growth</h1>
          <p className="text-sm text-neutral-500">
            Records added and daily scrape-run health over a rolling window.
          </p>
        </div>
        <WindowSelector value={days} onChange={setDays} />
      </header>

      <section
        className="mb-10 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-5"
        aria-labelledby="counts-heading"
      >
        <h2 id="counts-heading" className="sr-only">
          Records added
        </h2>
        <StatCard
          label="Clubs added"
          caption={caption}
          value={countsCell(countsQuery.data, countsQuery, (d) => d.clubsAdded)}
        />
        <StatCard
          label="Coaches added"
          caption={caption}
          value={countsCell(countsQuery.data, countsQuery, (d) => d.coachesAdded)}
        />
        <StatCard
          label="Events added"
          caption={caption}
          value={countsCell(countsQuery.data, countsQuery, (d) => d.eventsAdded)}
        />
        <StatCard
          label="Roster snapshots added"
          caption={caption}
          value={countsCell(
            countsQuery.data,
            countsQuery,
            (d) => d.rosterSnapshotsAdded,
          )}
        />
        <StatCard
          label="Matches added"
          caption={caption}
          value={countsCell(
            countsQuery.data,
            countsQuery,
            (d) => d.matchesAdded,
          )}
        />
      </section>

      <section aria-labelledby="trend-heading">
        <h2
          id="trend-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Daily scrape runs
        </h2>
        <TrendChart
          data={trendQuery.data}
          isLoading={trendQuery.isLoading}
          error={trendQuery.error}
          days={days}
        />
      </section>
    </main>
  );
}

function WindowSelector({
  value,
  onChange,
}: {
  value: Window;
  onChange: (next: Window) => void;
}) {
  const options: Window[] = [7, 30, 90];
  return (
    <div
      role="radiogroup"
      aria-label="Window size"
      className="inline-flex overflow-hidden rounded-md border border-neutral-200 text-sm"
    >
      {options.map((opt) => {
        const active = opt === value;
        return (
          <button
            key={opt}
            type="button"
            role="radio"
            aria-checked={active}
            onClick={() => onChange(opt)}
            className={
              active
                ? "bg-neutral-900 px-3 py-1.5 font-medium text-white"
                : "bg-white px-3 py-1.5 text-neutral-600 hover:bg-neutral-50"
            }
          >
            {opt}d
          </button>
        );
      })}
    </div>
  );
}

function StatCard({
  label,
  caption,
  value,
}: {
  label: string;
  caption: string;
  value: CountsCell;
}) {
  return (
    <div className="rounded-lg border border-neutral-200 bg-white p-4">
      <div className="text-xs font-medium uppercase tracking-wide text-neutral-500">
        {label}
      </div>
      <div
        className="mt-1 text-3xl font-semibold text-neutral-900"
        data-testid={`stat-${slug(label)}`}
      >
        {value.kind === "ok"
          ? value.n.toLocaleString()
          : value.kind === "loading"
            ? "…"
            : "—"}
      </div>
      <div className="mt-1 text-xs text-neutral-500">{caption}</div>
    </div>
  );
}

function TrendChart({
  data,
  isLoading,
  error,
  days,
}: {
  data: CoverageTrendResponse | undefined;
  isLoading: boolean;
  error: unknown;
  days: Window;
}) {
  if (isLoading) return <ChartPlaceholder label="Loading…" />;
  if (error)
    return <ChartPlaceholder label={`Failed to load: ${formatError(error)}`} />;

  const points = data?.points ?? [];
  if (points.length === 0) {
    return <ChartPlaceholder label={`No runs in the last ${days} days.`} />;
  }

  return (
    <Suspense fallback={<ChartPlaceholder label="Loading chart…" />}>
      <LazyCoverageChart points={points as CoverageTrendPoint[]} />
    </Suspense>
  );
}

// --- helpers --------------------------------------------------------------

function countsCell(
  data: ScrapedCountsDelta | undefined,
  status: { isLoading: boolean; isError: boolean },
  pick: (d: ScrapedCountsDelta) => number,
): CountsCell {
  if (data) return { kind: "ok", n: pick(data) };
  if (status.isError) return { kind: "error" };
  if (status.isLoading) return { kind: "loading" };
  // Fallback: no data, not loading, not errored — treat as loading so the
  // card shows "…" rather than "—".
  return { kind: "loading" };
}

function ChartPlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-16 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

function slug(s: string): string {
  return s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
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
