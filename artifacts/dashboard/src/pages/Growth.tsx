import { lazy, Suspense, useMemo, useState } from "react";
import { Users, UserCog, CalendarRange, ListChecks, Trophy } from "lucide-react";
import {
  useGetGrowthCoverageTrend,
  useGetGrowthScrapedCounts,
  type CoverageTrendResponse,
  type ScrapedCountsDelta,
} from "@workspace/api-client-react";
import type { CoverageTrendPoint } from "@hlbiv/api-zod/admin";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";
import { KpiStrip } from "../components/primitives/KpiStrip";
import { KpiCard } from "../components/primitives/KpiCard";
import { SectionCard } from "../components/primitives/SectionCard";
import { EmptyState } from "../components/primitives/EmptyState";

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
 * Five `<KpiCard>`s across the top (clubs / coaches / events / roster
 * snapshots / matches added) + a daily successes-vs-failures line chart
 * underneath. Window selector chooses the rolling horizon (7 / 30 / 90d).
 */

type Window = 7 | 30 | 90;

export default function GrowthPage() {
  const [days, setDays] = useState<Window>(30);

  // `since` is memoized off the chosen window so React Query's queryKey
  // (which includes params) stays stable across re-renders until `days`
  // changes.
  const since = useMemo(
    () => new Date(Date.now() - days * 86400000).toISOString(),
    [days],
  );

  const countsQuery = useGetGrowthScrapedCounts({ since });
  const trendQuery = useGetGrowthCoverageTrend({ days });

  const caption = `last ${days}d`;
  const d: ScrapedCountsDelta | undefined = countsQuery.data;

  return (
    <AppShell>
      <PageHeader
        title="Growth"
        description="Records added and daily scrape-run health over a rolling window."
        actions={<WindowSelector value={days} onChange={setDays} />}
      />

      <KpiStrip cols={5}>
        <KpiCard
          label="Clubs added"
          caption={caption}
          icon={Users}
          tone="primary"
          value={(d?.clubsAdded ?? 0).toLocaleString()}
          isLoading={countsQuery.isLoading}
          isError={countsQuery.isError}
        />
        <KpiCard
          label="Coaches added"
          caption={caption}
          icon={UserCog}
          tone="primary"
          value={(d?.coachesAdded ?? 0).toLocaleString()}
          isLoading={countsQuery.isLoading}
          isError={countsQuery.isError}
        />
        <KpiCard
          label="Events added"
          caption={caption}
          icon={CalendarRange}
          tone="primary"
          value={(d?.eventsAdded ?? 0).toLocaleString()}
          isLoading={countsQuery.isLoading}
          isError={countsQuery.isError}
        />
        <KpiCard
          label="Roster snapshots"
          caption={caption}
          icon={ListChecks}
          tone="primary"
          value={(d?.rosterSnapshotsAdded ?? 0).toLocaleString()}
          isLoading={countsQuery.isLoading}
          isError={countsQuery.isError}
        />
        <KpiCard
          label="Matches added"
          caption={caption}
          icon={Trophy}
          tone="primary"
          value={(d?.matchesAdded ?? 0).toLocaleString()}
          isLoading={countsQuery.isLoading}
          isError={countsQuery.isError}
        />
      </KpiStrip>

      <SectionCard
        title="Daily scrape runs"
        description={`Successes vs failures over the last ${days} days.`}
      >
        <div className="p-2">
          <TrendChart
            data={trendQuery.data}
            isLoading={trendQuery.isLoading}
            error={trendQuery.error}
            days={days}
          />
        </div>
      </SectionCard>
    </AppShell>
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
      className="inline-flex overflow-hidden rounded-md border border-slate-200 bg-white text-sm shadow-xs"
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
                ? "bg-slate-900 px-3 py-1.5 font-medium text-white"
                : "bg-white px-3 py-1.5 text-slate-600 hover:bg-slate-50"
            }
          >
            {opt}d
          </button>
        );
      })}
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
    return (
      <EmptyState
        title={`No runs in the last ${days} days`}
        description="Trigger a job from the scheduler to populate the chart."
      />
    );
  }

  return (
    <Suspense fallback={<ChartPlaceholder label="Loading chart…" />}>
      <LazyCoverageChart points={points as CoverageTrendPoint[]} />
    </Suspense>
  );
}

function ChartPlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-slate-200 bg-white px-4 py-16 text-center text-sm text-slate-500">
      {label}
    </div>
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
