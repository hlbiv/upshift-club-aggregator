import { lazy, Suspense, useMemo } from "react";
import { Link } from "react-router-dom";
import {
  AlertOctagon,
  Activity,
  ArrowRight,
  BarChart3,
  GitMerge,
  PlusCircle,
  ShieldAlert,
} from "lucide-react";
import {
  useGetGrowthCoverageTrend,
  useGetGrowthScrapedCounts,
  useGetStaleScrapes,
  useListClubDuplicates,
  useListScrapeRuns,
  type ScrapeRunLog,
} from "@workspace/api-client-react";
import type { CoverageTrendPoint } from "@hlbiv/api-zod/admin";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";
import { KpiStrip } from "../components/primitives/KpiStrip";
import { KpiCard } from "../components/primitives/KpiCard";
import { SectionCard } from "../components/primitives/SectionCard";
import { EmptyState } from "../components/primitives/EmptyState";
import {
  StatusBadge,
  toneForScrapeStatus,
} from "../components/primitives/StatusBadge";
import {
  useEmptyStaffCount,
  useFailingScrapeRunsCount,
  useOpenNavLeakedCount,
  useOpenNumericOnlyCount,
  usePendingDedupCount,
  useStaleScrapesCount,
} from "../lib/queueCounts";

// Recharts is heavy — keep it lazy-loaded so the Overview shell can paint
// before the trend chart hydrates.
const LazyCoverageChart = lazy(() => import("../components/CoverageChart"));

/**
 * Overview / Home — default landing route at `/`.
 *
 *  - KPI strip: failing jobs, stale scrapers, dedup pairs, records added 7d
 *  - "Needs attention" lists: top failing recent runs, oldest stale scrapers,
 *    pending dedup pairs, open quality flags
 *  - 7-day trend strip: daily scrape successes vs failures
 *
 * Composed entirely from existing endpoints — no new backend work needed
 * for this surface beyond what the queues already expose.
 */
export default function OverviewPage() {
  const sevenDaysAgoIso = useMemo(
    () => new Date(Date.now() - 7 * 86400_000).toISOString(),
    [],
  );
  const counts = useGetGrowthScrapedCounts({ since: sevenDaysAgoIso });
  const trend = useGetGrowthCoverageTrend({ days: 7 });
  const recentRuns = useListScrapeRuns({ limit: 50 });
  const staleQuery = useGetStaleScrapes({
    page: 1,
    page_size: 5,
    threshold_days: 14,
  });
  const dedupQuery = useListClubDuplicates({
    status: "pending",
    limit: 5,
    page: 1,
  });

  const failingCount = useFailingScrapeRunsCount();
  const staleTotal = useStaleScrapesCount();
  const dedupTotal = usePendingDedupCount();
  const navLeakedTotal = useOpenNavLeakedCount();
  const numericOnlyTotal = useOpenNumericOnlyCount();
  const emptyStaffTotal = useEmptyStaffCount();

  const recordsAdded =
    (counts.data?.clubsAdded ?? 0) +
    (counts.data?.coachesAdded ?? 0) +
    (counts.data?.eventsAdded ?? 0) +
    (counts.data?.rosterSnapshotsAdded ?? 0) +
    (counts.data?.matchesAdded ?? 0);

  const recentFailing: ScrapeRunLog[] = useMemo(() => {
    return (recentRuns.data?.runs ?? [])
      .filter((r) => r.status === "failed")
      .slice(0, 5);
  }, [recentRuns.data]);

  return (
    <AppShell>
      <PageHeader
        eyebrow="Overview"
        title="Operations dashboard"
        description="Snapshot of what's broken, what's stale, what's queued for review, and what we've added this week."
        actions={
          <Link
            to="/scheduler"
            className="inline-flex items-center gap-1.5 rounded-md bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white shadow-xs hover:bg-indigo-700"
          >
            Open scheduler
            <ArrowRight className="h-3.5 w-3.5" />
          </Link>
        }
      />

      <KpiStrip cols={4}>
        <KpiCard
          label="Failing scrape runs"
          value={failingCount ?? "…"}
          caption="Last 100 runs"
          tone={failingCount && failingCount > 0 ? "fail" : "ok"}
          icon={AlertOctagon}
          to="/scraper-health"
          isLoading={failingCount === null}
        />
        <KpiCard
          label="Stale scrapers"
          value={staleTotal ?? "…"}
          caption=">14 days since last scrape"
          tone={staleTotal && staleTotal > 0 ? "warn" : "ok"}
          icon={Activity}
          to="/data-quality/stale-scrapes"
          isLoading={staleTotal === null}
        />
        <KpiCard
          label="Dedup pairs"
          value={dedupTotal ?? "…"}
          caption="Awaiting review"
          tone={dedupTotal && dedupTotal > 0 ? "warn" : "ok"}
          icon={GitMerge}
          to="/dedup"
          isLoading={dedupTotal === null}
        />
        <KpiCard
          label="Records added"
          value={recordsAdded.toLocaleString()}
          caption="Last 7 days, all entities"
          tone="primary"
          icon={PlusCircle}
          to="/growth"
          isLoading={counts.isLoading}
          isError={counts.isError}
        />
      </KpiStrip>

      <div className="mb-6 grid grid-cols-1 gap-4 lg:grid-cols-3">
        <SectionCard
          title="Needs attention"
          description="Top items on fire right now."
          className="lg:col-span-2"
        >
          {/*
           * Combined empty state — only render the full "all clear" if
           * none of the three feeds (failing runs, stale scrapers,
           * pending dedup pairs) have anything to show. Otherwise we
           * fall through to the per-section rows so an operator can see
           * what IS open even when one feed is empty.
           */}
          {recentFailing.length === 0 &&
          (staleQuery.data?.rows ?? []).length === 0 &&
          (dedupQuery.data?.pairs ?? []).length === 0 ? (
            <div className="px-4 py-10">
              <EmptyState
                title="All clear"
                description={`No failing runs, no stale scrapers, no pending dedup pairs — checked ${new Date().toLocaleTimeString()}.`}
                tone="ok"
              />
            </div>
          ) : (
          <ul className="divide-y divide-slate-100">
            {recentFailing.length === 0 ? null : (
              recentFailing.map((r) => (
                <AttentionRow
                  key={r.id}
                  href="/scraper-health"
                  primary={r.scraperKey}
                  secondary={r.jobKey ?? "—"}
                  badge={
                    <StatusBadge
                      tone={toneForScrapeStatus(r.status)}
                      label={r.status}
                      timestamp={r.startedAt}
                    />
                  }
                />
              ))
            )}

            {(staleQuery.data?.rows ?? []).slice(0, 3).map((r) => (
              <AttentionRow
                key={`stale-${r.entityType}-${r.entityId}`}
                href="/data-quality/stale-scrapes"
                primary={r.entityName ?? `(id ${r.entityId})`}
                secondary={`${r.entityType} · stale`}
                badge={
                  <StatusBadge
                    tone="stale"
                    label="stale"
                    timestamp={r.lastScrapedAt}
                  />
                }
              />
            ))}

            {(dedupQuery.data?.pairs ?? []).slice(0, 3).map((p) => (
              <AttentionRow
                key={`dedup-${p.id}`}
                href={`/dedup/${p.id}`}
                primary={`Dedup pair #${p.id}`}
                secondary={`score ${p.score.toFixed(3)} · ${p.method}`}
                badge={
                  <StatusBadge
                    tone="pending"
                    label="pending"
                    timestamp={p.createdAt}
                  />
                }
              />
            ))}
          </ul>
          )}
        </SectionCard>

        <SectionCard
          title="Review queues"
          description="Open items by category."
        >
          <ul className="divide-y divide-slate-100 text-sm">
            <QueueRow
              to="/data-quality/nav-leaked"
              label="Nav-leaked names"
              count={navLeakedTotal}
              icon={ShieldAlert}
            />
            <QueueRow
              to="/data-quality/numeric-only"
              label="Numeric-only names"
              count={numericOnlyTotal}
              icon={ShieldAlert}
            />
            <QueueRow
              to="/data-quality/empty-staff"
              label="Empty staff pages"
              count={emptyStaffTotal}
              icon={Activity}
            />
            <QueueRow
              to="/dedup"
              label="Dedup pairs"
              count={dedupTotal}
              icon={GitMerge}
            />
          </ul>
        </SectionCard>
      </div>

      <SectionCard
        title="Daily scrape runs"
        description="Successes vs failures over the last 7 days."
      >
        <div className="p-2">
          {trend.isLoading ? (
            <div className="h-72 animate-pulse rounded-lg bg-slate-50" />
          ) : trend.data && trend.data.points.length > 0 ? (
            <Suspense
              fallback={<div className="h-72 animate-pulse bg-slate-50" />}
            >
              <LazyCoverageChart
                points={trend.data.points as CoverageTrendPoint[]}
              />
            </Suspense>
          ) : (
            <EmptyState
              icon={BarChart3}
              title="No runs in the last 7 days"
              description="Trigger a job from the scheduler to populate the chart."
            />
          )}
        </div>
      </SectionCard>
    </AppShell>
  );
}

function AttentionRow({
  href,
  primary,
  secondary,
  badge,
}: {
  href: string;
  primary: string;
  secondary: string;
  badge?: React.ReactNode;
}) {
  return (
    <li>
      <Link
        to={href}
        className="flex items-center justify-between gap-3 px-4 py-2.5 text-sm hover:bg-slate-50"
      >
        <div className="min-w-0">
          <p className="truncate font-medium text-slate-900">{primary}</p>
          <p className="truncate text-xs text-slate-500">{secondary}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {badge}
          <ArrowRight className="h-3.5 w-3.5 text-slate-400" />
        </div>
      </Link>
    </li>
  );
}

function QueueRow({
  to,
  label,
  count,
  icon: Icon,
}: {
  to: string;
  label: string;
  count: number | null;
  icon: React.ComponentType<{ className?: string }>;
}) {
  const tone =
    count === null ? "stale" : count > 0 ? "warn" : "ok";
  return (
    <li>
      <Link
        to={to}
        className="flex items-center justify-between px-4 py-2.5 hover:bg-slate-50"
      >
        <span className="flex items-center gap-2 text-slate-700">
          <Icon className="h-4 w-4 text-slate-400" />
          {label}
        </span>
        <span className="flex items-center gap-2">
          <StatusBadge
            tone={tone}
            label={count === null ? "…" : `${count} open`}
          />
          <ArrowRight className="h-3.5 w-3.5 text-slate-400" />
        </span>
      </Link>
    </li>
  );
}

