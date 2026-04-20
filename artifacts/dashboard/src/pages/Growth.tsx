import { lazy, Suspense, useEffect, useState } from "react";
import type {
  CoverageTrendPoint,
  CoverageTrendResponse,
  ScrapedCountsDelta,
} from "@hlbiv/api-zod/admin";
import { adminFetch } from "../lib/api";
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
 * Both requests re-fire on window change. No debounce — selector is three
 * hard-coded choices. Empty state triggers when the chart has zero points.
 */

type Window = 7 | 30 | 90;

type FetchState<T> =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ok"; data: T };

export default function GrowthPage() {
  const [days, setDays] = useState<Window>(30);
  const [counts, setCounts] = useState<FetchState<ScrapedCountsDelta>>({
    kind: "loading",
  });
  const [trend, setTrend] = useState<FetchState<CoverageTrendResponse>>({
    kind: "loading",
  });

  useEffect(() => {
    let cancelled = false;
    setCounts({ kind: "loading" });
    setTrend({ kind: "loading" });

    const since = new Date(Date.now() - days * 86400000).toISOString();

    adminFetch(
      `/api/v1/admin/growth/scraped-counts?since=${encodeURIComponent(since)}`,
    )
      .then(async (res) => {
        if (cancelled) return;
        if (!res.ok) {
          setCounts({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const data = (await res.json()) as ScrapedCountsDelta;
        setCounts({ kind: "ok", data });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setCounts({
          kind: "error",
          message: e instanceof Error ? e.message : "Network error",
        });
      });

    adminFetch(`/api/v1/admin/growth/coverage-trend?days=${days}`)
      .then(async (res) => {
        if (cancelled) return;
        if (!res.ok) {
          setTrend({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const data = (await res.json()) as CoverageTrendResponse;
        setTrend({ kind: "ok", data });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setTrend({
          kind: "error",
          message: e instanceof Error ? e.message : "Network error",
        });
      });

    return () => {
      cancelled = true;
    };
  }, [days]);

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
          value={countsValue(counts, (d) => d.clubsAdded)}
        />
        <StatCard
          label="Coaches added"
          caption={caption}
          value={countsValue(counts, (d) => d.coachesAdded)}
        />
        <StatCard
          label="Events added"
          caption={caption}
          value={countsValue(counts, (d) => d.eventsAdded)}
        />
        <StatCard
          label="Roster snapshots added"
          caption={caption}
          value={countsValue(counts, (d) => d.rosterSnapshotsAdded)}
        />
        <StatCard
          label="Matches added"
          caption={caption}
          value={countsValue(counts, (d) => d.matchesAdded)}
        />
      </section>

      <section aria-labelledby="trend-heading">
        <h2
          id="trend-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Daily scrape runs
        </h2>
        <TrendChart state={trend} days={days} />
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
  value: { kind: "loading" } | { kind: "error" } | { kind: "ok"; n: number };
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
  state,
  days,
}: {
  state: FetchState<CoverageTrendResponse>;
  days: Window;
}) {
  if (state.kind === "loading") return <ChartPlaceholder label="Loading…" />;
  if (state.kind === "error")
    return <ChartPlaceholder label={`Failed to load: ${state.message}`} />;

  const points = state.data.points;
  if (points.length === 0) {
    return (
      <ChartPlaceholder label={`No runs in the last ${days} days.`} />
    );
  }

  return (
    <Suspense fallback={<ChartPlaceholder label="Loading chart…" />}>
      <LazyCoverageChart points={points as CoverageTrendPoint[]} />
    </Suspense>
  );
}

// --- helpers --------------------------------------------------------------

function countsValue(
  state: FetchState<ScrapedCountsDelta>,
  pick: (d: ScrapedCountsDelta) => number,
): { kind: "loading" } | { kind: "error" } | { kind: "ok"; n: number } {
  if (state.kind === "loading") return { kind: "loading" };
  if (state.kind === "error") return { kind: "error" };
  return { kind: "ok", n: pick(state.data) };
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
