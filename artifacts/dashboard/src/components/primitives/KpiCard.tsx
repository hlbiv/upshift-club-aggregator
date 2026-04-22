import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import type { LucideIcon } from "lucide-react";

/**
 * Single key-performance card. Used both in the page-level KPI strip and on
 * the Overview "needs attention" list. Includes optional deep-link, tone,
 * trend caption, and a small icon.
 *
 * Trend slot
 * ----------
 * `trend` is an optional inline mini-chart + delta badge slot for trend
 * data. Pass `{ history, deltaDirection }`:
 *   - `history`: numeric series (oldest-first) drawn as a tiny inline SVG
 *     sparkline. <2 points → no line drawn (badge still renders).
 *   - `deltaDirection` lets the call site flip the colour semantics for
 *     "lower is better" KPIs (never_scraped, stale): a *decrease* over
 *     the window should render green, an *increase* red. Defaults to
 *     "higher_better" (more roster snapshots is good).
 *
 * The sparkline + delta render together below the value, so the strip
 * gains height by ~24px when trend data is present. Pages without trend
 * data (Overview list, status drilldowns) opt out by omitting `trend`.
 */
export type KpiTone = "neutral" | "ok" | "warn" | "fail" | "primary";

const TONE: Record<KpiTone, { value: string; bar: string; ring: string }> = {
  neutral: {
    value: "text-slate-900",
    bar: "bg-slate-200",
    ring: "ring-slate-200",
  },
  primary: {
    value: "text-indigo-700",
    bar: "bg-indigo-500",
    ring: "ring-indigo-200",
  },
  ok: {
    value: "text-emerald-700",
    bar: "bg-emerald-500",
    ring: "ring-emerald-200",
  },
  warn: {
    value: "text-amber-700",
    bar: "bg-amber-500",
    ring: "ring-amber-200",
  },
  fail: {
    value: "text-red-700",
    bar: "bg-red-500",
    ring: "ring-red-200",
  },
};

export interface KpiTrend {
  /** Numeric series, oldest-first. */
  history: number[];
  /**
   * Whether higher values are "better" (e.g. clubs with rosters → more is
   * good) or "lower is better" (e.g. never_scraped → more is bad).
   * Controls the badge colour: defaults to higher_better.
   */
  deltaDirection?: "higher_better" | "lower_better";
  /**
   * How many trailing points to compare when computing the delta badge.
   * Defaults to 7 (week-over-week).
   */
  windowDays?: number;
}

export function KpiCard({
  label,
  value,
  caption,
  tone = "neutral",
  icon: Icon,
  to,
  isLoading,
  isError,
  trend,
}: {
  label: ReactNode;
  value: ReactNode;
  caption?: ReactNode;
  tone?: KpiTone;
  icon?: LucideIcon;
  to?: string;
  isLoading?: boolean;
  isError?: boolean;
  trend?: KpiTrend;
}) {
  const cfg = TONE[tone];
  const inner = (
    <div
      className={`group relative h-full overflow-hidden rounded-xl border border-slate-200 bg-white p-4 shadow-xs transition-shadow hover:shadow-sm ring-1 ring-inset ${cfg.ring}`}
    >
      <div className={`absolute inset-x-0 top-0 h-0.5 ${cfg.bar}`} />
      <div className="flex items-start justify-between gap-3">
        <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
          {label}
        </p>
        {Icon ? (
          <Icon
            aria-hidden
            className="h-4 w-4 text-slate-400 group-hover:text-indigo-500"
          />
        ) : null}
      </div>
      <p className={`mt-2 text-3xl font-semibold tabular-nums ${cfg.value}`}>
        {isLoading ? (
          <span className="text-slate-300">…</span>
        ) : isError ? (
          <span className="text-slate-300">—</span>
        ) : (
          value
        )}
      </p>
      {trend && !isLoading && !isError ? (
        <TrendRow trend={trend} />
      ) : null}
      {caption ? (
        <p className="mt-1 text-xs text-slate-500">{caption}</p>
      ) : null}
    </div>
  );
  if (to) {
    return (
      <Link to={to} className="block focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 rounded-xl">
        {inner}
      </Link>
    );
  }
  return inner;
}

/**
 * Inline sparkline + delta badge. Renders nothing meaningful when the
 * series has 0 or 1 points (so the strip doesn't lie about a trend that
 * doesn't yet exist on a fresh deploy).
 */
function TrendRow({ trend }: { trend: KpiTrend }) {
  const series = trend.history;
  const direction = trend.deltaDirection ?? "higher_better";
  const windowDays = trend.windowDays ?? 7;

  if (series.length < 2) {
    return (
      <p className="mt-2 text-[11px] text-slate-400" data-testid="kpi-trend-empty">
        No trend yet
      </p>
    );
  }

  const last = series[series.length - 1] ?? 0;
  // Compare to the value `windowDays` ago; if not enough history, fall
  // back to the oldest point we have.
  const refIndex = Math.max(0, series.length - 1 - windowDays);
  const ref = series[refIndex] ?? last;
  const delta = last - ref;

  let badgeClass: string;
  if (delta === 0) {
    badgeClass = "text-slate-500 bg-slate-100";
  } else {
    const isGood =
      direction === "higher_better" ? delta > 0 : delta < 0;
    badgeClass = isGood
      ? "text-emerald-700 bg-emerald-50"
      : "text-red-700 bg-red-50";
  }
  const sign = delta > 0 ? "+" : "";
  const span = Math.min(series.length - 1, windowDays);
  const label = `${sign}${delta.toLocaleString()} (${span}d)`;

  return (
    <div className="mt-2 flex items-center justify-between gap-2">
      <Sparkline values={series} tone={trend.deltaDirection} />
      <span
        className={`rounded px-1.5 py-0.5 text-[11px] font-medium tabular-nums ${badgeClass}`}
        data-testid="kpi-trend-delta"
        title={`Change over the last ${span} day${span === 1 ? "" : "s"}`}
      >
        {label}
      </span>
    </div>
  );
}

/**
 * Tiny inline SVG sparkline. No external charting lib — recharts is
 * overkill for a 60×16 line.
 */
function Sparkline({
  values,
  tone,
}: {
  values: number[];
  tone?: "higher_better" | "lower_better";
}) {
  const w = 80;
  const h = 18;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const stepX = values.length === 1 ? 0 : w / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = i * stepX;
      const y = h - ((v - min) / range) * h;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  // Stroke colour mirrors the delta semantics so a regression line is
  // visually distinct from progress.
  const last = values[values.length - 1] ?? 0;
  const first = values[0] ?? 0;
  const delta = last - first;
  let stroke = "stroke-slate-400";
  if (delta !== 0) {
    const isGood = tone === "lower_better" ? delta < 0 : delta > 0;
    stroke = isGood ? "stroke-emerald-500" : "stroke-red-500";
  }

  return (
    <svg
      viewBox={`0 0 ${w} ${h}`}
      width={w}
      height={h}
      className="overflow-visible"
      role="img"
      aria-label="trend sparkline"
      data-testid="kpi-trend-sparkline"
    >
      <polyline
        fill="none"
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
        className={stroke}
        points={points}
      />
    </svg>
  );
}
