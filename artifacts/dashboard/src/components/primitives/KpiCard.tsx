import type { ReactNode } from "react";
import { Link } from "react-router-dom";
import type { LucideIcon } from "lucide-react";

/**
 * Single key-performance card. Used both in the page-level KPI strip and on
 * the Overview "needs attention" list. Includes optional deep-link, tone,
 * trend caption, and a small icon.
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

export function KpiCard({
  label,
  value,
  caption,
  tone = "neutral",
  icon: Icon,
  to,
  isLoading,
  isError,
}: {
  label: ReactNode;
  value: ReactNode;
  caption?: ReactNode;
  tone?: KpiTone;
  icon?: LucideIcon;
  to?: string;
  isLoading?: boolean;
  isError?: boolean;
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
