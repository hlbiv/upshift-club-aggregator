import {
  AlertTriangle,
  CheckCircle2,
  CircleDashed,
  Clock,
  Loader2,
  XCircle,
  type LucideIcon,
} from "lucide-react";
import { formatRelative } from "../../lib/relativeTime";

/**
 * Semantic status pill used everywhere a row/job/scrape has a state. Combines
 * a colored dot/icon, a label, and an optional relative timestamp ("3h ago")
 * so failed/stale rows pull the eye instead of blending into a solid color.
 *
 * The `tone` prop is the canonical semantic — every page maps its
 * domain-specific status string to one of these via small adapters
 * (e.g. `mapScrapeStatus("failed") => "fail"`). Keeping the mapping in the
 * adapters means a new domain status doesn't need a new tone class.
 */
export type StatusTone =
  | "ok"
  | "warn"
  | "fail"
  | "stale"
  | "running"
  | "pending"
  | "neutral";

const TONE: Record<
  StatusTone,
  { bg: string; text: string; ring: string; icon: LucideIcon; emphasis: boolean }
> = {
  ok: {
    bg: "bg-emerald-50",
    text: "text-emerald-700",
    ring: "ring-emerald-200",
    icon: CheckCircle2,
    emphasis: false,
  },
  warn: {
    bg: "bg-amber-50",
    text: "text-amber-800",
    ring: "ring-amber-200",
    icon: AlertTriangle,
    emphasis: true,
  },
  fail: {
    bg: "bg-red-50",
    text: "text-red-700",
    ring: "ring-red-200",
    icon: XCircle,
    emphasis: true,
  },
  stale: {
    bg: "bg-slate-100",
    text: "text-slate-600",
    ring: "ring-slate-200",
    icon: Clock,
    emphasis: false,
  },
  running: {
    bg: "bg-blue-50",
    text: "text-blue-700",
    ring: "ring-blue-200",
    icon: Loader2,
    emphasis: false,
  },
  pending: {
    bg: "bg-yellow-50",
    text: "text-yellow-800",
    ring: "ring-yellow-200",
    icon: CircleDashed,
    emphasis: false,
  },
  neutral: {
    bg: "bg-slate-100",
    text: "text-slate-700",
    ring: "ring-slate-200",
    icon: CircleDashed,
    emphasis: false,
  },
};

export function StatusBadge({
  tone,
  label,
  timestamp,
  className,
}: {
  tone: StatusTone;
  label: string;
  /** ISO datetime to render as relative time alongside the label. */
  timestamp?: string | null;
  className?: string;
}) {
  const cfg = TONE[tone];
  const Icon = cfg.icon;
  const spinning = tone === "running" ? "animate-spin" : "";
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${cfg.bg} ${cfg.text} ${cfg.ring} ${cfg.emphasis ? "shadow-sm" : ""} ${className ?? ""}`}
    >
      <Icon aria-hidden className={`h-3.5 w-3.5 ${spinning}`} />
      <span>{label}</span>
      {timestamp ? (
        <>
          <span aria-hidden className="text-slate-400">·</span>
          <span className="font-normal text-slate-500">
            {formatRelative(timestamp)}
          </span>
        </>
      ) : null}
    </span>
  );
}

/** Map a scrape-run/scrape-health status string to a semantic tone. */
export function toneForScrapeStatus(
  s: string | null | undefined,
): StatusTone {
  if (!s) return "neutral";
  if (s === "ok" || s === "success") return "ok";
  if (s === "partial") return "warn";
  if (s === "failed" || s === "error") return "fail";
  if (s === "running") return "running";
  if (s === "pending") return "pending";
  if (s === "canceled") return "stale";
  return "neutral";
}

/** Map a dedup pair status to a semantic tone. */
export function toneForDedupStatus(
  s: "pending" | "merged" | "rejected",
): StatusTone {
  if (s === "pending") return "pending";
  if (s === "merged") return "ok";
  return "neutral";
}
