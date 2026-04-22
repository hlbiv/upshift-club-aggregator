import type { ReactNode } from "react";
import { Inbox, type LucideIcon } from "lucide-react";

export function EmptyState({
  title,
  description,
  icon: Icon = Inbox,
  action,
  tone = "neutral",
}: {
  title: ReactNode;
  description?: ReactNode;
  icon?: LucideIcon;
  action?: ReactNode;
  tone?: "neutral" | "ok" | "fail";
}) {
  const ringTone =
    tone === "ok"
      ? "ring-emerald-200 bg-emerald-50/40 text-emerald-700"
      : tone === "fail"
        ? "ring-red-200 bg-red-50/40 text-red-700"
        : "ring-slate-200 bg-slate-50/60 text-slate-500";
  return (
    <div
      className={`flex flex-col items-center justify-center rounded-xl border border-dashed px-6 py-10 text-center ring-1 ring-inset ${ringTone}`}
    >
      <Icon aria-hidden className="mb-3 h-6 w-6 opacity-70" />
      <p className="text-sm font-medium text-slate-900">{title}</p>
      {description ? (
        <p className="mt-1 max-w-md text-sm text-slate-500">{description}</p>
      ) : null}
      {action ? <div className="mt-4">{action}</div> : null}
    </div>
  );
}
