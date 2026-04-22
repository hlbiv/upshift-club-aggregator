import type { ReactNode } from "react";

/**
 * Standard page header: title / description on the left, optional primary
 * action and breadcrumbs on the right or above. Every page uses this so the
 * shell looks the same across Overview, Coverage, Scheduler, etc.
 */
export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
  breadcrumbs,
}: {
  eyebrow?: string;
  title: ReactNode;
  description?: ReactNode;
  actions?: ReactNode;
  breadcrumbs?: ReactNode;
}) {
  return (
    <header className="mb-6">
      {breadcrumbs ? (
        <nav
          aria-label="Breadcrumb"
          className="mb-2 text-xs text-slate-500"
        >
          {breadcrumbs}
        </nav>
      ) : null}
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          {eyebrow ? (
            <p className="mb-1 text-xs font-semibold uppercase tracking-wide text-indigo-600">
              {eyebrow}
            </p>
          ) : null}
          <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
            {title}
          </h1>
          {description ? (
            <p className="mt-1 max-w-3xl text-sm text-slate-500">
              {description}
            </p>
          ) : null}
        </div>
        {actions ? (
          <div className="flex shrink-0 items-center gap-2">{actions}</div>
        ) : null}
      </div>
    </header>
  );
}
