import type { ReactNode } from "react";

/**
 * Responsive grid wrapper for `<KpiCard>`s — used at the top of every
 * standardized page. Defaults to a 4-column grid at >= md, 2-col on sm,
 * 1-col on xs. Pass a different `cols` (typed) to override.
 */
export function KpiStrip({
  children,
  cols = 4,
  className,
}: {
  children: ReactNode;
  cols?: 2 | 3 | 4 | 5 | 6;
  className?: string;
}) {
  const map: Record<2 | 3 | 4 | 5 | 6, string> = {
    2: "sm:grid-cols-2",
    3: "sm:grid-cols-2 lg:grid-cols-3",
    4: "sm:grid-cols-2 lg:grid-cols-4",
    5: "sm:grid-cols-2 lg:grid-cols-5",
    6: "sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6",
  };
  return (
    <section
      aria-label="Key metrics"
      className={`mb-6 grid grid-cols-1 gap-3 ${map[cols]} ${className ?? ""}`}
    >
      {children}
    </section>
  );
}
