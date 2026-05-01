import { NavLink } from "react-router-dom";
import {
  AlertOctagon,
  Activity,
  BarChart3,
  Database,
  Filter,
  GitMerge,
  Home,
  Layers,
  LineChart,
  Link,
  ListChecks,
  Map,
  Settings2,
  ShieldAlert,
  Swords,
  Trophy,
  Workflow,
  type LucideIcon,
} from "lucide-react";
import {
  useEmptyStaffCount,
  useOpenNavLeakedCount,
  useOpenNumericOnlyCount,
  usePendingDedupCount,
  useStaleScrapesCount,
} from "../lib/queueCounts";

/**
 * Persistent left sidebar. Replaces the old top text-link `AdminNav`.
 *
 * Sections — Overview / Operations / Data / Review — group related work
 * the way an operator thinks about it. Review items show a live pending
 * count pulled from each queue's list endpoint (1-row pages so we get
 * `total` cheaply).
 *
 * Active route detection uses NavLink with `end` only on the root so
 * sub-routes light up the parent (`/data-quality/nav-leaked` ⇒ "Nav-leaked
 * names" active).
 */

interface NavItem {
  label: string;
  to: string;
  icon: LucideIcon;
  end?: boolean;
  badge?: number | null;
  badgeTone?: "amber" | "red" | "slate";
}

interface NavGroup {
  heading: string;
  items: NavItem[];
}

export default function Sidebar() {
  const dedupCount = usePendingDedupCount();
  const navLeakedCount = useOpenNavLeakedCount();
  const numericOnlyCount = useOpenNumericOnlyCount();
  const emptyStaffCount = useEmptyStaffCount();
  const staleCount = useStaleScrapesCount();

  const groups: NavGroup[] = [
    {
      heading: "Overview",
      items: [{ label: "Home", to: "/", icon: Home, end: true }],
    },
    {
      heading: "Operations",
      items: [
        { label: "Scheduler", to: "/scheduler", icon: Workflow },
        { label: "Scraper health", to: "/scraper-health", icon: Activity },
      ],
    },
    {
      heading: "Data",
      items: [
        { label: "Coverage", to: "/coverage", icon: Map },
        { label: "Growth", to: "/growth", icon: LineChart },
        { label: "Matches", to: "/matches", icon: Swords },
      ],
    },
    {
      heading: "Review",
      items: [
        {
          label: "Dedup",
          to: "/dedup",
          icon: GitMerge,
          badge: dedupCount,
          badgeTone: "amber",
        },
        {
          label: "Nav-leaked names",
          to: "/data-quality/nav-leaked",
          icon: ShieldAlert,
          badge: navLeakedCount,
          badgeTone: "amber",
        },
        {
          label: "Numeric-only names",
          to: "/data-quality/numeric-only",
          icon: Filter,
          badge: numericOnlyCount,
          badgeTone: "amber",
        },
        {
          label: "Empty staff pages",
          to: "/data-quality/empty-staff",
          icon: ListChecks,
          badge: emptyStaffCount,
          badgeTone: "slate",
        },
        {
          label: "Stale scrapes",
          to: "/data-quality/stale-scrapes",
          icon: AlertOctagon,
          badge: staleCount,
          badgeTone: "slate",
        },
        {
          label: "Coach misses",
          to: "/data-quality/coach-misses",
          icon: AlertOctagon,
        },
        {
          label: "College URL triage",
          to: "/data-quality/college-url-triage",
          icon: Link,
        },
        {
          label: "College roster quality",
          to: "/data-quality/college-roster",
          icon: Layers,
        },
        {
          label: "Pro academies",
          to: "/data-quality/pro-academies",
          icon: Trophy,
        },
        {
          label: "GA Premier orphans",
          to: "/data-quality/ga-premier",
          icon: Database,
        },
      ],
    },
  ];

  return (
    <aside
      aria-label="Primary navigation"
      className="hidden w-60 shrink-0 flex-col border-r border-slate-200 bg-white md:flex"
    >
      <div className="flex h-14 items-center gap-2 border-b border-slate-200 px-4">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-indigo-500 to-violet-600 text-white shadow-sm">
          <Layers aria-hidden className="h-4.5 w-4.5" />
        </div>
        <div className="leading-tight">
          <p className="text-sm font-semibold text-slate-900">Upshift Data</p>
          <p className="text-[11px] text-slate-500">Admin console</p>
        </div>
      </div>

      <nav className="scrollbar-thin flex-1 overflow-y-auto px-2 py-4">
        {groups.map((group) => (
          <div key={group.heading} className="mb-5">
            <p className="mb-1.5 px-2 text-[11px] font-semibold uppercase tracking-wider text-slate-400">
              {group.heading}
            </p>
            <ul className="space-y-0.5">
              {group.items.map((item) => (
                <li key={item.to}>
                  <NavLinkItem item={item} />
                </li>
              ))}
            </ul>
          </div>
        ))}
      </nav>

      <div className="border-t border-slate-200 px-4 py-3 text-[11px] text-slate-400">
        <div className="flex items-center gap-1.5">
          <BarChart3 aria-hidden className="h-3 w-3" />
          <span>Internal admin · v2</span>
        </div>
        <div className="mt-1 flex items-center gap-1.5">
          <Settings2 aria-hidden className="h-3 w-3" />
          <span>Press “?” for shortcuts</span>
        </div>
      </div>
    </aside>
  );
}

function NavLinkItem({ item }: { item: NavItem }) {
  const Icon = item.icon;
  return (
    <NavLink
      to={item.to}
      end={item.end}
      className={({ isActive }) =>
        `group flex items-center justify-between rounded-md px-2 py-1.5 text-sm transition-colors ${
          isActive
            ? "bg-indigo-50 font-medium text-indigo-700"
            : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
        }`
      }
    >
      {({ isActive }) => (
        <>
          <span className="flex items-center gap-2">
            <Icon
              aria-hidden
              className={`h-4 w-4 ${isActive ? "text-indigo-600" : "text-slate-400 group-hover:text-slate-600"}`}
            />
            <span className="truncate">{item.label}</span>
          </span>
          {item.badge !== undefined && item.badge !== null && item.badge > 0 ? (
            <NavBadge value={item.badge} tone={item.badgeTone ?? "slate"} />
          ) : null}
        </>
      )}
    </NavLink>
  );
}

function NavBadge({
  value,
  tone,
}: {
  value: number;
  tone: "amber" | "red" | "slate";
}) {
  const cls =
    tone === "red"
      ? "bg-red-100 text-red-700"
      : tone === "amber"
        ? "bg-amber-100 text-amber-800"
        : "bg-slate-200 text-slate-700";
  return (
    <span
      className={`ml-2 inline-flex min-w-[1.25rem] justify-center rounded-full px-1.5 py-0.5 text-[10px] font-semibold tabular-nums ${cls}`}
    >
      {value > 999 ? "999+" : value}
    </span>
  );
}
