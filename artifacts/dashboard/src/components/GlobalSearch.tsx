import { useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Search, X } from "lucide-react";
import {
  getListCollegesQueryKey,
  getListLeaguesQueryKey,
  getSearchClubsQueryKey,
  getSearchCoachesQueryKey,
  useListColleges,
  useListLeagues,
  useSearchClubs,
  useSearchCoaches,
  type College,
} from "@workspace/api-client-react";

/**
 * Global search in the top header. Searches three domains in parallel —
 * clubs, coaches, leagues — and renders a grouped Combobox-style dropdown.
 *
 * Keyboard model:
 *   - "/" or "Cmd/Ctrl+K"  → focus search input
 *   - ArrowDown / ArrowUp  → move highlight across flat result list
 *   - Enter                → activate highlighted item (or "see all" if none)
 *   - Escape               → clear and blur
 *
 * Selection navigates to a `/search?q=…` results page (no per-club detail
 * page exists in the admin yet); the items themselves carry external link
 * affordances when applicable so operators can jump straight to a club's
 * website.
 *
 * Colleges are queried via the generated `useListColleges` hook (added to
 * the OpenAPI spec alongside this overhaul) so the four domains share one
 * cache + auth surface.
 */
export default function GlobalSearch() {
  const navigate = useNavigate();
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const [highlight, setHighlight] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Debounced query — useSearchClubs/etc would refetch on every keystroke.
  const [debounced, setDebounced] = useState("");
  useEffect(() => {
    const t = window.setTimeout(() => setDebounced(q.trim()), 200);
    return () => window.clearTimeout(t);
  }, [q]);

  const enabled = debounced.length >= 2;

  // Gate every search hook on `enabled` so an empty / too-short query never
  // hits the network. Without this, mounting the shell on any route would
  // fire four list endpoints just to render the header. Each call passes
  // its `queryKey` explicitly via the matching `get…QueryKey` helper to
  // satisfy v5's `UseQueryOptions` shape (which types `queryKey` as
  // required) without redeclaring the cache shape inline.
  const clubsParams = { q: debounced };
  const clubs = useSearchClubs(clubsParams, {
    query: { queryKey: getSearchClubsQueryKey(clubsParams), enabled },
  });
  const coachParams = { name: debounced, page: 1, page_size: 5 };
  const coaches = useSearchCoaches(coachParams, {
    query: { queryKey: getSearchCoachesQueryKey(coachParams), enabled },
  });
  const leagues = useListLeagues({
    query: { queryKey: getListLeaguesQueryKey(), enabled },
  });
  const collegeParams = { q: debounced, page: 1, page_size: 10 };
  const colleges = useListColleges(collegeParams, {
    query: { queryKey: getListCollegesQueryKey(collegeParams), enabled },
  });

  const collegeResults: College[] = useMemo(
    () => (colleges.data?.colleges ?? []).slice(0, 5),
    [colleges.data],
  );

  const leagueResults = useMemo(() => {
    if (!enabled) return [];
    const needle = debounced.toLowerCase();
    return (leagues.data?.leagues ?? [])
      .filter((l) => l.league_name?.toLowerCase().includes(needle))
      .slice(0, 5);
  }, [leagues.data, debounced, enabled]);

  const clubResults = useMemo(
    () => (clubs.data?.results ?? []).slice(0, 6),
    [clubs.data],
  );
  const coachResults = useMemo(
    () => (coaches.data?.coaches ?? []).slice(0, 5),
    [coaches.data],
  );

  type Hit =
    | { kind: "club"; id: number; label: string; sub?: string; href?: string }
    | { kind: "coach"; id: number; label: string; sub?: string }
    | { kind: "college"; id: number; label: string; sub?: string }
    | { kind: "league"; id: number; label: string; sub?: string };

  const flat: Hit[] = useMemo(() => {
    const out: Hit[] = [];
    for (const c of clubResults) {
      const sub = [c.city, c.state].filter(Boolean).join(", ");
      out.push({
        kind: "club",
        id: c.id,
        label: c.club_name_canonical,
        sub: sub || undefined,
        href: c.website ?? undefined,
      });
    }
    for (const c of coachResults) {
      out.push({
        kind: "coach",
        id: c.id,
        label: c.name,
        sub: c.title ?? undefined,
      });
    }
    for (const c of collegeResults) {
      out.push({
        kind: "college",
        id: c.id,
        label: c.name,
        sub: [c.division, c.state].filter(Boolean).join(" · ") || undefined,
      });
    }
    for (const l of leagueResults) {
      out.push({
        kind: "league",
        id: l.id,
        label: l.league_name ?? "(unnamed)",
        sub: l.tier_label ?? undefined,
      });
    }
    return out;
  }, [clubResults, coachResults, collegeResults, leagueResults]);

  // Reset highlight when results change.
  useEffect(() => {
    setHighlight(0);
  }, [debounced]);

  // Global keyboard hot-keys to focus the input.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const target = e.target as HTMLElement | null;
      const inEditable =
        target &&
        (target.tagName === "INPUT" ||
          target.tagName === "TEXTAREA" ||
          target.isContentEditable);
      if (
        ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") ||
        (!inEditable && e.key === "/")
      ) {
        e.preventDefault();
        inputRef.current?.focus();
      }
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  // Click-away closes dropdown.
  useEffect(() => {
    function onClick(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setOpen(false);
      }
    }
    window.addEventListener("mousedown", onClick);
    return () => window.removeEventListener("mousedown", onClick);
  }, []);

  function activate(hit: Hit | undefined) {
    if (!hit) {
      navigate(`/search?q=${encodeURIComponent(debounced)}`);
      setOpen(false);
      return;
    }
    // Entity-aware deep links:
    //   - leagues  → admin coverage drilldown for that league
    //   - clubs    → open the club's external website if we have one,
    //                AND fall through to the global search results page
    //                (no admin /clubs/:id route exists yet)
    //   - coaches/colleges → /search results scoped to the entity name
    //                        (no admin detail route for either domain)
    setOpen(false);
    if (hit.kind === "league") {
      navigate(`/coverage/${hit.id}`);
      return;
    }
    if (hit.kind === "club" && hit.href) {
      window.open(hit.href, "_blank", "noopener");
    }
    navigate(`/search?q=${encodeURIComponent(hit.label)}`);
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlight((h) => Math.min(flat.length - 1, h + 1));
      setOpen(true);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlight((h) => Math.max(0, h - 1));
    } else if (e.key === "Enter") {
      e.preventDefault();
      activate(flat[highlight]);
    } else if (e.key === "Escape") {
      e.preventDefault();
      setQ("");
      setOpen(false);
      inputRef.current?.blur();
    }
  }

  return (
    <div ref={containerRef} className="relative w-full max-w-md">
      <div className="relative">
        <Search
          aria-hidden
          className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400"
        />
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => {
            setQ(e.target.value);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          onKeyDown={onKeyDown}
          placeholder="Search clubs, coaches, leagues…"
          aria-label="Global search"
          className="w-full rounded-lg border border-slate-200 bg-white py-1.5 pl-8 pr-16 text-sm text-slate-900 shadow-xs placeholder:text-slate-400 focus:border-indigo-400 focus:outline-none focus:ring-2 focus:ring-indigo-100"
        />
        {q ? (
          <button
            type="button"
            onClick={() => {
              setQ("");
              setOpen(false);
              inputRef.current?.focus();
            }}
            aria-label="Clear search"
            className="absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-700"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        ) : (
          <kbd className="absolute right-2 top-1/2 hidden -translate-y-1/2 rounded border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-[10px] font-mono text-slate-500 sm:inline">
            /
          </kbd>
        )}
      </div>

      {open && enabled ? (
        <div
          role="listbox"
          aria-label="Search results"
          className="absolute left-0 right-0 z-30 mt-1 max-h-[28rem] overflow-y-auto rounded-lg border border-slate-200 bg-white shadow-lg"
        >
          {flat.length === 0 ? (
            <div className="p-4 text-sm text-slate-500">
              {clubs.isFetching || coaches.isFetching
                ? "Searching…"
                : `No matches for “${debounced}”.`}
            </div>
          ) : (
            <>
              <ResultGroup
                heading="Clubs"
                items={clubResults.map((c, i) => ({
                  key: `c-${c.id}`,
                  index: flatIndexFor(flat, "club", c.id),
                  primary: c.club_name_canonical,
                  secondary: [c.city, c.state]
                    .filter(Boolean)
                    .join(", "),
                  badge: c.website_status ?? null,
                  i,
                }))}
                highlight={highlight}
                onPick={(idx) => activate(flat[idx])}
              />
              <ResultGroup
                heading="Coaches"
                items={coachResults.map((c, i) => ({
                  key: `co-${c.id}`,
                  index: flatIndexFor(flat, "coach", c.id),
                  primary: c.name,
                  secondary: c.title ?? "",
                  badge: c.confidence_score
                    ? `conf ${c.confidence_score.toFixed(2)}`
                    : null,
                  i,
                }))}
                highlight={highlight}
                onPick={(idx) => activate(flat[idx])}
              />
              <ResultGroup
                heading="Colleges"
                items={collegeResults.map((c, i) => ({
                  key: `cl-${c.id}`,
                  index: flatIndexFor(flat, "college", c.id),
                  primary: c.name,
                  secondary: [c.division, c.state]
                    .filter(Boolean)
                    .join(" · "),
                  badge: c.conference ?? null,
                  i,
                }))}
                highlight={highlight}
                onPick={(idx) => activate(flat[idx])}
              />
              <ResultGroup
                heading="Leagues"
                items={leagueResults.map((l, i) => ({
                  key: `l-${l.id}`,
                  index: flatIndexFor(flat, "league", l.id),
                  primary: l.league_name ?? "(unnamed)",
                  secondary: l.tier_label ?? "",
                  badge: l.governing_body ?? null,
                  i,
                }))}
                highlight={highlight}
                onPick={(idx) => activate(flat[idx])}
              />
              <button
                type="button"
                onClick={() => activate(undefined)}
                className="block w-full border-t border-slate-100 px-3 py-2 text-left text-xs font-medium text-indigo-600 hover:bg-indigo-50"
              >
                See all results for “{debounced}” →
              </button>
            </>
          )}
        </div>
      ) : null}
    </div>
  );
}

function flatIndexFor(
  flat: { kind: string; id: number }[],
  kind: string,
  id: number,
): number {
  return flat.findIndex((h) => h.kind === kind && h.id === id);
}

function ResultGroup({
  heading,
  items,
  highlight,
  onPick,
}: {
  heading: string;
  items: {
    key: string;
    index: number;
    primary: string;
    secondary?: string;
    badge?: string | null;
  }[];
  highlight: number;
  onPick: (flatIndex: number) => void;
}) {
  if (items.length === 0) return null;
  return (
    <div className="border-b border-slate-100 last:border-b-0">
      <p className="px-3 pb-1 pt-2 text-[10px] font-semibold uppercase tracking-wide text-slate-400">
        {heading}
      </p>
      <ul>
        {items.map((it) => {
          const active = it.index === highlight;
          return (
            <li key={it.key}>
              <button
                type="button"
                onClick={() => onPick(it.index)}
                role="option"
                aria-selected={active}
                className={`flex w-full items-center justify-between gap-3 px-3 py-1.5 text-left text-sm ${active ? "bg-indigo-50 text-indigo-900" : "text-slate-800 hover:bg-slate-50"}`}
              >
                <span className="min-w-0 truncate">
                  <span className="font-medium">{it.primary}</span>
                  {it.secondary ? (
                    <span className="ml-2 text-xs text-slate-500">
                      {it.secondary}
                    </span>
                  ) : null}
                </span>
                {it.badge ? (
                  <span className="ml-2 shrink-0 rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-mono text-slate-600">
                    {it.badge}
                  </span>
                ) : null}
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
