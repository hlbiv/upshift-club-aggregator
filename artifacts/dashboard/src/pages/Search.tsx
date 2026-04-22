import { useMemo } from "react";
import { Link, useSearchParams } from "react-router-dom";
// `useSearchParams` only used for reading; we never mutate the URL from
// inside row clicks anymore (league rows deep-link to /coverage/:id).
import { ExternalLink } from "lucide-react";
import {
  useListColleges,
  useListLeagues,
  useSearchClubs,
  useSearchCoaches,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";
import { SectionCard } from "../components/primitives/SectionCard";
import { EmptyState } from "../components/primitives/EmptyState";

/**
 * Full search results page, opened from the global header search ("see all"
 * link or Enter on an empty selection). Shows clubs / coaches / leagues
 * grouped — the same three domains the header dropdown queries.
 */
export default function SearchPage() {
  const [params] = useSearchParams();
  const q = (params.get("q") ?? "").trim();

  const enabled = q.length >= 2;
  // See note in `GlobalSearch.tsx` — we run the hook unconditionally with
  // an empty query rather than fight the generated UseQueryOptions type.
  const clubs = useSearchClubs({ q: enabled ? q : "" });
  const coaches = useSearchCoaches({
    name: enabled ? q : "",
    page: 1,
    page_size: 25,
  });
  const leagues = useListLeagues();
  const colleges = useListColleges({
    q: enabled ? q : "",
    page: 1,
    page_size: 25,
  });

  const matchedLeagues = useMemo(() => {
    if (!enabled) return [];
    const needle = q.toLowerCase();
    return (leagues.data?.leagues ?? [])
      .filter((l) => l.league_name?.toLowerCase().includes(needle))
      .slice(0, 25);
  }, [leagues.data, enabled, q]);

  const totalHits =
    (clubs.data?.results.length ?? 0) +
    (coaches.data?.coaches.length ?? 0) +
    (colleges.data?.colleges.length ?? 0) +
    matchedLeagues.length;

  return (
    <AppShell>
      <PageHeader
        eyebrow="Search"
        title={q ? `Results for “${q}”` : "Global search"}
        description={
          q
            ? `${totalHits.toLocaleString()} matches across clubs, coaches, and leagues.`
            : "Type a name in the header search to see grouped results."
        }
      />

      {!q ? (
        <EmptyState
          title="No search yet"
          description="Use the search bar at the top of the page to find a club, coach, or league."
        />
      ) : (
        <div className="space-y-4">
          <SectionCard
            title="Clubs"
            description={`${(clubs.data?.results.length ?? 0).toLocaleString()} matches`}
          >
            {clubs.isLoading ? (
              <RowsSkeleton />
            ) : (clubs.data?.results.length ?? 0) === 0 ? (
              <EmptyState title="No clubs matched." />
            ) : (
              <ul className="divide-y divide-slate-100">
                {(clubs.data?.results ?? []).map((c) => (
                  <li
                    key={c.id}
                    className="flex items-center justify-between gap-3 px-4 py-3 text-sm"
                  >
                    <div className="min-w-0">
                      <p className="truncate font-medium text-slate-900">
                        {c.club_name_canonical}
                      </p>
                      <p className="truncate text-xs text-slate-500">
                        {[c.city, c.state].filter(Boolean).join(", ") || "—"} ·
                        id #{c.id}
                      </p>
                    </div>
                    {c.website ? (
                      <a
                        href={c.website}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 text-xs font-medium text-indigo-600 hover:text-indigo-800"
                      >
                        Site
                        <ExternalLink className="h-3 w-3" />
                      </a>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </SectionCard>

          <SectionCard
            title="Coaches"
            description={`${(coaches.data?.coaches.length ?? 0).toLocaleString()} matches`}
          >
            {coaches.isLoading ? (
              <RowsSkeleton />
            ) : (coaches.data?.coaches.length ?? 0) === 0 ? (
              <EmptyState title="No coaches matched." />
            ) : (
              <ul className="divide-y divide-slate-100">
                {(coaches.data?.coaches ?? []).map((c) => (
                  <li
                    key={c.id}
                    className="flex items-center justify-between gap-3 px-4 py-3 text-sm"
                  >
                    <div className="min-w-0">
                      <p className="truncate font-medium text-slate-900">
                        {c.name}
                      </p>
                      <p className="truncate text-xs text-slate-500">
                        {c.title ?? "—"} · club #{c.club_id ?? "?"}
                      </p>
                    </div>
                    {c.confidence_score != null ? (
                      <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-600">
                        conf {c.confidence_score.toFixed(2)}
                      </span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </SectionCard>

          <SectionCard
            title="Colleges"
            description={`${(colleges.data?.colleges.length ?? 0).toLocaleString()} matches`}
          >
            {colleges.isLoading ? (
              <RowsSkeleton />
            ) : (colleges.data?.colleges.length ?? 0) === 0 ? (
              <EmptyState title="No colleges matched." />
            ) : (
              <ul className="divide-y divide-slate-100">
                {(colleges.data?.colleges ?? []).map((c) => (
                  <li
                    key={c.id}
                    className="flex items-center justify-between gap-3 px-4 py-3 text-sm"
                  >
                    <div className="min-w-0">
                      <p className="truncate font-medium text-slate-900">
                        {c.name}
                      </p>
                      <p className="truncate text-xs text-slate-500">
                        {[c.division, c.state, c.city]
                          .filter(Boolean)
                          .join(" · ") || "—"}
                      </p>
                    </div>
                    {c.conference ? (
                      <span className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-600">
                        {c.conference}
                      </span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </SectionCard>

          <SectionCard
            title="Leagues"
            description={`${matchedLeagues.length.toLocaleString()} matches`}
          >
            {leagues.isLoading ? (
              <RowsSkeleton />
            ) : matchedLeagues.length === 0 ? (
              <EmptyState title="No leagues matched." />
            ) : (
              <ul className="divide-y divide-slate-100">
                {matchedLeagues.map((l) => (
                  <li
                    key={l.id}
                    className="flex items-center justify-between gap-3 px-4 py-3 text-sm"
                  >
                    <div className="min-w-0">
                      <p className="truncate font-medium text-slate-900">
                        {l.league_name ?? "(unnamed)"}
                      </p>
                      <p className="truncate text-xs text-slate-500">
                        {l.tier_label ?? "—"} · {l.governing_body ?? "—"}
                      </p>
                    </div>
                    <Link
                      to={`/coverage/${l.id}`}
                      className="text-xs font-medium text-indigo-600 hover:text-indigo-800"
                    >
                      Coverage
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </SectionCard>
        </div>
      )}
    </AppShell>
  );
}

function RowsSkeleton() {
  return (
    <ul className="divide-y divide-slate-100">
      {[...Array(3)].map((_, i) => (
        <li key={i} className="flex items-center justify-between px-4 py-3">
          <div className="h-4 w-48 animate-pulse rounded bg-slate-100" />
          <div className="h-4 w-16 animate-pulse rounded bg-slate-100" />
        </li>
      ))}
    </ul>
  );
}
