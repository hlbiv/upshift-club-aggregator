import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  useGetClub,
  useGetClubResults,
  useGetClubStaff,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";

/**
 * Club detail page — renders metadata, record (W/L/D), affiliations,
 * aliases, and top staff for a single canonical club.
 *
 *   GET /api/clubs/:id          → ClubDetailResponse (club + affiliations + aliases)
 *   GET /api/clubs/:id/results  → ClubResultsResponse
 *   GET /api/clubs/:id/staff    → ClubStaffResponse
 *
 * Uses the Orval-generated React Query hooks (same pattern as all other
 * pages post-Workstream-A migration).
 */

const TIER_LABELS: Record<string, string> = {
  recreational: "Recreational",
  recreational_plus: "Recreational+",
  competitive: "Competitive",
  elite: "Elite",
  academy: "Academy",
};

const TIER_COLORS: Record<string, string> = {
  recreational: "bg-slate-100 text-slate-700",
  recreational_plus: "bg-slate-100 text-slate-700",
  competitive: "bg-blue-100 text-blue-700",
  elite: "bg-purple-100 text-purple-700",
  academy: "bg-indigo-100 text-indigo-700",
};

function TierBadge({ tier }: { tier?: string | null }) {
  if (!tier) return null;
  const label = TIER_LABELS[tier] ?? tier;
  const color = TIER_COLORS[tier] ?? "bg-slate-100 text-slate-700";
  return (
    <span
      className={`inline-block rounded-full px-2.5 py-0.5 text-xs font-medium ${color}`}
    >
      {label}
    </span>
  );
}

function Placeholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

export default function ClubDetailPage() {
  const { id } = useParams<{ id: string }>();
  const [aliasesExpanded, setAliasesExpanded] = useState(false);

  const idIsValid = !!id && /^\d+$/.test(id);
  const numericId = idIsValid ? Number(id) : 0;

  const clubQuery = useGetClub(numericId);
  const resultsQuery = useGetClubResults(numericId);
  const staffQuery = useGetClubStaff(numericId);

  const club = clubQuery.data;

  // Treat 404-like errors as "not found"
  const isNotFound =
    !idIsValid ||
    (clubQuery.isError &&
      (clubQuery.error as { status?: number })?.status === 404);

  return (
    <AppShell>
      <PageHeader
        title={
          club
            ? club.club_name_canonical
            : idIsValid
              ? "Loading…"
              : "Club not found"
        }
        actions={
          <Link
            to="/search"
            className="rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-100"
          >
            Back to search
          </Link>
        }
      />

      {isNotFound ? (
        <Placeholder label="Club not found." />
      ) : clubQuery.isLoading ? (
        <Placeholder label="Loading…" />
      ) : clubQuery.isError ? (
        <Placeholder label="Failed to load club. Please try again." />
      ) : club ? (
        <div className="space-y-4">
          {/* Header metadata */}
          <div className="flex flex-wrap items-center gap-2 text-sm text-slate-500">
            <TierBadge tier={(club as unknown as Record<string, unknown>).competitive_tier as string | null | undefined} />
            <span>id #{club.id}</span>
            {club.city || club.state ? (
              <>
                <span className="text-slate-300">·</span>
                <span>
                  {[club.city, club.state].filter(Boolean).join(", ")}
                </span>
              </>
            ) : null}
            {club.website ? (
              <>
                <span className="text-slate-300">·</span>
                <a
                  href={club.website}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-indigo-600 hover:text-indigo-800 underline"
                >
                  Website
                </a>
              </>
            ) : null}
          </div>

          {/* Record card */}
          <section className="rounded-lg border border-neutral-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-neutral-700 uppercase tracking-wide">
              Match Record
            </h2>
            {resultsQuery.isLoading ? (
              <div className="h-6 w-48 animate-pulse rounded bg-slate-100" />
            ) : (resultsQuery.data?.results.length ?? 0) === 0 ? (
              <p className="text-sm text-neutral-500">No results linked yet.</p>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase tracking-wide">
                      <th className="pb-2 pr-4">Season</th>
                      <th className="pb-2 pr-4">League / Source</th>
                      <th className="pb-2 pr-4">Age</th>
                      <th className="pb-2 pr-4">Gender</th>
                      <th className="pb-2 pr-3 text-right">W</th>
                      <th className="pb-2 pr-3 text-right">L</th>
                      <th className="pb-2 text-right">D</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-50">
                    {(resultsQuery.data?.results ?? []).map((r) => (
                      <tr key={r.id} className="text-neutral-800">
                        <td className="py-2 pr-4 font-medium">{r.season}</td>
                        <td className="py-2 pr-4 text-neutral-500">
                          {r.league ?? "—"}
                          {r.division ? ` · ${r.division}` : ""}
                        </td>
                        <td className="py-2 pr-4">{r.age_group ?? "—"}</td>
                        <td className="py-2 pr-4">{r.gender ?? "—"}</td>
                        <td className="py-2 pr-3 text-right font-mono">{r.wins}</td>
                        <td className="py-2 pr-3 text-right font-mono">{r.losses}</td>
                        <td className="py-2 text-right font-mono">{r.draws}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>

          {/* Affiliations */}
          <section className="rounded-lg border border-neutral-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-neutral-700 uppercase tracking-wide">
              Affiliations
            </h2>
            {(club.affiliations?.length ?? 0) === 0 ? (
              <p className="text-sm text-neutral-500">No affiliations on record.</p>
            ) : (
              <ul className="divide-y divide-neutral-50">
                {(club.affiliations ?? []).map((a) => (
                  <li
                    key={a.id}
                    className="flex items-center justify-between py-2 text-sm"
                  >
                    <span className="font-medium text-neutral-900">
                      {a.source_name}
                    </span>
                    <span className="text-xs text-neutral-500">
                      {[a.season, a.gender_program, a.division_name]
                        .filter(Boolean)
                        .join(" · ") || "—"}
                    </span>
                  </li>
                ))}
              </ul>
            )}
          </section>

          {/* Aliases */}
          <section className="rounded-lg border border-neutral-200 bg-white p-4">
            <div className="mb-3 flex items-center justify-between">
              <h2 className="text-sm font-semibold text-neutral-700 uppercase tracking-wide">
                Known Aliases
              </h2>
              {(club.aliases?.length ?? 0) > 3 ? (
                <button
                  type="button"
                  onClick={() => setAliasesExpanded((v) => !v)}
                  className="text-xs text-indigo-600 hover:text-indigo-800"
                >
                  {aliasesExpanded
                    ? "Show fewer"
                    : `Show all ${club.aliases?.length ?? 0}`}
                </button>
              ) : null}
            </div>
            {(club.aliases?.length ?? 0) === 0 ? (
              <p className="text-sm text-neutral-500">No aliases on record.</p>
            ) : (
              <ul className="flex flex-wrap gap-1.5">
                {(aliasesExpanded
                  ? club.aliases
                  : (club.aliases ?? []).slice(0, 3)
                ).map((a) => (
                  <li
                    key={a.id}
                    className="rounded-full bg-slate-100 px-2.5 py-0.5 text-xs text-slate-700"
                  >
                    {a.alias_name}
                  </li>
                ))}
              </ul>
            )}
          </section>

          {/* Staff */}
          <section className="rounded-lg border border-neutral-200 bg-white p-4">
            <h2 className="mb-3 text-sm font-semibold text-neutral-700 uppercase tracking-wide">
              Staff
            </h2>
            {staffQuery.isLoading ? (
              <div className="h-6 w-48 animate-pulse rounded bg-slate-100" />
            ) : (staffQuery.data?.staff.length ?? 0) === 0 ? (
              <p className="text-sm text-neutral-500">No staff on record.</p>
            ) : (
              <ul className="divide-y divide-neutral-50">
                {(staffQuery.data?.staff ?? []).map((c) => (
                  <li
                    key={c.id}
                    className="flex items-center justify-between py-2 text-sm"
                  >
                    <div className="min-w-0">
                      <p className="truncate font-medium text-neutral-900">
                        {c.name}
                      </p>
                      <p className="truncate text-xs text-neutral-500">
                        {c.title ?? "—"}
                        {c.email ? ` · ${c.email}` : ""}
                      </p>
                    </div>
                    {c.confidence != null ? (
                      <span className="ml-4 rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] text-slate-600">
                        {Math.round(c.confidence * 100)}%
                      </span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </section>
        </div>
      ) : null}
    </AppShell>
  );
}
