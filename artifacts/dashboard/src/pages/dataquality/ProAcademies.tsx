import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  getGetProAcademiesQueryKey,
  useGetProAcademies,
  useUpdateProAcademy,
} from "@workspace/api-client-react";
import type {
  ProAcademyRow as ProAcademyRowDto,
  ProAcademiesResponse,
} from "@workspace/api-client-react";
import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import { Pager } from "../../components/primitives/Pager";

/**
 * `/data-quality/pro-academies` — operator surface for the
 * `canonical_clubs.is_pro_academy` allow-list.
 *
 * Replaces the curated TS constant in
 * `scripts/src/seed-pro-academies.ts` with an interactive view: every
 * canonical club with at least one tier-1 academy-family affiliation
 * (MLS NEXT / NWSL Academy / USL Academy) shows up here. Toggling the
 * row PATCHes the flag and re-runs that club's tier rollup, so
 * `competitive_tier` stays consistent without a backfill rerun.
 *
 * Pagination defaults to 50 rows; the filter chip at the top scopes to
 * "All", "Flagged" (currently `is_pro_academy = TRUE`), or "Unflagged"
 * (the borderline-candidate work queue — academy-family clubs not yet
 * on the allow-list).
 */
const PAGE_SIZE = 50;
type FlagFilter = "all" | "flagged" | "unflagged";

export default function ProAcademiesPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="Pro academies"
        description="Toggle is_pro_academy for clubs with a tier-1 academy-family affiliation. Saving re-runs the per-club tier rollup."
      />
      <ProAcademiesPanel />
    </AppShell>
  );
}

function ProAcademiesPanel() {
  const [flag, setFlag] = useState<FlagFilter>("all");
  const [page, setPage] = useState(1);

  const params = useMemo(
    () => ({ flag, page, page_size: PAGE_SIZE }),
    [flag, page],
  );
  const query = useGetProAcademies(params, {
    query: { queryKey: getGetProAcademiesQueryKey(params) },
  });

  const data = query.data;
  const rows = data?.rows ?? [];
  const total = data?.total ?? 0;
  const flaggedTotal = data?.flaggedTotal ?? 0;

  return (
    <section aria-labelledby="pro-academies-heading">
      <header className="mb-4 flex flex-wrap items-end justify-between gap-3">
        <p className="text-sm text-slate-600">
          <span className="font-semibold tabular-nums text-slate-900">
            {flaggedTotal.toLocaleString()}
          </span>{" "}
          clubs currently flagged as pro academies.{" "}
          <span className="text-slate-400">
            (Borderline candidates: clubs with a tier-1 academy-family
            affiliation but no flag — switch to <em>Unflagged</em>.)
          </span>
        </p>
        <FilterChips
          value={flag}
          onChange={(v) => {
            setFlag(v);
            setPage(1);
          }}
        />
      </header>

      {query.isError && (
        <ErrorBanner error={query.error} />
      )}
      {query.isLoading && <Placeholder>Loading…</Placeholder>}
      {query.isSuccess && rows.length === 0 && (
        <Placeholder>No clubs match the current filter.</Placeholder>
      )}
      {query.isSuccess && rows.length > 0 && (
        <>
          <ProAcademiesTable rows={rows} listParams={params} />
          <div className="mt-3">
            <Pager
              page={page}
              pageSize={PAGE_SIZE}
              total={total}
              onPage={setPage}
            />
          </div>
        </>
      )}
    </section>
  );
}

function FilterChips({
  value,
  onChange,
}: {
  value: FlagFilter;
  onChange: (v: FlagFilter) => void;
}) {
  const opts: Array<{ id: FlagFilter; label: string }> = [
    { id: "all", label: "All" },
    { id: "flagged", label: "Flagged" },
    { id: "unflagged", label: "Unflagged" },
  ];
  return (
    <div role="tablist" aria-label="Filter" className="inline-flex rounded-md border border-slate-200 bg-white p-0.5 text-sm">
      {opts.map((o) => (
        <button
          key={o.id}
          role="tab"
          aria-selected={value === o.id}
          onClick={() => onChange(o.id)}
          className={
            value === o.id
              ? "rounded bg-indigo-600 px-3 py-1 font-medium text-white"
              : "rounded px-3 py-1 text-slate-700 hover:bg-slate-100"
          }
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

function ProAcademiesTable({
  rows,
  listParams,
}: {
  rows: ProAcademyRowDto[];
  listParams: { flag: FlagFilter; page: number; page_size: number };
}) {
  return (
    <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
      <table className="w-full text-left text-sm">
        <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
          <tr>
            <th className="px-3 py-2 font-medium">Club</th>
            <th className="px-3 py-2 font-medium">Location</th>
            <th className="px-3 py-2 font-medium">Academy families</th>
            <th className="px-3 py-2 font-medium">Tier</th>
            <th className="px-3 py-2 font-medium">Pro academy</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {rows.map((row) => (
            <ProAcademyTableRow
              key={row.clubId}
              row={row}
              listParams={listParams}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ProAcademyTableRow({
  row,
  listParams,
}: {
  row: ProAcademyRowDto;
  listParams: { flag: FlagFilter; page: number; page_size: number };
}) {
  const queryClient = useQueryClient();
  // Local optimistic copy so the toggle flips immediately. We reconcile
  // from the response (or the eventual list refetch) afterwards.
  const [pendingFlag, setPendingFlag] = useState<boolean | null>(null);
  const [lastError, setLastError] = useState<string | null>(null);
  const [transitionNote, setTransitionNote] = useState<string | null>(null);

  const mutation = useUpdateProAcademy({
    mutation: {
      onSuccess: (resp) => {
        setPendingFlag(null);
        if (resp.previousCompetitiveTier !== resp.competitiveTier) {
          setTransitionNote(
            `Tier ${resp.previousCompetitiveTier} → ${resp.competitiveTier}`,
          );
        } else {
          setTransitionNote(null);
        }
        // Patch the cached list row in place so the table updates without
        // a full refetch (and the filter doesn't shuffle results out from
        // under the operator).
        queryClient.setQueryData<ProAcademiesResponse>(
          getGetProAcademiesQueryKey(listParams),
          (prev) => {
            if (!prev) return prev;
            return {
              ...prev,
              rows: prev.rows.map((r) =>
                r.clubId === row.clubId
                  ? {
                      ...r,
                      isProAcademy: resp.isProAcademy,
                      competitiveTier: resp.competitiveTier,
                    }
                  : r,
              ),
            };
          },
        );
        // Invalidate any other cached pages of the same resource so they
        // re-fetch on next mount (operators often switch filters).
        queryClient.invalidateQueries({
          queryKey: getGetProAcademiesQueryKey().slice(0, 1),
          refetchType: "none",
        });
      },
      onError: (err) => {
        setPendingFlag(null);
        setLastError(formatError(err));
      },
    },
  });

  const displayedFlag = pendingFlag ?? row.isProAcademy;

  function onToggle(next: boolean) {
    setLastError(null);
    setTransitionNote(null);
    setPendingFlag(next);
    mutation.mutate({
      clubId: row.clubId,
      data: { isProAcademy: next },
    });
  }

  return (
    <tr className={mutation.isPending ? "opacity-70" : undefined}>
      <td className="px-3 py-2">
        <div className="font-medium text-slate-900">
          {row.clubNameCanonical}
        </div>
        <div className="text-xs text-slate-400">#{row.clubId}</div>
      </td>
      <td className="px-3 py-2 text-slate-700">
        {[row.city, row.state].filter(Boolean).join(", ") || (
          <span className="text-slate-400">—</span>
        )}
      </td>
      <td className="px-3 py-2">
        <div className="flex flex-wrap gap-1">
          {row.families.map((f) => (
            <span
              key={f}
              className="rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700"
            >
              {f}
            </span>
          ))}
          {row.affiliationCount > row.families.length && (
            <span className="ml-1 text-xs text-slate-400">
              ({row.affiliationCount} affs)
            </span>
          )}
        </div>
      </td>
      <td className="px-3 py-2">
        <TierBadge tier={row.competitiveTier} />
        {transitionNote && (
          <div className="mt-1 text-[11px] text-emerald-700">
            {transitionNote}
          </div>
        )}
      </td>
      <td className="px-3 py-2">
        <label className="inline-flex cursor-pointer items-center gap-2">
          <input
            type="checkbox"
            className="h-4 w-4"
            checked={displayedFlag}
            disabled={mutation.isPending}
            onChange={(e) => onToggle(e.target.checked)}
          />
          <span className="text-xs text-slate-500">
            {mutation.isPending
              ? "Saving…"
              : displayedFlag
                ? "Pro academy"
                : "Not flagged"}
          </span>
        </label>
        {lastError && (
          <div role="alert" className="mt-1 text-[11px] text-red-700">
            {lastError}
          </div>
        )}
      </td>
    </tr>
  );
}

function TierBadge({ tier }: { tier: ProAcademyRowDto["competitiveTier"] }) {
  const cls =
    tier === "academy"
      ? "bg-violet-100 text-violet-800"
      : tier === "elite"
        ? "bg-indigo-100 text-indigo-800"
        : tier === "competitive"
          ? "bg-slate-100 text-slate-700"
          : "bg-slate-50 text-slate-500";
  return (
    <span
      className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${cls}`}
    >
      {tier}
    </span>
  );
}

function Placeholder({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-lg border border-dashed border-slate-200 bg-white p-8 text-center text-sm text-slate-500">
      {children}
    </div>
  );
}

function ErrorBanner({ error }: { error: unknown }) {
  return (
    <div
      role="alert"
      className="mb-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
    >
      Failed: {formatError(error)}
    </div>
  );
}

function formatError(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  return "Unknown error";
}
