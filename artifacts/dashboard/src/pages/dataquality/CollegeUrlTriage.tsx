import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useResolveCollegeUrl } from "@workspace/api-client-react";
import { AppShell } from "../../components/AppShell";
import { PageHeader } from "../../components/primitives/PageHeader";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../components/ui/table";

/**
 * `/data-quality/college-url-triage` — operator surface for colleges that
 * have open `url_needs_review` flags in `college_roster_quality_flags`.
 *
 * For each flagged college the operator can supply a `soccer_program_url`
 * and click "Resolve". The PATCH /api/v1/admin/colleges/:id/resolve-url
 * endpoint atomically:
 *   1. Sets `colleges.soccer_program_url` to the new URL.
 *   2. Resolves all open `url_needs_review` flags for that college.
 *
 * On success the row is removed from the list (optimistic via refetch).
 * No pagination — the max expected row count is ≤200 flagged colleges.
 */

// ---------------------------------------------------------------------------
// Types (inferred from the existing GET endpoint response shape).
// ---------------------------------------------------------------------------

interface CollegeFlagRow {
  id: number;
  collegeId: number;
  collegeName: string;
  academicYear: string;
  flagType: string;
  metadata: {
    reason?: string;
    [key: string]: unknown;
  };
  createdAt: string;
  resolvedAt: string | null;
  resolvedByEmail: string | null;
  resolutionNote: string | null;
}

interface CollegeFlagsResponse {
  items: CollegeFlagRow[];
  total: number;
  page: number;
  pageSize: number;
}

// ---------------------------------------------------------------------------
// Data fetching — no Orval hook for this endpoint yet; use raw fetch.
// ---------------------------------------------------------------------------

const COLLEGE_FLAGS_QUERY_KEY = ["college-roster-quality-flags", "url_needs_review"];

async function fetchOpenUrlFlags(): Promise<CollegeFlagsResponse> {
  const res = await fetch(
    "/api/v1/admin/data-quality/college-roster-quality-flags?flag_type=url_needs_review&resolved=false&page_size=200",
    { credentials: "include" },
  );
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status}${text ? `: ${text}` : ""}`);
  }
  return res.json() as Promise<CollegeFlagsResponse>;
}

// ---------------------------------------------------------------------------
// Panel component.
// ---------------------------------------------------------------------------

function CollegeUrlTriagePanel() {
  const queryClient = useQueryClient();
  const [urlInputs, setUrlInputs] = useState<Record<number, string>>({});
  const [toast, setToast] = useState<string | null>(null);

  const query = useQuery({
    queryKey: COLLEGE_FLAGS_QUERY_KEY,
    queryFn: fetchOpenUrlFlags,
  });

  const resolveMutation = useResolveCollegeUrl({
    mutation: {
      onSuccess: (_data, variables) => {
        // Clear the input for the resolved college.
        setUrlInputs((prev) => {
          const next = { ...prev };
          delete next[variables.id];
          return next;
        });
        void queryClient.invalidateQueries({ queryKey: COLLEGE_FLAGS_QUERY_KEY });
        setToast("URL set and flags resolved");
        window.setTimeout(() => setToast(null), 4000);
      },
    },
  });

  function handleResolve(collegeId: number) {
    const url = urlInputs[collegeId]?.trim() ?? "";
    if (!url) {
      setToast("Enter a URL first");
      window.setTimeout(() => setToast(null), 3000);
      return;
    }
    resolveMutation.mutate({
      id: collegeId,
      data: { url },
    });
  }

  const openFlags = query.data?.items ?? [];

  // Deduplicate by collegeId — show one row per college regardless of how
  // many flag rows there are (multiple academic_year flags collapse).
  const byCollege = new Map<
    number,
    { collegeName: string; flags: CollegeFlagRow[] }
  >();
  for (const f of openFlags) {
    const entry = byCollege.get(f.collegeId);
    if (entry) {
      entry.flags.push(f);
    } else {
      byCollege.set(f.collegeId, { collegeName: f.collegeName, flags: [f] });
    }
  }
  const colleges = Array.from(byCollege.entries());

  return (
    <section aria-labelledby="college-url-triage-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Colleges with open <code>url_needs_review</code> flags in{" "}
        <code>college_roster_quality_flags</code>. Supply a{" "}
        <code>soccer_program_url</code> and click <strong>Resolve</strong> to
        atomically set the URL and close all open flags for that college.
      </p>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed to load: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && (
        <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
          Loading…
        </div>
      )}

      {query.isSuccess && colleges.length === 0 && (
        <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
          No open url_needs_review flags. All colleges are resolved.
        </div>
      )}

      {query.isSuccess && colleges.length > 0 && (
        <>
          <p className="mb-2 text-sm text-neutral-500">
            {colleges.length.toLocaleString()} college
            {colleges.length === 1 ? "" : "s"} with open URL flags (
            {openFlags.length} flag{openFlags.length === 1 ? "" : "s"} total)
          </p>
          <div className="overflow-hidden rounded-lg border border-neutral-200">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>College</TableHead>
                  <TableHead>Reason(s)</TableHead>
                  <TableHead>Flagged</TableHead>
                  <TableHead className="min-w-[320px]">
                    soccer_program_url
                  </TableHead>
                  <TableHead>Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {colleges.map(([collegeId, { collegeName, flags }]) => {
                  const isPending =
                    resolveMutation.isPending &&
                    resolveMutation.variables?.id === collegeId;
                  const reasons = [
                    ...new Set(
                      flags.map(
                        (f) =>
                          (f.metadata.reason as string | undefined) ??
                          f.flagType,
                      ),
                    ),
                  ];
                  const oldestFlag = flags.reduce((a, b) =>
                    new Date(a.createdAt) < new Date(b.createdAt) ? a : b,
                  );

                  return (
                    <TableRow key={collegeId}>
                      <TableCell className="font-medium">
                        {collegeName}
                        <span className="ml-2 text-xs text-neutral-400">
                          #{collegeId}
                        </span>
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-wrap gap-1">
                          {reasons.map((r) => (
                            <span
                              key={r}
                              className="rounded bg-amber-100 px-2 py-0.5 font-mono text-xs text-amber-800"
                            >
                              {r}
                            </span>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs text-neutral-600">
                        {formatDate(oldestFlag.createdAt)}
                      </TableCell>
                      <TableCell>
                        <input
                          type="url"
                          placeholder="https://example.edu/sports/soccer"
                          value={urlInputs[collegeId] ?? ""}
                          onChange={(e) =>
                            setUrlInputs((prev) => ({
                              ...prev,
                              [collegeId]: e.target.value,
                            }))
                          }
                          disabled={isPending}
                          className="w-full rounded border border-neutral-300 px-2 py-1 text-sm disabled:cursor-not-allowed disabled:bg-neutral-50"
                          aria-label={`soccer_program_url for ${collegeName}`}
                        />
                      </TableCell>
                      <TableCell>
                        <button
                          type="button"
                          onClick={() => handleResolve(collegeId)}
                          disabled={isPending || !urlInputs[collegeId]?.trim()}
                          className="rounded bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:cursor-not-allowed disabled:bg-neutral-300"
                          aria-label={`Resolve flags for ${collegeName}`}
                        >
                          {isPending ? "Saving…" : "Resolve"}
                        </button>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        </>
      )}

      {toast && (
        <div
          role="status"
          className="fixed bottom-6 right-6 rounded-lg border border-green-200 bg-green-50 px-4 py-3 text-sm font-medium text-green-800 shadow-lg"
        >
          {toast}
        </div>
      )}
    </section>
  );
}

// ---------------------------------------------------------------------------
// Page wrapper.
// ---------------------------------------------------------------------------

export default function CollegeUrlTriage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="College URL triage"
        description="Colleges whose soccer program URL is missing or broken. Supply a URL and Resolve to set it and close all open flags in one atomic action."
      />
      <CollegeUrlTriagePanel />
    </AppShell>
  );
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function formatError(err: unknown): string {
  if (!err) return "Network error";
  if (err instanceof Error) {
    const status = (err as unknown as { status?: unknown }).status;
    if (typeof status === "number") return `HTTP ${status}`;
    return err.message;
  }
  return String(err);
}
