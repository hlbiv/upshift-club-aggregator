import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  getGetCollegeRosterQualityFlagsQueryKey,
  useGetCollegeRosterQualityFlags,
  useResolveCollegeRosterQualityFlagWithUrl,
  type CollegeRosterQualityFlagItem,
  type CollegeRosterQualityFlagsResponse,
} from "@workspace/api-client-react";
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

const PAGE_SIZE = 50;

type ResolvedFilter = "all" | "open" | "resolved";

/**
 * `/data-quality/college-roster-quality` — admin panel for
 * `college_roster_quality_flags`. Shows unresolved URL gaps and lets
 * operators supply a corrected `soccer_program_url` inline.
 *
 * "Resolve with URL" PATCH writes to `colleges.soccer_program_url` and
 * marks the flag resolved in one transaction.
 */
export default function CollegeRosterQualityPage() {
  return (
    <AppShell>
      <PageHeader
        eyebrow="Data quality"
        title="College roster quality flags"
        description="URL gaps and parser issues flagged during NCAA roster scrapes. Supply a corrected URL to fix the college record and close the flag in one step."
      />
      <CollegeRosterQualityPanel />
    </AppShell>
  );
}

function CollegeRosterQualityPanel() {
  const [flagType, setFlagType] = useState<string>("");
  const [resolvedFilter, setResolvedFilter] = useState<ResolvedFilter>("open");
  const [page, setPage] = useState(1);

  const resolvedParam: boolean | undefined =
    resolvedFilter === "open"
      ? false
      : resolvedFilter === "resolved"
        ? true
        : undefined;

  const query = useGetCollegeRosterQualityFlags({
    flag_type: flagType === "" ? undefined : (flagType as CollegeRosterQualityFlagItem["flagType"]),
    resolved: resolvedParam,
    page,
    page_size: PAGE_SIZE,
  });

  function onChangeFilter() {
    setPage(1);
  }

  return (
    <section aria-labelledby="college-quality-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Flags written by the NCAA roster scraper when a college's{" "}
        <code>soccer_program_url</code> is missing, broken, or returned too few
        players. Use <strong>Resolve with URL</strong> to write the corrected URL
        and close the flag in one step.
      </p>

      {/* Filters */}
      <form
        onSubmit={(e) => e.preventDefault()}
        className="mb-6 flex flex-wrap items-end gap-4 rounded-lg border border-neutral-200 bg-white p-4"
      >
        <label className="flex flex-col gap-1 text-sm text-neutral-800">
          <span className="font-medium">Flag type</span>
          <select
            value={flagType}
            onChange={(e) => {
              setFlagType(e.target.value);
              onChangeFilter();
            }}
            className="w-52 rounded border border-neutral-300 px-2 py-1"
          >
            <option value="">All types</option>
            <option value="url_needs_review">url_needs_review</option>
            <option value="partial_parse">partial_parse</option>
            <option value="historical_no_data">historical_no_data</option>
          </select>
        </label>

        <fieldset
          className="flex items-center gap-4 text-sm text-neutral-800"
          aria-label="Resolved filter"
        >
          <legend className="mb-1 block text-xs font-medium text-neutral-600">
            Show
          </legend>
          {(["open", "resolved", "all"] as const).map((v) => (
            <label key={v} className="flex items-center gap-2">
              <input
                type="radio"
                name="resolved-filter"
                value={v}
                checked={resolvedFilter === v}
                onChange={() => {
                  setResolvedFilter(v);
                  onChangeFilter();
                }}
                className="h-4 w-4"
              />
              {v === "open" ? "Open" : v === "resolved" ? "Resolved" : "All"}
            </label>
          ))}
        </fieldset>
      </form>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed to load flags: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && <TablePlaceholder label="Loading…" />}

      {query.isSuccess && query.data.items.length === 0 && (
        <TablePlaceholder
          label={
            resolvedFilter === "open"
              ? "No open flags."
              : "No flags match the current filter."
          }
        />
      )}

      {query.isSuccess && query.data.items.length > 0 && (
        <CollegeQualityFlagsTable
          data={query.data}
          onPage={(p) => setPage(p)}
        />
      )}
    </section>
  );
}

function CollegeQualityFlagsTable({
  data,
  onPage,
}: {
  data: CollegeRosterQualityFlagsResponse;
  onPage: (p: number) => void;
}) {
  const queryClient = useQueryClient();

  const resolveWithUrl = useResolveCollegeRosterQualityFlagWithUrl({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: getGetCollegeRosterQualityFlagsQueryKey().slice(0, 1),
        });
      },
    },
  });

  return (
    <>
      <p className="mb-2 text-sm text-neutral-500">
        {data.total.toLocaleString()} flags
      </p>
      <div className="overflow-hidden rounded-lg border border-neutral-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>College</TableHead>
              <TableHead>Year</TableHead>
              <TableHead>Flag type</TableHead>
              <TableHead>Reason</TableHead>
              <TableHead>Flagged</TableHead>
              <TableHead>Status</TableHead>
              <TableHead className="min-w-[280px]">Resolve with URL</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.items.map((flag) => (
              <FlagRow
                key={flag.id}
                flag={flag}
                isPending={
                  resolveWithUrl.isPending &&
                  resolveWithUrl.variables?.id === flag.id
                }
                error={
                  resolveWithUrl.isError &&
                  resolveWithUrl.variables?.id === flag.id
                    ? formatError(resolveWithUrl.error)
                    : null
                }
                onResolve={(newUrl) =>
                  resolveWithUrl.mutate({
                    id: flag.id,
                    data: { new_soccer_program_url: newUrl },
                  })
                }
              />
            ))}
          </TableBody>
        </Table>
      </div>
      <Pager
        page={data.page}
        pageSize={data.pageSize}
        total={data.total}
        onPage={onPage}
      />
    </>
  );
}

function FlagRow({
  flag,
  isPending,
  error,
  onResolve,
}: {
  flag: CollegeRosterQualityFlagItem;
  isPending: boolean;
  error: string | null;
  onResolve: (url: string) => void;
}) {
  const [urlInput, setUrlInput] = useState("");

  const isResolved = flag.resolvedAt !== null;
  const reason =
    typeof flag.metadata === "object" &&
    flag.metadata !== null &&
    "reason" in flag.metadata
      ? String((flag.metadata as Record<string, unknown>).reason)
      : null;

  return (
    <TableRow
      className={isResolved ? "opacity-50" : undefined}
    >
      <TableCell className="font-medium">
        {flag.collegeName}
        <span className="ml-2 text-xs text-neutral-400">#{flag.collegeId}</span>
      </TableCell>
      <TableCell className="text-xs text-neutral-700">
        {flag.academicYear}
      </TableCell>
      <TableCell>
        <FlagTypeBadge flagType={flag.flagType} />
      </TableCell>
      <TableCell className="text-xs text-neutral-600">
        {reason ?? <span className="text-neutral-400">—</span>}
      </TableCell>
      <TableCell className="text-xs text-neutral-600">
        {formatDate(flag.createdAt)}
      </TableCell>
      <TableCell>
        {isResolved ? (
          <span className="text-xs text-neutral-500">
            Resolved {formatDate(flag.resolvedAt)}
            {flag.resolutionNote && (
              <span className="ml-1 text-neutral-400">
                ({flag.resolutionNote})
              </span>
            )}
            {flag.resolvedByEmail && (
              <span className="ml-1 text-neutral-400">
                by {flag.resolvedByEmail}
              </span>
            )}
          </span>
        ) : (
          <span className="rounded bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
            Open
          </span>
        )}
      </TableCell>
      <TableCell>
        {isResolved ? (
          <span className="text-xs text-neutral-400">—</span>
        ) : (
          <div className="flex flex-col gap-1">
            <div className="flex gap-2">
              <input
                type="url"
                value={urlInput}
                onChange={(e) => setUrlInput(e.target.value)}
                placeholder="https://athletics.example.edu/soccer"
                className="min-w-0 flex-1 rounded border border-neutral-300 px-2 py-1 text-xs"
                aria-label={`New URL for ${flag.collegeName}`}
                disabled={isPending}
              />
              <button
                type="button"
                onClick={() => {
                  if (urlInput.trim()) onResolve(urlInput.trim());
                }}
                disabled={isPending || urlInput.trim() === ""}
                className="rounded border border-emerald-300 bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-800 hover:bg-emerald-100 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {isPending ? "Saving…" : "Resolve"}
              </button>
            </div>
            {error && (
              <p className="text-xs text-red-700" role="alert">
                {error}
              </p>
            )}
          </div>
        )}
      </TableCell>
    </TableRow>
  );
}

function FlagTypeBadge({ flagType }: { flagType: CollegeRosterQualityFlagItem["flagType"] }) {
  const colors: Record<string, string> = {
    url_needs_review: "bg-red-100 text-red-800",
    partial_parse: "bg-orange-100 text-orange-800",
    historical_no_data: "bg-slate-100 text-slate-700",
  };
  return (
    <span
      className={`rounded px-2 py-0.5 text-xs font-medium ${colors[flagType] ?? "bg-neutral-100 text-neutral-700"}`}
    >
      {flagType}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function TablePlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

function Pager({
  page,
  pageSize,
  total,
  onPage,
}: {
  page: number;
  pageSize: number;
  total: number;
  onPage: (p: number) => void;
}) {
  const lastPage = Math.max(1, Math.ceil(total / pageSize));
  if (lastPage <= 1) return null;
  return (
    <nav
      className="mt-4 flex items-center justify-between text-sm text-neutral-600"
      aria-label="Pagination"
    >
      <span>
        Page {page} of {lastPage}
      </span>
      <div className="flex gap-2">
        <button
          type="button"
          onClick={() => onPage(Math.max(1, page - 1))}
          disabled={page <= 1}
          className="rounded border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Previous
        </button>
        <button
          type="button"
          onClick={() => onPage(Math.min(lastPage, page + 1))}
          disabled={page >= lastPage}
          className="rounded border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Next
        </button>
      </div>
    </nav>
  );
}

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
