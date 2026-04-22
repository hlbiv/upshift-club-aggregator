import { useEffect, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import {
  getGetNavLeakedNamesQueryKey,
  getGetNumericOnlyNamesQueryKey,
  useGaPremierOrphanCleanup,
  useGetEmptyStaffPages,
  useGetNavLeakedNames,
  useGetNumericOnlyNames,
  useGetStaleScrapes,
  useResolveRosterQualityFlag,
  type EmptyStaffPagesResponse,
  type GaPremierOrphanCleanupResponse,
  type NavLeakedNamesResponse,
  type NumericOnlyNamesResponse,
  type StaleScrapesResponse,
} from "@workspace/api-client-react";
import { useQueueShortcuts } from "../hooks/useQueueShortcuts";
import { Navigate } from "react-router-dom";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "../components/ui/alert-dialog";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";

/**
 * Data-quality admin page.
 *
 * Five panels surfaced as tabs:
 *
 *   1. GA Premier orphans — POST /api/v1/admin/data-quality/ga-premier-orphans
 *   2. Empty staff pages — GET /api/v1/admin/data-quality/empty-staff-pages
 *      Clubs with staff_page_url set but zero distinct coach discoveries in
 *      the last `window_days` days. Default 30.
 *   3. Stale scrapes — GET /api/v1/admin/data-quality/stale-scrapes
 *      scrape_health rows whose last_scraped_at is older than
 *      `threshold_days` days or never scraped. Default 14.
 *   4. Nav-leaked names — GET /api/v1/admin/data-quality/nav-leaked-names
 *      Roster snapshots flagged as containing navigation-menu strings
 *      rather than real player names.
 *   5. Numeric-only names — GET /api/v1/admin/data-quality/numeric-only-names
 *      Roster snapshots flagged as containing jersey numbers or dates in
 *      the player_name column instead of actual names.
 *
 * All five panels drive off Orval-generated React Query hooks. Radix Tabs
 * unmount inactive TabsContent by default, so the read-only panels don't
 * fire until the operator clicks their tab.
 */

const MAX_LIMIT = 10_000;
const PANEL_DEFAULT_PAGE_SIZE = 20;
const EMPTY_STAFF_DEFAULT_WINDOW_DAYS = 30;
const STALE_SCRAPES_DEFAULT_THRESHOLD_DAYS = 14;

/**
 * The old monolithic Data Quality page has been split into per-check routes
 * under `/data-quality/*`. The 5 panels now live in `pages/dataquality/*`
 * (each a thin wrapper around the panel components below). The legacy
 * `/data-quality` route redirects to the first sub-page so existing
 * bookmarks keep working.
 */
export default function DataQualityRedirect() {
  return <Navigate to="/data-quality/nav-leaked" replace />;
}

// ---------------------------------------------------------------------------
// GA Premier orphan cleanup
// ---------------------------------------------------------------------------

export function GaPremierPanel() {
  const [dryRun, setDryRun] = useState(true);
  const [limit, setLimit] = useState(500);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<{
    response: GaPremierOrphanCleanupResponse;
    dryRun: boolean;
  } | null>(null);

  const mutation = useGaPremierOrphanCleanup();

  async function runSweep(nextDryRun: boolean, nextLimit: number) {
    try {
      const response = await mutation.mutateAsync({
        data: { dryRun: nextDryRun, limit: nextLimit },
      });
      setLastResult({ response, dryRun: nextDryRun });
      if (!nextDryRun) {
        setToast(`Deleted ${response.deleted} rows`);
        // Reset form back to dry-run mode so the next click can't re-delete.
        setDryRun(true);
        window.setTimeout(() => setToast(null), 4000);
      }
    } catch {
      // The mutation's error state is surfaced via `mutation.error` below —
      // no further action needed here. `mutateAsync` re-throws so we swallow.
    }
  }

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    void runSweep(dryRun, limit);
  }

  async function confirmDelete() {
    setConfirmOpen(false);
    if (lastResult) {
      await runSweep(false, limit);
    }
  }

  const canCommit =
    lastResult !== null && lastResult.dryRun && lastResult.response.flagged > 0;
  const flaggedForDialog = lastResult?.response.flagged ?? 0;

  return (
    <>
      <p className="mb-4 text-sm text-neutral-500">
        GA Premier orphan cleanup — scans <code>club_roster_snapshots</code>{" "}
        for malformed <code>club_name_raw</code> rows leaked into the
        pipeline. Dry-run first, then commit.
      </p>

      <section
        className="mb-8 rounded-lg border border-neutral-200 bg-white p-6"
        aria-labelledby="sweep-form-heading"
      >
        <h2
          id="sweep-form-heading"
          className="mb-4 text-lg font-semibold text-neutral-900"
        >
          Run sweep
        </h2>
        <form onSubmit={onSubmit} className="flex flex-wrap items-end gap-6">
          <label className="flex items-center gap-2 text-sm text-neutral-800">
            <input
              type="checkbox"
              checked={dryRun}
              onChange={(e) => setDryRun(e.target.checked)}
              className="h-4 w-4 rounded border-neutral-300"
            />
            Dry run
          </label>
          <label className="flex flex-col gap-1 text-sm text-neutral-800">
            <span className="font-medium">Limit</span>
            <input
              type="number"
              min={1}
              max={MAX_LIMIT}
              value={limit}
              onChange={(e) => {
                const n = Number(e.target.value);
                if (!Number.isNaN(n)) setLimit(n);
              }}
              className="w-32 rounded border border-neutral-300 px-2 py-1"
            />
          </label>
          <button
            type="submit"
            disabled={mutation.isPending}
            className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
          >
            {mutation.isPending ? "Running…" : "Run sweep"}
          </button>
        </form>
      </section>

      {mutation.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {formatError(mutation.error)}
        </div>
      )}

      {lastResult && (
        <section aria-labelledby="results-heading" className="mb-8">
          <h2
            id="results-heading"
            className="mb-3 text-lg font-semibold text-neutral-900"
          >
            Results
          </h2>

          <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-3">
            <StatCard label="Scanned" value={lastResult.response.scanned} />
            <StatCard label="Flagged" value={lastResult.response.flagged} />
            <StatCard
              label="Deleted"
              value={lastResult.response.deleted}
              emphasize={lastResult.response.deleted > 0}
            />
          </div>

          <div className="rounded-lg border border-neutral-200 bg-white p-4">
            <h3 className="mb-2 text-sm font-semibold text-neutral-900">
              Sample flagged names{" "}
              <span className="font-normal text-neutral-500">
                ({lastResult.response.sampleNames.length})
              </span>
            </h3>
            {lastResult.response.sampleNames.length === 0 ? (
              <p className="text-sm text-neutral-500">No samples returned.</p>
            ) : (
              <ul className="list-disc space-y-1 pl-5 text-sm text-neutral-800">
                {lastResult.response.sampleNames.map((name, i) => (
                  <li key={`${i}-${name}`} className="font-mono">
                    {name}
                  </li>
                ))}
              </ul>
            )}
          </div>

          {canCommit && (
            <div className="mt-4">
              <button
                type="button"
                onClick={() => setConfirmOpen(true)}
                className="rounded bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
              >
                Commit deletion
              </button>
            </div>
          )}
        </section>
      )}

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              Delete {flaggedForDialog} roster snapshot rows?
            </AlertDialogTitle>
            <AlertDialogDescription>
              This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault();
                void confirmDelete();
              }}
              className="bg-red-600 hover:bg-red-700"
            >
              Delete
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {toast && (
        <div
          role="status"
          className="fixed bottom-6 right-6 rounded-lg border border-green-200 bg-green-50 px-4 py-3 text-sm font-medium text-green-800 shadow-lg"
        >
          {toast}
        </div>
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Empty staff pages
// ---------------------------------------------------------------------------

export function EmptyStaffPanel() {
  const [windowDays, setWindowDays] = useState(EMPTY_STAFF_DEFAULT_WINDOW_DAYS);
  const [appliedWindowDays, setAppliedWindowDays] = useState(
    EMPTY_STAFF_DEFAULT_WINDOW_DAYS,
  );
  const [page, setPage] = useState(1);

  const query = useGetEmptyStaffPages({
    window_days: appliedWindowDays,
    page,
    page_size: PANEL_DEFAULT_PAGE_SIZE,
  });

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setPage(1);
    setAppliedWindowDays(windowDays);
  }

  return (
    <section aria-labelledby="empty-staff-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Clubs with a <code>staff_page_url</code> set but zero distinct coach
        discoveries in the last <strong>{appliedWindowDays}</strong> days. Good
        candidates for a re-scrape or extractor fix.
      </p>

      <form
        onSubmit={onSubmit}
        className="mb-6 flex flex-wrap items-end gap-4 rounded-lg border border-neutral-200 bg-white p-4"
      >
        <label className="flex flex-col gap-1 text-sm text-neutral-800">
          <span className="font-medium">Window (days)</span>
          <input
            type="number"
            min={1}
            max={365}
            value={windowDays}
            onChange={(e) => {
              const n = Number(e.target.value);
              if (!Number.isNaN(n)) setWindowDays(n);
            }}
            className="w-32 rounded border border-neutral-300 px-2 py-1"
          />
        </label>
        <button
          type="submit"
          disabled={query.isFetching}
          className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
        >
          {query.isFetching ? "Loading…" : "Refresh"}
        </button>
      </form>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && <TablePlaceholder label="Loading…" />}

      {query.isSuccess && query.data.rows.length === 0 && (
        <TablePlaceholder label="No clubs matched." />
      )}

      {query.isSuccess && query.data.rows.length > 0 && (
        <EmptyStaffTable data={query.data} onPage={(p) => setPage(p)} />
      )}
    </section>
  );
}

function EmptyStaffTable({
  data,
  onPage,
}: {
  data: EmptyStaffPagesResponse;
  onPage: (p: number) => void;
}) {
  return (
    <>
      <p className="mb-2 text-sm text-neutral-500">
        {data.total.toLocaleString()} matching clubs
      </p>
      <div className="overflow-hidden rounded-lg border border-neutral-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Club</TableHead>
              <TableHead>Staff page</TableHead>
              <TableHead>Last scraped</TableHead>
              <TableHead>Coaches in window</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row) => (
              <TableRow key={row.clubId}>
                <TableCell className="font-medium">
                  {row.clubNameCanonical}
                  <span className="ml-2 text-xs text-neutral-400">
                    #{row.clubId}
                  </span>
                </TableCell>
                <TableCell>
                  <a
                    href={row.staffPageUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-blue-600 underline break-all"
                  >
                    {row.staffPageUrl}
                  </a>
                </TableCell>
                <TableCell>{formatDate(row.lastScrapedAt)}</TableCell>
                <TableCell>{row.coachCountWindow}</TableCell>
              </TableRow>
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

// ---------------------------------------------------------------------------
// Stale scrapes
// ---------------------------------------------------------------------------

export function StaleScrapesPanel() {
  const [thresholdDays, setThresholdDays] = useState(
    STALE_SCRAPES_DEFAULT_THRESHOLD_DAYS,
  );
  const [appliedThresholdDays, setAppliedThresholdDays] = useState(
    STALE_SCRAPES_DEFAULT_THRESHOLD_DAYS,
  );
  const [page, setPage] = useState(1);

  const query = useGetStaleScrapes({
    threshold_days: appliedThresholdDays,
    page,
    page_size: PANEL_DEFAULT_PAGE_SIZE,
  });

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setPage(1);
    setAppliedThresholdDays(thresholdDays);
  }

  return (
    <section aria-labelledby="stale-scrapes-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Entities in <code>scrape_health</code> whose{" "}
        <code>last_scraped_at</code> is older than{" "}
        <strong>{appliedThresholdDays}</strong> days or have never been
        scraped.
      </p>

      <form
        onSubmit={onSubmit}
        className="mb-6 flex flex-wrap items-end gap-4 rounded-lg border border-neutral-200 bg-white p-4"
      >
        <label className="flex flex-col gap-1 text-sm text-neutral-800">
          <span className="font-medium">Threshold (days)</span>
          <input
            type="number"
            min={1}
            max={365}
            value={thresholdDays}
            onChange={(e) => {
              const n = Number(e.target.value);
              if (!Number.isNaN(n)) setThresholdDays(n);
            }}
            className="w-32 rounded border border-neutral-300 px-2 py-1"
          />
        </label>
        <button
          type="submit"
          disabled={query.isFetching}
          className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
        >
          {query.isFetching ? "Loading…" : "Refresh"}
        </button>
      </form>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && <TablePlaceholder label="Loading…" />}

      {query.isSuccess && query.data.rows.length === 0 && (
        <TablePlaceholder label="No stale entities." />
      )}

      {query.isSuccess && query.data.rows.length > 0 && (
        <StaleScrapesTable data={query.data} onPage={(p) => setPage(p)} />
      )}
    </section>
  );
}

function StaleScrapesTable({
  data,
  onPage,
}: {
  data: StaleScrapesResponse;
  onPage: (p: number) => void;
}) {
  return (
    <>
      <p className="mb-2 text-sm text-neutral-500">
        {data.total.toLocaleString()} stale entities
      </p>
      <div className="overflow-hidden rounded-lg border border-neutral-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Type</TableHead>
              <TableHead>Entity</TableHead>
              <TableHead>Last scraped</TableHead>
              <TableHead>Last status</TableHead>
              <TableHead>Consecutive failures</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row) => (
              <TableRow key={`${row.entityType}-${row.entityId}`}>
                <TableCell className="font-mono text-xs">
                  {row.entityType}
                </TableCell>
                <TableCell>
                  {row.entityName ?? (
                    <span className="text-neutral-400">
                      (id {row.entityId})
                    </span>
                  )}
                  <span className="ml-2 text-xs text-neutral-400">
                    #{row.entityId}
                  </span>
                </TableCell>
                <TableCell>{formatDate(row.lastScrapedAt)}</TableCell>
                <TableCell>
                  <span className="text-xs text-neutral-700">
                    {row.lastStatus ?? "—"}
                  </span>
                </TableCell>
                <TableCell>
                  <FailureBadge count={row.consecutiveFailures} />
                </TableCell>
              </TableRow>
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

// ---------------------------------------------------------------------------
// Nav-leaked names
// ---------------------------------------------------------------------------

type NavLeakedState = "open" | "resolved" | "dismissed";

export function NavLeakedNamesPanel() {
  const [state, setState] = useState<NavLeakedState>("open");
  const [appliedState, setAppliedState] = useState<NavLeakedState>("open");
  const [page, setPage] = useState(1);
  const [cursor, setCursor] = useState(0);
  const queryClient = useQueryClient();

  const query = useGetNavLeakedNames({
    page,
    page_size: PANEL_DEFAULT_PAGE_SIZE,
    state: appliedState,
  });

  // Auto-advance is implicit: the resolved row drops out of the result set
  // on refetch, so the existing cursor index naturally points to what was
  // the *next* row. We just clamp on data churn so cursor never points past
  // the array end.
  useEffect(() => {
    const len = query.data?.rows.length ?? 0;
    if (cursor >= len) setCursor(Math.max(0, len - 1));
  }, [query.data?.rows.length, cursor]);

  // Shortcut-only resolve hook (the per-row buttons keep their own copy so
  // the spinner stays scoped to the clicked row). Both share the same
  // invalidation, which is keyed off the base query key, so either entry
  // point updates the table.
  const shortcutResolve = useResolveRosterQualityFlag({
    mutation: {
      onSuccess: () =>
        queryClient.invalidateQueries({
          queryKey: getGetNavLeakedNamesQueryKey().slice(0, 1),
        }),
    },
  });

  const rows = query.data?.rows ?? [];
  const total = query.data?.total ?? 0;
  const pageStart = (query.data?.page ?? page) - 1;
  const positionInPage = rows.length === 0 ? 0 : cursor + 1;
  const positionGlobal =
    rows.length === 0
      ? 0
      : pageStart * PANEL_DEFAULT_PAGE_SIZE + cursor + 1;

  useQueueShortcuts({
    enabled: appliedState === "open" && rows.length > 0,
    onNext: () => setCursor((c) => Math.min(rows.length - 1, c + 1)),
    onPrev: () => setCursor((c) => Math.max(0, c - 1)),
    onPrimary: () => {
      const row = rows[cursor];
      if (row && row.resolvedAt === null) {
        shortcutResolve.mutate({
          id: row.id,
          data: { reason: "resolved" },
        });
      }
    },
    onSecondary: () => {
      const row = rows[cursor];
      if (row && row.resolvedAt === null) {
        shortcutResolve.mutate({
          id: row.id,
          data: { reason: "dismissed" },
        });
      }
    },
  });

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setPage(1);
    setCursor(0);
    setAppliedState(state);
  }

  return (
    <section aria-labelledby="nav-leaked-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Roster snapshots flagged as containing navigation-menu strings (e.g.{" "}
        <code>"HOME"</code>, <code>"CONTACT"</code>) instead of real player
        names. Use <strong>Confirm</strong> when the flag was legitimate and
        you've cleaned up the leak out of band; use <strong>Dismiss</strong>{" "}
        for false positives.
      </p>

      <form
        onSubmit={onSubmit}
        className="mb-6 flex flex-wrap items-end gap-4 rounded-lg border border-neutral-200 bg-white p-4"
      >
        <fieldset
          className="flex items-center gap-4 text-sm text-neutral-800"
          aria-label="State filter"
        >
          <legend className="sr-only">State filter</legend>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="nav-leaked-state"
              value="open"
              checked={state === "open"}
              onChange={() => setState("open")}
              className="h-4 w-4"
            />
            Open
          </label>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="nav-leaked-state"
              value="resolved"
              checked={state === "resolved"}
              onChange={() => setState("resolved")}
              className="h-4 w-4"
            />
            Resolved
          </label>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="nav-leaked-state"
              value="dismissed"
              checked={state === "dismissed"}
              onChange={() => setState("dismissed")}
              className="h-4 w-4"
            />
            Dismissed
          </label>
        </fieldset>
        <button
          type="submit"
          disabled={query.isFetching}
          className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
        >
          {query.isFetching ? "Loading…" : "Refresh"}
        </button>
      </form>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && <TablePlaceholder label="Loading…" />}

      {query.isSuccess && query.data.rows.length === 0 && (
        <TablePlaceholder label="No flagged snapshots." />
      )}

      {query.isSuccess && query.data.rows.length > 0 && (
        <>
          {appliedState === "open" ? (
            <p
              className="mb-2 inline-flex items-center gap-2 rounded-full border border-indigo-100 bg-indigo-50 px-3 py-1 text-xs font-medium text-indigo-700"
              aria-live="polite"
            >
              Working{" "}
              <span className="tabular-nums">
                {positionGlobal} of {total.toLocaleString()}
              </span>
              <span className="text-indigo-400">·</span>
              <span className="text-indigo-500">
                row {positionInPage} on page · J/K to move, M confirm, R dismiss
              </span>
            </p>
          ) : null}
          <NavLeakedNamesTable
            data={query.data}
            cursor={cursor}
            onCursor={setCursor}
            onPage={(p) => {
              setPage(p);
              setCursor(0);
            }}
          />
        </>
      )}
    </section>
  );
}

function NavLeakedNamesTable({
  data,
  cursor,
  onCursor,
  onPage,
}: {
  data: NavLeakedNamesResponse;
  cursor: number;
  /**
   * Sync the parent panel's cursor when the operator clicks a row's
   * Confirm/Dismiss button with the mouse — keeps mouse + keyboard in
   * lockstep so the next M/R press operates on whatever was just touched.
   */
  onCursor: (idx: number) => void;
  onPage: (p: number) => void;
}) {
  const queryClient = useQueryClient();
  const resolve = useResolveRosterQualityFlag({
    mutation: {
      onSuccess: () => {
        // Refetch every nav-leaked-names page (any params) so the row
        // either disappears (default view) or flips to the Resolved
        // badge (when "Include resolved flags" is on).
        queryClient.invalidateQueries({
          queryKey: getGetNavLeakedNamesQueryKey().slice(0, 1),
        });
      },
    },
  });

  return (
    <>
      <p className="mb-2 text-sm text-neutral-500">
        {data.total.toLocaleString()} flagged snapshots
      </p>
      <div className="overflow-hidden rounded-lg border border-neutral-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Club</TableHead>
              <TableHead>Leaked strings</TableHead>
              <TableHead>Roster size</TableHead>
              <TableHead>Flagged at</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row, rowIdx) => (
              <TableRow
                key={row.id}
                aria-current={rowIdx === cursor ? "true" : undefined}
                className={
                  rowIdx === cursor
                    ? "bg-indigo-50/60 ring-2 ring-inset ring-indigo-300"
                    : undefined
                }
              >
                <TableCell className="font-medium">
                  {row.clubNameCanonical ?? (
                    <span className="text-neutral-400">
                      (unlinked snapshot #{row.snapshotId})
                    </span>
                  )}
                  {row.clubId !== null && (
                    <span className="ml-2 text-xs text-neutral-400">
                      #{row.clubId}
                    </span>
                  )}
                </TableCell>
                <TableCell>
                  {row.leakedStrings.length === 0 ? (
                    <span className="text-neutral-400">—</span>
                  ) : (
                    <div className="flex flex-wrap gap-1">
                      {row.leakedStrings.map((s, i) => (
                        <span
                          key={`${i}-${s}`}
                          className="rounded bg-neutral-100 px-2 py-0.5 font-mono text-xs text-neutral-800"
                        >
                          {s}
                        </span>
                      ))}
                    </div>
                  )}
                </TableCell>
                <TableCell>{row.snapshotRosterSize}</TableCell>
                <TableCell>{formatDate(row.flaggedAt)}</TableCell>
                <TableCell>
                  <ResolvedBadge
                    resolvedAt={row.resolvedAt}
                    resolvedByEmail={row.resolvedByEmail}
                    resolutionReason={row.resolutionReason}
                  />
                </TableCell>
                <TableCell>
                  {row.resolvedAt === null ? (
                    <div className="flex gap-2">
                      <button
                        type="button"
                        onClick={() => {
                          onCursor(rowIdx);
                          resolve.mutate({
                            id: row.id,
                            data: { reason: "resolved" },
                          });
                        }}
                        disabled={
                          resolve.isPending && resolve.variables?.id === row.id
                        }
                        className="rounded border border-emerald-300 bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-800 hover:bg-emerald-100 disabled:cursor-not-allowed disabled:opacity-50"
                        aria-label={`Confirm flag ${row.id}`}
                      >
                        {resolve.isPending &&
                        resolve.variables?.id === row.id &&
                        resolve.variables?.data?.reason === "resolved"
                          ? "Confirming…"
                          : "Confirm"}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          onCursor(rowIdx);
                          resolve.mutate({
                            id: row.id,
                            data: { reason: "dismissed" },
                          });
                        }}
                        disabled={
                          resolve.isPending && resolve.variables?.id === row.id
                        }
                        className="rounded border border-neutral-300 bg-white px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-50 disabled:cursor-not-allowed disabled:opacity-50"
                        aria-label={`Dismiss flag ${row.id}`}
                      >
                        {resolve.isPending &&
                        resolve.variables?.id === row.id &&
                        resolve.variables?.data?.reason === "dismissed"
                          ? "Dismissing…"
                          : "Dismiss"}
                      </button>
                    </div>
                  ) : (
                    <span className="text-xs text-neutral-400">—</span>
                  )}
                </TableCell>
              </TableRow>
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

function ResolvedBadge({
  resolvedAt,
  resolvedByEmail,
  resolutionReason,
}: {
  resolvedAt: string | null;
  resolvedByEmail: string | null;
  resolutionReason: "resolved" | "dismissed" | null;
}) {
  if (resolvedAt === null) {
    return (
      <span className="rounded bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-800">
        Active
      </span>
    );
  }
  // `resolutionReason` should always be set when `resolvedAt` is set (the
  // DB CHECK constraint enforces this), but fall back to "Resolved" so a
  // legacy row that predates the column doesn't blow up the UI.
  const label =
    resolutionReason === "dismissed" ? "Dismissed" : "Resolved";
  return (
    <span className="text-xs text-neutral-600">
      {label} {formatDate(resolvedAt)}
      {resolvedByEmail && (
        <span className="ml-1 text-neutral-400">by {resolvedByEmail}</span>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Numeric-only names
// ---------------------------------------------------------------------------

type NumericOnlyState = "open" | "resolved" | "dismissed";

export function NumericOnlyNamesPanel() {
  const [state, setState] = useState<NumericOnlyState>("open");
  const [appliedState, setAppliedState] = useState<NumericOnlyState>("open");
  const [page, setPage] = useState(1);
  const [cursor, setCursor] = useState(0);
  const queryClient = useQueryClient();

  const query = useGetNumericOnlyNames({
    page,
    page_size: PANEL_DEFAULT_PAGE_SIZE,
    state: appliedState,
  });

  useEffect(() => {
    const len = query.data?.rows.length ?? 0;
    if (cursor >= len) setCursor(Math.max(0, len - 1));
  }, [query.data?.rows.length, cursor]);

  const shortcutResolve = useResolveRosterQualityFlag({
    mutation: {
      onSuccess: () =>
        queryClient.invalidateQueries({
          queryKey: getGetNumericOnlyNamesQueryKey().slice(0, 1),
        }),
    },
  });

  const rows = query.data?.rows ?? [];
  const total = query.data?.total ?? 0;
  const pageStart = (query.data?.page ?? page) - 1;
  const positionInPage = rows.length === 0 ? 0 : cursor + 1;
  const positionGlobal =
    rows.length === 0
      ? 0
      : pageStart * PANEL_DEFAULT_PAGE_SIZE + cursor + 1;

  useQueueShortcuts({
    enabled: appliedState === "open" && rows.length > 0,
    onNext: () => setCursor((c) => Math.min(rows.length - 1, c + 1)),
    onPrev: () => setCursor((c) => Math.max(0, c - 1)),
    onPrimary: () => {
      const row = rows[cursor];
      if (row && row.resolvedAt === null) {
        shortcutResolve.mutate({
          id: row.id,
          data: { reason: "resolved" },
        });
      }
    },
    onSecondary: () => {
      const row = rows[cursor];
      if (row && row.resolvedAt === null) {
        shortcutResolve.mutate({
          id: row.id,
          data: { reason: "dismissed" },
        });
      }
    },
  });

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setPage(1);
    setCursor(0);
    setAppliedState(state);
  }

  return (
    <section aria-labelledby="numeric-only-heading">
      <p className="mb-4 text-sm text-neutral-500">
        Roster snapshots flagged as containing bare numbers (e.g.{" "}
        <code>"14"</code>, <code>"2024-05-15"</code>) instead of real player
        names — usually a scraper grabbing jersey numbers or dates. Use{" "}
        <strong>Confirm</strong> when you've fixed the extractor; use{" "}
        <strong>Dismiss</strong> for false positives.
      </p>

      <form
        onSubmit={onSubmit}
        className="mb-6 flex flex-wrap items-end gap-4 rounded-lg border border-neutral-200 bg-white p-4"
      >
        <fieldset
          className="flex items-center gap-4 text-sm text-neutral-800"
          aria-label="State filter"
        >
          <legend className="sr-only">State filter</legend>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="numeric-only-state"
              value="open"
              checked={state === "open"}
              onChange={() => setState("open")}
              className="h-4 w-4"
            />
            Open
          </label>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="numeric-only-state"
              value="resolved"
              checked={state === "resolved"}
              onChange={() => setState("resolved")}
              className="h-4 w-4"
            />
            Resolved
          </label>
          <label className="flex items-center gap-2">
            <input
              type="radio"
              name="numeric-only-state"
              value="dismissed"
              checked={state === "dismissed"}
              onChange={() => setState("dismissed")}
              className="h-4 w-4"
            />
            Dismissed
          </label>
        </fieldset>
        <button
          type="submit"
          disabled={query.isFetching}
          className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
        >
          {query.isFetching ? "Loading…" : "Refresh"}
        </button>
      </form>

      {query.isError && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {formatError(query.error)}
        </div>
      )}

      {query.isLoading && <TablePlaceholder label="Loading…" />}

      {query.isSuccess && query.data.rows.length === 0 && (
        <TablePlaceholder label="No flagged snapshots." />
      )}

      {query.isSuccess && query.data.rows.length > 0 && (
        <>
          {appliedState === "open" ? (
            <p
              className="mb-2 inline-flex items-center gap-2 rounded-full border border-indigo-100 bg-indigo-50 px-3 py-1 text-xs font-medium text-indigo-700"
              aria-live="polite"
            >
              Working{" "}
              <span className="tabular-nums">
                {positionGlobal} of {total.toLocaleString()}
              </span>
              <span className="text-indigo-400">·</span>
              <span className="text-indigo-500">
                row {positionInPage} on page · J/K to move, M confirm, R dismiss
              </span>
            </p>
          ) : null}
          <NumericOnlyNamesTable
            data={query.data}
            cursor={cursor}
            onCursor={setCursor}
            onPage={(p) => {
              setPage(p);
              setCursor(0);
            }}
          />
        </>
      )}
    </section>
  );
}

function NumericOnlyNamesTable({
  data,
  cursor,
  onCursor,
  onPage,
}: {
  data: NumericOnlyNamesResponse;
  cursor: number;
  /** See NavLeakedNamesTable — sync mouse clicks to the keyboard cursor. */
  onCursor: (idx: number) => void;
  onPage: (p: number) => void;
}) {
  const queryClient = useQueryClient();
  const resolve = useResolveRosterQualityFlag({
    mutation: {
      onSuccess: () => {
        queryClient.invalidateQueries({
          queryKey: getGetNumericOnlyNamesQueryKey().slice(0, 1),
        });
      },
    },
  });

  return (
    <>
      <p className="mb-2 text-sm text-neutral-500">
        {data.total.toLocaleString()} flagged snapshots
      </p>
      <div className="overflow-hidden rounded-lg border border-neutral-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Club</TableHead>
              <TableHead>Numeric strings</TableHead>
              <TableHead>Roster size</TableHead>
              <TableHead>Flagged at</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.rows.map((row, rowIdx) => (
              <TableRow
                key={row.id}
                aria-current={rowIdx === cursor ? "true" : undefined}
                className={
                  rowIdx === cursor
                    ? "bg-indigo-50/60 ring-2 ring-inset ring-indigo-300"
                    : undefined
                }
              >
                <TableCell className="font-medium">
                  {row.clubNameCanonical ?? (
                    <span className="text-neutral-400">
                      (unlinked snapshot #{row.snapshotId})
                    </span>
                  )}
                  {row.clubId !== null && (
                    <span className="ml-2 text-xs text-neutral-400">
                      #{row.clubId}
                    </span>
                  )}
                </TableCell>
                <TableCell>
                  {row.numericStrings.length === 0 ? (
                    <span className="text-neutral-400">—</span>
                  ) : (
                    <div className="flex flex-wrap gap-1">
                      {row.numericStrings.map((s, i) => (
                        <span
                          key={`${i}-${s}`}
                          className="rounded bg-neutral-100 px-2 py-0.5 font-mono text-xs text-neutral-800"
                        >
                          {s}
                        </span>
                      ))}
                    </div>
                  )}
                </TableCell>
                <TableCell>{row.snapshotRosterSize}</TableCell>
                <TableCell>{formatDate(row.flaggedAt)}</TableCell>
                <TableCell>
                  <ResolvedBadge
                    resolvedAt={row.resolvedAt}
                    resolvedByEmail={row.resolvedByEmail}
                    resolutionReason={row.resolutionReason}
                  />
                </TableCell>
                <TableCell>
                  {row.resolvedAt === null ? (
                    <div className="flex gap-2">
                      <button
                        type="button"
                        onClick={() => {
                          onCursor(rowIdx);
                          resolve.mutate({
                            id: row.id,
                            data: { reason: "resolved" },
                          });
                        }}
                        disabled={
                          resolve.isPending && resolve.variables?.id === row.id
                        }
                        className="rounded border border-emerald-300 bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-800 hover:bg-emerald-100 disabled:cursor-not-allowed disabled:opacity-50"
                        aria-label={`Confirm flag ${row.id}`}
                      >
                        {resolve.isPending &&
                        resolve.variables?.id === row.id &&
                        resolve.variables?.data?.reason === "resolved"
                          ? "Confirming…"
                          : "Confirm"}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          onCursor(rowIdx);
                          resolve.mutate({
                            id: row.id,
                            data: { reason: "dismissed" },
                          });
                        }}
                        disabled={
                          resolve.isPending && resolve.variables?.id === row.id
                        }
                        className="rounded border border-neutral-300 bg-white px-2 py-1 text-xs font-medium text-neutral-800 hover:bg-neutral-50 disabled:cursor-not-allowed disabled:opacity-50"
                        aria-label={`Dismiss flag ${row.id}`}
                      >
                        {resolve.isPending &&
                        resolve.variables?.id === row.id &&
                        resolve.variables?.data?.reason === "dismissed"
                          ? "Dismissing…"
                          : "Dismiss"}
                      </button>
                    </div>
                  ) : (
                    <span className="text-xs text-neutral-400">—</span>
                  )}
                </TableCell>
              </TableRow>
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

// ---------------------------------------------------------------------------
// Shared presentational helpers
// ---------------------------------------------------------------------------

function StatCard({
  label,
  value,
  emphasize,
}: {
  label: string;
  value: number;
  emphasize?: boolean;
}) {
  return (
    <div className="rounded-lg border border-neutral-200 bg-white p-4">
      <p className="text-xs uppercase tracking-wide text-neutral-500">
        {label}
      </p>
      <p
        className={`mt-1 text-2xl font-semibold ${
          emphasize ? "text-red-700" : "text-neutral-900"
        }`}
      >
        {value.toLocaleString()}
      </p>
    </div>
  );
}

function TablePlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

function FailureBadge({ count }: { count: number }) {
  if (count === 0) return <span className="text-neutral-500">0</span>;
  const heavy = count >= 3;
  return (
    <span className={heavy ? "font-semibold text-red-700" : "text-neutral-800"}>
      {count}
    </span>
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
    // The customFetch ApiError class attaches a numeric `status` — surface
    // that verbatim so operators can grep the log lines.
    const status = (err as unknown as { status?: unknown }).status;
    if (typeof status === "number") return `HTTP ${status}`;
    return err.message;
  }
  return String(err);
}
