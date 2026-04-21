import { useState } from "react";
import {
  useListScraperScheduleRuns,
  useRunScraperScheduleNow,
  type SchedulerJob,
} from "@workspace/api-client-react";
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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "../components/ui/dialog";
import AdminNav from "../components/AdminNav";

/**
 * Scheduler admin page (S.4).
 *
 *   GET  /api/v1/admin/scraper-schedules/:jobKey/runs?limit=10
 *   POST /api/v1/admin/scraper-schedules/:jobKey/run   (super_admin only)
 *   GET  /api/v1/admin/scheduler-jobs/:id              (available; not
 *                                                       currently needed —
 *                                                       detail dialog reuses
 *                                                       the list row's data.)
 *
 * Three hardcoded job-key cards match the API's allow-list in
 * artifacts/api-server/src/routes/admin/scheduler.ts (ALLOWED_JOB_KEYS).
 * Cron-editing, cancel, and "run all" are intentionally out of scope.
 *
 * 403 handling for plain-admin role: the "Run now" mutation is gated by
 * requireSuperAdmin on the server; if a plain admin clicks it, the inline
 * error banner for that job card sticks until they dismiss it (click
 * anywhere on the card header).
 *
 * Implementation note: driven off Orval-generated React Query hooks
 * (`useListScraperScheduleRuns` / `useRunScraperScheduleNow`). Three
 * `useListScraperScheduleRuns` invocations — one per allow-listed jobKey —
 * fire in parallel on mount. The query hook expects a jobKey literal, so
 * we unroll the three calls at the component root rather than looping
 * JOBS.map.
 */

type JobKey = "nightly_tier1" | "weekly_state" | "hourly_linker";

const JOBS: ReadonlyArray<{
  readonly jobKey: JobKey;
  readonly description: string;
}> = [
  {
    jobKey: "nightly_tier1",
    description: "Nightly Tier 1 league scraper",
  },
  {
    jobKey: "weekly_state",
    description: "Weekly state associations sweep",
  },
  {
    jobKey: "hourly_linker",
    description: "Hourly canonical-club linker",
  },
] as const;

type RunNowError =
  | { kind: "forbidden" }
  | { kind: "other"; message: string };

export default function SchedulerPage() {
  const [confirmFor, setConfirmFor] = useState<JobKey | null>(null);
  const [submittingKey, setSubmittingKey] = useState<JobKey | null>(null);
  const [runErrors, setRunErrors] = useState<
    Partial<Record<JobKey, RunNowError>>
  >({});
  const [toast, setToast] = useState<string | null>(null);
  const [detailJob, setDetailJob] = useState<SchedulerJob | null>(null);

  // One `useListScraperScheduleRuns` per allow-listed jobKey. Radix makes
  // us unroll these because hooks can't be called inside `.map()`.
  const nightlyQuery = useListScraperScheduleRuns("nightly_tier1", {
    limit: 10,
  });
  const weeklyQuery = useListScraperScheduleRuns("weekly_state", { limit: 10 });
  const hourlyQuery = useListScraperScheduleRuns("hourly_linker", {
    limit: 10,
  });

  const queryByKey: Record<JobKey, typeof nightlyQuery> = {
    nightly_tier1: nightlyQuery,
    weekly_state: weeklyQuery,
    hourly_linker: hourlyQuery,
  };

  const mutation = useRunScraperScheduleNow();

  async function onConfirmRun(jobKey: JobKey) {
    setConfirmFor(null);
    setSubmittingKey(jobKey);
    setRunErrors((prev) => {
      const next = { ...prev };
      delete next[jobKey];
      return next;
    });
    try {
      const result = await mutation.mutateAsync({
        jobKey,
        data: { jobKey, args: {} },
      });
      setToast(`Job queued: #${result.id}`);
      window.setTimeout(() => setToast(null), 4000);
      // Re-fetch runs so the new pending row shows up.
      await queryByKey[jobKey].refetch();
    } catch (e: unknown) {
      const status =
        e instanceof Error
          ? (e as unknown as { status?: unknown }).status
          : undefined;
      if (status === 403) {
        setRunErrors((prev) => ({ ...prev, [jobKey]: { kind: "forbidden" } }));
      } else if (typeof status === "number") {
        setRunErrors((prev) => ({
          ...prev,
          [jobKey]: { kind: "other", message: `HTTP ${status}` },
        }));
      } else {
        setRunErrors((prev) => ({
          ...prev,
          [jobKey]: {
            kind: "other",
            message: e instanceof Error ? e.message : "Network error",
          },
        }));
      }
    } finally {
      setSubmittingKey(null);
    }
  }

  function dismissError(jobKey: JobKey) {
    setRunErrors((prev) => {
      const next = { ...prev };
      delete next[jobKey];
      return next;
    });
  }

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-8">
        <h1 className="text-2xl font-semibold text-neutral-900">Scheduler</h1>
        <p className="text-sm text-neutral-500">
          Trigger scraper jobs on demand and inspect the last 10 runs per job.
          Cron schedules are edited in the Replit console — this page only
          handles <em>Run now</em>.
        </p>
      </header>

      <div className="space-y-8">
        {JOBS.map((job) => {
          const q = queryByKey[job.jobKey];
          return (
            <section
              key={job.jobKey}
              aria-labelledby={`job-${job.jobKey}-heading`}
              className="rounded-lg border border-neutral-200 bg-white p-6"
              onClick={() => dismissError(job.jobKey)}
            >
              <div className="mb-4 flex items-start justify-between gap-4">
                <div>
                  <h2
                    id={`job-${job.jobKey}-heading`}
                    className="text-lg font-semibold text-neutral-900"
                  >
                    <code className="font-mono">{job.jobKey}</code>
                  </h2>
                  <p className="text-sm text-neutral-500">{job.description}</p>
                </div>
                <button
                  type="button"
                  disabled={submittingKey === job.jobKey}
                  onClick={(e) => {
                    e.stopPropagation();
                    setConfirmFor(job.jobKey);
                  }}
                  className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
                >
                  {submittingKey === job.jobKey ? "Queuing…" : "Run now"}
                </button>
              </div>

              {runErrors[job.jobKey] && (
                <div
                  role="alert"
                  className="mb-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800"
                >
                  {runErrors[job.jobKey]!.kind === "forbidden" ? (
                    <>
                      <strong>super_admin role required.</strong> Your account
                      is plain <code>admin</code>; only <code>super_admin</code>{" "}
                      can trigger scheduler jobs. Ask an owner to promote you
                      or run this from a super_admin session.
                    </>
                  ) : (
                    <>
                      Run failed:{" "}
                      {(runErrors[job.jobKey] as { message: string }).message}
                    </>
                  )}
                </div>
              )}

              <RunsTable
                isLoading={q.isLoading}
                error={q.error}
                jobs={q.data?.jobs}
                onRowClick={(jobRow) => setDetailJob(jobRow)}
              />
            </section>
          );
        })}
      </div>

      <AlertDialog
        open={confirmFor !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmFor(null);
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              Queue run for{" "}
              <code className="font-mono">{confirmFor ?? ""}</code>?
            </AlertDialogTitle>
            <AlertDialogDescription>
              The in-process worker will pick up this job on its next tick.
              This is safe to re-run — idempotency is the scraper's
              responsibility.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault();
                if (confirmFor) void onConfirmRun(confirmFor);
              }}
            >
              Run now
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <Dialog
        open={detailJob !== null}
        onOpenChange={(open) => {
          if (!open) setDetailJob(null);
        }}
      >
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>
              Job #{detailJob?.id} —{" "}
              <code className="font-mono">{detailJob?.jobKey}</code>
            </DialogTitle>
            <DialogDescription>
              Status: {detailJob?.status} · exit {detailJob?.exitCode ?? "—"}
            </DialogDescription>
          </DialogHeader>
          {detailJob && (
            <div className="space-y-4 text-sm">
              <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-neutral-700">
                <dt className="font-medium text-neutral-500">Requested at</dt>
                <dd className="font-mono">{detailJob.requestedAt}</dd>
                <dt className="font-medium text-neutral-500">Started at</dt>
                <dd className="font-mono">{detailJob.startedAt ?? "—"}</dd>
                <dt className="font-medium text-neutral-500">Completed at</dt>
                <dd className="font-mono">{detailJob.completedAt ?? "—"}</dd>
              </dl>
              <div>
                <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                  stdout tail
                </h3>
                <pre className="max-h-64 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-2 text-xs">
                  {detailJob.stdoutTail ?? "(empty)"}
                </pre>
              </div>
              <div>
                <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                  stderr tail
                </h3>
                <pre className="max-h-64 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-2 text-xs">
                  {detailJob.stderrTail ?? "(empty)"}
                </pre>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>

      {toast && (
        <div
          role="status"
          className="fixed bottom-6 right-6 rounded-lg border border-green-200 bg-green-50 px-4 py-3 text-sm font-medium text-green-800 shadow-lg"
        >
          {toast}
        </div>
      )}
    </main>
  );
}

function RunsTable({
  isLoading,
  error,
  jobs,
  onRowClick,
}: {
  isLoading: boolean;
  error: unknown;
  jobs: SchedulerJob[] | undefined;
  onRowClick: (job: SchedulerJob) => void;
}) {
  if (isLoading) {
    return (
      <div className="rounded border border-dashed border-neutral-300 bg-neutral-50 px-3 py-6 text-center text-sm text-neutral-500">
        Loading runs…
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded border border-dashed border-red-300 bg-red-50 px-3 py-6 text-center text-sm text-red-700">
        Failed to load runs: {formatError(error)}
      </div>
    );
  }
  if (!jobs || jobs.length === 0) {
    return (
      <div className="rounded border border-dashed border-neutral-300 bg-neutral-50 px-3 py-6 text-center text-sm text-neutral-500">
        No runs yet.
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded border border-neutral-200">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-neutral-50 text-left text-neutral-600">
          <tr>
            <Th>Job ID</Th>
            <Th>Status</Th>
            <Th>Requested at</Th>
            <Th>Started at</Th>
            <Th>Completed at</Th>
            <Th>Exit code</Th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job, i) => (
            <tr
              key={job.id}
              className={`cursor-pointer hover:bg-neutral-100 ${
                i % 2 === 0 ? "bg-white" : "bg-neutral-50/50"
              }`}
              onClick={(e) => {
                e.stopPropagation();
                onRowClick(job);
              }}
            >
              <Td>#{job.id}</Td>
              <Td>
                <StatusBadge status={job.status} />
              </Td>
              <Td>{formatDate(job.requestedAt)}</Td>
              <Td>{formatDate(job.startedAt)}</Td>
              <Td>{formatDate(job.completedAt)}</Td>
              <Td>{job.exitCode ?? "—"}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Th({ children }: { children: React.ReactNode }) {
  return (
    <th className="border-b border-neutral-200 px-3 py-2 font-medium">
      {children}
    </th>
  );
}

function Td({ children }: { children: React.ReactNode }) {
  return <td className="px-3 py-2 text-neutral-800">{children}</td>;
}

function StatusBadge({ status }: { status: SchedulerJob["status"] }) {
  const base = "inline-block rounded px-2 py-0.5 text-xs font-medium";
  switch (status) {
    case "pending":
      return (
        <span className={`${base} bg-yellow-100 text-yellow-800`}>pending</span>
      );
    case "running":
      return (
        <span className={`${base} animate-pulse bg-blue-100 text-blue-800`}>
          running
        </span>
      );
    case "success":
      return (
        <span className={`${base} bg-green-100 text-green-800`}>success</span>
      );
    case "failed":
      return <span className={`${base} bg-red-100 text-red-800`}>failed</span>;
    case "canceled":
      return (
        <span className={`${base} bg-neutral-200 text-neutral-700`}>
          canceled
        </span>
      );
    default:
      return <span className={`${base} text-neutral-500`}>{status}</span>;
  }
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
