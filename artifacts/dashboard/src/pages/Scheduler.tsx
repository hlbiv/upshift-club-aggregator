import { useState } from "react";
import {
  useGetSchedulerJob,
  useListScraperSchedules,
  useRunScraperScheduleNow,
  type ScraperSchedule,
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
 * Scheduler admin page.
 *
 *   GET  /api/v1/admin/scraper-schedules              — all schedules + runs
 *   POST /api/v1/admin/scraper-schedules/:jobKey/run  (super_admin only)
 *   GET  /api/v1/admin/scheduler-jobs/:id
 *
 * The page is driven by a single GET /scraper-schedules — adding a jobKey
 * server-side (JOB_METADATA in scheduler.ts) makes it show up here with no
 * UI change. Cron-editing, cancel, and "run all" are intentionally out of
 * scope; cron lives in `.replit`.
 *
 * 403 handling for plain-admin role: "Run now" is gated by requireSuperAdmin
 * on the server; if a plain admin clicks it, the inline error banner for
 * that job card sticks until they dismiss it (click anywhere on the header).
 *
 * Row-click opens a detail dialog backed by GET /scheduler-jobs/:id so the
 * stdout/stderr tails always reflect current server state (tails may arrive
 * after the row first appears pending in the list).
 */

// The mutation hook is typed against the OpenAPI enum of allow-listed
// jobKeys. Keep that narrow literal union here so we cast once when
// dispatching instead of sprinkling casts per call-site.
type MutationJobKey = "nightly_tier1" | "weekly_state" | "hourly_linker";

type RunNowError =
  | { kind: "forbidden" }
  | { kind: "other"; message: string };

export default function SchedulerPage() {
  const [confirmFor, setConfirmFor] = useState<string | null>(null);
  const [submittingKey, setSubmittingKey] = useState<string | null>(null);
  const [runErrors, setRunErrors] = useState<Record<string, RunNowError>>({});
  const [toast, setToast] = useState<string | null>(null);
  const [selectedJobId, setSelectedJobId] = useState<number | null>(null);

  const detailQuery = useGetSchedulerJob(selectedJobId ?? 0);
  const schedulesQuery = useListScraperSchedules({ limit: 10 });
  const mutation = useRunScraperScheduleNow();

  async function onConfirmRun(jobKey: string) {
    setConfirmFor(null);
    setSubmittingKey(jobKey);
    setRunErrors((prev) => {
      const next = { ...prev };
      delete next[jobKey];
      return next;
    });
    try {
      const result = await mutation.mutateAsync({
        jobKey: jobKey as MutationJobKey,
        data: { jobKey, args: {} },
      });
      setToast(`Job queued: #${result.id}`);
      window.setTimeout(() => setToast(null), 4000);
      await schedulesQuery.refetch();
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

  function dismissError(jobKey: string) {
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

      {schedulesQuery.isLoading ? (
        <div className="rounded border border-dashed border-neutral-300 bg-neutral-50 px-3 py-10 text-center text-sm text-neutral-500">
          Loading schedules…
        </div>
      ) : schedulesQuery.error ? (
        <div
          role="alert"
          className="rounded border border-red-200 bg-red-50 px-3 py-4 text-sm text-red-800"
        >
          Failed to load schedules: {formatError(schedulesQuery.error)}
        </div>
      ) : !schedulesQuery.data ||
        schedulesQuery.data.schedules.length === 0 ? (
        <div className="rounded border border-dashed border-neutral-300 bg-neutral-50 px-3 py-10 text-center text-sm text-neutral-500">
          No scheduled jobs configured.
        </div>
      ) : (
        <div className="space-y-8">
          {schedulesQuery.data.schedules.map((schedule) => (
            <ScheduleCard
              key={schedule.jobKey}
              schedule={schedule}
              submitting={submittingKey === schedule.jobKey}
              error={runErrors[schedule.jobKey]}
              onDismissError={() => dismissError(schedule.jobKey)}
              onRunNowClick={() => setConfirmFor(schedule.jobKey)}
              onRowClick={(job) => setSelectedJobId(job.id)}
            />
          ))}
        </div>
      )}

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
        open={selectedJobId !== null}
        onOpenChange={(open) => {
          if (!open) setSelectedJobId(null);
        }}
      >
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>
              Job #{selectedJobId ?? ""}
              {detailQuery.data ? (
                <>
                  {" "}
                  —{" "}
                  <code className="font-mono">{detailQuery.data.jobKey}</code>
                </>
              ) : null}
            </DialogTitle>
            <DialogDescription>
              {detailQuery.data
                ? `Status: ${detailQuery.data.status} · exit ${
                    detailQuery.data.exitCode ?? "—"
                  }`
                : "\u00A0"}
            </DialogDescription>
          </DialogHeader>
          {detailQuery.isLoading ? (
            <div className="rounded border border-dashed border-neutral-300 bg-neutral-50 px-3 py-6 text-center text-sm text-neutral-500">
              Loading job detail…
            </div>
          ) : detailQuery.error ? (
            <div
              role="alert"
              className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800"
            >
              Failed to load job detail: {formatError(detailQuery.error)}
            </div>
          ) : detailQuery.data ? (
            <div className="space-y-4 text-sm">
              <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-neutral-700">
                <dt className="font-medium text-neutral-500">Requested at</dt>
                <dd className="font-mono">{detailQuery.data.requestedAt}</dd>
                <dt className="font-medium text-neutral-500">Started at</dt>
                <dd className="font-mono">
                  {detailQuery.data.startedAt ?? "—"}
                </dd>
                <dt className="font-medium text-neutral-500">Completed at</dt>
                <dd className="font-mono">
                  {detailQuery.data.completedAt ?? "—"}
                </dd>
              </dl>
              <div>
                <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                  stdout tail
                </h3>
                <pre className="max-h-64 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-2 text-xs">
                  {detailQuery.data.stdoutTail ?? "(empty)"}
                </pre>
              </div>
              <div>
                <h3 className="mb-1 text-xs font-semibold uppercase tracking-wide text-neutral-500">
                  stderr tail
                </h3>
                <pre className="max-h-64 overflow-auto rounded border border-neutral-200 bg-neutral-50 p-2 text-xs">
                  {detailQuery.data.stderrTail ?? "(empty)"}
                </pre>
              </div>
            </div>
          ) : null}
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

function ScheduleCard({
  schedule,
  submitting,
  error,
  onDismissError,
  onRunNowClick,
  onRowClick,
}: {
  schedule: ScraperSchedule;
  submitting: boolean;
  error: RunNowError | undefined;
  onDismissError: () => void;
  onRunNowClick: () => void;
  onRowClick: (job: SchedulerJob) => void;
}) {
  return (
    <section
      aria-labelledby={`job-${schedule.jobKey}-heading`}
      className="rounded-lg border border-neutral-200 bg-white p-6"
      onClick={onDismissError}
    >
      <div className="mb-4 flex items-start justify-between gap-4">
        <div>
          <h2
            id={`job-${schedule.jobKey}-heading`}
            className="text-lg font-semibold text-neutral-900"
          >
            <code className="font-mono">{schedule.jobKey}</code>
          </h2>
          <p className="text-sm text-neutral-500">{schedule.description}</p>
          {schedule.cronExpression && (
            <p className="mt-0.5 text-xs text-neutral-500">
              Schedule:{" "}
              <code className="font-mono">{schedule.cronExpression}</code>
            </p>
          )}
        </div>
        <button
          type="button"
          disabled={submitting}
          onClick={(e) => {
            e.stopPropagation();
            onRunNowClick();
          }}
          className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
        >
          {submitting ? "Queuing…" : "Run now"}
        </button>
      </div>

      {error && (
        <div
          role="alert"
          className="mb-4 rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800"
        >
          {error.kind === "forbidden" ? (
            <>
              <strong>super_admin role required.</strong> Your account is plain{" "}
              <code>admin</code>; only <code>super_admin</code> can trigger
              scheduler jobs. Ask an owner to promote you or run this from a
              super_admin session.
            </>
          ) : (
            <>Run failed: {error.message}</>
          )}
        </div>
      )}

      <RunsTable jobs={schedule.recentRuns} onRowClick={onRowClick} />
    </section>
  );
}

function RunsTable({
  jobs,
  onRowClick,
}: {
  jobs: SchedulerJob[];
  onRowClick: (job: SchedulerJob) => void;
}) {
  if (jobs.length === 0) {
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
