import { useState } from "react";
import {
  GaPremierOrphanCleanupResponse,
  type GaPremierOrphanCleanupResponse as GaPremierOrphanCleanupResponseType,
} from "@hlbiv/api-zod/admin";
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
import { adminFetch } from "../lib/api";
import AdminNav from "../components/AdminNav";

/**
 * Data-quality admin page.
 *
 *   POST /api/v1/admin/data-quality/ga-premier-orphans
 *   body: { dryRun: boolean, limit: number }  (see GaPremierOrphanCleanupRequest)
 *
 * Flow:
 *   1. Operator submits form (dryRun=true, limit=500 by default).
 *   2. Response panel renders scanned / flagged / deleted counts and up to 20
 *      `sampleNames` of rows flagged by the cleanup heuristic.
 *   3. If the response came back as a dry-run with flagged > 0, a "Commit
 *      deletion" button appears behind a Radix AlertDialog. Confirming
 *      re-submits the same limit with dryRun=false. The success state shows
 *      a transient toast with the `deleted` count and resets the form back
 *      to dry-run mode so the next click can't accidentally delete again.
 *
 * The toast is a plain absolutely-positioned div rather than the shadcn
 * <Toaster /> — the app doesn't currently mount a toaster root and this PR
 * is scoped to a single page.
 */

const MAX_LIMIT = 10_000;

type SubmitState =
  | { kind: "idle" }
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | {
      kind: "ok";
      response: GaPremierOrphanCleanupResponseType;
      dryRun: boolean;
    };

export default function DataQualityPage() {
  const [dryRun, setDryRun] = useState(true);
  const [limit, setLimit] = useState(500);
  const [state, setState] = useState<SubmitState>({ kind: "idle" });
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  async function runSweep(nextDryRun: boolean, nextLimit: number) {
    setState({ kind: "loading" });
    try {
      const res = await adminFetch(
        "/api/v1/admin/data-quality/ga-premier-orphans",
        {
          method: "POST",
          body: JSON.stringify({ dryRun: nextDryRun, limit: nextLimit }),
        },
      );
      if (!res.ok) {
        setState({ kind: "error", message: `HTTP ${res.status}` });
        return;
      }
      const body = (await res.json()) as unknown;
      const parsed = GaPremierOrphanCleanupResponse.safeParse(body);
      if (!parsed.success) {
        setState({
          kind: "error",
          message: "Invalid response from server",
        });
        return;
      }
      setState({ kind: "ok", response: parsed.data, dryRun: nextDryRun });
      if (!nextDryRun) {
        setToast(`Deleted ${parsed.data.deleted} rows`);
        // Reset form back to dry-run mode so the next click can't re-delete.
        setDryRun(true);
        window.setTimeout(() => setToast(null), 4000);
      }
    } catch (e: unknown) {
      setState({
        kind: "error",
        message: e instanceof Error ? e.message : "Network error",
      });
    }
  }

  function onSubmit(e: React.FormEvent<HTMLFormElement>) {
    e.preventDefault();
    void runSweep(dryRun, limit);
  }

  async function confirmDelete() {
    setConfirmOpen(false);
    if (state.kind === "ok") {
      await runSweep(false, limit);
    }
  }

  const canCommit =
    state.kind === "ok" && state.dryRun && state.response.flagged > 0;
  const flaggedForDialog =
    state.kind === "ok" ? state.response.flagged : 0;

  return (
    <main className="mx-auto max-w-4xl px-6 py-8">
      <AdminNav />
      <header className="mb-8">
        <h1 className="text-2xl font-semibold text-neutral-900">
          Data quality
        </h1>
        <p className="text-sm text-neutral-500">
          GA Premier orphan cleanup — scans <code>club_roster_snapshots</code>{" "}
          for malformed <code>club_name_raw</code> rows leaked into the
          pipeline. Dry-run first, then commit.
        </p>
      </header>

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
            disabled={state.kind === "loading"}
            className="rounded bg-neutral-900 px-4 py-2 text-sm font-medium text-white hover:bg-neutral-700 disabled:cursor-not-allowed disabled:bg-neutral-400"
          >
            {state.kind === "loading" ? "Running…" : "Run sweep"}
          </button>
        </form>
      </section>

      {state.kind === "error" && (
        <div
          role="alert"
          className="mb-6 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800"
        >
          Failed: {state.message}
        </div>
      )}

      {state.kind === "ok" && (
        <section aria-labelledby="results-heading" className="mb-8">
          <h2
            id="results-heading"
            className="mb-3 text-lg font-semibold text-neutral-900"
          >
            Results
          </h2>

          <div className="mb-4 grid grid-cols-1 gap-3 sm:grid-cols-3">
            <StatCard label="Scanned" value={state.response.scanned} />
            <StatCard label="Flagged" value={state.response.flagged} />
            <StatCard
              label="Deleted"
              value={state.response.deleted}
              emphasize={state.response.deleted > 0}
            />
          </div>

          <div className="rounded-lg border border-neutral-200 bg-white p-4">
            <h3 className="mb-2 text-sm font-semibold text-neutral-900">
              Sample flagged names{" "}
              <span className="font-normal text-neutral-500">
                ({state.response.sampleNames.length})
              </span>
            </h3>
            {state.response.sampleNames.length === 0 ? (
              <p className="text-sm text-neutral-500">No samples returned.</p>
            ) : (
              <ul className="list-disc space-y-1 pl-5 text-sm text-neutral-800">
                {state.response.sampleNames.map((name, i) => (
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
    </main>
  );
}

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
