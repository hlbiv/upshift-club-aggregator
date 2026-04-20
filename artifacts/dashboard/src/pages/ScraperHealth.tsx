import { useEffect, useState } from "react";
import type {
  ScrapeHealthList,
  ScrapeHealthRow,
  ScrapeRunLog,
  ScrapeRunLogList,
} from "@hlbiv/api-zod/admin";
import { adminFetch } from "../lib/api";
import AdminNav from "../components/AdminNav";

/**
 * Scraper health dashboard.
 *
 *   GET /api/v1/admin/scrape-health         → ScrapeHealthList (rollup)
 *   GET /api/v1/admin/scrape-runs?limit=50  → ScrapeRunLogList (recent)
 *
 * Both requests run in parallel on mount. No polling this phase — refresh
 * = full page reload. Filters / pagination / "Run now" are future work.
 * The two tables intentionally use plain Tailwind utility classes so a
 * future designer pass doesn't have to unwind bespoke CSS.
 */

type FetchState<T> =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ok"; data: T };

export default function ScraperHealthPage() {
  const [health, setHealth] = useState<FetchState<ScrapeHealthList>>({
    kind: "loading",
  });
  const [runs, setRuns] = useState<FetchState<ScrapeRunLogList>>({
    kind: "loading",
  });

  useEffect(() => {
    let cancelled = false;

    adminFetch("/api/v1/admin/scrape-health")
      .then(async (res) => {
        if (cancelled) return;
        if (!res.ok) {
          setHealth({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const data = (await res.json()) as ScrapeHealthList;
        setHealth({ kind: "ok", data });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setHealth({
          kind: "error",
          message: e instanceof Error ? e.message : "Network error",
        });
      });

    adminFetch("/api/v1/admin/scrape-runs?limit=50")
      .then(async (res) => {
        if (cancelled) return;
        if (!res.ok) {
          setRuns({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const data = (await res.json()) as ScrapeRunLogList;
        setRuns({ kind: "ok", data });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setRuns({
          kind: "error",
          message: e instanceof Error ? e.message : "Network error",
        });
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-8">
        <h1 className="text-2xl font-semibold text-neutral-900">
          Scraper health
        </h1>
        <p className="text-sm text-neutral-500">
          Rolling status per entity and the 50 most recent runs.
        </p>
      </header>

      <section className="mb-10" aria-labelledby="rollup-heading">
        <h2
          id="rollup-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Rollup
        </h2>
        <HealthTable state={health} />
      </section>

      <section aria-labelledby="runs-heading">
        <h2
          id="runs-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Recent runs
        </h2>
        <RunsTable state={runs} />
      </section>
    </main>
  );
}

function HealthTable({ state }: { state: FetchState<ScrapeHealthList> }) {
  if (state.kind === "loading") return <TablePlaceholder label="Loading…" />;
  if (state.kind === "error")
    return <TablePlaceholder label={`Failed to load: ${state.message}`} />;
  const rows = state.data.rows;
  if (rows.length === 0) return <TablePlaceholder label="No entries yet." />;

  return (
    <div className="overflow-hidden rounded-lg border border-neutral-200">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-neutral-50 text-left text-neutral-600">
          <tr>
            <Th>Entity type</Th>
            <Th>Entity ID</Th>
            <Th>Last status</Th>
            <Th>Consecutive failures</Th>
            <Th>Last scraped</Th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={`${row.entityType}-${row.entityId}`}
              className={i % 2 === 0 ? "bg-white" : "bg-neutral-50/50"}
            >
              <Td>{row.entityType}</Td>
              <Td>{row.entityId}</Td>
              <Td>
                <StatusBadge status={row.lastStatus} />
              </Td>
              <Td>
                <FailureCount
                  count={row.consecutiveFailures}
                  status={row.lastStatus}
                />
              </Td>
              <Td>{formatDate(row.lastScrapedAt)}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RunsTable({ state }: { state: FetchState<ScrapeRunLogList> }) {
  if (state.kind === "loading") return <TablePlaceholder label="Loading…" />;
  if (state.kind === "error")
    return <TablePlaceholder label={`Failed to load: ${state.message}`} />;
  const runs = state.data.runs;
  if (runs.length === 0) return <TablePlaceholder label="No runs yet." />;

  return (
    <div className="overflow-hidden rounded-lg border border-neutral-200">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-neutral-50 text-left text-neutral-600">
          <tr>
            <Th>Source</Th>
            <Th>Job key</Th>
            <Th>Status</Th>
            <Th>Started</Th>
            <Th>Duration</Th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run, i) => (
            <tr
              key={run.id}
              className={i % 2 === 0 ? "bg-white" : "bg-neutral-50/50"}
            >
              <Td>{run.source}</Td>
              <Td className="text-neutral-500">{run.jobKey ?? "—"}</Td>
              <Td>
                <StatusBadge status={run.status} />
              </Td>
              <Td>{formatDate(run.startedAt)}</Td>
              <Td>{formatDuration(run)}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// --- helpers --------------------------------------------------------------

function Th({ children }: { children: React.ReactNode }) {
  return (
    <th className="border-b border-neutral-200 px-4 py-2 font-medium">
      {children}
    </th>
  );
}

function Td({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <td
      className={`px-4 py-2 text-neutral-800 ${className ?? ""}`.trim()}
    >
      {children}
    </td>
  );
}

function StatusBadge({
  status,
}: {
  status: ScrapeHealthRow["lastStatus"] | ScrapeRunLog["status"];
}) {
  if (status === null || status === undefined) {
    return (
      <span className="inline-block rounded px-2 py-0.5 text-xs font-medium text-neutral-500">
        —
      </span>
    );
  }
  const base = "inline-block rounded px-2 py-0.5 text-xs font-medium";
  if (status === "success") {
    return (
      <span className={`${base} bg-green-100 text-green-800`}>success</span>
    );
  }
  if (status === "failure") {
    return <span className={`${base} bg-red-100 text-red-800`}>failure</span>;
  }
  // status === "running"
  return (
    <span className={`${base} animate-pulse bg-blue-100 text-blue-800`}>
      running
    </span>
  );
}

function FailureCount({
  count,
  status,
}: {
  count: number;
  status: ScrapeHealthRow["lastStatus"];
}) {
  if (count === 0) {
    return <span className="text-neutral-500">0</span>;
  }
  const color =
    status === "failure" && count >= 3 ? "text-red-700 font-semibold" : "";
  return <span className={color}>{count}</span>;
}

function TablePlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

function formatDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function formatDuration(run: ScrapeRunLog): string {
  if (!run.finishedAt) return "—";
  const start = new Date(run.startedAt).getTime();
  const end = new Date(run.finishedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return "—";
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  const s = Math.round(ms / 100) / 10;
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s - m * 60);
  return `${m}m ${rem}s`;
}
