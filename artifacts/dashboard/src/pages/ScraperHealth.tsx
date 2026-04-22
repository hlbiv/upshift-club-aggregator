import {
  useListScrapeHealth,
  useListScrapeRuns,
  type ScrapeHealthList,
  type ScrapeHealthRow,
  type ScrapeRunLog,
  type ScrapeRunLogList,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";

/**
 * Scraper health dashboard.
 *
 *   GET /api/v1/admin/scrape-health         → ScrapeHealthList (rollup)
 *   GET /api/v1/admin/scrape-runs?limit=50  → ScrapeRunLogList (recent)
 */

export default function ScraperHealthPage() {
  const healthQuery = useListScrapeHealth();
  const runsQuery = useListScrapeRuns({ limit: 50 });

  return (
    <AppShell>
      <PageHeader
        title="Scraper health"
        description="Rolling status per entity and the 50 most recent runs."
      />

      <section className="mb-10" aria-labelledby="rollup-heading">
        <h2
          id="rollup-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Rollup
        </h2>
        <HealthTable
          data={healthQuery.data}
          isLoading={healthQuery.isLoading}
          error={healthQuery.error}
        />
      </section>

      <section aria-labelledby="runs-heading">
        <h2
          id="runs-heading"
          className="mb-3 text-lg font-semibold text-neutral-900"
        >
          Recent runs
        </h2>
        <RunsTable
          data={runsQuery.data}
          isLoading={runsQuery.isLoading}
          error={runsQuery.error}
        />
      </section>
    </AppShell>
  );
}

function formatError(err: unknown): string {
  if (!err) return "Network error";
  if (err instanceof Error) {
    // The customFetch ApiError class attaches a numeric `status` — surface
    // that verbatim so operators can grep the log lines. Cast via unknown
    // so TS lets us narrow an arbitrary property off of an Error subclass.
    const status = (err as unknown as { status?: unknown }).status;
    if (typeof status === "number") return `HTTP ${status}`;
    return err.message;
  }
  return String(err);
}

function HealthTable({
  data,
  isLoading,
  error,
}: {
  data: ScrapeHealthList | undefined;
  isLoading: boolean;
  error: unknown;
}) {
  if (isLoading) return <TablePlaceholder label="Loading…" />;
  if (error)
    return <TablePlaceholder label={`Failed to load: ${formatError(error)}`} />;
  const rows = data?.rows ?? [];
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

function RunsTable({
  data,
  isLoading,
  error,
}: {
  data: ScrapeRunLogList | undefined;
  isLoading: boolean;
  error: unknown;
}) {
  if (isLoading) return <TablePlaceholder label="Loading…" />;
  if (error)
    return <TablePlaceholder label={`Failed to load: ${formatError(error)}`} />;
  const runs = data?.runs ?? [];
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
              <Td>{run.scraperKey}</Td>
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
  // ok → green success; partial → amber (it finished but not cleanly);
  // failed → red; running → blue pulse.
  if (status === "ok") {
    return <span className={`${base} bg-green-100 text-green-800`}>ok</span>;
  }
  if (status === "partial") {
    return (
      <span className={`${base} bg-amber-100 text-amber-800`}>partial</span>
    );
  }
  if (status === "failed") {
    return <span className={`${base} bg-red-100 text-red-800`}>failed</span>;
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
    status === "failed" && count >= 3 ? "text-red-700 font-semibold" : "";
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
  if (!run.completedAt) return "—";
  const start = new Date(run.startedAt).getTime();
  const end = new Date(run.completedAt).getTime();
  if (Number.isNaN(start) || Number.isNaN(end) || end < start) return "—";
  const ms = end - start;
  if (ms < 1000) return `${ms}ms`;
  const s = Math.round(ms / 100) / 10;
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const rem = Math.round(s - m * 60);
  return `${m}m ${rem}s`;
}
