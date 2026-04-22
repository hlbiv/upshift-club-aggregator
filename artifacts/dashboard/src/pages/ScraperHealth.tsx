import { CheckCircle2, AlertTriangle, XCircle, Activity } from "lucide-react";
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
import { KpiStrip } from "../components/primitives/KpiStrip";
import { KpiCard } from "../components/primitives/KpiCard";
import {
  StatusBadge as StatusBadgePrimitive,
  toneForScrapeStatus,
} from "../components/primitives/StatusBadge";

/**
 * Scraper health dashboard.
 *
 *   GET /api/v1/admin/scrape-health         → ScrapeHealthList (rollup)
 *   GET /api/v1/admin/scrape-runs?limit=50  → ScrapeRunLogList (recent)
 */

export default function ScraperHealthPage() {
  const healthQuery = useListScrapeHealth();
  const runsQuery = useListScrapeRuns({ limit: 50 });

  // KPI rollup from the health rows. We bucket by `lastStatus` so the
  // strip mirrors the StatusBadge tones (ok / warn / fail / running)
  // and operators can eyeball "is anything red" without scanning the
  // table.
  const rollup = summarizeHealth(healthQuery.data?.rows);

  return (
    <AppShell>
      <PageHeader
        title="Scraper health"
        description="Rolling status per entity and the 50 most recent runs."
      />

      <KpiStrip cols={4}>
        <KpiCard
          label="Tracked entities"
          icon={Activity}
          tone="neutral"
          value={rollup.total.toLocaleString()}
          isLoading={healthQuery.isLoading}
          isError={healthQuery.isError}
        />
        <KpiCard
          label="OK"
          icon={CheckCircle2}
          tone="ok"
          value={rollup.ok.toLocaleString()}
          isLoading={healthQuery.isLoading}
          isError={healthQuery.isError}
        />
        <KpiCard
          label="Partial"
          icon={AlertTriangle}
          tone="warn"
          value={rollup.partial.toLocaleString()}
          isLoading={healthQuery.isLoading}
          isError={healthQuery.isError}
        />
        <KpiCard
          label="Failed"
          icon={XCircle}
          tone={rollup.failed > 0 ? "fail" : "neutral"}
          value={rollup.failed.toLocaleString()}
          isLoading={healthQuery.isLoading}
          isError={healthQuery.isError}
        />
      </KpiStrip>

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
  // Route through the design-system primitive so health rows match the
  // tone palette used on Scheduler / Dedup / Overview attention list.
  return (
    <StatusBadgePrimitive
      tone={toneForScrapeStatus(status)}
      label={status}
    />
  );
}

function summarizeHealth(rows: ScrapeHealthRow[] | undefined): {
  total: number;
  ok: number;
  partial: number;
  failed: number;
} {
  const acc = { total: 0, ok: 0, partial: 0, failed: 0 };
  if (!rows) return acc;
  for (const r of rows) {
    acc.total += 1;
    if (r.lastStatus === "ok") acc.ok += 1;
    else if (r.lastStatus === "partial") acc.partial += 1;
    else if (r.lastStatus === "failed") acc.failed += 1;
  }
  return acc;
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
