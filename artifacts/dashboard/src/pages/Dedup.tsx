import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  useListClubDuplicates,
  type ClubDuplicate,
  type ClubDuplicateList,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";
import {
  StatusBadge as StatusBadgePrimitive,
  toneForDedupStatus,
} from "../components/primitives/StatusBadge";
import { useQueueShortcuts } from "../hooks/useQueueShortcuts";

/**
 * Dedup review queue.
 *
 *   GET /api/v1/admin/dedup/clubs?status=pending&limit=50&page=N
 *     → ClubDuplicateList (pairs, total, page, pageSize)
 *
 * Shows one row per candidate club-duplicate pair. A reviewer clicks
 * "Review" to open the detail view, or uses the status filter to see
 * merged / rejected / all pairs. Pagination appears only when total
 * exceeds page size.
 *
 * Migrated from `adminFetch()` to the Orval-generated `useListClubDuplicates`
 * hook (Workstream A). The hook encodes query params and routes through
 * the shared customFetch mutator, so the session cookie still travels.
 */

const PAGE_SIZE = 50;

type StatusFilter = "pending" | "merged" | "rejected" | "all";

export default function DedupPage() {
  const [status, setStatus] = useState<StatusFilter>("pending");
  const [page, setPage] = useState(1);
  const location = useLocation();
  const flash = readFlash(location.state);
  const [flashVisible, setFlashVisible] = useState<string | null>(flash);

  useEffect(() => {
    if (flash) setFlashVisible(flash);
  }, [flash]);

  // `status=all` is the server default — omit it so the URL matches the
  // pre-migration shape (no `status` param at all).
  const params =
    status === "all"
      ? { limit: PAGE_SIZE, page }
      : { status, limit: PAGE_SIZE, page };
  const query = useListClubDuplicates(params);

  const pairs = query.data?.pairs ?? [];
  const total = query.data?.total ?? 0;
  const [cursor, setCursor] = useState(0);
  const navigateRouter = useNavigate();
  const pageStart = (query.data?.page ?? page) - 1;
  const positionGlobal =
    pairs.length === 0 ? 0 : pageStart * PAGE_SIZE + cursor + 1;

  // Clamp the cursor when results shrink (status switch, page nav).
  useEffect(() => {
    if (cursor >= pairs.length) setCursor(Math.max(0, pairs.length - 1));
  }, [pairs.length, cursor]);

  useQueueShortcuts({
    enabled: pairs.length > 0,
    onNext: () => setCursor((c) => Math.min(pairs.length - 1, c + 1)),
    onPrev: () => setCursor((c) => Math.max(0, c - 1)),
    onOpen: () => {
      const pair = pairs[cursor];
      if (pair) navigateRouter(`/dedup/${pair.id}`);
    },
  });

  return (
    <AppShell>
      <PageHeader
        title="Dedup review"
        description="Candidate club-duplicate pairs queued by the dedup scraper. Pick a winner to merge, or reject the pair."
      />
      {pairs.length > 0 ? (
        <p
          className="mb-4 inline-flex items-center gap-2 rounded-full border border-indigo-100 bg-indigo-50 px-3 py-1 text-xs font-medium text-indigo-700"
          aria-live="polite"
        >
          Pair{" "}
          <span className="tabular-nums">
            {positionGlobal} of {total.toLocaleString()}
          </span>
          <span className="text-indigo-400">·</span>
          <span className="text-indigo-500">
            J/K to move, Enter to open
          </span>
        </p>
      ) : null}

      {flashVisible ? (
        <div
          role="status"
          className="mb-4 flex items-center justify-between rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-800"
        >
          <span>{flashVisible}</span>
          <button
            type="button"
            aria-label="Dismiss"
            onClick={() => setFlashVisible(null)}
            className="ml-4 text-green-700 hover:text-green-900"
          >
            ×
          </button>
        </div>
      ) : null}

      <div className="mb-4 flex items-center gap-3">
        <label
          htmlFor="status-filter"
          className="text-sm font-medium text-neutral-700"
        >
          Status
        </label>
        <select
          id="status-filter"
          value={status}
          onChange={(e) => {
            setStatus(e.target.value as StatusFilter);
            setPage(1);
          }}
          className="rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-sm text-neutral-800 focus:border-neutral-900 focus:outline-none focus:ring-1 focus:ring-neutral-900"
        >
          <option value="pending">Pending</option>
          <option value="merged">Merged</option>
          <option value="rejected">Rejected</option>
          <option value="all">All</option>
        </select>
      </div>

      <DedupTable
        data={query.data}
        isLoading={query.isLoading}
        error={query.error}
        cursor={cursor}
      />

      {query.data && query.data.total > PAGE_SIZE ? (
        <Pagination
          page={page}
          pageSize={PAGE_SIZE}
          total={query.data.total}
          onChange={setPage}
        />
      ) : null}
    </AppShell>
  );
}

function DedupTable({
  data,
  isLoading,
  error,
  cursor,
}: {
  data: ClubDuplicateList | undefined;
  isLoading: boolean;
  error: unknown;
  cursor: number;
}) {
  const navigate = useNavigate();

  if (isLoading) return <TablePlaceholder label="Loading…" />;
  if (error)
    return <TablePlaceholder label={`Failed to load: ${formatError(error)}`} />;

  const pairs = data?.pairs ?? [];
  if (pairs.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-10 text-center text-sm text-neutral-500">
        <p className="mb-1 font-medium text-neutral-700">
          No pending dedup pairs.
        </p>
        <p>
          Run{" "}
          <code className="rounded bg-neutral-100 px-1 py-0.5 text-xs text-neutral-800">
            python3 scraper/run.py --source club-dedup --persist
          </code>{" "}
          to refresh the queue.
        </p>
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-lg border border-neutral-200">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-neutral-50 text-left text-neutral-600">
          <tr>
            <Th>ID</Th>
            <Th>Left club</Th>
            <Th>Right club</Th>
            <Th>Score</Th>
            <Th>Method</Th>
            <Th>Status</Th>
            <Th>Created</Th>
            <Th>Actions</Th>
          </tr>
        </thead>
        <tbody>
          {pairs.map((pair, i) => (
            <tr
              key={pair.id}
              aria-current={i === cursor ? "true" : undefined}
              className={
                i === cursor
                  ? "bg-indigo-50/60 ring-2 ring-inset ring-indigo-300"
                  : i % 2 === 0
                    ? "bg-white"
                    : "bg-neutral-50/50"
              }
            >
              <Td>{pair.id}</Td>
              <Td>
                <ClubCell snapshot={pair.leftSnapshot} id={pair.leftClubId} />
              </Td>
              <Td>
                <ClubCell snapshot={pair.rightSnapshot} id={pair.rightClubId} />
              </Td>
              <Td>{pair.score.toFixed(3)}</Td>
              <Td className="text-neutral-500">{pair.method}</Td>
              <Td>
                <StatusBadge status={pair.status} />
              </Td>
              <Td>{formatDate(pair.createdAt)}</Td>
              <Td>
                <button
                  type="button"
                  onClick={() => navigate(`/dedup/${pair.id}`)}
                  className="rounded-md border border-neutral-300 bg-white px-3 py-1 text-xs font-medium text-neutral-800 transition-colors hover:bg-neutral-100"
                >
                  Review
                </button>
              </Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Pagination({
  page,
  pageSize,
  total,
  onChange,
}: {
  page: number;
  pageSize: number;
  total: number;
  onChange: (p: number) => void;
}) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const canPrev = page > 1;
  const canNext = page < totalPages;

  return (
    <nav
      aria-label="Pagination"
      className="mt-4 flex items-center justify-between text-sm text-neutral-600"
    >
      <span>
        Page {page} of {totalPages} — {total} total
      </span>
      <div className="flex gap-2">
        <button
          type="button"
          disabled={!canPrev}
          onClick={() => onChange(page - 1)}
          className="rounded-md border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Previous
        </button>
        <button
          type="button"
          disabled={!canNext}
          onClick={() => onChange(page + 1)}
          className="rounded-md border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
        >
          Next
        </button>
      </div>
    </nav>
  );
}

function ClubCell({
  snapshot,
  id,
}: {
  snapshot: Record<string, unknown>;
  id: number;
}) {
  const name = useMemo(() => snapshotName(snapshot), [snapshot]);
  return (
    <div className="flex flex-col">
      <span className="font-medium text-neutral-900">{name}</span>
      <span className="text-xs text-neutral-500">id: {id}</span>
    </div>
  );
}

// --- shared helpers (exported for DedupDetail) ---------------------------

export function snapshotName(snapshot: Record<string, unknown>): string {
  const keys = [
    "clubNameCanonical",
    "club_name_canonical",
    "name",
    "clubName",
    "club_name",
  ];
  for (const k of keys) {
    const v = snapshot[k];
    if (typeof v === "string" && v.trim().length > 0) return v;
  }
  return "—";
}

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
    <td className={`px-4 py-2 align-top text-neutral-800 ${className ?? ""}`.trim()}>
      {children}
    </td>
  );
}

export function StatusBadge({ status }: { status: ClubDuplicate["status"] }) {
  // Route through the design-system primitive so dedup pills match the
  // tone palette used on Scheduler / ScraperHealth / Overview.
  return (
    <StatusBadgePrimitive
      tone={toneForDedupStatus(status)}
      label={status}
    />
  );
}

function TablePlaceholder({ label }: { label: string }) {
  return (
    <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
      {label}
    </div>
  );
}

export function formatDate(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

export function formatError(err: unknown): string {
  if (!err) return "Network error";
  if (err instanceof Error) {
    const status = (err as unknown as { status?: unknown }).status;
    if (typeof status === "number") return `HTTP ${status}`;
    return err.message;
  }
  return String(err);
}

function readFlash(state: unknown): string | null {
  if (state && typeof state === "object" && "flash" in state) {
    const v = (state as { flash: unknown }).flash;
    if (typeof v === "string") return v;
  }
  return null;
}
