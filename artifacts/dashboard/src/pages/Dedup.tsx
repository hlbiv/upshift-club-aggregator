import { useEffect, useMemo, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  ClubDuplicateList,
  type ClubDuplicate,
} from "@hlbiv/api-zod/admin";
import { adminFetch } from "../lib/api";
import AdminNav from "../components/AdminNav";

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
 * API dependency: phase-C.3 `feat/admin-dedup-routes`. Until that ships,
 * the fetch will 404 live — tests mock fetch so CI still passes.
 */

const PAGE_SIZE = 50;

type StatusFilter = "pending" | "merged" | "rejected" | "all";

type FetchState =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ok"; data: ClubDuplicateList };

export default function DedupPage() {
  const [status, setStatus] = useState<StatusFilter>("pending");
  const [page, setPage] = useState(1);
  const [state, setState] = useState<FetchState>({ kind: "loading" });
  const location = useLocation();
  const flash = readFlash(location.state);
  const [flashVisible, setFlashVisible] = useState<string | null>(flash);

  useEffect(() => {
    if (flash) setFlashVisible(flash);
  }, [flash]);

  useEffect(() => {
    let cancelled = false;
    setState({ kind: "loading" });

    const params = new URLSearchParams();
    if (status !== "all") params.set("status", status);
    params.set("limit", String(PAGE_SIZE));
    params.set("page", String(page));

    adminFetch(`/api/v1/admin/dedup/clubs?${params.toString()}`)
      .then(async (res) => {
        if (cancelled) return;
        if (!res.ok) {
          setState({ kind: "error", message: `HTTP ${res.status}` });
          return;
        }
        const raw = await res.json();
        const parsed = ClubDuplicateList.safeParse(raw);
        if (!parsed.success) {
          setState({
            kind: "error",
            message: `Invalid response: ${parsed.error.message}`,
          });
          return;
        }
        setState({ kind: "ok", data: parsed.data });
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setState({
          kind: "error",
          message: e instanceof Error ? e.message : "Network error",
        });
      });

    return () => {
      cancelled = true;
    };
  }, [status, page]);

  return (
    <main className="mx-auto max-w-6xl px-6 py-8">
      <AdminNav />
      <header className="mb-6">
        <h1 className="text-2xl font-semibold text-neutral-900">
          Dedup review
        </h1>
        <p className="text-sm text-neutral-500">
          Candidate club-duplicate pairs queued by the dedup scraper. Pick a
          winner to merge, or reject the pair.
        </p>
      </header>

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

      <DedupTable state={state} />

      {state.kind === "ok" && state.data.total > PAGE_SIZE ? (
        <Pagination
          page={page}
          pageSize={PAGE_SIZE}
          total={state.data.total}
          onChange={setPage}
        />
      ) : null}
    </main>
  );
}

function DedupTable({ state }: { state: FetchState }) {
  const navigate = useNavigate();

  if (state.kind === "loading") return <TablePlaceholder label="Loading…" />;
  if (state.kind === "error")
    return <TablePlaceholder label={`Failed to load: ${state.message}`} />;

  const pairs = state.data.pairs;
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
              className={i % 2 === 0 ? "bg-white" : "bg-neutral-50/50"}
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
  const base = "inline-block rounded px-2 py-0.5 text-xs font-medium";
  if (status === "pending") {
    return (
      <span className={`${base} bg-yellow-100 text-yellow-800`}>pending</span>
    );
  }
  if (status === "merged") {
    return (
      <span className={`${base} bg-green-100 text-green-800`}>merged</span>
    );
  }
  return (
    <span className={`${base} bg-neutral-200 text-neutral-700`}>rejected</span>
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

function readFlash(state: unknown): string | null {
  if (state && typeof state === "object" && "flash" in state) {
    const v = (state as { flash: unknown }).flash;
    if (typeof v === "string") return v;
  }
  return null;
}
