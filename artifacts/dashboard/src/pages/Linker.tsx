import { useCallback, useEffect, useRef, useState } from "react";
import {
  getSearchClubsQueryKey,
  useSearchClubs,
  type Club,
} from "@workspace/api-client-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";

/**
 * Linker Resolution page.
 *
 *   GET  /api/v1/admin/linker/unmatched?page=1&page_size=50
 *     → { items: [{ raw_name: string, total_count: number }], total: number, page: number, page_size: number }
 *   POST /api/v1/admin/linker/resolve
 *     body: { raw_name: string, canonical_club_id: number }
 *     → { ok: true, alias_id: number, already_existed: boolean }
 *   POST /api/v1/admin/linker/ignore
 *     body: { raw_name: string, reason?: string }
 *     → { ok: true }
 *
 * Uses adminFetch (direct fetch) for the unmatched list and mutations.
 * Uses useSearchClubs from @workspace/api-client-react for the per-row
 * club typeahead — same hook as GlobalSearch.tsx.
 *
 * Each row manages its own search state independently so 50 open rows
 * don't fan out to 50 simultaneous network calls.
 */

const PAGE_SIZE = 50;
const FLASH_DURATION_MS = 3000;

interface UnmatchedItem {
  raw_name: string;
  total_count: number;
}

interface UnmatchedResponse {
  items: UnmatchedItem[];
  total: number;
  page: number;
  page_size: number;
}

async function adminPost(path: string, body: unknown): Promise<unknown> {
  const res = await fetch(path, {
    method: "POST",
    credentials: "include",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

export default function LinkerPage() {
  const [page, setPage] = useState(1);
  const [items, setItems] = useState<UnmatchedItem[]>([]);
  const [total, setTotal] = useState(0);
  const [isLoading, setIsLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [flash, setFlash] = useState<string | null>(null);
  const flashTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function showFlash(msg: string) {
    if (flashTimer.current) clearTimeout(flashTimer.current);
    setFlash(msg);
    flashTimer.current = setTimeout(() => setFlash(null), FLASH_DURATION_MS);
  }

  useEffect(() => {
    return () => {
      if (flashTimer.current) clearTimeout(flashTimer.current);
    };
  }, []);

  const loadPage = useCallback(async (p: number) => {
    setIsLoading(true);
    setLoadError(null);
    try {
      const res = await fetch(
        `/api/v1/admin/linker/unmatched?page=${p}&page_size=${PAGE_SIZE}`,
        { credentials: "include" },
      );
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = (await res.json()) as UnmatchedResponse;
      setItems(data.items ?? []);
      setTotal(data.total ?? 0);
    } catch (err) {
      setLoadError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadPage(page);
  }, [page, loadPage]);

  function removeRow(rawName: string) {
    setItems((prev) => prev.filter((it) => it.raw_name !== rawName));
    setTotal((t) => Math.max(0, t - 1));
  }

  async function handleResolve(rawName: string, clubId: number, clubName: string) {
    await adminPost("/api/v1/admin/linker/resolve", {
      raw_name: rawName,
      canonical_club_id: clubId,
    });
    removeRow(rawName);
    showFlash(`Mapped "${rawName}" → ${clubName}`);
  }

  async function handleIgnore(rawName: string) {
    await adminPost("/api/v1/admin/linker/ignore", {
      raw_name: rawName,
      reason: "bracket_placeholder",
    });
    removeRow(rawName);
    showFlash(`Ignored "${rawName}"`);
  }

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));

  return (
    <AppShell>
      <PageHeader
        title="Linker Resolution"
        description="Map unmatched raw team names to canonical clubs"
      />

      {flash !== null ? (
        <div
          role="status"
          className="mb-4 flex items-center justify-between rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-800"
        >
          <span>{flash}</span>
          <button
            type="button"
            aria-label="Dismiss"
            onClick={() => setFlash(null)}
            className="ml-4 text-green-700 hover:text-green-900"
          >
            ×
          </button>
        </div>
      ) : null}

      {isLoading ? (
        <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-8 text-center text-sm text-neutral-500">
          Loading…
        </div>
      ) : loadError !== null ? (
        <div
          role="alert"
          className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
        >
          Failed to load: {loadError}
        </div>
      ) : items.length === 0 ? (
        <div className="rounded-lg border border-dashed border-neutral-300 bg-white px-4 py-10 text-center text-sm text-neutral-500">
          <p className="font-medium text-neutral-700">
            No unmatched names — linker queue is clear ✓
          </p>
        </div>
      ) : (
        <>
          <div className="overflow-hidden rounded-lg border border-neutral-200">
            <table className="w-full border-collapse text-sm">
              <thead className="bg-neutral-50 text-left text-neutral-600">
                <tr>
                  <Th>Raw Team Name</Th>
                  <Th>Occurrences</Th>
                  <Th>Map to Club</Th>
                  <Th>Actions</Th>
                </tr>
              </thead>
              <tbody>
                {items.map((item, i) => (
                  <LinkerRow
                    key={item.raw_name}
                    item={item}
                    stripe={i % 2 !== 0}
                    onResolve={handleResolve}
                    onIgnore={handleIgnore}
                  />
                ))}
              </tbody>
            </table>
          </div>

          {total > PAGE_SIZE ? (
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
                  disabled={page <= 1}
                  onClick={() => setPage((p) => p - 1)}
                  className="rounded-md border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Previous
                </button>
                <button
                  type="button"
                  disabled={page >= totalPages}
                  onClick={() => setPage((p) => p + 1)}
                  className="rounded-md border border-neutral-300 bg-white px-3 py-1 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Next
                </button>
              </div>
            </nav>
          ) : null}
        </>
      )}
    </AppShell>
  );
}

// ---------------------------------------------------------------------------
// Per-row component — manages its own typeahead state
// ---------------------------------------------------------------------------

interface LinkerRowProps {
  item: UnmatchedItem;
  stripe: boolean;
  onResolve: (rawName: string, clubId: number, clubName: string) => Promise<void>;
  onIgnore: (rawName: string) => Promise<void>;
}

function LinkerRow({ item, stripe, onResolve, onIgnore }: LinkerRowProps) {
  const [inputValue, setInputValue] = useState("");
  const [debounced, setDebounced] = useState("");
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [selectedClub, setSelectedClub] = useState<Club | null>(null);
  const [saving, setSaving] = useState(false);
  const [ignoring, setIgnoring] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Debounce input 200ms before firing search
  useEffect(() => {
    const t = window.setTimeout(() => setDebounced(inputValue.trim()), 200);
    return () => window.clearTimeout(t);
  }, [inputValue]);

  const enabled = debounced.length >= 2 && selectedClub === null;
  const params = { q: debounced };
  const searchQuery = useSearchClubs(params, {
    query: { queryKey: getSearchClubsQueryKey(params), enabled },
  });

  const results: Club[] = searchQuery.data?.results ?? [];

  // Close dropdown on click-outside
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  function handleInputChange(value: string) {
    setInputValue(value);
    setSelectedClub(null);
    setDropdownOpen(value.trim().length >= 2);
  }

  function handleSelectClub(club: Club) {
    setSelectedClub(club);
    setInputValue("");
    setDropdownOpen(false);
  }

  async function handleSave() {
    if (!selectedClub) return;
    setSaving(true);
    try {
      await onResolve(item.raw_name, selectedClub.id, selectedClub.club_name_canonical);
    } finally {
      setSaving(false);
    }
  }

  async function handleIgnoreClick() {
    setIgnoring(true);
    try {
      await onIgnore(item.raw_name);
    } finally {
      setIgnoring(false);
    }
  }

  const rowCls = stripe ? "bg-neutral-50/50" : "bg-white";

  return (
    <tr className={rowCls}>
      <Td>
        <span className="font-medium text-neutral-900">{item.raw_name}</span>
      </Td>
      <Td>
        <span className="tabular-nums">{item.total_count.toLocaleString()}</span>
      </Td>
      <Td>
        <div ref={containerRef} className="relative w-64">
          {selectedClub !== null ? (
            <span className="inline-flex items-center gap-1.5 rounded-full border border-indigo-200 bg-indigo-50 px-2.5 py-1 text-xs font-medium text-indigo-800">
              {selectedClub.club_name_canonical}
              <button
                type="button"
                aria-label="Clear selection"
                onClick={() => {
                  setSelectedClub(null);
                  setInputValue("");
                }}
                className="text-indigo-500 hover:text-indigo-800"
              >
                ×
              </button>
            </span>
          ) : (
            <>
              <input
                type="text"
                value={inputValue}
                placeholder="Search clubs…"
                onChange={(e) => handleInputChange(e.target.value)}
                onFocus={() => {
                  if (debounced.length >= 2) setDropdownOpen(true);
                }}
                className="w-full rounded-md border border-neutral-300 bg-white px-3 py-1.5 text-sm text-neutral-900 placeholder:text-neutral-400 focus:border-indigo-400 focus:outline-none focus:ring-1 focus:ring-indigo-200"
              />
              {dropdownOpen && results.length > 0 ? (
                <ul
                  role="listbox"
                  className="absolute left-0 right-0 z-20 mt-1 max-h-48 overflow-y-auto rounded-md border border-neutral-200 bg-white shadow-lg"
                >
                  {results.map((club) => (
                    <li key={club.id}>
                      <button
                        type="button"
                        role="option"
                        aria-selected={false}
                        onClick={() => handleSelectClub(club)}
                        className="flex w-full flex-col px-3 py-2 text-left text-sm hover:bg-indigo-50"
                      >
                        <span className="font-medium text-neutral-900">
                          {club.club_name_canonical}
                        </span>
                        {(club.city || club.state) ? (
                          <span className="text-xs text-neutral-500">
                            {[club.city, club.state].filter(Boolean).join(", ")}
                          </span>
                        ) : null}
                      </button>
                    </li>
                  ))}
                </ul>
              ) : null}
            </>
          )}
        </div>
      </Td>
      <Td>
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={selectedClub === null || saving}
            onClick={() => void handleSave()}
            className="rounded-md bg-neutral-900 px-3 py-1 text-xs font-medium text-white hover:bg-neutral-800 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {saving ? "Saving…" : "Save"}
          </button>
          <button
            type="button"
            disabled={ignoring}
            onClick={() => void handleIgnoreClick()}
            className="rounded-md border border-neutral-300 bg-white px-3 py-1 text-xs font-medium text-neutral-700 hover:bg-neutral-100 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {ignoring ? "Ignoring…" : "Ignore"}
          </button>
        </div>
      </Td>
    </tr>
  );
}

// ---------------------------------------------------------------------------
// Table helpers
// ---------------------------------------------------------------------------

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
      className={`px-4 py-2 align-top text-neutral-800 ${className ?? ""}`.trim()}
    >
      {children}
    </td>
  );
}
