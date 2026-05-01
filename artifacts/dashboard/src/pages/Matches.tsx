import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { CalendarDays } from "lucide-react";
import { AppShell } from "../components/AppShell";
import { PageHeader } from "../components/primitives/PageHeader";

/**
 * Matches browse page.
 *
 *   GET /api/matches?page_size=50&page=N
 *
 * Simple table showing recent matches pulled from the matches table.
 * Columns: Date, Home Team, Score, Away Team, Source, Season.
 */

interface MatchItem {
  id: number;
  home_team_name: string;
  away_team_name: string;
  home_club_name: string | null;
  away_club_name: string | null;
  home_score: number | null;
  away_score: number | null;
  match_date: string | null;
  season: string | null;
  source: string | null;
  age_group: string | null;
  gender: string | null;
  status: string;
}

interface MatchListResponse {
  matches: MatchItem[];
  total: number;
  page: number;
  page_size: number;
}

const PAGE_SIZE = 50;

async function fetchMatches(page: number): Promise<MatchListResponse> {
  const url = `/api/matches?page=${page}&page_size=${PAGE_SIZE}`;
  const res = await fetch(url, { credentials: "include" });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json() as Promise<MatchListResponse>;
}

export default function MatchesPage() {
  const [page, setPage] = useState(1);

  const query = useQuery({
    queryKey: ["matches", page],
    queryFn: () => fetchMatches(page),
  });

  const data = query.data;
  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 1;

  return (
    <AppShell>
      <PageHeader
        title="Matches"
        description="Recent match records scraped from GotSport and other sources."
      />

      <div className="mb-4 flex items-center gap-2 text-sm text-slate-500">
        <CalendarDays aria-hidden className="h-4 w-4" />
        {data ? (
          <span>
            {data.total.toLocaleString()} total matches — page {data.page} of{" "}
            {totalPages}
          </span>
        ) : null}
      </div>

      <MatchesTable
        matches={data?.matches}
        isLoading={query.isLoading}
        error={query.error}
      />

      {totalPages > 1 && (
        <div className="mt-4 flex items-center gap-2">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1}
            className="rounded border border-slate-200 px-3 py-1 text-sm disabled:opacity-40"
          >
            Previous
          </button>
          <span className="text-sm text-slate-600">
            Page {page} / {totalPages}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages}
            className="rounded border border-slate-200 px-3 py-1 text-sm disabled:opacity-40"
          >
            Next
          </button>
        </div>
      )}
    </AppShell>
  );
}

function MatchesTable({
  matches,
  isLoading,
  error,
}: {
  matches: MatchItem[] | undefined;
  isLoading: boolean;
  error: unknown;
}) {
  if (isLoading) return <TablePlaceholder label="Loading…" />;
  if (error) return <TablePlaceholder label={`Failed to load: ${formatError(error)}`} />;
  if (!matches || matches.length === 0)
    return <TablePlaceholder label="No matches found." />;

  return (
    <div className="overflow-hidden rounded-lg border border-neutral-200">
      <table className="w-full border-collapse text-sm">
        <thead className="bg-neutral-50 text-left text-neutral-600">
          <tr>
            <Th>Date</Th>
            <Th>Home Team</Th>
            <Th>Score</Th>
            <Th>Away Team</Th>
            <Th>Source</Th>
            <Th>Season</Th>
          </tr>
        </thead>
        <tbody>
          {matches.map((m, i) => (
            <tr
              key={m.id}
              className={i % 2 === 0 ? "bg-white" : "bg-neutral-50/50"}
            >
              <Td>{formatDate(m.match_date)}</Td>
              <Td>
                <span className="font-medium">
                  {m.home_club_name ?? m.home_team_name}
                </span>
              </Td>
              <Td>
                <ScoreCell
                  homeScore={m.home_score}
                  awayScore={m.away_score}
                  status={m.status}
                />
              </Td>
              <Td>
                <span className="font-medium">
                  {m.away_club_name ?? m.away_team_name}
                </span>
              </Td>
              <Td className="text-neutral-500">{m.source ?? "—"}</Td>
              <Td className="text-neutral-500">{m.season ?? "—"}</Td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ScoreCell({
  homeScore,
  awayScore,
  status,
}: {
  homeScore: number | null;
  awayScore: number | null;
  status: string;
}) {
  if (homeScore !== null && awayScore !== null) {
    return (
      <span className="font-mono font-semibold">
        {homeScore} – {awayScore}
      </span>
    );
  }
  if (status && status !== "scheduled") {
    return <span className="text-neutral-400 capitalize">{status}</span>;
  }
  return <span className="text-neutral-400">TBD</span>;
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
    <td className={`px-4 py-2 text-neutral-800 ${className ?? ""}`.trim()}>
      {children}
    </td>
  );
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
  return d.toLocaleDateString();
}

function formatError(err: unknown): string {
  if (!err) return "Network error";
  if (err instanceof Error) return err.message;
  return String(err);
}
