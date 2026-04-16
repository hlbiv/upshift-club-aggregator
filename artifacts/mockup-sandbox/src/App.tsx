import { useCallback, useEffect, useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Spinner } from "@/components/ui/spinner";
import { Separator } from "@/components/ui/separator";

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

const API_BASE = "/api";

async function api<T>(path: string, params?: Record<string, string>): Promise<T> {
  const url = new URL(`${API_BASE}${path}`, window.location.origin);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v) url.searchParams.set(k, v);
    }
  }
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

// ---------------------------------------------------------------------------
// Types (minimal, matching API responses)
// ---------------------------------------------------------------------------

interface Club {
  id: number;
  name: string;
  normalized_name?: string;
  state?: string | null;
  gender_program?: string | null;
  website_url?: string | null;
  leagues?: string[];
  tier?: number | null;
}

interface League {
  id: number;
  name: string;
  abbreviation?: string | null;
  tier?: number | null;
  gender?: string | null;
  sport?: string | null;
  club_count?: number;
}

interface Coach {
  id: number;
  name: string;
  title?: string | null;
  club_name?: string | null;
  confidence?: number | null;
}

interface CoverageStats {
  total_clubs: number;
  states: { state: string; club_count: number }[];
  leagues: { league_name: string; club_count: number }[];
  website_coverage_pct: number;
}

interface DuplicateCluster {
  normalized_name: string;
  clubs: Club[];
}

// ---------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------

function useFetch<T>(path: string, params?: Record<string, string>) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api<T>(path, params)
      .then((d) => {
        if (!cancelled) setData(d);
      })
      .catch((e) => {
        if (!cancelled) setError(e.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, JSON.stringify(params)]);

  return { data, loading, error };
}

// ---------------------------------------------------------------------------
// Dashboard Tab
// ---------------------------------------------------------------------------

function DashboardTab() {
  const { data, loading, error } = useFetch<CoverageStats>(
    "/analytics/coverage"
  );

  if (loading) {
    return (
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-32" />
        ))}
      </div>
    );
  }
  if (error) return <ErrorBox message={error} />;
  if (!data) return null;

  const topStates = [...data.states]
    .sort((a, b) => b.club_count - a.club_count)
    .slice(0, 12);
  const topLeagues = [...data.leagues]
    .sort((a, b) => b.club_count - a.club_count)
    .slice(0, 12);

  return (
    <div className="space-y-6">
      {/* Stats cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatCard title="Total Clubs" value={data.total_clubs.toLocaleString()} />
        <StatCard title="States Covered" value={String(data.states.length)} />
        <StatCard title="Leagues" value={String(data.leagues.length)} />
        <StatCard
          title="Website Coverage"
          value={`${data.website_coverage_pct.toFixed(1)}%`}
        />
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Top states */}
        <Card>
          <CardHeader>
            <CardTitle>Top States</CardTitle>
            <CardDescription>By club count</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {topStates.map((s) => (
                <div key={s.state} className="flex items-center justify-between">
                  <span className="text-sm font-medium">{s.state}</span>
                  <div className="flex items-center gap-2">
                    <div
                      className="h-2 rounded-full bg-primary"
                      style={{
                        width: `${(s.club_count / topStates[0].club_count) * 120}px`,
                      }}
                    />
                    <span className="text-xs text-muted-foreground w-8 text-right">
                      {s.club_count}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Top leagues */}
        <Card>
          <CardHeader>
            <CardTitle>Top Leagues</CardTitle>
            <CardDescription>By club count</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {topLeagues.map((l) => (
                <div key={l.league_name} className="flex items-center justify-between">
                  <span className="text-sm font-medium truncate max-w-[200px]">
                    {l.league_name}
                  </span>
                  <div className="flex items-center gap-2">
                    <div
                      className="h-2 rounded-full bg-primary"
                      style={{
                        width: `${(l.club_count / topLeagues[0].club_count) * 120}px`,
                      }}
                    />
                    <span className="text-xs text-muted-foreground w-8 text-right">
                      {l.club_count}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function StatCard({ title, value }: { title: string; value: string }) {
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardDescription>{title}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold">{value}</div>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Clubs Tab
// ---------------------------------------------------------------------------

function ClubsTab() {
  const [query, setQuery] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [results, setResults] = useState<Club[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [searched, setSearched] = useState(false);

  const search = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      try {
        const params: Record<string, string> = {
          page: String(p),
          page_size: "25",
        };
        if (query) params.name = query;
        if (stateFilter) params.state = stateFilter;

        const data = await api<{ clubs: Club[]; total: number }>(
          "/clubs/search",
          params
        );
        setResults(data.clubs ?? []);
        setTotal(data.total ?? 0);
        setSearched(true);
      } catch {
        setResults([]);
        setTotal(0);
      } finally {
        setLoading(false);
      }
    },
    [query, stateFilter]
  );

  // Load initial data
  useEffect(() => {
    search(1);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="space-y-4">
      <div className="flex gap-2 flex-wrap">
        <Input
          placeholder="Search clubs..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && search(1)}
          className="max-w-xs"
        />
        <Input
          placeholder="State (e.g. GA)"
          value={stateFilter}
          onChange={(e) => setStateFilter(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === "Enter" && search(1)}
          className="max-w-[120px]"
        />
        <Button onClick={() => search(1)} disabled={loading}>
          {loading ? <Spinner className="mr-2" /> : null}
          Search
        </Button>
      </div>

      {searched && (
        <p className="text-sm text-muted-foreground">
          {total.toLocaleString()} club{total !== 1 ? "s" : ""} found
        </p>
      )}

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>State</TableHead>
            <TableHead>Gender</TableHead>
            <TableHead>Leagues</TableHead>
            <TableHead>Website</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading && !results.length ? (
            Array.from({ length: 5 }).map((_, i) => (
              <TableRow key={i}>
                {Array.from({ length: 5 }).map((__, j) => (
                  <TableCell key={j}>
                    <Skeleton className="h-4 w-full" />
                  </TableCell>
                ))}
              </TableRow>
            ))
          ) : results.length === 0 ? (
            <TableRow>
              <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                {searched ? "No clubs found" : "Search for clubs above"}
              </TableCell>
            </TableRow>
          ) : (
            results.map((club) => (
              <TableRow key={club.id}>
                <TableCell className="font-medium">{club.name}</TableCell>
                <TableCell>{club.state ?? "-"}</TableCell>
                <TableCell>
                  {club.gender_program ? (
                    <Badge variant="outline">{club.gender_program}</Badge>
                  ) : (
                    "-"
                  )}
                </TableCell>
                <TableCell>
                  <div className="flex gap-1 flex-wrap">
                    {(club.leagues ?? []).map((l) => (
                      <Badge key={l} variant="secondary" className="text-xs">
                        {l}
                      </Badge>
                    ))}
                  </div>
                </TableCell>
                <TableCell>
                  {club.website_url ? (
                    <a
                      href={club.website_url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-xs text-blue-500 hover:underline truncate max-w-[200px] block"
                    >
                      {new URL(club.website_url).hostname}
                    </a>
                  ) : (
                    <span className="text-muted-foreground text-xs">-</span>
                  )}
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {total > 25 && (
        <div className="flex items-center justify-center gap-2 pt-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1 || loading}
            onClick={() => search(page - 1)}
          >
            Previous
          </Button>
          <span className="text-sm text-muted-foreground">
            Page {page} of {Math.ceil(total / 25)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 25) || loading}
            onClick={() => search(page + 1)}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Leagues Tab
// ---------------------------------------------------------------------------

function LeaguesTab() {
  const { data, loading, error } = useFetch<{ leagues: League[] }>("/leagues");

  if (loading) {
    return (
      <div className="space-y-2">
        {Array.from({ length: 8 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  }
  if (error) return <ErrorBox message={error} />;
  if (!data?.leagues?.length)
    return <p className="text-muted-foreground">No leagues found</p>;

  const byTier = new Map<number, League[]>();
  for (const l of data.leagues) {
    const t = l.tier ?? 0;
    if (!byTier.has(t)) byTier.set(t, []);
    byTier.get(t)!.push(l);
  }

  const tierLabel = (t: number) =>
    t === 1
      ? "Tier 1 - National"
      : t === 2
        ? "Tier 2 - Regional"
        : t === 3
          ? "Tier 3 - State / Other"
          : `Tier ${t}`;

  return (
    <div className="space-y-6">
      {[...byTier.entries()]
        .sort(([a], [b]) => a - b)
        .map(([tier, leagues]) => (
          <div key={tier}>
            <h3 className="text-sm font-semibold text-muted-foreground mb-2">
              {tierLabel(tier)}
            </h3>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {leagues.map((l) => (
                <Card key={l.id} className="p-4">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <p className="font-medium text-sm">{l.name}</p>
                      {l.abbreviation && (
                        <p className="text-xs text-muted-foreground">
                          {l.abbreviation}
                        </p>
                      )}
                    </div>
                    <div className="flex gap-1">
                      {l.gender && (
                        <Badge variant="outline" className="text-xs">
                          {l.gender}
                        </Badge>
                      )}
                      {l.club_count != null && (
                        <Badge variant="secondary" className="text-xs">
                          {l.club_count} clubs
                        </Badge>
                      )}
                    </div>
                  </div>
                </Card>
              ))}
            </div>
          </div>
        ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Coaches Tab
// ---------------------------------------------------------------------------

function CoachesTab() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<Coach[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [searched, setSearched] = useState(false);

  const search = useCallback(
    async (p = 1) => {
      if (!query.trim()) return;
      setLoading(true);
      setPage(p);
      try {
        const data = await api<{ coaches: Coach[]; total: number }>(
          "/coaches/search",
          { name: query, page: String(p), page_size: "25" }
        );
        setResults(data.coaches ?? []);
        setTotal(data.total ?? 0);
        setSearched(true);
      } catch {
        setResults([]);
        setTotal(0);
      } finally {
        setLoading(false);
      }
    },
    [query]
  );

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input
          placeholder="Search coaches by name..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && search(1)}
          className="max-w-sm"
        />
        <Button onClick={() => search(1)} disabled={loading || !query.trim()}>
          {loading ? <Spinner className="mr-2" /> : null}
          Search
        </Button>
      </div>

      {searched && (
        <p className="text-sm text-muted-foreground">
          {total.toLocaleString()} coach{total !== 1 ? "es" : ""} found
        </p>
      )}

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Title</TableHead>
            <TableHead>Club</TableHead>
            <TableHead>Confidence</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {results.length === 0 ? (
            <TableRow>
              <TableCell colSpan={4} className="text-center text-muted-foreground py-8">
                {searched ? "No coaches found" : "Search for coaches above"}
              </TableCell>
            </TableRow>
          ) : (
            results.map((c) => (
              <TableRow key={c.id}>
                <TableCell className="font-medium">{c.name}</TableCell>
                <TableCell>{c.title ?? "-"}</TableCell>
                <TableCell>{c.club_name ?? "-"}</TableCell>
                <TableCell>
                  {c.confidence != null ? (
                    <Badge
                      variant={c.confidence >= 0.8 ? "default" : "outline"}
                      className="text-xs"
                    >
                      {(c.confidence * 100).toFixed(0)}%
                    </Badge>
                  ) : (
                    "-"
                  )}
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {total > 25 && (
        <div className="flex items-center justify-center gap-2 pt-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1 || loading}
            onClick={() => search(page - 1)}
          >
            Previous
          </Button>
          <span className="text-sm text-muted-foreground">
            Page {page} of {Math.ceil(total / 25)}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 25) || loading}
            onClick={() => search(page + 1)}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Analytics Tab
// ---------------------------------------------------------------------------

function AnalyticsTab() {
  const [view, setView] = useState<"duplicates" | "overlap">("duplicates");

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Button
          variant={view === "duplicates" ? "default" : "outline"}
          size="sm"
          onClick={() => setView("duplicates")}
        >
          Duplicates
        </Button>
        <Button
          variant={view === "overlap" ? "default" : "outline"}
          size="sm"
          onClick={() => setView("overlap")}
        >
          Multi-League Overlap
        </Button>
      </div>

      {view === "duplicates" ? <DuplicatesPanel /> : <OverlapPanel />}
    </div>
  );
}

function DuplicatesPanel() {
  const { data, loading, error } = useFetch<{
    clusters: DuplicateCluster[];
    total: number;
  }>("/analytics/duplicates", { page_size: "20" });

  if (loading)
    return (
      <div className="space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-16 w-full" />
        ))}
      </div>
    );
  if (error) return <ErrorBox message={error} />;
  if (!data?.clusters?.length)
    return (
      <p className="text-muted-foreground py-8 text-center">
        No duplicate clusters found
      </p>
    );

  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        {data.total} duplicate cluster{data.total !== 1 ? "s" : ""}
      </p>
      {data.clusters.map((cluster, i) => (
        <Card key={i}>
          <CardHeader className="pb-2">
            <CardTitle className="text-sm">
              "{cluster.normalized_name}"
            </CardTitle>
            <CardDescription>
              {cluster.clubs.length} potential duplicates
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-1">
              {cluster.clubs.map((c) => (
                <div key={c.id} className="flex items-center gap-2 text-sm">
                  <span className="font-medium">{c.name}</span>
                  {c.state && (
                    <Badge variant="outline" className="text-xs">
                      {c.state}
                    </Badge>
                  )}
                  {c.gender_program && (
                    <Badge variant="secondary" className="text-xs">
                      {c.gender_program}
                    </Badge>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function OverlapPanel() {
  const { data, loading, error } = useFetch<{
    clubs: (Club & { league_count: number })[];
    total: number;
  }>("/analytics/overlap", { min_leagues: "2", page_size: "20" });

  if (loading)
    return (
      <div className="space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-10 w-full" />
        ))}
      </div>
    );
  if (error) return <ErrorBox message={error} />;
  if (!data?.clubs?.length)
    return (
      <p className="text-muted-foreground py-8 text-center">
        No multi-league clubs found
      </p>
    );

  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        {data.total} club{data.total !== 1 ? "s" : ""} in multiple leagues
      </p>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Club</TableHead>
            <TableHead>State</TableHead>
            <TableHead>Leagues</TableHead>
            <TableHead className="text-right"># Leagues</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.clubs.map((c) => (
            <TableRow key={c.id}>
              <TableCell className="font-medium">{c.name}</TableCell>
              <TableCell>{c.state ?? "-"}</TableCell>
              <TableCell>
                <div className="flex gap-1 flex-wrap">
                  {(c.leagues ?? []).map((l) => (
                    <Badge key={l} variant="secondary" className="text-xs">
                      {l}
                    </Badge>
                  ))}
                </div>
              </TableCell>
              <TableCell className="text-right">{c.league_count}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Events Tab
// ---------------------------------------------------------------------------

interface EventItem {
  id: number;
  name: string;
  slug: string;
  league_name: string | null;
  season: string | null;
  age_group: string | null;
  gender: string | null;
  division: string | null;
  location_city: string | null;
  location_state: string | null;
  start_date: string | null;
  end_date: string | null;
  source: string | null;
  platform_event_id: string | null;
  source_url: string | null;
  team_count: number;
}

interface EventStats {
  total_events: number;
  total_seasons: number;
  upcoming_events: number;
  past_events: number;
  total_teams: number;
  by_season: { season: string; event_count: number; team_count: number }[];
  by_source: { source: string; event_count: number }[];
}

interface EventListResponse {
  events: EventItem[];
  filters: { seasons: string[]; sources: string[] };
  total: number;
  page: number;
  page_size: number;
}

function formatDate(iso: string | null): string {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function formatDateRange(start: string | null, end: string | null): string {
  if (!start) return "-";
  const s = new Date(start);
  const startStr = s.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  if (!end) return `${startStr}, ${s.getFullYear()}`;
  const e = new Date(end);
  if (s.getMonth() === e.getMonth() && s.getFullYear() === e.getFullYear()) {
    return `${startStr}-${e.getDate()}, ${s.getFullYear()}`;
  }
  const endStr = e.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${startStr} - ${endStr}, ${s.getFullYear()}`;
}

function SourceBadge({ source }: { source: string | null }) {
  if (!source) return <Badge variant="outline" className="text-xs">unknown</Badge>;
  const variants: Record<string, string> = {
    gotsport: "bg-blue-500/15 text-blue-400 border-blue-500/30",
    sincsports: "bg-green-500/15 text-green-400 border-green-500/30",
    other: "bg-zinc-500/15 text-zinc-400 border-zinc-500/30",
    manual: "bg-purple-500/15 text-purple-400 border-purple-500/30",
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium border ${variants[source] ?? variants.other}`}>
      {source}
    </span>
  );
}

function TimeframeBadge({ startDate }: { startDate: string | null }) {
  if (!startDate) return null;
  const now = new Date();
  const start = new Date(startDate);
  const isPast = start < now;
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${
      isPast
        ? "bg-zinc-500/10 text-zinc-500"
        : "bg-emerald-500/15 text-emerald-400"
    }`}>
      {isPast ? "Past" : "Upcoming"}
    </span>
  );
}

function EventsTab() {
  const [stats, setStats] = useState<EventStats | null>(null);
  const [data, setData] = useState<EventListResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);

  // Filters
  const [seasonFilter, setSeasonFilter] = useState("");
  const [sourceFilter, setSourceFilter] = useState("");
  const [stateFilter, setStateFilter] = useState("");
  const [nameFilter, setNameFilter] = useState("");
  const [timeframe, setTimeframe] = useState("all");

  // Load stats once
  useEffect(() => {
    api<EventStats>("/events/stats").then(setStats).catch(() => {});
  }, []);

  // Load event list
  const loadEvents = useCallback(
    async (p = 1) => {
      setLoading(true);
      setPage(p);
      try {
        const params: Record<string, string> = {
          page: String(p),
          page_size: "25",
        };
        if (seasonFilter) params.season = seasonFilter;
        if (sourceFilter) params.source = sourceFilter;
        if (stateFilter) params.state = stateFilter;
        if (nameFilter) params.name = nameFilter;
        if (timeframe !== "all") params.timeframe = timeframe;

        const result = await api<EventListResponse>("/events/list", params);
        setData(result);
      } catch {
        setData(null);
      } finally {
        setLoading(false);
      }
    },
    [seasonFilter, sourceFilter, stateFilter, nameFilter, timeframe],
  );

  useEffect(() => {
    loadEvents(1);
  }, [loadEvents]);

  const totalPages = data ? Math.ceil(data.total / (data.page_size || 25)) : 0;

  return (
    <div className="space-y-6">
      {/* Stats cards */}
      {stats && (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-5">
          <StatCard title="Total Events" value={stats.total_events.toLocaleString()} />
          <StatCard title="Seasons" value={String(stats.total_seasons)} />
          <StatCard title="Upcoming" value={stats.upcoming_events.toLocaleString()} />
          <StatCard title="Past" value={stats.past_events.toLocaleString()} />
          <StatCard title="Team Entries" value={stats.total_teams.toLocaleString()} />
        </div>
      )}

      {/* Season breakdown */}
      {stats && stats.by_season.length > 0 && (
        <div className="grid gap-6 lg:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle>Events by Season</CardTitle>
              <CardDescription>Event and team counts per season</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {stats.by_season.map((s) => (
                  <div key={s.season} className="flex items-center justify-between">
                    <span className="text-sm font-medium">{s.season}</span>
                    <div className="flex items-center gap-3">
                      <Badge variant="secondary" className="text-xs">
                        {s.event_count} events
                      </Badge>
                      <Badge variant="outline" className="text-xs">
                        {s.team_count.toLocaleString()} teams
                      </Badge>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Events by Source</CardTitle>
              <CardDescription>Platform distribution</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-2">
                {stats.by_source.map((s) => (
                  <div key={s.source} className="flex items-center justify-between">
                    <SourceBadge source={s.source} />
                    <span className="text-xs text-muted-foreground">
                      {s.event_count} events
                    </span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      <Separator />

      {/* Filters */}
      <div className="flex gap-2 flex-wrap items-end">
        <Input
          placeholder="Search events..."
          value={nameFilter}
          onChange={(e) => setNameFilter(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && loadEvents(1)}
          className="max-w-xs"
        />
        <Input
          placeholder="State (e.g. GA)"
          value={stateFilter}
          onChange={(e) => setStateFilter(e.target.value.toUpperCase())}
          onKeyDown={(e) => e.key === "Enter" && loadEvents(1)}
          className="max-w-[100px]"
        />
        {data?.filters.seasons && data.filters.seasons.length > 0 && (
          <Select value={seasonFilter} onValueChange={(v) => setSeasonFilter(v === "__all__" ? "" : v)}>
            <SelectTrigger className="w-[150px]">
              <SelectValue placeholder="All seasons" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all__">All seasons</SelectItem>
              {data.filters.seasons.map((s) => (
                <SelectItem key={s} value={s}>{s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
        {data?.filters.sources && data.filters.sources.length > 0 && (
          <Select value={sourceFilter} onValueChange={(v) => setSourceFilter(v === "__all__" ? "" : v)}>
            <SelectTrigger className="w-[140px]">
              <SelectValue placeholder="All sources" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="__all__">All sources</SelectItem>
              {data.filters.sources.map((s) => (
                <SelectItem key={s} value={s}>{s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        )}
        <Select value={timeframe} onValueChange={setTimeframe}>
          <SelectTrigger className="w-[130px]">
            <SelectValue placeholder="Timeframe" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All time</SelectItem>
            <SelectItem value="upcoming">Upcoming</SelectItem>
            <SelectItem value="past">Past</SelectItem>
          </SelectContent>
        </Select>
        <Button onClick={() => loadEvents(1)} disabled={loading} size="sm">
          {loading ? <Spinner className="mr-2" /> : null}
          Search
        </Button>
      </div>

      {/* Results count */}
      {data && (
        <p className="text-sm text-muted-foreground">
          {data.total.toLocaleString()} event{data.total !== 1 ? "s" : ""}
        </p>
      )}

      {/* Events table */}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Event</TableHead>
            <TableHead>Dates</TableHead>
            <TableHead>Location</TableHead>
            <TableHead>Source</TableHead>
            <TableHead>Season</TableHead>
            <TableHead className="text-right">Teams</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading && !data?.events.length ? (
            Array.from({ length: 8 }).map((_, i) => (
              <TableRow key={i}>
                {Array.from({ length: 6 }).map((__, j) => (
                  <TableCell key={j}>
                    <Skeleton className="h-4 w-full" />
                  </TableCell>
                ))}
              </TableRow>
            ))
          ) : !data?.events.length ? (
            <TableRow>
              <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                No events found
              </TableCell>
            </TableRow>
          ) : (
            data.events.map((ev) => (
              <TableRow key={ev.id}>
                <TableCell>
                  <div className="space-y-1">
                    <div className="font-medium text-sm leading-tight">{ev.name}</div>
                    <div className="flex gap-1.5 items-center">
                      {ev.league_name && (
                        <span className="text-xs text-muted-foreground">{ev.league_name}</span>
                      )}
                      <TimeframeBadge startDate={ev.start_date} />
                    </div>
                  </div>
                </TableCell>
                <TableCell className="text-sm whitespace-nowrap">
                  {formatDateRange(ev.start_date, ev.end_date)}
                </TableCell>
                <TableCell className="text-sm">
                  {[ev.location_city, ev.location_state].filter(Boolean).join(", ") || "-"}
                </TableCell>
                <TableCell>
                  <SourceBadge source={ev.source} />
                </TableCell>
                <TableCell className="text-sm">{ev.season ?? "-"}</TableCell>
                <TableCell className="text-right">
                  {ev.team_count > 0 ? (
                    <Badge variant="secondary" className="text-xs">
                      {ev.team_count}
                    </Badge>
                  ) : (
                    <span className="text-xs text-muted-foreground">-</span>
                  )}
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-center gap-2 pt-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1 || loading}
            onClick={() => loadEvents(page - 1)}
          >
            Previous
          </Button>
          <span className="text-sm text-muted-foreground">
            Page {page} of {totalPages}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= totalPages || loading}
            onClick={() => loadEvents(page + 1)}
          >
            Next
          </Button>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared components
// ---------------------------------------------------------------------------

function ErrorBox({ message }: { message: string }) {
  return (
    <Card className="border-destructive">
      <CardContent className="py-4">
        <p className="text-sm text-destructive">
          Failed to load data: {message}
        </p>
      </CardContent>
    </Card>
  );
}

// ---------------------------------------------------------------------------
// Health indicator
// ---------------------------------------------------------------------------

function HealthDot() {
  const [ok, setOk] = useState<boolean | null>(null);

  useEffect(() => {
    fetch("/healthz")
      .then((r) => setOk(r.ok))
      .catch(() => setOk(false));
  }, []);

  return (
    <span
      className={`inline-block h-2 w-2 rounded-full ${
        ok === null
          ? "bg-muted-foreground animate-pulse"
          : ok
            ? "bg-green-500"
            : "bg-red-500"
      }`}
      title={ok === null ? "Checking..." : ok ? "API connected" : "API unreachable"}
    />
  );
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

function App() {
  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="border-b">
        <div className="mx-auto max-w-6xl px-4 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <h1 className="text-xl font-bold tracking-tight">Upshift Data</h1>
            <Badge variant="outline" className="text-xs">
              Explorer
            </Badge>
          </div>
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <HealthDot />
            <span>API</span>
          </div>
        </div>
      </header>

      {/* Main */}
      <main className="mx-auto max-w-6xl px-4 py-6">
        <Tabs defaultValue="dashboard">
          <TabsList className="mb-4">
            <TabsTrigger value="dashboard">Dashboard</TabsTrigger>
            <TabsTrigger value="clubs">Clubs</TabsTrigger>
            <TabsTrigger value="leagues">Leagues</TabsTrigger>
            <TabsTrigger value="events">Events</TabsTrigger>
            <TabsTrigger value="coaches">Coaches</TabsTrigger>
            <TabsTrigger value="analytics">Analytics</TabsTrigger>
          </TabsList>

          <TabsContent value="dashboard">
            <DashboardTab />
          </TabsContent>
          <TabsContent value="clubs">
            <ClubsTab />
          </TabsContent>
          <TabsContent value="leagues">
            <LeaguesTab />
          </TabsContent>
          <TabsContent value="events">
            <EventsTab />
          </TabsContent>
          <TabsContent value="coaches">
            <CoachesTab />
          </TabsContent>
          <TabsContent value="analytics">
            <AnalyticsTab />
          </TabsContent>
        </Tabs>
      </main>
    </div>
  );
}

export default App;
