import { useEffect, useState } from "react";

interface Summary {
  generated_at: string;
  domains: {
    d1_clubs: { canonical_clubs: number; club_affiliations: number; club_aliases: number };
    d2_colleges: { colleges: number; college_coaches: number };
    d3_coaches: { coach_discoveries: number };
    d4_events: { events: number; event_teams: number };
    d5_matches: { matches: number; club_results: number };
    d6_rosters: { roster_snapshots: number; roster_diffs: number };
    d7_tryouts: { tryouts: number };
    d8_scrape_health: { scrape_run_logs: number; scrape_health: number };
  };
  totals: { leagues: number };
}

function fmt(n: number) {
  return n.toLocaleString();
}

function StatusBadge({ status }: { status: "GOOD" | "PARTIAL" | "EMPTY" | "NOT STARTED" | "TABLE EMPTY" }) {
  const map: Record<string, { cls: string; label: string }> = {
    GOOD: { cls: "status-good", label: "GOOD" },
    PARTIAL: { cls: "status-partial", label: "PARTIAL" },
    EMPTY: { cls: "status-empty", label: "NOT STARTED" },
    "NOT STARTED": { cls: "status-empty", label: "NOT STARTED" },
    "TABLE EMPTY": { cls: "status-partial", label: "TABLE EMPTY" },
  };
  const { cls, label } = map[status] ?? { cls: "status-empty", label: status };
  return <span className={`status-badge ${cls}`}>{label}</span>;
}

function ProgressBar({ pct, color }: { pct: number; color: "red" | "yellow" | "green" }) {
  return (
    <div className="progress-row">
      <div className="progress-track">
        <div
          className={`progress-fill fill-${color}`}
          style={{ width: `${pct}%`, transition: "width 1s ease" }}
        />
      </div>
      <span className="progress-pct">{pct}%</span>
    </div>
  );
}

function Dot({ color }: { color: "green" | "yellow" | "red" | "muted" }) {
  return <span className={`dot dot-${color}`} />;
}

function Row({ color, label, count }: { color: "green" | "yellow" | "red" | "muted"; label: string; count?: number | string }) {
  return (
    <div className="existing-row">
      <Dot color={color} />
      <span>{label}</span>
      {count !== undefined && <span className="row-count">{typeof count === "number" ? fmt(count) : count}</span>}
    </div>
  );
}

function MissingItem({ name, source, blocker }: { name: string; source: string; blocker?: string }) {
  return (
    <div className="missing-item">
      <div className="missing-item-name">{name}</div>
      <div className="missing-item-source">{source}</div>
      {blocker && <div className="missing-item-blocker">⚠ {blocker}</div>}
    </div>
  );
}

function DomainCard({
  num,
  title,
  priority,
  subtitle,
  status,
  pct,
  color,
  children,
}: {
  num: string;
  title: string;
  priority: "P0" | "P1" | "P2";
  subtitle: string;
  status: "GOOD" | "PARTIAL" | "EMPTY" | "NOT STARTED" | "TABLE EMPTY";
  pct: number;
  color: "red" | "yellow" | "green";
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(true);
  return (
    <div className="domain-card">
      <div className="domain-header" onClick={() => setOpen(!open)}>
        <span className="domain-number">{num}</span>
        <div className="domain-title-block">
          <div className="domain-title">
            {title}{" "}
            <span className={`priority-flag p${priority[1]}`}>{priority}</span>
          </div>
          <div className="domain-subtitle">{subtitle}</div>
        </div>
        <StatusBadge status={status} />
      </div>
      {open && (
        <div className="domain-body">
          <ProgressBar pct={pct} color={color} />
          {children}
        </div>
      )}
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return <div className="section-label">{children}</div>;
}

function BlockerBanner({ children }: { children: React.ReactNode }) {
  return <div className="blocker-banner">{children}</div>;
}

function MoatCallout({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="moat-callout">
      <div className="moat-label">{label}</div>
      {children}
    </div>
  );
}

export default function App() {
  const [summary, setSummary] = useState<Summary | null>(null);
  const [error, setError] = useState(false);

  useEffect(() => {
    fetch("/api/analytics/summary")
      .then((r) => r.json())
      .then(setSummary)
      .catch(() => setError(true));
  }, []);

  const d = summary?.domains;
  const now = summary
    ? new Date(summary.generated_at).toLocaleDateString("en-US", {
        year: "numeric",
        month: "short",
        day: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
    : "—";

  function domainStatus(primaryCount: number, secondaryCount?: number): "GOOD" | "PARTIAL" | "NOT STARTED" {
    if (primaryCount > 0) {
      if (secondaryCount !== undefined && secondaryCount === 0) return "PARTIAL";
      return "GOOD";
    }
    return "NOT STARTED";
  }

  const d1Status = d ? (d.d1_clubs.canonical_clubs > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d2Status = d ? (d.d2_colleges.colleges > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d3Status = d ? (d.d3_coaches.coach_discoveries > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d4Status = d ? (d.d4_events.events > 0 ? "GOOD" : "TABLE EMPTY") : "TABLE EMPTY";
  const d5Status = d ? (d.d5_matches.matches > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d6Status = d ? (d.d6_rosters.roster_snapshots > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d7Status = d ? (d.d7_tryouts.tryouts > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";
  const d8Status = d ? (d.d8_scrape_health.scrape_run_logs > 0 ? "PARTIAL" : "NOT STARTED") : "NOT STARTED";

  return (
    <>
      <style>{`
        .container { max-width: 900px; margin: 0 auto; padding: 40px 24px 80px; }
        .header { margin-bottom: 48px; }
        .header-eyebrow { font-family: 'Space Mono', monospace; font-size: 11px; color: var(--accent); letter-spacing: 0.2em; text-transform: uppercase; margin-bottom: 12px; }
        .header h1 { font-size: clamp(28px, 5vw, 42px); font-weight: 800; line-height: 1.1; letter-spacing: -0.02em; }
        .header h1 span { color: var(--accent); }
        .header-meta { display: flex; gap: 12px; margin-top: 16px; flex-wrap: wrap; }
        .meta-pill { font-family: 'Space Mono', monospace; font-size: 11px; color: var(--muted); background: var(--surface); border: 1px solid var(--border); padding: 4px 10px; border-radius: 4px; }
        .boundary { background: linear-gradient(135deg, rgba(0,229,160,0.08), rgba(124,92,252,0.08)); border: 1px solid rgba(0,229,160,0.2); border-radius: 8px; padding: 16px 20px; margin-bottom: 40px; font-family: 'Space Mono', monospace; font-size: 12px; color: var(--accent); line-height: 1.6; }
        .boundary strong { color: var(--text); }
        .stats-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 12px; margin-bottom: 48px; }
        .stat-card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; padding: 16px; text-align: center; }
        .stat-number { font-family: 'Space Mono', monospace; font-size: 22px; font-weight: 700; color: var(--accent); display: block; }
        .stat-number.loading { color: var(--border); }
        .stat-label { font-size: 11px; color: var(--muted); margin-top: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
        .domains { display: flex; flex-direction: column; gap: 24px; }
        .domain-card { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; transition: border-color 0.2s; }
        .domain-card:hover { border-color: rgba(0,229,160,0.3); }
        .domain-header { display: flex; align-items: center; gap: 14px; padding: 20px 24px 16px; cursor: pointer; user-select: none; }
        .domain-number { font-family: 'Space Mono', monospace; font-size: 11px; color: var(--muted); min-width: 24px; }
        .domain-title-block { flex: 1; }
        .domain-title { font-size: 16px; font-weight: 700; letter-spacing: -0.01em; }
        .domain-subtitle { font-size: 12px; color: var(--muted); margin-top: 2px; }
        .status-badge { font-family: 'Space Mono', monospace; font-size: 10px; padding: 4px 10px; border-radius: 100px; font-weight: 700; white-space: nowrap; }
        .status-empty { background: rgba(255,68,102,0.15); color: var(--red); border: 1px solid rgba(255,68,102,0.3); }
        .status-partial { background: rgba(255,193,66,0.15); color: var(--yellow); border: 1px solid rgba(255,193,66,0.3); }
        .status-good { background: rgba(0,229,160,0.12); color: var(--green); border: 1px solid rgba(0,229,160,0.25); }
        .domain-body { padding: 0 24px 24px; border-top: 1px solid var(--border); }
        .progress-row { display: flex; align-items: center; gap: 12px; padding: 14px 0; border-bottom: 1px solid var(--border); }
        .progress-track { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; }
        .progress-fill { height: 100%; border-radius: 2px; }
        .fill-red { background: var(--red); }
        .fill-yellow { background: var(--yellow); }
        .fill-green { background: var(--green); }
        .progress-pct { font-family: 'Space Mono', monospace; font-size: 11px; color: var(--muted); min-width: 32px; text-align: right; }
        .section-label { font-family: 'Space Mono', monospace; font-size: 10px; color: var(--accent); text-transform: uppercase; letter-spacing: 0.15em; margin: 18px 0 10px; }
        .existing-row { display: flex; align-items: center; gap: 10px; padding: 6px 0; border-bottom: 1px solid rgba(255,255,255,0.04); font-size: 13px; }
        .existing-row:last-child { border-bottom: none; }
        .dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
        .dot-green { background: var(--green); }
        .dot-yellow { background: var(--yellow); }
        .dot-red { background: var(--red); }
        .dot-muted { background: var(--muted); }
        .row-count { font-family: 'Space Mono', monospace; font-size: 11px; color: var(--muted); margin-left: auto; }
        .missing-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 8px; margin-top: 4px; }
        .missing-item { background: rgba(255,255,255,0.03); border: 1px solid var(--border); border-radius: 6px; padding: 10px 12px; font-size: 12px; }
        .missing-item-name { font-weight: 600; margin-bottom: 3px; }
        .missing-item-source { font-family: 'Space Mono', monospace; font-size: 10px; color: var(--accent3); }
        .missing-item-blocker { font-family: 'Space Mono', monospace; font-size: 10px; color: var(--red); margin-top: 3px; }
        .moat-callout { background: linear-gradient(135deg, rgba(124,92,252,0.1), rgba(0,229,160,0.05)); border: 1px solid rgba(124,92,252,0.3); border-radius: 8px; padding: 14px 16px; margin-top: 16px; font-size: 12px; line-height: 1.6; }
        .moat-label { font-family: 'Space Mono', monospace; font-size: 10px; color: var(--accent3); text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 4px; }
        .priority-flag { font-family: 'Space Mono', monospace; font-size: 10px; padding: 2px 8px; border-radius: 3px; margin-left: 8px; }
        .p0 { background: rgba(255,68,102,0.2); color: var(--red); }
        .p1 { background: rgba(255,193,66,0.2); color: var(--yellow); }
        .p2 { background: rgba(0,229,160,0.1); color: var(--green); }
        .blocker-banner { background: rgba(255,68,102,0.08); border: 1px solid rgba(255,68,102,0.2); border-radius: 6px; padding: 10px 14px; margin-top: 14px; font-family: 'Space Mono', monospace; font-size: 11px; color: var(--red); line-height: 1.5; }
        .footer { margin-top: 60px; padding-top: 24px; border-top: 1px solid var(--border); font-family: 'Space Mono', monospace; font-size: 11px; color: var(--muted); display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }
        .error-pill { background: rgba(255,68,102,0.1); border: 1px solid rgba(255,68,102,0.2); color: var(--red); padding: 4px 10px; border-radius: 4px; font-size: 11px; font-family: 'Space Mono', monospace; }
      `}</style>
      <div className="container">
        <div className="header">
          <div className="header-eyebrow">Upshift Data Platform</div>
          <h1>Missing Data <span>Spec</span><br />by Domain</h1>
          <div className="header-meta">
            <span className="meta-pill">Generated: {now}</span>
            <span className="meta-pill">Integration: REST API → Player</span>
            <span className="meta-pill">8 Domains</span>
            {error && <span className="error-pill">⚠ API unreachable — showing cached data</span>}
          </div>
        </div>

        <div className="boundary">
          <strong>Architectural boundary:</strong> Public + scraped data lives here. Player identity, enrichment, and user-facing logic stays in Upshift Player. No shared DB — Player consumes via REST only.
        </div>

        <div className="stats-row">
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(d.d1_clubs.canonical_clubs) : "···"}</span>
            <div className="stat-label">Canonical Clubs</div>
          </div>
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(d.d1_clubs.club_affiliations) : "···"}</span>
            <div className="stat-label">Club Affiliations</div>
          </div>
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(d.d1_clubs.club_aliases) : "···"}</span>
            <div className="stat-label">Club Aliases</div>
          </div>
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(d.d3_coaches.coach_discoveries) : "···"}</span>
            <div className="stat-label">Coach Discoveries</div>
          </div>
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(summary!.totals.leagues) : "···"}</span>
            <div className="stat-label">Leagues</div>
          </div>
          <div className="stat-card">
            <span className={`stat-number${!d ? " loading" : ""}`}>{d ? fmt(d.d4_events.events) : "···"}</span>
            <div className="stat-label">Events</div>
          </div>
        </div>

        <div className="domains">

          {/* D1 */}
          <DomainCard num="D1" title="Clubs" priority="P1" subtitle="Canonical club graph, affiliations, events, results" status={d1Status} pct={d && d.d1_clubs.canonical_clubs > 0 ? 40 : 0} color="yellow">
            <SectionLabel>What exists</SectionLabel>
            <Row color="green" label="canonical_clubs" count={d?.d1_clubs.canonical_clubs ?? 0} />
            <Row color="green" label="club_affiliations" count={d?.d1_clubs.club_affiliations ?? 0} />
            <Row color="green" label="club_aliases" count={d?.d1_clubs.club_aliases ?? 0} />
            <Row color={d && d.d4_events.events > 0 ? "green" : "red"} label="events" count={d?.d4_events.events ?? 0} />
            <Row color={d && d.d4_events.event_teams > 0 ? "green" : "muted"} label="event_teams" count={d?.d4_events.event_teams ?? 0} />
            <Row color={d && d.d5_matches.club_results > 0 ? "green" : "muted"} label={d && d.d5_matches.club_results > 0 ? "club_results" : "club_results — table exists, no data"} count={d && d.d5_matches.club_results > 0 ? d.d5_matches.club_results : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              {d && d.d4_events.events === 0 && <MissingItem name="Events scraper" source="Source: GotSport, SincSports" />}
              {d && d.d5_matches.club_results === 0 && <MissingItem name="club_results rollup" source="Source: GotSport brackets" />}
              <MissingItem name="Logo enrichment" source="Source: Club sites, Google" />
              <MissingItem name="Social handles" source="Source: Instagram, X search" />
              <MissingItem name="Fuzzy name dedup" source="Task: open in Boswell" blocker='"Concorde Fire SC" vs "Concorde Fire"' />
            </div>
            <BlockerBanner>Blocks Player: events_batch_by_ids, events_teams_extensions (touchCount, gameLabel, divisionCode)</BlockerBanner>
          </DomainCard>

          {/* D2 */}
          <DomainCard num="D2" title="Colleges" priority="P0" subtitle="NCAA/NAIA/NJCAA programs, college coaches, contact info" status={d2Status} pct={d && d.d2_colleges.colleges > 0 ? 20 : 0} color="red">
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d2_colleges.colleges > 0 ? "green" : "red"} label={d && d.d2_colleges.colleges > 0 ? "colleges" : "colleges — no data"} count={d && d.d2_colleges.colleges > 0 ? d.d2_colleges.colleges : undefined} />
            <Row color={d && d.d2_colleges.college_coaches > 0 ? "green" : "red"} label={d && d.d2_colleges.college_coaches > 0 ? "college_coaches" : "college_coaches — no data"} count={d && d.d2_colleges.college_coaches > 0 ? d.d2_colleges.college_coaches : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              <MissingItem name="NCAA scraper" source="Source: ncaa.org rosters" />
              <MissingItem name="NAIA scraper" source="Source: naia.org" />
              <MissingItem name="NJCAA scraper" source="Source: njcaa.org" />
              <MissingItem name="Coach email/phone enrichment" source="Source: TopDrawerSoccer" />
              <MissingItem name="Roster spots / openings" source="Source: College sites, TDS" />
              <MissingItem name="Recruiting activity signals" source="Source: NCSA, Berecruited" />
            </div>
            <BlockerBanner>Blocks Player: PR 1 — routes/schools.ts entirely (13 endpoints). Highest priority gap.</BlockerBanner>
          </DomainCard>

          {/* D3 */}
          <DomainCard num="D3" title="Youth Coaches" priority="P0" subtitle="Career history, club movement, D1 production effectiveness" status={d3Status} pct={d && d.d3_coaches.coach_discoveries > 0 ? 25 : 0} color="yellow">
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d3_coaches.coach_discoveries > 0 ? "yellow" : "red"} label="coach_discoveries" count={d?.d3_coaches.coach_discoveries ?? 0} />
            <Row color="muted" label="No career history, no movement tracking, no effectiveness data" />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              <MissingItem name="coach_career_history" source="Schema: coach_id, club_id, role, years_start, years_end" />
              <MissingItem name="coach_placements" source="Schema: coach_id, player_id, college_id, year" />
              <MissingItem name="Club movement tracker" source="Source: Club sites, LinkedIn" />
              <MissingItem name="D1 placement graph" source="Source: College rosters → backtrack" />
              <MissingItem name="Effectiveness score" source="Derived: placements / roster size" />
              <MissingItem name="Current roster endpoint" source="Blocks: /coach/my/roster in Player" />
            </div>
            <MoatCallout label="🔑 Key Moat">
              Which youth coaches produce D1 players. Nobody in the market has this. Requires cross-referencing college rosters backward to youth clubs. High effort, high defensibility.
            </MoatCallout>
            <BlockerBanner>Blocks Player: coaches_history_roster (/:id/career, /placements, /my/roster), reportCoachClaim</BlockerBanner>
          </DomainCard>

          {/* D4 */}
          <DomainCard num="D4" title="Events & Schedules" priority="P0" subtitle="Tournaments, showcases, league play, brackets" status={d4Status} pct={d && d.d4_events.events > 0 ? 30 : 5} color={d && d.d4_events.events > 0 ? "yellow" : "red"}>
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d4_events.events > 0 ? "green" : "yellow"} label={`events — ${d && d.d4_events.events > 0 ? "" : "schema exists"}`} count={d?.d4_events.events ?? 0} />
            <Row color={d && d.d4_events.event_teams > 0 ? "green" : "muted"} label="event_teams" count={d?.d4_events.event_teams ?? 0} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              {d && d.d4_events.events === 0 && <MissingItem name="GotSport scraper" source="Source: gotsport.com" />}
              {d && d.d4_events.events === 0 && <MissingItem name="SincSports scraper" source="Source: sincsports.com" />}
              <MissingItem name="EventBrite / manual events" source="Source: Club sites, TYSA" />
              <MissingItem name="Age group tagging" source="U9–U19 normalization" />
              <MissingItem name="events_batch_by_ids endpoint" source="Needed by Player routes/events.ts" blocker="Current Player blocker" />
            </div>
            <BlockerBanner>Blocks Player: routes/events.ts full swap — currently using local fallback only. events_batch_by_ids + events_teams_extensions (touchCount, gameLabel, divisionCode) both pending.</BlockerBanner>
          </DomainCard>

          {/* D5 */}
          <DomainCard num="D5" title="Matches & Results" priority="P1" subtitle="Historical W/L/D, season-over-season club performance" status={d5Status} pct={d && d.d5_matches.matches > 0 ? 20 : 0} color={d && d.d5_matches.matches > 0 ? "yellow" : "red"}>
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d5_matches.matches > 0 ? "green" : "red"} label={d && d.d5_matches.matches > 0 ? "matches" : "No matches data"} count={d && d.d5_matches.matches > 0 ? d.d5_matches.matches : undefined} />
            <Row color={d && d.d5_matches.club_results > 0 ? "green" : "muted"} label={d && d.d5_matches.club_results > 0 ? "club_results" : "club_results — no rollup data"} count={d && d.d5_matches.club_results > 0 ? d.d5_matches.club_results : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              {d && d.d5_matches.matches === 0 && <MissingItem name="GotSport bracket scraper" source="Source: gotsport.com brackets" />}
              <MissingItem name="Historical season snapshots" source="2+ seasons back minimum" />
              <MissingItem name="W/L/D trend analytics" source="Derived from matches table" />
            </div>
          </DomainCard>

          {/* D6 */}
          <DomainCard num="D6" title="Historical Rosters" priority="P1" subtitle="Year-over-year snapshots, player churn metric" status={d6Status} pct={d && d.d6_rosters.roster_snapshots > 0 ? 20 : 0} color={d && d.d6_rosters.roster_snapshots > 0 ? "yellow" : "red"}>
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d6_rosters.roster_snapshots > 0 ? "green" : "red"} label={d && d.d6_rosters.roster_snapshots > 0 ? "club_roster_snapshots" : "club_roster_snapshots — no data"} count={d && d.d6_rosters.roster_snapshots > 0 ? d.d6_rosters.roster_snapshots : undefined} />
            <Row color={d && d.d6_rosters.roster_diffs > 0 ? "green" : "muted"} label={d && d.d6_rosters.roster_diffs > 0 ? "roster_diffs" : "roster_diffs — no data"} count={d && d.d6_rosters.roster_diffs > 0 ? d.d6_rosters.roster_diffs : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              <MissingItem name="Roster scraper" source="Source: GotSport, club sites" />
              <MissingItem name="Player churn metric" source="Derived: YoY roster deltas" />
              <MissingItem name="shadow_players linkage" source="Blocks PR 6 in Player" blocker="Player blocker" />
            </div>
            <BlockerBanner>Blocks Player: shadow_players endpoints — PR 6 (games.ts), /coach/players, /coach/shadow-search, /coach/appearance-feed all pending.</BlockerBanner>
          </DomainCard>

          {/* D7 */}
          <DomainCard num="D7" title="Tryout Listings" priority="P2" subtitle="Club tryouts, open sessions — TYSA board seat distribution" status={d7Status} pct={d && d.d7_tryouts.tryouts > 0 ? 30 : 0} color={d && d.d7_tryouts.tryouts > 0 ? "yellow" : "red"}>
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d7_tryouts.tryouts > 0 ? "green" : "red"} label={d && d.d7_tryouts.tryouts > 0 ? "tryouts" : "tryouts — no data"} count={d && d.d7_tryouts.tryouts > 0 ? d.d7_tryouts.tryouts : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              {d && d.d7_tryouts.tryouts === 0 && <MissingItem name="Club site scraper" source="Source: Club websites, social" />}
              <MissingItem name="Manual submission API" source="Clubs submit directly via form" />
              <MissingItem name="TYSA distribution hook" source="Board seat = direct channel" />
            </div>
            <MoatCallout label="📡 Distribution advantage">
              TYSA board seat gives direct distribution to member clubs. This domain has the clearest go-to-market path — clubs want their tryouts listed, players want to find them.
            </MoatCallout>
          </DomainCard>

          {/* D8 */}
          <DomainCard num="D8" title="Scrape Health" priority="P0" subtitle="Required before paid API access goes live" status={d8Status} pct={d && d.d8_scrape_health.scrape_run_logs > 0 ? 40 : 0} color={d && d.d8_scrape_health.scrape_run_logs > 0 ? "yellow" : "red"}>
            <SectionLabel>What exists</SectionLabel>
            <Row color={d && d.d8_scrape_health.scrape_run_logs > 0 ? "green" : "red"} label={d && d.d8_scrape_health.scrape_run_logs > 0 ? "scrape_run_logs" : "scrape_run_logs — no data"} count={d && d.d8_scrape_health.scrape_run_logs > 0 ? d.d8_scrape_health.scrape_run_logs : undefined} />
            <Row color={d && d.d8_scrape_health.scrape_health > 0 ? "green" : "muted"} label={d && d.d8_scrape_health.scrape_health > 0 ? "scrape_health" : "scrape_health — rollup empty"} count={d && d.d8_scrape_health.scrape_health > 0 ? d.d8_scrape_health.scrape_health : undefined} />
            <SectionLabel>What's missing</SectionLabel>
            <div className="missing-grid">
              <MissingItem name="Freshness SLA definition" source="Events: daily. Coaches: weekly." />
              <MissingItem name="Alert on scraper failure" source="Email/webhook on run failure" />
              <MissingItem name="Coverage analytics endpoint" source="GET /api/analytics/coverage ✓" />
            </div>
            <MoatCallout label="⚠ Gate">
              This is a prerequisite for charging for API access. Without scrape health tracking, you can't guarantee data freshness to paying consumers. Build this in parallel with D2 and D4 scrapers.
            </MoatCallout>
          </DomainCard>

        </div>

        <div className="footer">
          <span>Upshift Data — Missing Data Spec v1.1</span>
          <span>8 domains · Live data from /api/analytics/summary</span>
        </div>
      </div>
    </>
  );
}
