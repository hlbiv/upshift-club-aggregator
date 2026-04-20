# Data-quality SLAs

## Status

Draft — 2026-04-19. Anchors the scrape_health admin panel + alerting thresholds.

## Why formalize

The telemetry exists — `scrape_run_logs` (per-run), `scrape_health` (per-entity
rollup), `raw_html_archive` (Wave 2) — but there's no canonical "is the fleet
healthy right now?" view. Without thresholds, the admin panel is a wall of
numbers, nothing pages, and every incident re-litigates "is 3% zero-result
runs bad or normal?" This doc picks **six** KPIs with concrete green/yellow/red
thresholds; it's the contract the admin panel reads and, eventually, what an
alerting layer reads.

## The 6 KPIs

Four axes: **freshness**, **correctness**, **coverage**, **infra health**.

### 1. Tier-1 league scrape freshness (freshness)

- **Definition.** Percentage of Tier-1 leagues (`leagues_master` rows with
  `tier_numeric = 1`) whose most recent `scrape_run_logs` row where
  `status = 'ok'` and `league_name = leagues_master.name` has
  `completed_at >= now() - interval '7 days'`.
- **Thresholds.** Green ≥ 95%, yellow 85–95%, red < 85%.
- **Why.** Tier-1 is the ~7 national-elite league directories (ECNL, MLS
  Next, GA, etc.) — everything downstream is load-bearing on these.

### 2. Zero-result rate per Tier-1 scraper_key (correctness)

- **Definition.** For each `scraper_key` whose runs touched a Tier-1 league
  in the last 7 days, the percentage of `scrape_run_logs` rows with
  `status = 'ok'` but `records_touched = 0`.
- **Thresholds.** Green < 1%, yellow 1–5%, red > 5%.
- **Why.** A clean finish with zero rows is the classic parser-drift
  signature — page loaded, selectors stopped matching. Catches silent
  regressions that `status = 'failed'` doesn't.

### 3. Consecutive failure depth (infra health)

- **Definition.** `max(consecutive_failures)` across all
  `scrape_health` rows, broken out by `entity_type`.
- **Thresholds.** Green ≤ 2, yellow 3–5, red > 5.
- **Why.** 1–2 failures is noise; 3+ on a single entity is a persistent
  outage the scheduler won't self-heal from.

### 4. Raw HTML archive gap (infra health)

- **Definition.** Since `ARCHIVE_RAW_HTML_ENABLED=true` shipped, the percentage
  of `scrape_run_logs` rows with `status = 'ok'` that have at least one
  matching row in `raw_html_archive` (joined via `run_id`).
- **Thresholds.** Green ≥ 99%, yellow 95–99%, red < 95%.
- **Why.** Archiving is the rollback plane for every parser change. A
  silent bucket-write regression costs us replay coverage; this KPI
  surfaces the drift within a day.

### 5. Canonical-club NULL FK backlog (coverage)

- **Definition.** `count(*)` of rows with `canonical_club_id IS NULL` in
  `event_teams`, plus `home_club_id IS NULL OR away_club_id IS NULL` in
  `matches`, plus `club_id IS NULL` in `roster_diffs`, `tryouts`, and
  `club_roster_snapshots`. Reported both in aggregate and per table.
- **Thresholds.** Green ≤ 100, yellow 100–1000, red > 1000 (aggregate).
- **Why.** The linker is the seam between raw scrapes and queryable data.
  A growing backlog silently under-returns `/api/events/search?club_id=N`
  and stalls the `club_results` rollup — almost always because the linker
  job stopped running on Replit.

### 6. Per-scraper_key daily volume drift (correctness)

- **Definition.** For each `scraper_key` in the last 24h, ratio of today's
  summed `records_touched` to the rolling 7-day median. Reported as
  `abs(1 - ratio)` in percent.
- **Thresholds.** Green ≤ 20% deviation, yellow 20–50%, red > 50%.
- **Why.** Complements KPI 2: zero-result catches total parser failure,
  drift catches partial failure (80 clubs → 42, roster down by half).

### What was dropped and why

- **Scheduler run latency.** No persisted source-of-truth for expected
  cron time; inferring from `triggered_by='scheduler'` alone needs a new
  table, violating "no new tables." Revisit when scheduler config is
  in-database.
- **403 rate per source.** KPI 3 already pages the same condition; a
  dedicated 403 metric is too scraper-specific for a top-line dashboard.
  Keep as a drill-down query in the details pane.
- **Replay-html coverage.** Code-shape metric, not data-quality — belongs
  in CI, already partially covered by extractor-registry tests.
- **Tryouts/rosters freshness.** Analogous to KPI 1 but parallel freshness
  KPIs quadruple the panel footprint for marginal signal. Aggregate into
  one "non-Tier-1 freshness" sub-panel later.

## Where they live

All six compute on demand via SQL — **no new tables.** The admin
`/scraper-health` page already exposes `GET /v1/admin/scrape-health` and
`GET /v1/admin/scrape-runs` (#111); wiring these queries is additive
follow-up.

Alerting is out of scope for v1 — thresholds drive the UI color badge
only. Real paging is a follow-up once the badges have been live long
enough to tune false positives.

## Example SQL for 2 of the 6

**KPI 1 — Tier-1 league scrape freshness.** Join `leagues_master` to the
most recent successful run per league name, bucket fresh/stale, take the
percentage. Group by `league_name` rather than `scraper_key` because a
league can migrate scrapers over time (GotSport → SincSports) and we want
the best signal per league.

```sql
WITH tier1 AS (
  SELECT name FROM leagues_master WHERE tier_numeric = 1
),
latest_ok AS (
  SELECT league_name, max(completed_at) AS last_ok_at
  FROM scrape_run_logs
  WHERE status = 'ok' AND league_name IS NOT NULL
  GROUP BY league_name
)
SELECT
  round(
    100.0 * count(*) FILTER (WHERE l.last_ok_at >= now() - interval '7 days')
    / nullif(count(*), 0),
    1
  ) AS pct_fresh_7d
FROM tier1 t
LEFT JOIN latest_ok l ON l.league_name = t.name;
```

**KPI 4 — Raw HTML archive gap.** Count successful runs since the flag
went live and check how many have ≥1 archive row. `raw_html_archive.run_id`
is a nullable UUID (not a serial FK), so the join assumes the scraper
stamps the same UUID on both sides. Runs that predate the flag are
excluded via the `started_at` cutoff.

```sql
-- Approximation: since scrape_run_logs.id is a serial and
-- raw_html_archive.run_id is a nullable UUID, there is no direct FK
-- join available today. We proxy "archive gap" as the ratio of
-- distinct scraper-runs that produced >=1 archive row over distinct
-- successful scrape_run_logs in the same window, joining on the
-- (started_at, source_url) window. Open question 2 proposes adding a
-- UUID column to scrape_run_logs to make this exact.
WITH cutoff AS (
  SELECT coalesce(
    (SELECT min(archived_at) FROM raw_html_archive),
    now() - interval '30 days'
  ) AS since
),
successful_runs AS (
  SELECT l.id, l.started_at
  FROM scrape_run_logs l, cutoff c
  WHERE l.status = 'ok' AND l.started_at >= c.since
),
runs_with_archive AS (
  SELECT DISTINCT l.id
  FROM successful_runs l
  JOIN raw_html_archive a
    ON a.archived_at BETWEEN l.started_at AND l.started_at + interval '1 hour'
)
SELECT
  round(
    100.0 * (SELECT count(*) FROM runs_with_archive)
    / nullif((SELECT count(*) FROM successful_runs), 0),
    2
  ) AS pct_archived;
```

## Thresholds review cadence

Monthly for the first quarter the panel is live, then quarterly. Tune
whenever a real incident reveals a false positive or missed fire.

## Not in scope

- Alerting infrastructure — thresholds drive UI color only in v1.
- Per-customer SLAs — `upshift-data` is ops-internal, indirectly
  backstopping `upshift-player-platform`'s customer-facing SLAs.
- Historical trend storage — queries run straight off existing tables.
  If they get slow (threshold: >500ms at panel poll cadence), add a
  nightly rollup, but only after measuring the problem.

## Open questions

1. **Tier-1 league matching by name is brittle.** KPI 1 joins
   `leagues_master.name` to `scrape_run_logs.league_name`, but some
   scrapers write slight variants ("ECNL Girls National" vs "ECNL
   National Girls"). Canonicalize in-query, add a `league_id` FK to
   `scrape_run_logs`, or accept the fuzziness?
2. **KPI 4's UUID join is indirect.** `scrape_run_logs.id` is a serial,
   `raw_html_archive.run_id` is a UUID. Add an explicit UUID column to
   `scrape_run_logs`, or keep the current convention?
3. **Seventh KPI for matches → club_results rollup lag?** KPI 5 catches
   the linker backlog but not the case where the linker ran and the
   rollup didn't. Review at first monthly cadence.
