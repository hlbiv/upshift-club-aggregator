# Replit Scheduled Deployments — scraper cron

Replit Scheduled Deployments (not GitHub Actions) drive recurring
scraper runs for this repo. The cron schedule + command config lives in
the Replit console, **not** in git; what lives in git is:

- `scraper/scheduled/nightly_tier1.sh`
- `scraper/scheduled/weekly_state.sh`
- `scraper/scheduled/hourly_linker.sh`
- `scraper/scheduled/ncaa_d1_rosters.sh`
- `scraper/scheduled/ncaa_d2_rosters.sh`
- `scraper/scheduled/ncaa_d3_rosters.sh`
- The `scrape_run_logs.triggered_by` column (populated by the Python
  logger from the `SCRAPE_TRIGGERED_BY` env var each script exports).

This doc is the step-by-step for wiring the scheduled deployments in
the Replit console and verifying they fire.

## Why these jobs

| Job              | What it runs                                      | Cadence        | Why this cadence                                                         |
| ---------------- | ------------------------------------------------- | -------------- | ------------------------------------------------------------------------ |
| `nightly-tier1`  | `python3 run.py --tier 1` (7 national leagues)    | daily 02:00 ET | Tier 1 churns the fastest; nightly catches roster + staff moves quickly. |
| `weekly-state`   | `python3 run.py --scope state` (54 state assocs)  | Sunday 03:00 ET | State-assoc sites are large + change slowly; weekly keeps load bounded.  |
| `hourly-linker`  | `python3 run.py --source link-canonical-clubs`    | hourly at :05  | Every event/match scraper leaves canonical FKs NULL by design; the linker fills them so `/api/events/search?club_id=N` and the club_results rollup work. Hourly keeps staleness under one hour. |
| `weekly-ncaa-d1` | `--source ncaa-rosters --all --division D1` (mens + womens) | Monday 04:00 ET | Coach + roster moves on D1 programs are weekly-news cadence; the wrapper also enables `COACH_MISSES_REPORT_ENABLED=true` so the `/data-quality/coach-misses` page populates. |
| `weekly-ncaa-d2` | `--source ncaa-rosters --all --division D2` (mens + womens) | Monday 06:00 ET | Same rationale as D1; staggered to avoid overlap. |
| `weekly-ncaa-d3` | `--source ncaa-rosters --all --division D3` (mens + womens) | Monday 09:00 ET | Same rationale as D1; D3 is the largest universe so it gets the longest start window. |

Tournaments, SincSports events, and tryouts stay operator-invoked on
Replit until we have a reason to schedule them.

## Cron expressions (UTC)

Replit Scheduled Deployments use UTC. Translate from ET:

| Local ET        | UTC (EST, winter, UTC-5) | UTC (EDT, summer, UTC-4) |
| --------------- | ------------------------ | ------------------------ |
| 02:00 ET daily  | `0 7 * * *`              | `0 6 * * *`              |
| 03:00 ET Sun    | `0 8 * * 0`              | `0 7 * * 0`              |
| hourly at :05   | `5 * * * *`              | `5 * * * *` (no drift)   |

**DST handling:** Replit does not auto-shift cron when ET crosses DST.
You have two options; pick one per job and note it in the Replit
description field so the next operator isn't confused:

1. **Pick UTC and ignore ET drift.** Set `0 7 * * *` for nightly and
   accept that the run lands at 02:00 EST in winter and 03:00 EDT in
   summer. Simplest — recommended for hands-off ops. This doc assumes
   option 1 unless otherwise noted.
2. **Edit the cron twice a year.** Set `0 7 * * *` in winter, flip to
   `0 6 * * *` on the DST spring-forward day, flip back on fall-back.
   Only worth it if there's a downstream system that cares about the
   exact local-time hour.

`hourly-linker` is insensitive to DST because it runs every hour.

## Configuring each scheduled deployment

Go to the Replit console → Deployments → "Scheduled Deployment" → "Add
scheduled run". Repeat for each of the jobs below.

### Shared form values

| Field              | Value                                                                                     |
| ------------------ | ----------------------------------------------------------------------------------------- |
| Working directory  | repo root (leave blank / default)                                                         |
| Secrets            | `DATABASE_URL` (required for logger writes to `scrape_run_logs`)                          |
| Timeout            | job-specific (see per-job tables)                                                          |

The wrapper scripts `cd "$(dirname "$0")/.."` up to the scraper
directory, so the working directory only needs to be somewhere the
script can be invoked from. Each script also exports
`SCRAPE_TRIGGERED_BY=scheduler` inline — you do not need to set it as
a secret or env var in the console.

### Job 1 — `nightly-tier1`

| Field    | Value                                 |
| -------- | ------------------------------------- |
| Name     | `nightly-tier1`                       |
| Cron     | `0 7 * * *`  (02:00 ET winter / 03:00 ET summer) |
| Command  | `bash scraper/scheduled/nightly_tier1.sh`         |
| Timeout  | 60 minutes                            |
| Notes    | Runs `python3 run.py --tier 1`. 7 leagues; typically finishes in 15–25 minutes. |

### Job 2 — `weekly-state`

| Field    | Value                                 |
| -------- | ------------------------------------- |
| Name     | `weekly-state`                        |
| Cron     | `0 8 * * 0`  (Sunday 03:00 ET winter / 04:00 ET summer) |
| Command  | `bash scraper/scheduled/weekly_state.sh`          |
| Timeout  | 180 minutes                           |
| Notes    | Runs `python3 run.py --scope state`. 54 associations; can take 60–120 minutes depending on how many sites are slow. |

### Job 3 — `hourly-linker`

| Field    | Value                                 |
| -------- | ------------------------------------- |
| Name     | `hourly-linker`                       |
| Cron     | `5 * * * *`                           |
| Command  | `bash scraper/scheduled/hourly_linker.sh`         |
| Timeout  | 15 minutes                            |
| Notes    | Runs `python3 run.py --source link-canonical-clubs`. Idempotent — only touches rows where the FK is currently NULL. Cheap when there's nothing to do. |

### Jobs 4–6 — `weekly-ncaa-d1` / `d2` / `d3`

Each wrapper exports `COACH_MISSES_REPORT_ENABLED=true` inline, so the
gated writer in `scraper/extractors/ncaa_rosters.py` records one row per
school where the head coach could not be extracted (feeds the
`/data-quality/coach-misses` dashboard). After the first scheduled cycle
expect a realistic miss list (rough order of magnitude per task #38:
~60 D1 mens + ~80 D1 womens; D2/D3 will be larger).

All three crons are written in fixed UTC per option 1 in the
"DST handling" section above — i.e. the local-ET hour drifts by one
between winter (EST) and summer (EDT). If you need the local hour
pinned, edit twice a year (option 2).

| Field         | `weekly-ncaa-d1`     | `weekly-ncaa-d2`     | `weekly-ncaa-d3`     |
| ------------- | -------------------- | -------------------- | -------------------- |
| Cron (UTC)    | `0 9 * * 1`          | `0 11 * * 1`         | `0 14 * * 1`         |
| ET equivalent | Mon 04:00 EST / 05:00 EDT | Mon 06:00 EST / 07:00 EDT | Mon 09:00 EST / 10:00 EDT |
| Command       | `bash scraper/scheduled/ncaa_d1_rosters.sh` | `bash scraper/scheduled/ncaa_d2_rosters.sh` | `bash scraper/scheduled/ncaa_d3_rosters.sh` |
| Timeout       | 180 minutes          | 240 minutes          | 360 minutes          |

Notes:

- Each wrapper runs men's then women's back-to-back. With `set -euo pipefail`
  in the wrapper, if the men's pass fails the women's pass will not run —
  re-trigger from the console once the cause is fixed.
- The wrappers do not pass `--backfill-seasons`, so each cycle scrapes only
  the current season. Coach-misses is gated to the current-season pass
  inside the extractor, so backfill flags would not change miss output.
- To temporarily stop populating coach-misses without disabling the whole
  job, comment out the `export COACH_MISSES_REPORT_ENABLED=true` line in
  the wrapper and open a PR. The scrape itself does not need the env var.

## Verifying a scheduled run fired

After the first scheduled execution, confirm the row landed in
`scrape_run_logs` with the right `triggered_by`:

```sql
-- Most recent five scheduled runs, any scraper.
SELECT id, scraper_key, league_name, status, triggered_by,
       started_at, completed_at,
       records_created, records_updated, records_failed
FROM scrape_run_logs
WHERE triggered_by = 'scheduler'
ORDER BY started_at DESC
LIMIT 5;
```

Per-job spot check:

```sql
-- Nightly Tier 1 — did last night's run finish ok?
SELECT scraper_key, status, started_at, completed_at
FROM scrape_run_logs
WHERE triggered_by = 'scheduler'
  AND started_at >= now() - interval '36 hours'
  AND scraper_key NOT IN ('link-canonical-clubs')
ORDER BY started_at DESC;

-- Hourly linker — last 6 runs.
SELECT scraper_key, status, started_at, completed_at,
       records_updated
FROM scrape_run_logs
WHERE triggered_by = 'scheduler'
  AND scraper_key = 'link-canonical-clubs'
ORDER BY started_at DESC
LIMIT 6;
```

If you see zero rows with `triggered_by = 'scheduler'` for a job you
believe has fired, one of these three is true:

1. The schedule isn't active in the Replit console (most common —
   "Add scheduled run" saves as "disabled" by default; toggle it on).
2. The script failed before the logger could write — check the Replit
   Deployment logs UI (see below).
3. The logger spilled to the JSONL fallback because `DATABASE_URL`
   was missing or unreachable. Look under `scraper/logs/` for
   date-stamped `.jsonl` files; they'll drain into `scrape_run_logs`
   on the next successful run.

## What to do if a run silently fails

Replit Scheduled Deployments don't surface failures in-repo. Check the
console.

1. **Replit console → Deployments → Scheduled → your job → "Logs"**.
   Every invocation has a captured stdout/stderr. `set -euo pipefail`
   in each wrapper means any non-zero exit aborts the script — look
   for the last Python traceback.
2. If the script never ran at all (no logs appear for the expected
   time window), the cron definition is the likely culprit. Verify
   with `date -u` that your UTC expression matches when you expected
   it to fire.
3. For intermittent failures inside the Python pipeline, cross-check
   `scrape_run_logs.status = 'failed'` plus `failure_kind` — the
   logger classifies the exception into `timeout / network /
   parse_error / zero_results / unknown` so you can eyeball the
   pattern without reading logs line-by-line.

```sql
-- Recent scheduled failures with their failure_kind.
SELECT scraper_key, league_name, failure_kind,
       left(error_message, 200) AS error_snippet,
       started_at
FROM scrape_run_logs
WHERE triggered_by = 'scheduler'
  AND status = 'failed'
  AND started_at >= now() - interval '7 days'
ORDER BY started_at DESC;
```

## Changing or disabling a schedule

Because the cron config lives in the Replit console, any change is a
one-click operation there — no PR needed. But if you change **what**
the script runs (e.g. add `--dry-run` for a diagnostic window, or
narrow the tier), edit the committed `.sh` under `scraper/scheduled/`
and open a PR so the change is reviewable and revertable. The console
can keep pointing at the same path.

To temporarily halt a job: flip the toggle in the Replit console. To
retire it permanently: delete from the console, then delete the
matching `.sh` in a follow-up PR.
