# Scheduler "Run now" — queue decision

## Status

Draft — 2026-04-20. Resolves the open question in
`upshift-studio/docs/planning/upshift-data-admin-ui.md` §"Scheduler without a
queue" and the `501 Not Implemented` stub documented in
`upshift-studio/docs/planning/upshift-data-admin-api-contract.md` for
`POST /api/v1/admin/scraper-schedules/:jobKey/run`.

Implementation is gated on user sign-off of this doc.

## Problem

The admin UI contract reserves `POST /api/v1/admin/scraper-schedules/:jobKey/run`
but the handler returns `501` until a queue exists. Today, triggering an
ad-hoc scrape means ssh-ing into Replit and running
`python3 scraper/run.py --source <key>` by hand. That's fine for the one
operator who already has shell access, but it blocks the admin-UI scheduler
panel from doing anything useful beyond displaying the three Replit
Scheduled Deployments (nightly tier-1, weekly state, hourly
canonical-club-linker — see `docs/replit-scheduled-deployments.md`).

We need the smallest possible piece of infrastructure that lets the panel's
"Run now" button actually dispatch a scrape, without walking back the
"no Redis" stance we took in `docs/design/adr-rejected.md` (PR #106).

## Options considered

### BullMQ + Redis

Industry-standard Node queue, well-documented, great observability.
Rejected: requires Redis, and Redis was explicitly declined in
`docs/design/adr-rejected.md`. The stated rationale — single-instance
API, scheduler writes are scheduled-not-request-path, no horizontal
scale yet — still holds six weeks later. Revisiting would need a new
justification, and "one admin wants a Run-now button" isn't it.

### Inngest / Trigger.dev / external SaaS

Durable event-driven schedulers with generous free tiers. Would work:
both can invoke an HTTPS webhook on a cron or on-demand basis, and we'd
wire the webhook to a new endpoint that shells out to `run.py`.
Tradeoffs are real though:

- Vendor lock-in for what is fundamentally an internal-ops feature.
- Account setup, secret management, another dashboard to keep an eye on.
- Our scheduler's entire job load (three cron jobs plus occasional manual
  runs) sits comfortably inside free-tier limits, so we'd be introducing
  a third-party dependency for ~10 invocations a day.
- Failure mode ambiguity: if Inngest has an incident, does our hourly
  linker stop? We'd need to keep Replit Scheduled Deployments as a
  fallback, at which point we're running two scheduler systems in
  parallel.

Worth noting and declining.

### Pg-backed jobs table + in-process worker (recommended)

A new `scheduler_jobs` table and a worker loop inside the existing
api-server process. Shape:

- **Table:** `scheduler_jobs(id, job_key, args jsonb, status enum
  ['pending','running','success','failed','canceled'], requested_by
  FK → admin_users, requested_at, started_at, completed_at, exit_code,
  stderr_tail text, stdout_tail text)`.
- **Admin POST** `/api/v1/admin/scraper-schedules/:jobKey/run` inserts
  a `pending` row and returns `202 Accepted` with the `job_id`.
- **Worker:** `setInterval` every 5s inside the api-server process.
  Polls with `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1` over `pending`
  rows, flips to `running`, spawns
  `python3 scraper/run.py --source <jobKey>`, captures the tail of
  stdout+stderr (e.g. last 8 KB), and updates the row with the child
  process exit code on completion.
- **Concurrency:** `LIMIT 1` per poll plus single-instance deploy means
  one scrape at a time. No contention. If we horizontal-scale the api
  later, `FOR UPDATE SKIP LOCKED` already gives us multi-worker safety
  for free.
- **Reads:** `GET /api/v1/admin/scraper-schedules/:jobKey/runs` returns
  recent pending + running + completed rows for the panel's run-log view.
  `GET /api/v1/admin/scheduler-jobs/:id` returns one run's detail
  including the captured stdout/stderr tails.

Matches the "Data is one Postgres" rationale from the Redis-rejection
ADR exactly. No new service dependencies, no new secrets, no new
dashboards. Polling at 5s is well inside acceptable latency for a
human-driven button and costs approximately nothing at our traffic
(one admin, ~10 triggers/day).

### Don't ship "Run now" yet

Keep the `501`. Admins continue using the Replit shell for ad-hoc runs.
Zero new infra, zero new schema, zero new code paths to maintain.
The cost is that the scheduler panel is half-functional — it can
display the three cron schedules but not trigger anything. Given
that the admin UI is already being built out and the panel is on
the near-term roadmap, the status-quo cost is real enough that
shipping option 3 beats shipping nothing.

## Decision

Adopt option 3: pg-backed `scheduler_jobs` table plus an in-process
worker in the api-server. Rationale: keeps Data on single-Postgres per
the existing ADR, introduces no new services, and makes the admin UI
scheduler panel functional within days rather than weeks.

## Execution sketch

Out of scope for this doc; tracked in a follow-up implementation PR.

- **Schema:** new `scheduler_jobs` table (shape above) in
  `lib/db/src/schema/`.
- **Worker:** `artifacts/api-server/src/scheduler/worker.ts` —
  `setInterval` loop, `FOR UPDATE SKIP LOCKED` poll, `child_process.spawn`
  into `python3 scraper/run.py`.
- **Routes:** `POST /api/v1/admin/scraper-schedules/:jobKey/run`
  (insert + 202); `GET /api/v1/admin/scraper-schedules/:jobKey/runs`
  (list); `GET /api/v1/admin/scheduler-jobs/:id` (detail).
- **Dashboard:** scheduler-page panel in the admin UI — schedules
  list, "Run now" button per schedule, recent-runs log.

## Non-goals

- No cross-instance queue (Redis, Kafka, BullMQ).
- No retry logic on failure — operator can re-click "Run now".
- No job prioritization — FIFO.
- No scheduled-job editing via the UI — cron editing still happens
  in the Replit console, matching the existing scheduler model.

## Open questions

- **Process placement.** Should the worker run inside the api-server
  process (simpler, shared pool) or a dedicated scheduler process
  (isolates long-running Python shell-outs from HTTP request latency)?
  This doc recommends api-server for now; split later if request
  latency starts suffering when a scrape is running.
- **Authorization.** Who can hit "Run now"? Every admin, or gated
  behind a `super_admin` role? A misclick on the nightly tier-1 job
  at 3pm could re-scrape 7 leagues unnecessarily.
- **Retention.** How long do we keep rows in `scheduler_jobs`? 90 days
  then archive? Unbounded? Truncate on a TTL job?
