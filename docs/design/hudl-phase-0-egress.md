# Hudl Phase 0 spike — production-egress IP requirement

## Status

**Draft.** Captures a known constraint for an upcoming Hudl Pipeline 3 Phase 0
spike (separately tracked in the project task graph, priority P2). Not an
approved plan; just a gotcha-note so whoever runs Phase 0 doesn't burn a day on
misleading data.

## Context

Phase 0 of the Hudl Pipeline 3 work probes `fan.hudl.com` to understand:

1. What's publicly reachable without auth.
2. What the rate-limit behavior looks like.
3. Whether any CDN / bot-protection layer sits in front.

The spike is intentionally small — it's a reconnaissance pass, not a scraper
build — and its output directly informs whether Pipeline 3 is feasible as
designed.

## The constraint

**Hudl's CDN (Cloudflare / Akamai / whichever vendor they run — confirm during
Phase 0) returns different responses based on the egress IP of the requester.**
Concretely:

- A laptop request from a residential IP may sail through with `200`s that
  production would never see.
- A Replit dev shell (laptop-adjacent IP range, often tunneled through
  consumer-grade exit nodes) has similar distortion.
- **Production egress IPs** — the ones that `scraper/run.py` invocations will
  actually use in the deployed environment — are the only ones whose response
  patterns matter for the pipeline decision.

Running Phase 0 from the wrong IP tier gives false-positive reachability and
false-negative rate-limit signals. Either way, the resulting spike report is
worse than useless: it looks credible but misroutes downstream decisions (e.g.
"we don't need a proxy" when production absolutely will).

## What to do

Run Phase 0 steps 3 and 4 (the actual `fan.hudl.com` probes) from the
production egress environment:

- **On Replit:** run from the *deployed* app, not the dev shell. The
  deployment has a different egress IP range than the dev shell. Quickest
  path: add a temporary `--source hudl-phase-0-probe` handler behind a feature
  flag, trigger it via the scheduled deployment (or manually invoke the
  deployed handler), and capture output to `scrape_run_logs` or a dedicated
  probe table.
- **Alternative:** run from a known-production-IP proxy via
  `scraper/proxy_config.yaml` (the proxy plumbing landed in
  [PR #68](https://github.com/hlbiv/upshift-data/pull/68)). Requires a proxy
  provider account; none is wired today.

## What not to do

- Do NOT run Phase 0 from a laptop terminal, even for "quick checks."
- Do NOT interpret a Replit dev-shell probe as production-representative.
- Do NOT spin up a throwaway VPS on the same cloud provider as production and
  assume IP parity — cloud providers rotate egress IPs, and Hudl may treat the
  entire cloud ASN as a separate reputation bucket.

## Verification

A probe was run from the correct environment if **at least one** of the
following is true:

- The probe execution row in `scrape_run_logs` has `triggered_by='scheduler'`
  (meaning it ran inside the deployed app, not an interactive shell).
- The probe explicitly logs the outbound egress IP **and** that IP is in the
  known production allocation.

If neither is documented, the probe result should be treated as inadmissible.

## Open questions for Phase 0

- Which bot-protection vendor sits in front of `fan.hudl.com`? (Inspect
  `Server:` / `CF-Ray:` / `X-Akamai-*` response headers.)
- Does Hudl fingerprint TLS (JA3 / JA4)? If yes, `requests` and `playwright`
  may behave differently even from the same IP.
- What's the observable `429` threshold per IP? One-request-per-second?
  Burstable?

## How to run the probe

The probe script lives at `scripts/src/probe-hudl-fan.ts` and is wired as
`pnpm --filter @workspace/scripts run probe-hudl-fan`. It must run from the
deployed Replit app so the requests egress from a production IP range — not
a laptop and not the Replit dev shell.

1. Merge this PR.
2. Deploy the api-server (or any container in the production Replit
   deployment) so the script executes from production egress.
3. SSH into the deployed container, or use Replit's **Run** button on the
   deployment if interactive shells aren't available.
4. From the repo root run:
   ```bash
   pnpm --filter @workspace/scripts run probe-hudl-fan
   # or, equivalently:
   # npx tsx scripts/src/probe-hudl-fan.ts
   ```
   Optional flags:
   - `--org-id <id>` — override the default Concorde Fire org id (`65443`).
   - `--player-id <id>` — override the default profile id placeholder.
   - `--url <url>` — probe only the given URL(s); skips the 3 defaults.
   - `--extra-url <url>` — probe an additional URL on top of the defaults.
5. Copy the `/tmp/hudl-fan-probe-*.json` file produced by the run and paste
   its contents into a new doc at
   `docs/design/hudl-phase-0-probe-report.md` (create the file if it does
   not exist yet — the probe report is the deliverable for Phase 0).
6. Interpret the results:
   - **Expected healthy signal:** HTTP `200` on all 3 default URL patterns
     with real HTML in the body snippet.
   - **Flag for Phase 1:** any `403` / `451`, a `js-challenge` or
     `bot-wall` body class, a `CF-Ray` / `X-Akamai-*` header, or a
     sub-2KB empty-shell body. Any of these mean Pipeline 3 Phase 1 must
     plan for Playwright + anti-bot evasion (or a proxy vendor).

The JSON report is a full audit record; the stdout summary is the
eyeballable view for the person running the probe.

## Related

- Task graph entry: **Hudl Pipeline 3 Phase 0** (see project task tracker for
  the current ID).
- Proxy plumbing: [PR #68](https://github.com/hlbiv/upshift-data/pull/68) —
  `scraper/proxy_config.yaml` is the lever for adding egress-control once a
  provider is chosen.
- Raw-HTML archive: [PR #73](https://github.com/hlbiv/upshift-data/pull/73) —
  if Phase 0 probes a lot of pages, archive them so the probe can be
  re-analyzed without re-hitting Hudl.
