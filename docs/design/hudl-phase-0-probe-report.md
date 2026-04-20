# Hudl Pipeline 3 — Phase 0 probe report

## Status

**Complete.** Probe executed 2026-04-20 from Replit. Phase 1 is unblocked
pending the egress-IP verification note below.

## TL;DR

All three `fan.hudl.com` URL patterns returned `200 OK` with real Next.js HTML
bodies (~155 KB each). No WAF challenge, no Cloudflare `CF-Ray`, no Akamai
fingerprint, no `403` / `451` / `429`. The CDN in front of Hudl is **Amazon
CloudFront**, not the Cloudflare / Akamai stack we hedged for in
[hudl-phase-0-egress.md](hudl-phase-0-egress.md).

Pipeline 3 Phase 1 can proceed with the `fetch`-based approach — Playwright is
**not** required for the public shell. yt-dlp / embed-following is still
unproven and belongs to Phase 1.

## Run metadata

| Field            | Value                                                                 |
| ---------------- | --------------------------------------------------------------------- |
| Ran at           | 2026-04-20T18:48:46.614Z                                              |
| Node             | v24.13.0 (linux x64)                                                  |
| Host             | Replit (`~/workspace`)                                                |
| Invocation       | `pnpm --filter @workspace/scripts run probe-hudl-fan`                 |
| User-Agent sent  | Chrome 124 / macOS (the script's default)                             |
| JSON report      | `/tmp/hudl-fan-probe-2026-04-20T18-48-46-614Z.json` (on Replit)       |

### Egress-IP verification — ⚠️ open

[hudl-phase-0-egress.md](hudl-phase-0-egress.md) requires the probe to run from
a production-egress IP (deployed Replit container), not the dev shell. The
probe script does not self-report its outbound IP, and this run was invoked
from an interactive shell prompt (`~/workspace$`). If that shell was SSH'd
into the deployed container, we're good; if it was the dev shell, findings may
under-count bot-wall risk.

**Action:** confirm which Replit tier the run came from. If dev-shell, re-run
from the deployed app before committing to the Phase 1 plan. A cheap way to
check without re-running: the JSON report captures `x-amz-cf-id` values — any
future production run will hit a different CloudFront edge, which doesn't
confirm the IP tier but is a tell if the edges diverge.

## Per-URL results

### 1. Profile page — `https://fan.hudl.com/profile/placeholder`

| Field       | Value                                                             |
| ----------- | ----------------------------------------------------------------- |
| Status      | `200 OK`                                                          |
| Elapsed     | 406 ms                                                            |
| Body size   | 155 536 bytes                                                     |
| Classifier  | `real-html`                                                       |
| Cache       | `Miss from cloudfront`                                            |

### 2. Organization page — `https://fan.hudl.com/organization/65443`

Concorde Fire, per `video-intelligence.md` in `upshift-studio`.

| Field       | Value                                                             |
| ----------- | ----------------------------------------------------------------- |
| Status      | `200 OK`                                                          |
| Elapsed     | 347 ms                                                            |
| Body size   | 155 532 bytes                                                     |
| Classifier  | `real-html`                                                       |
| Cache       | `Miss from cloudfront`                                            |

### 3. Fan index — `https://fan.hudl.com/`

| Field       | Value                                                             |
| ----------- | ----------------------------------------------------------------- |
| Status      | `200 OK`                                                          |
| Elapsed     | 39 ms                                                             |
| Body size   | 155 423 bytes                                                     |
| Classifier  | `real-html`                                                       |
| Cache       | `Hit from cloudfront`                                             |

The cache hit explains the 10x latency delta vs. the other two — the index is
hot at the edge, profile / org pages are not.

## Response-header fingerprint

Identical across all three URLs (values vary only in request-specific
identifiers):

```
server:                   CloudFront
via:                      1.1 <edge-id>.cloudfront.net (CloudFront)
x-cache:                  Miss from cloudfront   (Hit on the index)
x-amz-cf-id:              <per-request nonce>
content-type:             text/html; charset=utf-8
x-content-type-options:   nosniff
x-frame-options:          SAMEORIGIN
strict-transport-security: max-age=31536000
```

Absent (and this is the news):

- No `cf-ray` / `cf-cache-status` → not Cloudflare.
- No `x-akamai-*` → not Akamai.
- No `set-cookie` bot-challenge pair (`__cf_bm`, `ak_bmsc`, etc.).
- No `retry-after` / `x-ratelimit-*` on these three requests (one-shot only —
  rate-limit thresholds are still unknown, flagged for Phase 1).

## HTML shape

Body snippet (first 500 chars, identical shell across all three URLs):

```html
<!DOCTYPE html><html lang="en" data-host="fan.hudl.com"><head>
  <meta charSet="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <link rel="stylesheet" href="https://assets.hudl.com/_next/v2/fan/_next/static/css/9e188b79b0ec4455.css" .../>
  <link rel="stylesheet" href="https://assets.hudl.com/_next/v2/fan/_next/static/css/99dc298579bb6745.css" .../>
  ...
```

Key takeaways:

- `fan.hudl.com` is a **Next.js** app served out of `assets.hudl.com/_next/v2/fan/`.
- 155 KB real HTML means there's a meaningful server-rendered shell before
  hydration — good for a `fetch` + Cheerio / HTML parser approach. We don't
  need a headless browser to see the initial page structure.
- Whether the *video embed* data is in the initial HTML or lazy-loaded via a
  JSON endpoint is **not** something this probe answered. That's the first
  question of Phase 1.

## Implications for Phase 1

**Greenlit:**

- Proceed with a `requests` / `fetch`-based scraper for the public shell.
  Playwright is not required to get past the CDN.
- No proxy vendor needed yet; revisit only if Hudl tightens rate limits on
  repeated hits from the production Replit IP range.

**Still unknown — Phase 1 must answer:**

1. Is the video metadata (IDs, titles, publish dates) in the initial SSR HTML,
   or is it fetched client-side via a Hudl JSON API? Grep the 155 KB body for
   known video IDs to find out.
2. What's the `429` threshold per IP? Burstable? Per-path?
3. Does the embed URL for a video clip require an auth cookie, or is it
   session-less? (The $15 Access Pass question — pay only if session-less
   playback is blocked.)
4. Does Hudl fingerprint TLS (JA3 / JA4)? `fetch` and Playwright present
   different fingerprints — if the JSON API gates on JA3, `fetch` may get
   200s on HTML and 403s on the data call.

## Related

- Constraint doc: [hudl-phase-0-egress.md](hudl-phase-0-egress.md)
- Probe script: `scripts/src/probe-hudl-fan.ts`
- Proxy plumbing (if ever needed): [PR #68](https://github.com/hlbiv/upshift-data/pull/68)
- Raw-HTML archive (for replay of Phase 1 probes): [PR #73](https://github.com/hlbiv/upshift-data/pull/73)
- Pilot target: ECNL NC Spring Boys or SC Girls showcase, by 2026-05-08.
