# USSF Learning Center — Phase 1A unauthenticated browser probe

**Date:** 2026-04-21
**Probe:** `pnpm --filter @workspace/browser-probes run probe-ussf-browser`
**Source:** `scripts/browser-probes/src/probe-ussf-browser.ts`
**Raw output:** `/tmp/ussf-browser-probe-2026-04-21T02-13-05-137Z.json`
**Predecessor:** Phase 0 (`scripts/src/probe-ussf-directory.ts`, decision `needs-browser`, see `docs/design/ussf-phase-0-probe-report.md`)

---

## TL;DR

Phase 0 marked `learning.ussoccer.com/directory` as `needs-browser` because every shell-level fetch returned the same 3,611-byte Angular shell — none of the discovered API URLs returned JSON. **Phase 1A reverses that conclusion in part:** when Chromium executes the bundle, the SPA fires **12 XHR/fetch calls before any auth gate**, and **9 of them return JSON (62.3 KB total)** with no credentials supplied.

The good news: a meaningful slice of what the directory page renders comes from publicly-reachable WordPress REST and certifications-API endpoints. The bad news: the actual searchable coach / referee **roster** is not in those responses — it appears to live behind a different authenticated API surface that the directory page does not call until the user logs in.

**Revised decision:**

| Surface | Decision | Notes |
|---|---|---|
| License catalog (referee + coach license types) | `shell-fetchable` | `/certifications/public/licenses` returns 9.5 KB JSON. |
| WP CMS labels & directory UI copy | `shell-fetchable` | `/wp-json/wp/v2/directory/{1177-1185}` returns CMS-managed text blocks. |
| App init config / alerts | `shell-fetchable` | `/api/init` (45 KB), `/wp-json/wp/v2/alerts` (5.8 KB). |
| WP authors | `shell-fetchable` | `/wp-json/wp/v2/users` returns 2.9 KB. |
| Coach / referee individual records (the actual roster) | `needs-credentials` | No public endpoint discovered. Auth0 redirect happens but is `prompt=none`, so it's a silent session-check, not a forced login. Roster XHR likely fires only after a session is established. |

---

## What the probe does

`scripts/browser-probes/probe-ussf-browser.ts` drives a fresh-context headless Chromium against the target URL, with no credentials, no cookies, and no localStorage. For every XHR / fetch / document / script request the page initiates it captures:

- request URL, method, resource type, headers (with `cookie` stripped)
- response status, content-type, content-length, and the first 1.5 KB of body for textual responses
- timestamps relative to navigation start

It runs the SPA for `domcontentloaded + 6s` of post-load settling — long enough for the initial XHR storm but short enough that long-poll websockets don't keep us forever.

It then classifies the run on three signals:

1. **Auth-redirect detection.** Pattern-match navigation requests against `*.auth0.com`, `*.okta.com`, `cognito`, `b2clogin`, plus generic `/oauth2/authorize` paths. If matched, record the URL, parsed query string, tenant, and time-to-redirect.
2. **Pre-auth data accounting.** For every XHR/fetch response received *before* the first auth-redirect timestamp, count 2xx JSON responses, total bytes, and capture URLs.
3. **DOM shape.** Count `<tr>`, `<li>`, card-class divs, inputs, iframes; capture H1 / H2 text. Cheap heuristic for "did directory data actually render?".

Decision tree: `public-data-extractable` (any pre-auth JSON or directory-shaped DOM) → `auth-required-with-public-config` (auth-redirect with some pre-auth XHR) → `auth-required-no-public-surface` (auth-redirect with empty DOM and no XHR) → `blocked-or-failed`.

**Hard scope cuts:** no credentials loaded / sent / stored, no login form filled, no cookie injection, no DB writes. Sub-package `scripts/browser-probes/` keeps Playwright (167 MB chromium binary + 112 MB headless-shell) isolated from `@workspace/scripts`.

---

## Findings

### 1. Auth0 redirect is silent, not blocking

The page does redirect to Auth0 at **5,366 ms** post-navigation, but it does so with `prompt=none&response_mode=web_message`. That's the standard Auth0-Angular SDK pattern for "is there an existing session?" — the IdP returns immediately via `postMessage` without showing a login form. The SPA continues to render either way.

Captured Auth0 query parameters:

```
client_id          = hR4G2TnKbeprDVmCYHgJ0Uih3ULyXZby
audience           = https://ussf.us.auth0.com/api/v2/
scope              = openid profile email offline_access
redirect_uri       = https://learning.ussoccer.com/callback
response_type      = code
response_mode      = web_message       ← silent flow, no UI
prompt             = none               ← skip login UI; fail if no session
code_challenge_method = S256            ← PKCE
auth0Client        = @auth0/auth0-angular 2.2.3 / Angular 19.2.0
```

**Implication:** the redirect is a session probe, not a wall. Anything the SPA fetches before this redirect, or via XHRs that don't depend on the session result, is reachable unauthenticated.

### 2. Twelve XHR/fetch calls fire pre-auth, nine return 2xx JSON

| t (ms) | Endpoint | Status | Bytes | Content-Type |
|--:|---|--:|--:|---|
| 4,415 | `learning.ussoccer.com/api/init` | 200 | 45,476 | application/json |
| 4,774 | `learning.ussoccer.com/wp-json/wp/v2/alerts` | 200 | 5,848 | application/json |
| 4,907 | `learning.ussoccer.com/wp-json/wp/v2/directory/1177` | 200 | 2,376 | application/json |
| 4,907 | `learning.ussoccer.com/wp-json/wp/v2/directory/1178` | 200 | 1,373 | application/json |
| 4,910 | `learning.ussoccer.com/wp-json/wp/v2/directory/1179` | 200 | 1,396 | application/json |
| 4,910 | `learning.ussoccer.com/wp-json/wp/v2/directory/1180` | 200 | 1,477 | application/json |
| 4,910 | `learning.ussoccer.com/wp-json/wp/v2/directory/1185` | 200 | 1,314 | application/json |
| 4,910 | `learning.ussoccer.com/wp-json/wp/v2/directory/1183` | 200 | 1,467 | application/json |
| 4,910 | `learning.ussoccer.com/wp-json/wp/v2/directory/1184` | 200 | 1,556 | application/json |
| 4,911 | `connect.learning.ussoccer.com/certifications/public/licenses` | 200 | 9,552 | application/json |
| 4,415 | `learning.ussoccer.com/static/build` | 200 | 0 | binary/octet-stream |
| 4,771 | `sentry.io/api/103600/envelope/...` | 200 | 2 | application/json (POST telemetry) |

The Auth0 redirect happens at 5,366 ms, *after* every one of those JSON responses landed.

### 3. The "directory" WP responses are CMS labels, not people

I confirmed by curl from production egress:

```
GET https://learning.ussoccer.com/wp-json/wp/v2/directory/1177
→ {"id":1177,"slug":"header","title":{"rendered":"Header"},
   "content":{"rendered":"<p>The directory serves as a verification of record
   for users that have current coaching, referee and compliance related
   certifications on file with U.S. Soccer ..."}}
```

These are WordPress posts of post-type `directory` whose content is the *page chrome* (header copy, tooltips, search-result section labels) for the directory UI — not a list of coaches. Confirmed against IDs 1177-1185.

### 4. The license catalog IS the catalog, and it's open

```
GET https://connect.learning.ussoccer.com/certifications/public/licenses
→ [{"name":"Referee","license_id":"referee_8","discipline":"referee","rank":10,"type":"referee_pathway"},
   {"name":"FIFA Futsal Referee","license_id":"referee_11", ...},
   {"name":"National D","license_id":"coach_3","discipline":"coach", ...},
   {"name":"Pro License","license_id":"coach_16","discipline":"coach","rank":1, ...},
   ...]
```

This is the canonical license-type table — every coaching and refereeing certification U.S. Soccer issues, with a discipline, type, and rank. Useful as a reference table for any downstream extractor.

### 5. The actual roster has no public endpoint we could find

I tried obvious WP REST collection endpoints and the connect-API public namespace:

| Endpoint | Status | Notes |
|---|--:|---|
| `learning.ussoccer.com/wp-json/wp/v2/users` | 200 | 2.9 KB; WP authors only, not coaches. |
| `learning.ussoccer.com/wp-json/wp/v2/coaches` | 200 | 3.6 KB — **SPA fallback HTML**, not a real REST route. |
| `learning.ussoccer.com/wp-json/wp/v2/coach` | 200 | Same SPA fallback. |
| `learning.ussoccer.com/wp-json/wp/v2/referees` | 200 | Same SPA fallback. |
| `learning.ussoccer.com/wp-json/wp/v2/types` | 200 | 10.9 KB — WP post-type discovery (useful for finding more public CMS routes). |
| `connect.learning.ussoccer.com/certifications/public/coaches` | 404 | — |
| `connect.learning.ussoccer.com/certifications/public/search` | 404 | — |
| `connect.learning.ussoccer.com/directory/public/search` | 404 | — |
| `connect.learning.ussoccer.com/public/coaches` | 404 | — |
| `connect.learning.ussoccer.com/public/directory` | 404 | — |

The `connect.learning.ussoccer.com` host serves a small public surface (`/certifications/public/licenses`) and otherwise 404s on guesses. The roster XHR — whatever its path — must fire only after the SPA confirms an authenticated session.

The rendered DOM tells the same story: `<h1>Directory</h1>`, `<h2>Search</h2>`, 5 inputs, 13 list items, no `<tr>` rows. The directory UI scaffold rendered, but the result list did not populate without auth.

---

## What this changes for downstream work

**Phase 0's "needs-browser" verdict is now scoped:** browser automation is *not* required to extract the public reference data (license types, app config, alerts, CMS labels). A shell-level extractor using the same `fetch` + classifier infrastructure as `scripts/src/lib/spa-probe.ts` can ship today against:

- `https://connect.learning.ussoccer.com/certifications/public/licenses` — license-type reference table
- `https://learning.ussoccer.com/wp-json/wp/v2/alerts` — system alerts
- `https://learning.ussoccer.com/wp-json/wp/v2/directory` (and `/{id}`) — directory page CMS labels
- `https://learning.ussoccer.com/wp-json/wp/v2/users` — WP authors (low value)
- `https://learning.ussoccer.com/wp-json/wp/v2/types` — post-type registry (use to discover more public collections)
- `https://learning.ussoccer.com/api/init` — app config blob (worth grepping for additional public endpoints)

**Browser automation is still required for the actual roster.** Even with Playwright we cannot get past `prompt=none` without a real session. To extract individual coach / referee records and their license history, we need either:

1. **Credentials** (still unavailable per the most recent direction).
2. **A public roster endpoint we missed.** The `/api/init` blob (45 KB) and the `/wp-json/wp/v2/types` registry are the two best places to look — both are now in our shell-fetchable surface, so a single follow-up Phase-0-style probe can enumerate every public REST route the WP install advertises.
3. **An alternate USSF-published data source** (e.g. annual coach-development reports, NSCAA / United Soccer Coaches integrations, FIFA referee databases). Out of scope for this probe.

---

## Operational notes

- **Probe runtime:** ~12 s wall-clock on Replit container egress. Playwright launched headless chromium with `--no-sandbox --disable-dev-shm-usage` and reached the SPA's settled state at ~10s.
- **Disk footprint:** Playwright's chromium-1217 binary occupies 170 MB and the headless-shell binary 112 MB at `/home/runner/workspace/.cache/ms-playwright/`. These are ephemeral (not committed) and will re-download on a fresh container if not cached.
- **Sub-package isolation:** The new `@workspace/browser-probes` package keeps Playwright out of `@workspace/scripts`'s dependency tree. `pnpm --filter @workspace/scripts run typecheck` does not need playwright to resolve. Adding more browser probes belongs here.
- **Rerun:** `pnpm --filter @workspace/browser-probes run probe-ussf-browser` from any container with chromium installed. Pass `--url <other>` to point at a different USSF page (e.g. `/courses` if we want to map the course catalog API).
