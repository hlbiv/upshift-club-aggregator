# USSF Learning Center directory — Phase 0 probe report

## Status

**Complete.** Probe executed 2026-04-21 from the Replit dev shell (egress
caveat below). Phase 1 cannot proceed with a shell-only `fetch` extractor;
the directory data is not reachable without rendering the SPA or
authenticating against the underlying API.

## Decision

**`needs-browser`** — with an open question about whether an authenticated
direct-API path could replace the browser entirely.

`https://learning.ussoccer.com/directory` is a 3 611-byte Angular SPA shell
served from an S3 origin behind Amazon CloudFront. The shell ships zero
inlined data (no `__INITIAL_STATE__`, no `__PRELOADED_STATE__`, no
`__NEXT_DATA__`, no `__NUXT__`, no Apollo cache). All directory content is
fetched after hydration via Auth0-gated XHRs to a same-host backend. The
escalation tree fully exhausted at step 3:

1. **Naive fetch** — `200 OK`, identical 3 611-byte shell. No bot wall.
2. **UA-shod fetch** — `200 OK`, byte-identical to naive. UA filtering is not
   in play.
3. **Bundle inspection** — 16 `<script src>` chunks discovered, 12 fetched
   (4.5 KB – 1.9 MB each). No state hooks. The genuine app routes mined out
   of the chunks (`/api/coach/`, `/api/referee/`, `/api/safety/`,
   `/api/soccer-at-schools/`, `/api/identity/auth0/validate`) all 200 with
   the SPA fallback HTML when GET'd unauthenticated — except
   `/api/identity/auth0/validate`, which returns a Symfony `405 Method Not
   Allowed` page (1 011 bytes), confirming a real Symfony backend behind the
   Auth0 gate.

## TL;DR

USSF Learning Center is an Angular SPA on S3+CloudFront with a Symfony
backend gated by Auth0. The `/directory` HTML is empty; no inlined JSON.
Without a session cookie / Auth0 access token, the data API returns either
the SPA shell (route-not-matched on the API host) or a 405. Phase 1 needs
either:

- a headless browser to log a real account in and capture the rendered
  directory, or
- a reverse-engineered direct-API path that requires (a) discovering the
  Auth0 tenant + audience and (b) holding a valid bearer token for whatever
  account tier is needed to read the directory.

Neither is in scope for this probe. Recommend filing both as Phase 1
exploration tickets so the operator can pick.

## Run metadata

| Field           | Value                                                                 |
| --------------- | --------------------------------------------------------------------- |
| Ran at          | 2026-04-21T01:36:44.625Z                                              |
| Node            | v24.13.0 (linux x64)                                                  |
| Host            | Replit dev shell (see Caveat — egress IP)                             |
| Invocation      | `pnpm --filter @workspace/scripts run probe-ussf-directory`           |
| Naive headers   | (none — Node `fetch` defaults only)                                   |
| UA-shod headers | Chrome 124 / macOS + `Accept`, `Accept-Language`, `Sec-Fetch-*`       |
| JSON report     | `/tmp/ussf-directory-probe-2026-04-21T01-36-44-625Z.json` and committed copy at `docs/design/ussf-directory-probe-2026-04-21T01-36-44-625Z.json` |

## Per-URL results

Two URLs probed, each with back-to-back naive and UA-shod fetches.

| # | URL | Mode | Status | Body size | Class |
|---|---|---|---|---|---|
| 1 | `https://learning.ussoccer.com/directory` | naive   | `200` | 3 611 B | empty-shell-shaped HTML (classifier called it `real-html` — see note) |
| 1 | `https://learning.ussoccer.com/directory` | ua-shod | `200` | 3 611 B | same |
| 2 | `https://learning.ussoccer.com/`          | naive   | `200` | 3 611 B | same |
| 2 | `https://learning.ussoccer.com/`          | ua-shod | `200` | 3 611 B | same |

Naive vs. UA-shod delta: **zero**. Same byte count, same response shape,
same CloudFront cache behavior. No UA filtering. Latency 78–299 ms; the
first request to `/directory` was a `Error from cloudfront` cache miss, the
rest were cache hits / refresh hits.

> Classifier note: the script labelled all four bodies `real-html` because
> the body is > 2 KB and contains `<html`. In substance it's an empty SPA
> shell — every URL returns the *same* 3 611 bytes regardless of path,
> which is the textbook S3+CloudFront SPA-fallback rewrite. The discriminator
> the classifier missed: the body has no `<div id="root">` (Angular roots
> on `<app-root>`, which the script does match, but the bytes are above the
> 5 KB shell threshold so the heuristic falls through). For the decision
> the body's role as a shell is what matters — and step 3 confirms it.

## Response-header fingerprint

Identical across every request:

```
server:                    AmazonS3
via:                       1.1 <edge-id>.cloudfront.net (CloudFront)
x-cache:                   Error / Miss / RefreshHit from cloudfront
x-amz-cf-id:               <per-request nonce>
content-type:              text/html
x-content-type-options:    nosniff
x-frame-options:           DENY
strict-transport-security: max-age=31536000; preload
```

Absent and noteworthy:

- No `cf-ray`, `cf-cache-status`, `cf-mitigated` → not Cloudflare.
- No `x-akamai-*` → not Akamai.
- No challenge cookies (`__cf_bm`, `ak_bmsc`, `_abck`) — no bot product is
  in front of the static shell at all.
- No `set-cookie` of any kind on the static shell.
- No rate-limit headers seen in this one-shot run; not measured.

CDN verdict: **Amazon CloudFront in front of an S3 origin** for the static
SPA shell. The data API lives on the same hostname but is routed past S3
(presumably to a Symfony / API Gateway backend) — see the next section.

## HTML shape

First 500 chars of every 200 body (literally byte-identical across all four
fetches and across `/`, `/directory`, `/api/coach/`, `/api/referee/`,
`/api/safety/`, `/api/soccer-at-schools/`):

```html
<!doctype html>
<html lang="en">
<head>
  <script>
    const q = '&gtm_auth=-kXIOLb8Q6Q7fRnlLLfwLQ&gtm_preview=env-2&gtm_cookies_win=x';
    (function(w,d,s,l,i,q){
      w[l]=w[l]||[];
      w[l].push({'gtm.start': new Date().getTime(),event:'gtm.js'});
      ...
    })(window,document,'script','dataLayer','GTM-MFW96LW',q);
  ...
```

Key markers in the full shell body:

- `<app-root>` with no children → confirmed Angular.
- 16 `<script src="…-cachebust-*.js">` references — Angular CLI build
  output (`runtime~polyfills`, `vendors-*`, etc.).
- One `<gtm-noscript>` iframe (Google Tag Manager).
- A `link rel="preconnect"` to `https://learning.ussoccer.com/` only — no
  third-party API host preconnects (so the API hostname is the same as the
  shell hostname).
- No JSON state container, no SSR-rendered list elements, no
  `data-prerendered` markup.

## Bundle inspection

12 of the 16 chunks were fetched (script's `BUNDLE_FETCH_LIMIT`). Two
chunks carried the actionable signal:

| Bundle | Size | Notable hits |
|---|---|---|
| `vendors-9241dc7c-cachebust-50a641ac.js` | 825 KB | `/api/coach/`, `/api/referee/`, `/api/safety/`, `/api/soccer-at-schools/`, `/api/identity/auth0/validate`, `https://e.logrocket.com/api/3/store/…` |
| `vendors-27545368-cachebust-c60daa8f.js` | 621 KB | Sentry SDK + `https://o447951.ingest.sentry.io/api/4509632503087104/envelope/`; `graphql.document` / `graphql.operation` strings (Sentry's GraphQL instrumentation tags, not a USSF GraphQL endpoint) |
| `vendors-40226e3a-cachebust-754857a0.js` | 1.6 MB | 45 `/api/` matches, all from the bundled **AWS SDK** (false positives — `iam.amazonaws.com/doc/2010-05-08/` etc.). No USSF business path. |

The probe's first run flagged 33 candidate API URLs but most were noise:
AWS SDK doc namespaces and Angular framework `@link` references to
`angular.io/api/...`. The script was fixed mid-probe to (a) resolve
root-relative URLs against the page origin so they're actually fetchable,
and (b) deny-list framework-doc hosts (`angular.io`, `angular.dev`,
`docs.angularjs.org`, `react.dev`, MDN, etc.). Re-run yielded 28 candidates
of which 5 were the real USSF backend routes listed in the table above.

### State-hook search

| Hook | Found? |
|---|---|
| `__INITIAL_STATE__` | no |
| `__PRELOADED_STATE__` | no |
| `__NEXT_DATA__` | no |
| `__NUXT__` | no |
| `window.__APP_STATE__` | no |
| `window.__DATA__` | no |
| `window.__APOLLO_STATE__` | no |
| `self.__remixContext` | no |

Zero inlined data. The shell is pure boot scaffolding.

### Direct API GETs (probe step 3)

The probe attempted GETs against the discovered candidates. Results
(de-duplicated by behavior):

| URL pattern | Status | Content-type | Body |
|---|---|---|---|
| `https://learning.ussoccer.com/api/v1/*` (Box-style: `/documents`, `/users`, `/folders`, `/me/root`, …) | `200` | `text/html` | The 3 611-byte SPA shell. **SPA fallback** — these are AWS SDK strings, not USSF endpoints. |
| `https://learning.ussoccer.com/api/coach/`, `/api/referee/`, `/api/safety/`, `/api/soccer-at-schools/` | `200` | `text/html` | Same SPA shell. CloudFront falls through to S3 for any path the API tier doesn't claim. **These are Angular client-side routes**, not REST endpoints — the SPA owns the user-facing `/api/coach` page. |
| `https://learning.ussoccer.com/api/identity/auth0/validate` | `405` | `text/html; charset=utf-8` | Symfony `Method Not Allowed` error page (1 011 bytes). **Real backend confirmed.** This route accepts POST, gated by Auth0. |
| Sentry / LogRocket ingest URLs | n/a | n/a | Telemetry endpoints; not data. |

The 405 on `/api/identity/auth0/validate` is the smoking gun: the API tier
is alive, it just rejects unauthenticated GETs. Every other `/api/*` path
GET'd by the probe falls through to the static SPA shell because S3 is the
default origin.

## Implications for Phase 1

**Confirmed blockers for a shell-only `fetch` extractor:**

1. `/directory` ships no inlined data. Cannot parse names out of the HTML.
2. The data API is auth-gated. The tenant is Auth0 (`/api/identity/auth0/validate`
   route name + `auth0` substring inside `vendors-9241dc7c`).
3. The auth flow is interactive (Auth0 Universal Login), so a non-browser
   client would need to either (a) hold a long-lived service-account token
   or (b) drive the OAuth code-exchange dance against the USSF Auth0 tenant
   — the tenant ID and audience are not yet captured.

**What Phase 1 has to choose between:**

- **Headless browser path** (Playwright-in-TS, first-in-codebase
  precedent): log a real coach/instructor account in via Universal Login,
  navigate to `/directory`, scrape the rendered DOM. Pro: certain to work,
  matches user-visible reality. Con: introduces Playwright as a
  dependency and an account-credential requirement.
- **Direct-API path** (still no Playwright): a separate spike to (i) walk
  the network tab of an authenticated `/directory` session in DevTools to
  identify the actual REST/GraphQL data call and its required headers
  (`Authorization: Bearer …` is the working assumption), (ii) reverse the
  Auth0 token-acquisition for a service identity, (iii) build a token-cache
  and call the data endpoint directly. Pro: no browser. Con: probably
  needs USSF account credentials anyway, and Auth0 may rate-limit / detect
  non-browser token acquisitions.

**Not in scope for this probe:** picking between those two. Both require
authentication; the cost / risk profile differs.

**Outright dead-ends from this probe:** there is no inlined JSON, no
GraphQL endpoint discoverable in client bundles, no public read API, and
no SPA-shell trick (e.g. cached SSR snapshot, sitemap with names) that
would yield directory data without auth.

## Caveat — egress IP

Probe ran from the Replit dev shell, not from production egress. Same
caveat applies as in [hudl-phase-0-egress.md](hudl-phase-0-egress.md): if
USSF / CloudFront discriminates on egress IP, a production run might see
different cache behavior, different (or any) bot-protection headers, or
different Auth0 token-issuance behavior. The decision here is robust to
that risk because:

1. The block we hit is **auth, not IP** — `405 Method Not Allowed` proves
   the backend is reachable and just wants a credentialed POST. A
   different egress IP doesn't change authn requirements.
2. Naive and UA-shod fetches were byte-identical, ruling out UA-based
   discrimination at the static-asset layer.

If a Phase 1 follow-up exploration wants to check production-egress
behavior anyway, re-run with:

```
pnpm --filter @workspace/scripts run probe-ussf-directory
```

from the deployed Replit container.

## Related

- Probe script: `scripts/src/probe-ussf-directory.ts`
- Sibling probes:
  - [`hudl-phase-0-probe-report.md`](hudl-phase-0-probe-report.md)
  - [`usl-w-league-phase-0-probe-report.md`](usl-w-league-phase-0-probe-report.md)
- Egress constraint doc: [`hudl-phase-0-egress.md`](hudl-phase-0-egress.md)
- Raw JSON probe output (committed copy):
  `docs/design/ussf-directory-probe-2026-04-21T01-36-44-625Z.json`
