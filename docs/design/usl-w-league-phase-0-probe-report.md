# USL W League — Phase 0 probe report

## Status

**Complete.** Probe executed 2026-04-20 from the Claude agent's local egress
(see caveat at the bottom). All target URLs returned server-rendered HTML
with `fetch`; no bot wall, no challenge, no UA filtering. Phase 1 extractor
is unblocked.

## Decision

**`hard-go`.**

The `/league-schedule`, `/league-standings`, and per-team pages each return
`200 OK` with 40–132 KB of real server-rendered HTML on a naive `fetch` (no
`User-Agent`, no `Accept`), and each body contains a literal
`https://www.modular11.com/…/w-league` iframe reference. That single signal
clinches the call: USL W League is hosted on **SportsEngine / SportNgin
SiteBuilder** (page id `ngin55538`, assets on `app-assets3.sportngin.com`),
with a Modular11 widget embedded for schedule + standings. The SportNgin
site itself is the primary extractor target — the club directory is already
a flat list of `href="/<team-slug>"` anchors in the `/league-teams` HTML
(132 KB, server-rendered). Modular11 is a bonus secondary source for match
data if we want it later, and `scraper/extractors/usl_academy.py` is the
reference implementation for that path.

The 403 noted in the task brief could not be reproduced from this egress.
The naive fetch of `https://uslwleague.com/` (apex, no `www`) returns a
`302` redirect to `https://www.uslwleague.com/`; the redirect target is the
200 real-html body. If an older caller was following redirects automatically
and hitting a different path, or if the 403 was CloudFront-side rate-limit
fallout, it's not a platform-level block.

## TL;DR

USL W League runs on **SportNgin SiteBuilder + a Modular11 iframe for
schedule/standings**. Every content URL returns real server-rendered HTML on
a naive `fetch`, including the 132 KB `/league-teams` page which is a flat
list of `href="/<slug>"` anchors to per-club pages. Build the extractor as a
`fetch`+Cheerio parser — Playwright is not needed, and the Modular11 piece
is a clean clone of `scraper/extractors/usl_academy.py` if we want match
data beyond the club list.

## Run metadata

| Field           | Value                                                                 |
| --------------- | --------------------------------------------------------------------- |
| Ran at          | 2026-04-20T22:19:45.688Z                                              |
| Node            | v24.14.1 (darwin arm64)                                               |
| Host            | Claude agent local egress (see Caveat)                                |
| Invocation      | `pnpm --filter @workspace/scripts run probe-usl-w-league -- --url <…>` |
| Naive headers   | (none — Node `fetch` defaults only)                                   |
| UA-shod headers | Chrome 124 / macOS + `Accept`, `Accept-Language`, `Sec-Fetch-*`       |
| JSON report     | `/tmp/usl-w-league-probe-2026-04-20T22-19-45-689Z.json`               |

## Per-URL results

Six URLs probed, each with back-to-back naive and UA-shod fetches. Status /
body size / class / Modular11 hits were **identical between naive and
UA-shod for every URL**, so there is no UA filtering on this site — both
modes are reported together below.

| # | URL | Status | Body size | Class | Modular11 hits |
|---|---|---|---|---|---|
| 1 | `https://uslwleague.com/` (apex) | `302` → `www` | 93 B | redirect | 0 |
| 2 | `https://www.uslwleague.com/` (index) | `200` | 74 846 B | real-html | 0 |
| 3 | `https://www.uslwleague.com/league-teams` | `200` | 132 455 B | real-html | 0 |
| 4 | `https://www.uslwleague.com/league-schedule` | `200` | 40 060 B | real-html | **1** |
| 5 | `https://www.uslwleague.com/league-standings` | `200` | 61 604 B | real-html | **1** |
| 6 | `https://www.uslwleague.com/ac-connecticut` (team page) | `200` | 90 574 B | real-html | **1** |

Timings were 70–200 ms per request, all from Cloudflare's ATL edge.

### Naive vs. UA-shod delta

Zero. For every URL, the naive fetch and UA-shod fetch returned the same
status, the same byte count, and the same `cf-ray`-family response headers
(modulo per-request nonces). The site does not care about `User-Agent` in
front of the `www.` host.

### Apex 302 → `www` host

`https://uslwleague.com/` issues a `302 Found` with `Location:
https://www.uslwleague.com/`. The probe uses `redirect: "manual"`, so this
shows as a 93-byte redirect rather than falling through to the 200. Any
real extractor will either use `redirect: "follow"` (Node `fetch` default)
or hard-code the `www.` host up front.

### Default-URL guess miss (for the record)

The probe's default URL list (`/teams`, `/standings`, `/schedule`,
`/clubs`) all 404 on `www.uslwleague.com`. That's the SportNgin 404 page
(~4.5 KB, real HTML, still via Cloudflare), not a block. The actual paths
are `/league-teams`, `/league-schedule`, `/league-standings` — discovered
by grepping `href=` values out of the index HTML and visible in the
`pathway-link` anchors:

```html
<a href="https://www.uslwleague.com/league-schedule">…
<a href="https://www.uslwleague.com/league-teams">…
```

The report's "canonical" probe run invoked with explicit `--url` /
`--extra-url` flags to hit the real paths. The default URL guesses are kept
in the script because they're the paths an operator would try first when
onboarding a new SportNgin site — the 404s are informative.

## Modular11 investigation

**Hits:** 3 URLs, 1 hit each, both naive and UA-shod (i.e. the Modular11
reference is baked into the server-rendered HTML, not injected
client-side).

| URL | Context (80-char window around `modular11.com`) |
|---|---|
| `/league-schedule` | `l" > <div class="pageElement codeElement"> <iframe src="https://www.modular11.com/league-schedule/w-league" style="width: 100%; height: 150vh;" frameborder="0"><` |
| `/league-standings` | `<div class="pageElement codeElement"> <!---<iframe src="https://www.modular11.com/league-standings/w-league" style="width: 100%; height: 150vh;" frameborder="0">` (note: HTML-commented out on the standings page — live iframe may be elsewhere on the page) |
| `/ac-connecticut` (team page) | Same pattern — per-team pages embed the Modular11 widget too. |

**Tenant name:** `w-league`. That's the analogue of `usl-academy.com`'s
Modular11 tenant — the one whose standings API is scraped by
`scraper/extractors/usl_academy.py` via
`https://www.modular11.com/public_schedule/league/get_teams?…&UID_event=22&…`.

**Unknown and out of scope for this probe:** the specific `UID_event`,
`UID_age`, and `UID_gender` values for the `w-league` tenant. The
`usl_academy.py` docstring explains that Modular11 assigns a fresh
`UID_event` per competition year; for USL W League the `_CURRENT_EVENT_ID`
will have to be discovered via devtools on the live
`modular11.com/league-standings/w-league` page, exactly as documented in
the existing extractor's SEASONAL MAINTENANCE note.

## Response-header fingerprint

Identical across every 200, every 302, and every 404:

```
server:                   cloudflare
cf-ray:                   <edge>-ATL   (per-request)
cf-cache-status:          DYNAMIC
content-type:             text/html; charset=utf-8
set-cookie:               _cfuvid=…; HttpOnly; Secure; SameSite=None
x-content-type-options:   nosniff
```

Absent and noteworthy:

- No `cf-mitigated: challenge` → Cloudflare is NOT running a JS challenge or
  managed challenge against this egress for this site.
- No `cf-bot-score` or `cf-threat-score` returned to clients (expected; these
  stay server-side).
- No `x-akamai-*` → not Akamai.
- No SportNgin origin headers leaked past Cloudflare — can't confirm the
  backing tier directly, but the asset hostnames (`app-assets3.sportngin.com`,
  `cdn1.sportngin.com`, `cdn4.sportngin.com`) and the `ngin55538` page id in
  the `<html>` tag are unambiguous. SportNgin is the SportsEngine rebrand;
  the `seAnalyticsGateway` script reference hard-codes the legacy
  `sportngin.com` origin tests.

CDN verdict: **Cloudflare in front of SportsEngine/SportNgin origin.** No
bot-wall product in the response surface.

## HTML shape

First 500 chars of every 200 body (identical shell across index, teams,
schedule, standings, per-team):

```html
<!DOCTYPE html>
<html lang="en-us" id="ngin55538">
<head>

<!--[if lte IE 9]>
    <meta http-equiv="refresh" content="0; url=/unsupported_browser" />
    <script type="text/javascript">
        window.top.location = '/unsupported_browser';
    </script>
<![endif]-->
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="csrf-param" content="authenticity_token" />
<meta name="csrf-token" cont…
```

Key markers inside `/league-teams` (132 KB):

- SportNgin template (`id="ngin55538"` on `<html>`, `app-assets3.sportngin.com`
  script hosts).
- Flat club directory as `<a href="/<team-slug>">` anchors — confirmed a
  sample of 40 unique team slugs on the page, e.g. `/ac-connecticut`,
  `/albion-sc-colorado`, `/afc-ann-arbor`, `/birmingham-legion-wfc`,
  `/charlotte-eagles`, `/detroit-city-fc`, `/indy-eleven`, …
- Also: `href="http://uslwleague.com/annapolis-blues-fc"` (note apex + http)
  and `href="https://www.uslwleague.com/CLEVELAND-FORCE"` (uppercase slug) —
  the extractor parser will want to normalize scheme, case, and the apex /
  `www` split before deduping.

Per-team page sample (`/ac-connecticut`, 90 KB):

- Same SportNgin shell.
- Embeds the Modular11 iframe for that team's division standings.
- Contains team-level content (news posts, roster links, social) — the
  exact shape that needs a Cheerio parser decision is out of scope for
  Phase 0.

## Implications for Phase 1

**Greenlit: build a SportNgin `fetch`+Cheerio extractor.**

- **Clone target:** not `usl_academy.py` directly (that's the Modular11 API
  path). The cleanest precedent in `scraper/extractors/` for a
  SportNgin-SiteBuilder flat-list page is the pattern used by any extractor
  that parses a league index from static HTML — pick one that returns
  `List[Dict]` records with `club_name`, `city`, `state`, `source_url`.
- **URL to seed:** `https://www.uslwleague.com/league-teams` (the 132 KB
  server-rendered club directory).
- **Parse strategy:** BeautifulSoup over the HTML, grab every `<a>` whose
  `href` matches `^(https?://(www\\.)?uslwleague\\.com)?/[a-z0-9-]+$` and
  is not in a known non-team denylist (`/league-teams`, `/league-schedule`,
  `/league-standings`, `/about`, `/privacy_policy.pdf`, `/news_article/…`,
  `/unsupported_browser`, `/`, …). Normalize `http` → `https`, apex →
  `www`, lowercase slug. Deduplicate. One `club_name` per unique slug —
  the anchor text gives a human-readable name; if inconsistent,
  re-fetch each per-team page and read `<title>` or the `<h1>`.
- **No rate-limit findings in this probe.** One-shot per URL only; operator
  should add retry+backoff as usual and watch for Cloudflare bot-fight mode
  kicking in under the 96-team fan-out.

**Secondary path (optional, later PR):** clone `usl_academy.py` against the
`w-league` Modular11 tenant once a match-level source is useful. Requires
discovering `UID_event` for the current competition year (per the extractor
docstring's SEASONAL MAINTENANCE note), and swapping the URL template's
tenant path from `usl-academy.com`'s IDs to the `w-league`-tenant IDs.

**Not Playwright.** Nothing on the site required JS execution to return
usable HTML.

## Caveat — egress IP

This probe ran from the Claude agent's local egress (`darwin arm64`), not
from Replit production. Two reasons the decision is still robust here, but
flagged for completeness:

1. The naive fetch and UA-shod fetch produced identical responses for every
   URL, including identical byte counts. Cloudflare's edge is handing back
   the same cache object to both — no IP-reputation heuristic is flipping
   the page between modes.
2. The task brief reported a 403 somewhere; we didn't reproduce it. If
   Replit's egress IP tier is on a Cloudflare deny list that a laptop egress
   is not on, the 403 could re-appear in production. Operator should re-run
   the probe from the deployed Replit container before committing to the
   Phase 1 build PR — the JSON report under `/tmp/` makes that a single
   `pnpm --filter @workspace/scripts run probe-usl-w-league -- --url
   https://www.uslwleague.com/league-teams` invocation.

## Related

- Probe script: `scripts/src/probe-usl-w-league.ts`
- Sibling pattern: [`hudl-phase-0-probe-report.md`](hudl-phase-0-probe-report.md)
- Modular11 reference extractor: `scraper/extractors/usl_academy.py`
- JSON probe output (local machine): `/tmp/usl-w-league-probe-2026-04-20T22-19-45-689Z.json`
