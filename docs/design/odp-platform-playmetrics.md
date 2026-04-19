# ODP / PlayMetrics — platform research + blocker note

## Status

**Blocked.** Wave-3c-7 research pass on the 2 ODP states tagged
`platform: playmetrics` in `scraper/extractors/odp_seed_urls.yaml`
(MN, SC). No public roster surface reachable today; no extractor
built. Revisit once one of the unblocking conditions below changes.

Last verified: 2026-04-18.

## Context

The Wave-3b seed YAML (`feat/odp-seed-urls-wave-3b`, merged as part
of PR #94) classifies 54 state-association ODP hubs by the platform
that hosts their roster surface. Two states are tagged
`platform: playmetrics`:

| State | YAML hub                                          | `urls` | Parser registered |
|-------|---------------------------------------------------|--------|-------------------|
| MN    | `mnyouthsoccer.org` (ODP program hub)             | `[]`   | none              |
| SC    | `scyouthsoccer.com/youth-elite-program` ("Youth Elite Program") | `[]` | none |

Both entries ship with `urls: []` and an explanatory comment noting
"PlayMetrics registration links + trial info; no public roster"
(MN) and "Tryout dates + league registration links only; no public
roster" (SC). No candidate URLs were found during the 3b sweep; the
platform tag was recorded so a follow-up pass (this one) could
confirm whether PlayMetrics itself exposes any public roster surface
that 3b missed.

## URLs probed

All probes were unauthenticated GETs, respecting robots.txt / rate
limits, from the Claude Code host IP. Findings below.

### State-association hub pages

| URL                                                | Status | Content                                                                 |
|----------------------------------------------------|--------|-------------------------------------------------------------------------|
| `https://www.mnyouthsoccer.org/odp`                | 200    | Program info + registration CTA pointing to PlayMetrics. No roster.     |
| `https://www.mnyouthsoccer.org/programs/mn-odp`    | 404    | Not a valid path.                                                       |
| `https://www.scyouthsoccer.com/youth-elite-program`| 200    | Tryout dates + "Register on PlayMetrics" CTA. No roster.                |

Both 200 pages are marketing / registration funnels. Rosters, roster
communications, and selection results are explicitly routed through
PlayMetrics post-registration. SC's page additionally names tryout
dates and locations but stops short of publishing any selected-player
list.

### PlayMetrics-hosted probes

| URL                            | Status           | Content                                                                                       |
|--------------------------------|------------------|-----------------------------------------------------------------------------------------------|
| `https://playmetrics.com/`     | 200              | JS-rendered SPA shell. Server HTML is essentially `<div id="root">Loading PlayMetrics</div>`. |
| `https://playmetrics.com/club` | 200              | Same SPA shell ("Loading PlayMetrics"). Routing happens client-side.                          |
| `https://mn.playmetrics.com/`  | ECONNREFUSED     | DNS does not resolve. No per-state subdomain.                                                 |
| `https://sc.playmetrics.com/`  | ECONNREFUSED     | DNS does not resolve. No per-state subdomain.                                                 |

PlayMetrics does not use a `<state>.playmetrics.com` pattern. It is
a single-tenant SaaS served from `playmetrics.com` and the client
app handles all routing, auth redirects, and content loading after
the initial HTML shell.

## What's gated

Everything roster-adjacent:

- **Player pool lists / selected-player rosters** — not published on
  the state-association site at all. Registrations and selection
  communications are sent through PlayMetrics, which requires a
  participant (or staff) account to view.
- **Per-age-group cuts** — same story. The MN and SC public pages
  don't break selections down by age group or gender; those lists
  only exist inside PlayMetrics' authenticated org views.
- **Coach / staff directories** — not on the PlayMetrics public
  surface. Staff info, where it exists at all, lives on the
  state-association site (`mnyouthsoccer.org/contact-us`,
  `scyouthsoccer.com/staff`), not on PlayMetrics.

## Why `scraper_js.py` does not unblock this

`scraper/scraper_js.py` wraps Playwright (`sync_playwright`) and can
render JS-heavy pages. That handles a specific class of gating:
pages where the content **is** present after JS execution but
absent from server HTML. PlayMetrics is a different class:

- The SPA at `playmetrics.com/` routes **unauthenticated visitors
  to a login prompt**, not to any public roster view. There is no
  public URL pattern (e.g. `/public/org/<id>/roster`) documented or
  discovered in probing.
- Rendering the SPA with Playwright and then waiting for content
  would return the login form, not roster data. This is a login
  wall, not a JS-rendering wall.
- Bypassing the login is out of scope per the project guardrails
  (no auth tokens, no credentials, no session replay).

So Playwright would not help here. It would help **if** PlayMetrics
added a public read-only view in the future — in that case this
would drop into the same bucket as other ODP states that publish a
JS-rendered pool list (none today, but it's a plausible future).

## What would unblock this

Any one of the following:

1. **PlayMetrics publishes a public roster / pool view.** Unlikely
   absent product pressure from leagues. Would show up as a URL
   shape like `playmetrics.com/public/org/<id>/rosters` or similar.
   Unblocks cleanly — just add a parser to
   `extractors/odp_rosters.py` and populate the two YAML `urls`
   arrays.
2. **State association publishes selected-player lists on their
   own site.** MN or SC may eventually post a post-tryout "Pool
   Announcement" PDF or HTML page the way several other state
   associations do (see the `platform: public-html` rows in the
   YAML). The 3b sweep comment already hints at this as the
   plausible path ("rosters may be routed via PlayMetrics/Squadi
   registrations" for SC — meaning if that routing changes or a
   summary page is published, it's scrapeable).
3. **Partnership / API access with PlayMetrics.** Upshift would
   need a read-scope API credential from PlayMetrics itself, or a
   contractual data-sharing arrangement with the state associations.
   Either path is a business-level unblock, not a scraper
   engineering one. No known public API docs.

## Action items

- **None scheduled.** This is a parked research note. The two
  YAML entries already have `urls: []` and a descriptive comment —
  they won't be processed by `odp_runner.py` (the runner skips
  states with no URLs) so there's no active scraper risk.
- On the next ODP sweep (quarterly cadence, matching the ODP
  program year calendar) re-probe the two state-association hub
  pages. If either publishes a roster page, populate `urls:` in
  the YAML and register a parser.
- If PlayMetrics product announcements mention "public roster
  sharing" or similar, reopen this note.

## References

- `scraper/extractors/odp_seed_urls.yaml` — entries for MN (line
  ~294) and SC (line ~489) carry the `platform: playmetrics` tag.
- `scraper/odp_runner.py` — runner that dispatches to parsers in
  `extractors/odp_rosters.py`; skips states with empty `urls`.
- PR #94 — wave-3b seed-URL expansion that introduced the
  `platform:` tag and flagged MN/SC as playmetrics.
