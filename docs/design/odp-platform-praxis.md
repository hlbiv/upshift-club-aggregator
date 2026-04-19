# ODP platform — Praxis League Management (blocker note)

## Status

**Blocked.** No public ODP roster surface exists on Praxis for either of the
two states tagged `platform: praxis` in
`scraper/extractors/odp_seed_urls.yaml` (Nebraska, Oregon). No extractor
shipped; `urls: []` in the seed file is the correct state and `odp_runner.py`
already skips states with empty URL lists
([`scraper/odp_runner.py:171-176`](../../scraper/odp_runner.py)).

This note captures what was probed so the next operator doesn't burn time
re-doing the reconnaissance, and lists the unblocking paths if Praxis coverage
becomes a priority.

## Scope

Two states seeded as `platform: praxis` in wave-3b
([PR #94](https://github.com/hlbiv/upshift-data/pull/94)):

| State | Hub page | Roster URLs in seed YAML |
|---|---|---|
| NE | https://www.nebraskastatesoccer.org/odp/ | `[]` |
| OR | https://oregonyouthsoccer.org/programs/odp/ | `[]` |

Both states were already flagged "no public roster URL found" during the
seed-URL research wave. This note is the formal confirmation pass: we
re-probed the state hubs and Praxis itself, and verified no public endpoint
exists to point an extractor at.

## What was probed

All probes run 2026-04-18 from a laptop terminal (residential IP). No auth,
standard browser User-Agent.

### State-association hub pages

- `GET https://www.nebraskastatesoccer.org/odp/` → 200. Page references a
  single Praxis URL: `https://admin.praxissports.com/#/forms/start/7277782b-7e66-454b-9016-41aec0617af9`
  — this is a **registration form**, not a roster page. No team/player/pool
  links. Contact: `odp@nebraskastatesoccer.org`.
- `GET https://oregonyouthsoccer.org/programs/odp/` → 200. Page references:
  - `https://admin.praxissports.com/#/forms/start/2b239217-52a1-4e92-a6d3-124da5635c52`
    — Praxis registration form.
  - `https://registration.us.squadi.com/userRegistration?organisationId=…`
    — Squadi registration form (separate vendor).
  - `https://oysa.affinitysoccer.com/Foundation/Login.aspx?sessionguid=`
    — Affinity Sports login (gated).
  - A page link to `https://www.oregonyouthsoccer.org/coaching-staff/` (staff,
    not players).

  No roster/pool link on either page.

### Praxis itself

- `GET https://admin.praxissports.com/` → 200. Response body is a
  **Flutter web SPA shell** — `<base href="/">` + `flutter_bootstrap.js` and
  an otherwise empty `<body>`. All content is client-rendered from an
  authenticated API; the HTML tells you nothing about rosters. This is the
  admin/operator portal, not a public directory.
- `GET https://admin.praxissports.com/#/forms/start/<uuid>` → identical 200
  SPA shell; fragment-only routing means curl sees the same bytes as the root.
  The form itself loads post-auth-check inside Flutter.
- `GET https://praxissports.com/rosters` → 404.
- `GET https://praxissports.com/teams` → 404.
- `GET https://www.praxissports.com/clubs/nebraska` → 404.
- `GET https://app.praxissports.com/` → DNS does not resolve.

There is no `*.praxissports.com` subdomain that returns public roster HTML
(tested `admin`, `app`, `www`). The `admin` subdomain gates everything behind
the Flutter app's auth; the marketing site at the apex returns a branded 404
page for any non-marketing path.

### Google / web-search probes

- `"praxis-league.com" OR "praxis.com" ODP soccer roster public` → no public
  roster pages indexed. Praxis markets itself as an "end-to-end club
  operations platform" for the United Soccer League; its value prop is
  consolidating club ops **behind a login**, not exposing a public directory.
- `"Nebraska State Soccer ODP roster Praxis 2026"` → NE state-soccer page
  confirms "2025-26 ODP Cycle moving to Praxis for registration software"
  — registration/communication/evaluations, no mention of public rosters.
- `"Oregon Youth Soccer ODP roster Praxis Squadi 2026"` → same story; Praxis
  handles registration only, and Oregon additionally uses Squadi for some
  registration flows. Public ODP rosters are only announced for
  **National Select / Regional / Interregional** teams (published on
  usyouthsoccer.org, soccerwire.com, topdrawersoccer.com) — not for state
  pools.

## Why this is blocked, not deferred

Praxis is a **SaaS admin platform**, not a public-web CMS. Its core product
surface is (a) member registration forms and (b) an authenticated operator
console. Youth-soccer state associations that migrate to Praxis are moving
**away from** WordPress/SportsEngine-style public pages and **toward** closed
ops — which is the opposite of the trend that makes scraping viable.

Even if Praxis exposed a public roster route tomorrow, NE and OR would each
have to (1) opt in and (2) link to it from their hub pages. Today neither
state does either. Building an `odp_praxis.py` extractor now would mean
building against a surface that does not exist — the extractor would have
nothing to parse, no fixture to freeze, and no test that proves anything
useful.

## Unblocking paths

Listed roughly cheapest → most expensive. None of these are in-flight; this
section exists so a future operator can pick up the thread without
re-discovering the space.

1. **Wait for state publication on WordPress/SportsEngine.** The two states'
   public hub pages may eventually add a static roster PDF or a dedicated
   page ("2025-26 ODP Pool"). If they do, this becomes a `wordpress-pdf` or
   `public-html` extractor job, not a Praxis one — and the YAML
   `platform:` tag should be updated to match.
2. **Email the state ODP directors and ask for the roster format.** Contacts
   are published:
   - NE: `odp@nebraskastatesoccer.org` (director: Ed Meitzen)
   - OR: director listed on `https://oregonyouthsoccer.org/coaching-staff/`
     — email not scraped here; find via page.

   They may send a CSV/PDF on request. If yes, this becomes an **`email`-tier
   source** (like the 5 existing `platform: email` states) and should be
   handled by whatever future ingest path we build for that tier, not by a
   web extractor.
3. **Partner with Praxis for data access.** Praxis has no documented public
   API; a formal partnership (or a Praxis-approved OAuth flow per state
   association) would be the only clean way to pull structured roster data.
   Cost: enterprise relationship + per-state consent + custom ingest client.
   Out of scope for the ODP-coverage initiative; file under
   "if Praxis coverage ever becomes a product-tier P1."
4. **Follow the `National Select` / `Regional` / `Interregional` channels.**
   These rosters ARE published (usyouthsoccer.org press releases,
   soccerwire.com). That data is orthogonal to state-pool rosters but may
   be a useful adjacent dataset. Separate extractor, separate schema — do
   not conflate with state-ODP rows.

## Implications for the YAML

No change needed right now. `scraper/extractors/odp_seed_urls.yaml` for both
NE and OR already:

- Sets `platform: praxis` (correct — it's the registration vendor).
- Leaves `urls: []` (correct — no public roster URL to point at).
- Carries a comment explaining the search terms tried.

`odp_runner.py` short-circuits both states on line 171 (empty `urls` →
`"state %s has no seed URLs (follow-up) — skipping"`). No runner-level
behavior change needed.

If either state publishes a public roster page later, the fix is:

- Fill in the `urls:` list in the YAML.
- Either (a) swap `platform:` to the new vendor (e.g. `wordpress-pdf`,
  `public-html`) OR (b) keep `platform: praxis` and ship an
  `odp_praxis.py` extractor — only if the published page is Praxis-hosted
  (which is **not** what current Praxis tenants do, so (a) is the likelier
  outcome).

## Related

- Seed-URL wave-3b: [PR #94](https://github.com/hlbiv/upshift-data/pull/94)
  — established the `platform:` taxonomy and the two Praxis-tagged states.
- ODP extractor runner: `scraper/odp_runner.py` — already handles empty-URL
  states gracefully; no changes needed until a roster URL exists.
- ODP parser registry: `scraper/extractors/odp_rosters.py` — where an
  `odp_praxis` parser would live if/when (1) Praxis starts exposing public
  roster HTML and (2) at least one state links to it.
