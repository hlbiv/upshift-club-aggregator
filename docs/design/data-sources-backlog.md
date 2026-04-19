# Data sources backlog

## Context

This doc lists candidate data sources for future upshift-data scraper work,
ranked so the team can pull tasks off the top without re-researching each one.
Already-shipped sources are enumerated for landscape completeness but not
re-profiled here.

Ranking is a tuple sort `(value, public)` descending, **primary axis is
`value`**: hard-but-valuable beats easy-but-niche. See the methodology section
at the end for rationale.

## Already shipped

These four sources already have extractors, schemas, and working pipelines.
They're referenced here only so the full data-sources landscape is visible in
one place.

- **TopDrawerSoccer commitments** — [#74](https://github.com/hlbiv/upshift-data/pull/74), `commitments` table
- **US Soccer YNT call-ups** — [#77](https://github.com/hlbiv/upshift-data/pull/77), `ynt_call_ups` table
- **ODP state rosters** — [#75](https://github.com/hlbiv/upshift-data/pull/75) + [#94](https://github.com/hlbiv/upshift-data/pull/94), `odp_roster_entries` table (platform breakdown at `docs/design/odp-platform-*.md` and `docs/design/odp-platforms-unscrapeable.md`)
- **MaxPreps HS soccer rosters** — [#76](https://github.com/hlbiv/upshift-data/pull/76), `hs_rosters` table

## Backlog (ranked)

_Ranks updated 2026-04-19 after spike findings on NCAA and USSF sources — see "Spike outcomes" section below._

| # | Source | Recommendation | (value, public) | Schema impact |
|---|---|---|---|---|
| 1 | NCAA Transfer Portal (via TDS tracker) | **Build now — same shape as TDS commitments** | (5, 4) | New `transfer_portal_entries` table |
| 2 | Hudl (fan profiles) | Queue — gated on Phase 0 egress spike | (5, 2) | New `player_profiles` table + linker to existing player rows |
| 3 | HS athletic associations (CIF/UIL/FHSAA) | Build now — pilot 3 states | (4, 3) | Reuse `hs_rosters` (add `sanctioning_body` column) |
| 4 | US Soccer coaching license registry | Queue — needs Playwright search-shape spike | (4, 2) | New `coach_licenses` table + FK to `coaches` |
| 5 | USL W League | Build now | (3, 4) | Extend `event_teams` / add `pro_rosters` table |
| 6 | NPSL | Queue | (3, 3) | Extend same `pro_rosters` table |
| 7 | UPSL | Queue (low priority) | (2, 3) | Extend same `pro_rosters` table |
| 8 | US Soccer referee assignments | Skip | (1, 1) | — |

## Spike outcomes (2026-04-19)

Two entries in this doc were originally tagged `requires spike`. Both spikes ran on 2026-04-19 (web-search + web-fetch, no code). Findings:

### NCAA Transfer Portal — **PROMOTED from (5, 1) spike → (5, 4) build now**

- TopDrawerSoccer publishes free, public DI transfer trackers for both men's and women's soccer at `topdrawersoccer.com/college-soccer-articles/2026-mens-division-i-transfer-tracker_aid55358` and `...2026-womens-division-i-transfer-tracker_aid55352`. ~350–400 rows per tracker, table layout: Player | Position | Outgoing College | Incoming College.
- **No paywall.** No anti-bot measures visible. Same domain we already scrape via `scraper/extractors/topdrawer_commitments.py`.
- Mid-year vs. summer windows: TDS publishes two trackers per season. The summer window (post-May-1) is a separate URL; we'd pick both up.
- Additional aggregators noted but NOT needed for MVP: FieldLevel, Portal Report, New England Soccer Journal (mostly women's). TDS is sufficient to validate schema and cover DI.
- **What changes:** recommendation flips from `requires spike` to `build now` with the same extractor pattern as TDS commitments ([#74](https://github.com/hlbiv/upshift-data/pull/74)). Ranking `public` axis goes 1 → 4 (plain HTML, no paywall, same domain we already know).

### US Soccer coaching license registry — **confirmed exists, queued behind Playwright spike**

- Directory confirmed live at `https://learning.ussoccer.com/directory` (April 2026).
- Implementation note: the page is a **JavaScript SPA**. A plain HTTP fetch returns a "browser too old" shim. The real directory renders client-side and will require Playwright (already wired into the scraper via `scraper/scraper_js.py`).
- **What's still unknown (needs a short Playwright probe, not a full extractor):**
  - Search fields: can you query by name, by state, by license tier? Is there an "all coaches" index or only per-search results?
  - Pagination: page-size limit, total count disclosure.
  - Rate limits: does the underlying API rate-limit per-IP at scale?
  - Response shape: JSON API the SPA calls, or server-rendered HTML fragments?
- **What changes:** recommendation flips from `requires spike` to `queue — needs Playwright spike`. The spike is half a day of Playwright work, not a blocker on product decisions.

## Briefs

### 1. NCAA Transfer Portal (via TDS tracker) — `(5, 4)`, build now

_Spike resolved 2026-04-19: TDS publishes free public trackers. Original (5, 1) "fully gated" ranking was wrong._

- **URL surface:** `topdrawersoccer.com/college-soccer-articles/{season}-{gender}-division-i-transfer-tracker_aid<id>`. Example IDs as of April 2026: men's 2026 = `aid55358`, women's 2026 = `aid55352`. Two trackers per season (mid-year + summer), so four URLs per year total.
- **Auth wall:** none. Public HTML, no paywall on the tracker articles themselves. TDS Premium badges exist on adjacent content but not on the trackers.
- **Anti-bot:** none observed. Same anti-bot posture as TDS commitments, which we already scrape successfully.
- **Data fields available:** Player | Position (F/M/D/GK) | Outgoing College | Incoming College. ~350–400 entries per tracker.
- **Extractor complexity:** **low.** Near-clone of `scraper/extractors/topdrawer_commitments.py` — same domain, same HTML table pattern, likely the same parser with different header aliases.
- **Schema impact:** new `transfer_portal_entries` table — `{id, player_name, position, previous_school_raw, previous_college_id, new_school_raw, new_college_id, season, tracker_window (enum: mid-year / summer), source_url, first_seen_at, last_seen_at}`. Two FKs to `colleges.id`, both nullable at scrape time and backfilled by the college linker.
- **Downstream value:** **flagship for the recruiting graph.** Transfer movement is roughly half of NCAA roster churn today. Unblocks "which D1 programs reload via transfer vs. HS recruits" analytics for Upshift Player.
- **Recommendation:** **build now.** Near-trivial extractor, high value, zero legal ambiguity. No paid subscription needed. Could ship alongside or shortly after the HS athletic-assoc pilot.
- **Follow-up (not blocker):** additional aggregators to consider adding if/when TDS coverage shows gaps — FieldLevel (`fieldlevel.com/app/portal-announcements?sportEnum=soccerwomen`), Portal Report (`theportalreport.com`), New England Soccer Journal. Can ship TDS alone for MVP.
- **Ranking:** `value=5` (core recruiting signal), `public=4` (plain HTML, no auth, known-friendly domain).

### 2. Hudl (fan.hudl.com player profiles) — `(5, 2)`, queue

- **URL surface:** `fan.hudl.com/profile/<player-id>` — public player pages
  with highlight reels, HS team affiliation, graduation year, position.
- **Auth wall:** public pages readable without login; some sections lazy-load
  behind interaction. JS-rendered.
- **Anti-bot:** Phase 0 spike (`docs/design/hudl-phase-0-egress.md`, [#87](https://github.com/hlbiv/upshift-data/pull/87))
  identified that **the CDN returns different responses based on egress IP**.
  Any scrape must run from production-egress IPs, NOT laptop or Replit dev
  shell. TLS fingerprinting (JA3/JA4) suspected but not confirmed.
- **Data fields available:** player name, graduation year, position, HS team,
  club team, height/weight (where filled), highlight video links, college
  commitment (if logged).
- **Extractor complexity:** medium — plain HTML once you've cleared the
  egress-IP requirement. Playwright likely needed for JS-rendered sections.
- **Schema impact:** new `player_profiles` table — `{id, hudl_player_id,
  player_name, graduation_year, position, hs_team_raw, club_team_raw,
  height, weight, committed_college, source_url, first_seen_at,
  last_seen_at}`. Canonical-club-linker pattern for `club_team_raw`.
- **Downstream value:** **flagship — player-identity backbone.** Every other
  data source (YNT, ODP, TDS commitments, HS rosters) names players but
  doesn't fingerprint them. Hudl's `player_id` is the closest thing to a
  stable cross-source identity key.
- **Recommendation:** **queue — build once Phase 0 egress spike completes.**
  Don't start before the spike; running from wrong IP tier will produce
  false-positive reachability signal.
- **Ranking:** `value=5` (identity backbone), `public=2` (public but
  egress-IP-sensitive + probable TLS fingerprinting).

### 4. US Soccer coaching license registry — `(4, 2)`, queue (needs Playwright spike)

_Spike resolved 2026-04-19: directory confirmed live, but it's a JS SPA. Extractor work needs a preliminary Playwright probe._

- **URL surface:** `https://learning.ussoccer.com/directory` — confirmed present and returning content as of 2026-04-19.
- **Auth wall:** public-facing (no login required to reach the directory page), but the page is a **JavaScript single-page app**. Plain-HTTP fetches return a "Your browser version is too old" compatibility shim. Real content renders client-side. Must use Playwright.
- **Anti-bot:** unknown. Standard USSF CMS so probably minimal at low volume, but worth verifying in the Playwright probe — specifically whether the underlying API enforces per-IP rate limits.
- **Data fields available (expected, confirm in Playwright probe):** coach name, license tier (grassroots → D → C → B → A → Pro), city/state, club affiliation when filled, license issue date or expiration.
- **Extractor complexity:** **medium** — Playwright required. Pagination/enumeration strategy still unknown. Probable approaches: state-by-state sweeps or name-prefix enumeration. If the SPA calls an underlying JSON API, prefer calling that API directly through Playwright's network interception rather than scraping the rendered DOM.
- **Schema impact:** new `coach_licenses` table — `{id, coach_id, license_tier (enum: grassroots-online / grassroots-in-person / D / C / B / A / Pro), state, issue_date, expires_at, source_url, first_seen_at, last_seen_at}`. FK `coach_id` → `coaches.id` via existing coach linker.
- **Downstream value:** hardens the coach graph. Tier-1 coaches (A/Pro) are career-stage markers, useful for club-quality analytics and coach career-path tracking.
- **Recommendation:** **queue — needs Playwright probe before building the extractor.** Half a day of Playwright work to answer: (1) search fields, (2) pagination shape, (3) response format (JSON API vs DOM fragments), (4) per-IP rate limit behavior. Once those are known, the extractor itself is medium complexity — not a product blocker.
- **Ranking:** `value=4` (tier-1 coach signal), `public=2` (lookup is public but requires JS rendering + unresolved pagination cost).

### 3. HS athletic associations — `(4, 3)`, build now (pilot 3 states)

- **URL surface:** state-level orgs. Flagship candidates: CIF California
  (`cifstate.org`), UIL Texas (`uiltexas.org`), FHSAA Florida (`fhsaa.com`).
  Each publishes HS team records, rosters, and all-state honors.
- **Auth wall:** public HTML pages. No login.
- **Anti-bot:** minimal — these are government-adjacent non-profits with
  modest traffic; no Cloudflare typically.
- **Data fields available:** varies per state but typically includes team
  record, schedule, roster, all-conference / all-state selections.
  Sometimes post-season bracket data.
- **Extractor complexity:** medium — each state's site is bespoke, so this
  is a per-state pattern similar to `scraper/extractors/state_assoc.py`
  (which covers USYS youth soccer, distinct from HS). Pilot 3 states first.
- **Schema impact:** **reuse `hs_rosters`** (shipped in [#76](https://github.com/hlbiv/upshift-data/pull/76))
  but add a `sanctioning_body` column (`'MaxPreps' | 'CIF' | 'UIL' |
  'FHSAA' | ...`) so multi-source HS data can coexist without conflict.
- **Downstream value:** **orthogonal signal to MaxPreps** — state-association
  records are the authoritative source for all-state / all-conference
  honors, which MaxPreps doesn't always expose. Unlocks "player was
  all-state 2024" lookups for recruiting.
- **Recommendation:** **build now — pilot CIF, UIL, FHSAA.** These 3 states
  cover ~25% of US HS soccer participation. Remaining ~47 states are
  follow-ups (mirror the ODP rollout pattern).
- **Ranking:** `value=4` (strong recruiting signal + honors data),
  `public=3` (pilots easy, full-state rollout long-tail).

### 5. USL W League — `(3, 4)`, build now

- **URL surface:** `uslwleague.com` — franchise pages with rosters, staff,
  schedule. ~80 teams across the pre-pro women's league.
- **Auth wall:** public.
- **Anti-bot:** none observed. Standard CMS.
- **Data fields available:** player name, position, year, previous
  college/club, height, hometown (variable). Staff: head coach, assistants.
- **Extractor complexity:** low-medium — one page template per team, ~80
  team URLs. Similar shape to existing college/college-coaches extractors.
- **Schema impact:** new `pro_rosters` table — `{id, league (enum:
  USL-W/NPSL/UPSL), team_name_raw, team_id, season, player_name,
  position, year, previous_club_raw, previous_college_id, source_url,
  first_seen_at, last_seen_at}`. Or reuse `event_teams` + new rows — pick
  whichever gives cleaner temporal queries.
- **Downstream value:** women's pre-pro pipeline is under-covered in
  existing Upshift data. Unlocks "college → USL-W → NWSL Academy" path
  tracking for recruiting analytics.
- **Recommendation:** **build now.** Public, stable, reasonable volume.
  Target ~80 team pages behind the existing http + proxy pipeline.
- **Ranking:** `value=3` (valuable for women's pipeline, not a flagship
  feature), `public=4` (fully public, stable).

### 6. NPSL — `(3, 3)`, queue

- **URL surface:** `npsl.com` — men's semi-pro league, ~80 teams across
  regional conferences. Team pages carry rosters.
- **Auth wall:** public.
- **Anti-bot:** none observed.
- **Data fields available:** similar to USL-W — player name, position,
  year, previous affiliations.
- **Extractor complexity:** low-medium — one template per team.
- **Schema impact:** same `pro_rosters` table as USL-W with
  `league='NPSL'`.
- **Downstream value:** men's semi-pro pipeline. Less flagship than USL-W
  because the men's elite pipeline (MLS Next → college → MLS) already has
  stronger signal from shipped sources; NPSL is a "filler" tier.
- **Recommendation:** **queue** behind USL-W. Use the same table design so
  the second extractor is near-trivial.
- **Ranking:** `value=3`, `public=3` (public but content quality varies
  per club).

### 7. UPSL — `(2, 3)`, queue (low priority)

- **URL surface:** `upsl.com` — multi-tier amateur/semi-pro men's league.
  Multiple divisions (Pro Premier, Premier, Championship, etc.). Team
  pages on `upslsoccer.com` sub-routes.
- **Auth wall:** public.
- **Anti-bot:** none observed.
- **Data fields available:** roster names, positions, minimal bio.
- **Extractor complexity:** medium — multi-division structure, more
  variance per team page, lots of teams (200+).
- **Schema impact:** same `pro_rosters` table with `league='UPSL'`.
- **Downstream value:** UPSL sits below NPSL in the men's semi-pro
  hierarchy. Signal quality per-team is lower (teams come and go;
  rosters often incomplete). Not where we should spend effort until the
  higher tiers are done.
- **Recommendation:** **queue — low priority.** Build only after USL-W and
  NPSL ship and have been observed in production for a month.
- **Ranking:** `value=2` (marginal recruiting signal), `public=3`
  (extractable but noisy).

### 8. US Soccer referee assignments — `(1, 1)`, skip

- **URL surface:** No public referee-assignment URL. USSF distributes
  assignments via the internal referee portal (`refcenter.ussoccer.com` or
  similar) behind referee-specific login.
- **Auth wall:** fully gated to registered referees.
- **Anti-bot:** N/A — inaccessible at the identity tier.
- **Data fields available:** N/A without credentials.
- **Extractor complexity:** N/A — structurally impossible.
- **Schema impact:** N/A.
- **Downstream value:** low. Ref assignments are useful for match-ops
  analytics, but Upshift Player is a recruiting product — ref data isn't in
  the critical path.
- **Recommendation:** **skip.** Mark as structurally gated. Revisit only if
  a USSF partnership ever materializes — not a scraper target.
- **Ranking:** `value=1` (niche), `public=1` (impossible).

## Ranking methodology

Sources are ranked by tuple `(value, public)` descending, with **`value` as
the primary axis**. This matches the Upshift pattern of prioritizing
engineering capacity on things that matter over things that are easy: a
high-value source that's hard to extract ranks above an easy source with
marginal signal.

**Why not multiplicative?** A product of the two axes (e.g., `value × public`)
collapses resolution in the middle: a `(3, 4)` and a `(4, 3)` both score 12
despite representing very different investment profiles. Tuple sort keeps the
two axes legible.

**Why not additive?** A weighted sum like `value + 2×public` can let an
easy-but-low-value source (e.g., `value=2, public=5` → 12) out-rank a
hard-but-high-value one (`value=4, public=1` → 6). That's the opposite of
what we want.

**How to use this list:** pull off the top. Rows 1 (NCAA Transfer Portal), 3 (HS athletic assocs), and 5 (USL W League) are all immediately buildable. Row 2 (Hudl) is gated on the Phase 0 egress spike. Row 4 (USSF license registry) is confirmed live but needs a Playwright probe before the extractor is worth writing.

## Next actions (recommended, updated 2026-04-19)

1. **Build NCAA Transfer Portal extractor (row 1)** — same shape as TDS commitments ([#74](https://github.com/hlbiv/upshift-data/pull/74)). New `transfer_portal_entries` schema + 4 seed URLs (men's + women's × mid-year + summer). Small extractor PR.
2. **Build HS athletic assocs pilot (row 3)** — CIF / UIL / FHSAA in one wave. Extend `hs_rosters` with `sanctioning_body` column.
3. **Build USL W League extractor (row 5)** — new `pro_rosters` table shape, ~80 team pages.
4. **Run USSF license Playwright probe (row 4)** — half-day spike. Characterize search fields, pagination, rate limits, response format (JSON API vs DOM fragments). Outputs: short probe-report doc; then scope the real extractor.
5. **Wait on Hudl (row 2)** — the Phase 0 egress-IP spike at [docs/design/hudl-phase-0-egress.md](hudl-phase-0-egress.md) must complete first.

Rows 6 (NPSL), 7 (UPSL), and 8 (USSF referee assignments) stay queued / skipped per their individual briefs.

## Spike summary (post-2026-04-19)

Original doc flagged 2 entries as `requires spike`. Outcomes:

- ✅ **NCAA Transfer Portal** — resolved. Public TDS trackers confirmed. Promoted from (5, 1) spike → (5, 4) build now. Ships with same extractor pattern as TDS commitments.
- 🟡 **US Soccer coaching license registry** — partially resolved. Directory at `learning.ussoccer.com/directory` confirmed present. Implementation is a JS SPA, so extractor requires Playwright. A short Playwright probe (row 4 in Next Actions above) is now the only open question — not a product-decision blocker.

Neither question remains as a blocker on user decisions. Both are now engineering scoping tasks.
