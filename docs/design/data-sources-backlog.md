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

| # | Source | Recommendation | (value, public) | Schema impact |
|---|---|---|---|---|
| 1 | NCAA Transfer Portal | Requires spike | (5, 1) | New `transfer_portal_entries` table |
| 2 | Hudl (fan profiles) | Queue — gated on Phase 0 egress spike | (5, 2) | New `player_profiles` table + linker to existing player rows |
| 3 | US Soccer coaching license registry | Requires spike | (4, 2) | New `coach_licenses` table + FK to `coaches` |
| 4 | HS athletic associations (CIF/UIL/FHSAA) | Build now — pilot 3 states | (4, 3) | Reuse `hs_rosters` (add `sanctioning_body` column) |
| 5 | USL W League | Build now | (3, 4) | Extend `event_teams` / add `pro_rosters` table |
| 6 | NPSL | Queue | (3, 3) | Extend same `pro_rosters` table |
| 7 | UPSL | Queue (low priority) | (2, 3) | Extend same `pro_rosters` table |
| 8 | US Soccer referee assignments | Skip | (1, 1) | — |

## Briefs

### 1. NCAA Transfer Portal — `(5, 1)`, requires spike

- **URL surface:** The official NCAA Transfer Portal (`portal.ncaa.org`) is
  gated to compliance officers with institutional SSO. Third-party aggregators
  (On3, 247Sports, Rivals) republish portal activity but typically paywall the
  feed.
- **Auth wall:** fully gated on the official source. Aggregators: mixed —
  some entries public on news/blog pages, full feed behind paid sub.
- **Anti-bot:** aggregator sites run Cloudflare + standard bot detection.
- **Data fields available (hypothetical, via aggregator):** player name,
  previous school, intended destination, position, graduation year, entry
  date.
- **Extractor complexity:** high. Either negotiate aggregator API access or
  scrape news pages from multiple aggregators to stitch signal.
- **Schema impact:** new `transfer_portal_entries` table — `{id, player_name,
  previous_school, new_school, position, entry_date, exit_date, status,
  source_aggregator, source_url}`. FK to `colleges.id` on both ends.
- **Downstream value:** **flagship for the recruiting graph.** Transfer
  movement is half of NCAA roster churn today. Unblocks "which D1 programs
  reload via transfer vs. HS recruits" analytics for Upshift Player.
- **Recommendation:** **requires spike** — does ANY public soccer-specific
  feed exist that we can ingest legally without a paid subscription? If no,
  demote to `skip` and flag as a future partnership ask.
- **Ranking:** `value=5` (core recruiting signal), `public=1` (fully gated).

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

### 3. US Soccer coaching license registry — `(4, 2)`, requires spike

- **URL surface:** Historically USSF had a public "Find a Coach" lookup at
  `ussoccer.com/coach-directory` or similar. As of April 2026, confirm it
  still exists and returns D/C/B/A/Pro license-tier records.
- **Auth wall:** typically public lookup form + results page. May have rate
  limits or search-only-returns-first-N behavior.
- **Anti-bot:** unlikely on the directory itself; USSF hosts on a standard
  CMS.
- **Data fields available:** coach name, license tier, city/state, club
  affiliation (optionally), license issue date.
- **Extractor complexity:** medium — need a pagination / enumeration
  strategy since there's no single "all coaches" endpoint. State-by-state or
  name-prefix sweeps.
- **Schema impact:** new `coach_licenses` table — `{id, coach_id, license_tier
  (enum: D/C/B/A/Pro), state, issue_date, source_url, first_seen_at,
  last_seen_at}`. FK `coach_id` → `coaches.id` via existing coach linker.
- **Downstream value:** hardens the coach graph. Tier-1 coaches (A/Pro) are
  career-stage markers, useful for both club-quality analytics and coach
  career-path tracking.
- **Recommendation:** **requires spike** — confirm USSF still publishes the
  lookup, note the pagination/search strategy, estimate extraction cost. If
  lookup was quietly retired, demote to `queue` pending a partnership
  conversation with USSF.
- **Ranking:** `value=4` (tier-1 coach signal), `public=2` (public but may
  require complex enumeration).

### 4. HS athletic associations — `(4, 3)`, build now (pilot 3 states)

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

**How to use this list:** pull off the top. NCAA Transfer Portal + Hudl are
`value=5` but gated on spikes — run those spikes first, then decide whether
to promote or demote. In the meantime, HS athletic associations (row 4) and
USL W League (row 5) are immediately actionable and should be the next real
extractor PRs.

## Next actions (recommended)

1. Run the **NCAA Transfer Portal spike** (row 1) — 1 day of investigation to
   determine if any public soccer-specific feed exists.
2. Run the **USSF coaching license registry spike** (row 3) — half a day to
   confirm the lookup still exists and characterize pagination strategy.
3. After Hudl Phase 0 egress spike completes (per
   [docs/design/hudl-phase-0-egress.md](hudl-phase-0-egress.md)), unblock row 2.
4. In parallel with the spikes, scope **HS athletic assocs pilot (row 4)** as
   the next extractor PR. CIF, UIL, FHSAA in one wave.

## Requires-spike summary

Two entries flagged for user decision before extractor work starts:

- **NCAA Transfer Portal (row 1):** is there any public soccer-specific feed
  we can ingest legally without a paid aggregator subscription?
- **US Soccer coaching license registry (row 3):** does USSF still expose a
  public license-lookup endpoint as of April 2026, and what's the URL +
  result-set limit?

Both spikes are lightweight — a few web fetches + one hour of investigation —
but neither should be baked into the agent brief for the downstream extractor
PR. Answer them first.
