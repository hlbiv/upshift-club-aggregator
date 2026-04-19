# ODP platforms not automatable — BAND, Box, email

## Status

**Draft. Blocker ADR.** Captures three platform tags from
`scraper/extractors/odp_seed_urls.yaml` (introduced on `feat/odp-seed-urls-wave-3b`
/ [PR #94](https://github.com/hlbiv/upshift-data/pull/94)) that are structurally
or practically unscrapeable for the current pipeline. This is a "don't reopen
this investigation" note — not a roadmap item.

## Scope

7 ODP state entries across 3 platform tags in `scraper/extractors/odp_seed_urls.yaml`:

| Platform | States | Count |
|---|---|---|
| `band`   | CT                         | 1 |
| `box`    | TN                         | 1 |
| `email`  | AR, DE, LA, MD, RI         | 5 |

All 7 carry `urls: []` in the YAML today — the platform tag documents *why* no
URL was captured, it doesn't imply a pending URL.

## BAND (1 state)

- **State:** CT — Connecticut Junior Soccer Association (CJSA) ODP.
- **URL in YAML:** none (`urls: []`).
- **YAML note excerpt:** *"Contact + BAND group links only; no public roster.
  [...] selections appear to be communicated through PlayMetrics and a BAND
  invite-only app. Tagged as `band` because the BAND app is the destination
  for the roster itself; PlayMetrics is only the registration funnel."*
- **Why it's blocked:** BAND (band.us) is a Korean-owned private community /
  messaging app. Group content is gated behind invite + login; there is no
  public web surface for group posts, rosters, or files. Even the group
  "about" pages render an auth wall to unauthenticated browsers. This is not
  a CDN / bot-protection layer that probing could work around — there is
  simply no anonymous read path by product design.
- **What would unblock it:** membership in the specific CJSA ODP BAND group.
  Even with membership, automated scraping of BAND groups would violate
  BAND's Terms of Service and is not a path we're pursuing.
- **Recommendation:** do not pursue. If CT roster coverage becomes a
  priority, the path is a direct data-sharing agreement with CJSA — not
  BAND scraping.

## Box (1 state)

- **State:** TN — Tennessee State Soccer Association ODP.
- **URL probed:** `https://app.box.com/s/broovecsuwo6590o8r3267nbrywpflh8`
  (extracted from the TN ODP hub page at `https://www.tnsoccer.org/odp`,
  labeled "[connect via team app]"). The YAML currently carries `urls: []`
  with a note that the Box folder link "may gate access, not verified
  during sweep."
- **Probe result (2026-04-18):** `HTTP 200`, ~25 KB of HTML, no auth wall.
  The share metadata resolves to a single static file:
  `team_app_brochure (2022).pdf`, `itemType: file`. This is a marketing
  brochure for whichever team-management app TN uses (likely TeamSnap /
  SportsEngine-adjacent), not a folder, not a roster, not an index of
  rosters. There is nothing here for an extractor to ingest.
- **Why it's blocked:** the Box link is publicly reachable but contains no
  roster content. The roster itself is delivered *inside* the team app
  after a player signs in — the brochure just tells selected families how
  to get into that app. The team-app side is auth-gated.
- **What would unblock it:** TN publishing pool rosters on a public surface
  (WordPress post, GotSport event page, PDF on `tnsoccer.org`). Not
  something we can drive from this repo.
- **Decision:** no extractor. The probe was the entire investigation; future
  researchers should skip re-probing unless the link target on
  `tnsoccer.org/odp` changes.

## Email (5 states)

- **States:** AR (Arkansas Soccer), DE (Delaware Youth Soccer), LA (Louisiana
  Soccer — "Louisiana Select"), MD (Maryland State Youth Soccer), RI
  (Rhode Island Soccer).
- **URL in YAML:** none for any of the 5 (`urls: []`).
- **Why it's blocked:** these state associations distribute ODP pool rosters
  privately — typically a PDF or plain-text list mailed to selected players
  and their parents after the selection weekend. There is no public page,
  no portal, no login to beat. The YAML notes for each state enumerate the
  relevant contact person / director email, not a scrape target.
- **What would unblock it:** an operator subscribes to each state's ODP
  mailing list (or gets added to a parent-distribution), forwards roster
  emails to the ops inbox, and manually enters rosters. That workflow is
  out of scope for an automated scraper — it's a data-entry pipeline, not
  a crawl.
- **Alternative path:** direct partnership with each state association for
  a data feed (CSV drop, API, or shared folder). Would have to be
  negotiated 1:1 per state.

## Recommendation

Do not invest further engineering time in BAND, Box, or email-distributed
ODP rosters. Mark these 7 state entries in
`scraper/extractors/odp_seed_urls.yaml` with `deferred: blocked-by-platform`
(or equivalent YAML field — to be added in a follow-up housekeeping PR) so
future sweeps don't re-research the same ground.

Priorities for ODP coverage work should instead go to the 25 `unknown`-tagged
states and the `wordpress-pdf` / `hubspot-pdf` / `public-html` / `gotsport`
/ `playmetrics` / `praxis` buckets, which all have tractable public
surfaces.

## Related

- `scraper/extractors/odp_seed_urls.yaml` — seed inventory (on
  `feat/odp-seed-urls-wave-3b`; not yet on master as of this writing).
- PR #94 — Wave 3b seed sweep that introduced the `band` / `box` / `email`
  platform tags.
