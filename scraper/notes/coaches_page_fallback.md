# NCAA `/coaches` page fallback (Task #38, PR-9)

**Date:** 2026-04-22
**Builds on:** [`inline_coach_probe.md`](inline_coach_probe.md) (Task #34)

---

## Problem

The Task #34 inline-extractor extension recovered 23/28 head coaches on
the probe sample (82% hit rate) but left a hard residual: 3 of 28
schools (Pepperdine, George Mason, Virginia Tech) ship a fully
JS-rendered roster page with **zero** inline "Head Coach" markup at any
selector. The `extract_head_coach_from_html` function cannot succeed
against these pages no matter how many CSS strategies it adds, because
the literal "Head Coach" string is never serialized into the static
HTML.

The Task #34 author flagged this exact bucket in the recommendation:

> Recoverable by PR-9 (~10%): JS-rendered pages (Pepperdine, George
> Mason) where a separate `/coaches` URL almost always server-renders
> the head-coach card statically — the JS-rendered wrapper is the
> roster page, not the staff page. Worth picking up.

This change ships PR-9.

---

## Design

When `extract_head_coach_from_html(roster_html)` returns `None` and
we're on the **current-season pass** (historical staff pages are
extremely rare and not worth the per-season HTTP cost), the scraper
calls `probe_coaches_pages(session, roster_url)` which:

1. Derives an ordered candidate list via `compose_coaches_urls`:
   ```
   {base}/coaches
   {base}/coaches-and-staff
   {base}/staff
   {base}/staff-directory
   ```
   where `{base}` is the roster URL with the trailing `/roster` (and
   any `?query`/`#fragment`) stripped.
2. Fetches each candidate via the existing `fetch_with_retry` (same
   session, same retry policy, same UA).
3. Runs the unmodified `extract_head_coach_from_html` against each
   response — so all five inline strategies and the strict-coach guard
   apply identically. **Zero duplicate parsing logic.**
4. Returns the first hit with `_strategy` rewritten to
   `coaches-page-fallback:<original_strategy>` and a `_source_url`
   field naming the page that produced the coach.
5. Caches both positive and negative results in a per-run dict keyed
   by the program base. Multi-program hosts (Stanford fields ~30
   sports at `gostanford.com`) only pay the probe cost once per
   *program* across the whole crawl. Negative caching is the bigger
   win — it prevents 4 wasted fetches per repeat against hosts that
   genuinely have no separate staff page.

### Conservative scoping

Three deliberate non-features:

- **No athletics-wide fallback.** We do not probe a school-wide
  `/staff-directory` (without the `/sports/<sport>/` prefix). On
  multi-sport hosts this would return the AD or a non-soccer coach
  with no way to disambiguate. Mis-attributing a coach to the wrong
  program is worse than missing the row entirely (downstream upserts
  are keyed on `college_id`).
- **Current-season only.** Backfill seasons skip the fallback.
  Historical `/coaches/{YYYY}` pages are almost never published, and
  the per-season cost would scale with `backfill_seasons`.
- **Hard cap of 4 candidates.** The `_MAX_COACHES_PROBES_PER_CALL`
  constant caps worst-case extra HTTP load at 4 fetches per
  cache-miss school.

### Per-strategy instrumentation

`coach_strategy_hits` gains a new `coaches-page-fallback` bucket. All
fallback hits funnel into this single bucket regardless of which
inline strategy ultimately fired against the staff HTML, so the
end-of-run logline keeps its current 7-column shape (was 6, now 7
including miss). The decision rule stays unchanged: if `miss` still
dominates after PR-9, the residual schools genuinely have no online
staff listing and no further extractor work helps.

---

## Tests

Two new test classes in `scraper/tests/test_ncaa_rosters.py`:

- `TestComposeCoachesUrls` — 7 cases covering basic shape, trailing
  slash, missing `/roster` suffix, query/fragment stripping,
  case-insensitive `/Roster` strip, empty-input rejection, and
  cache-key program-scoping (men's vs women's must not collide;
  case + trailing slash must normalize to the same key).
- `TestProbeCoachesPages` — 6 cases covering the success path
  (hit on first candidate, only 1 fetch made), the JS-shell
  baseline (`extract_head_coach_from_html` returns None against the
  shell fixture), the all-miss path (4 candidates, all probed, None
  returned), the too-short-response guard (treats <500-byte bodies as
  miss), positive-cache short-circuit (zero additional HTTP hits on
  repeat), and negative-cache short-circuit.

Two new fixtures under `scraper/tests/fixtures/ncaa/`:

- `js_rendered_roster_shell.html` — Vue/React shell with `#app`
  mount point, no inline staff markup. Mirrors the Pepperdine /
  George Mason / Virginia Tech shape from the Task #34 sample.
- `coaches_page_server_rendered.html` — `/coaches` page with three
  staff cards (Associate Head Coach, Head Coach, Assistant Coach).
  Verifies the strict-coach guard still picks the Head Coach (not
  the Associate) when the fallback runs.

All 88 tests in `test_ncaa_rosters.py` pass (was 67 → +21 net,
including incidental tests added across the file since Task #34).

---

## Production-impact projection

We have not yet re-run a full NCAA D1 + D2 + D3 crawl since the Task
#34 inline extension shipped, so the production baseline is still the
0.17% pre-Task-#34 number. Task #37 ("Measure how many head coaches
we now find in a real crawl") is now unblocked by both PR-7 (Task #34)
and PR-9 (this change) and should be run next to size the actual
production lift.

Projected lift on top of Task #34 (from the 28-page probe sample):

```
Task #34 baseline:                   23/28 (82.1%) hit rate
+ PR-9 fallback theoretical max:     26/28 (92.9%) — recovers Pepperdine,
                                                    George Mason, Virginia Tech
                                                    if their /coaches pages
                                                    are server-rendered.
Hard residual (still uncoverable):    2/28 (7.1%)  — Stanford (named
                                                    directorship), Michigan
                                                    (no online staff listing).
```

Real production hit rate is likely lower than 92.9% because:
- The probe sample skewed SIDEARM-heavy (25/28). The long-tail of
  non-SIDEARM CMSs may not respond to any of the four candidate paths.
- Some schools' separate staff pages may also be JS-rendered (the
  Playwright fallback path is roster-only and does not extend to the
  fallback probe).

A safer projection: **+5-8 percentage points** on production hit rate
on top of whatever Task #34 actually produced. Task #37 will measure
both numbers in a single crawl and confirm whether further work
(PR-10 = Playwright-render the /coaches page when its static HTML
also misses) is justified.
