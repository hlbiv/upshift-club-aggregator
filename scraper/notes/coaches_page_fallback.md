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

## Live measurement — Task #34 residual (3 schools)

Re-running the targeted probe against the exact 3-school residual that
Task #34 identified as fully JS-rendered (zero inline staff markup at
any selector), via [`probe_coaches_fallback.py`](probe_coaches_fallback.py)
on 2026-04-22:

| School | Inline (Task #34) | Fallback (PR-9) | Source URL |
| --- | --- | --- | --- |
| Pepperdine (men) | MISS | **Tyler LaTorre (Head Coach)** | `https://pepperdinewaves.com/sports/mens-soccer/staff` |
| George Mason (men) | MISS | MISS | (4 candidates probed, all returned no static staff markup) |
| Virginia Tech (men) | MISS | MISS | (4 candidates probed, all returned no static staff markup) |

```
Inline-only baseline (Task #34 residual): 0/3  (0%)
With PR-9 fallback:                       1/3  (33%)
Net additional captures from PR-9:        +1
```

### What this tells us

- **PR-9 works as designed.** Pepperdine matches the exact pattern
  the design targets: the roster page is a Vue/React shell, but
  `/staff` is server-rendered with a legacy SIDEARM staff card. The
  fallback found `Tyler LaTorre — Head Coach` on the third candidate
  URL (`/staff`, after `/coaches` and `/coaches-and-staff` 404'd) and
  the writer would tag the row with `source='ncaa_coaches_page'` and
  `source_url=https://pepperdinewaves.com/sports/mens-soccer/staff`.
- **The remaining 2/3 are a deeper problem.** George Mason and
  Virginia Tech serve JS-rendered HTML on **every** candidate URL,
  not just the roster page. Static fetches see a shell on `/coaches`,
  `/coaches-and-staff`, `/staff`, AND `/staff-directory`. PR-9 cannot
  recover these without rendering — that's PR-10 (the
  `Render JS-only coaches pages` follow-up filed alongside this task).
- The probe-sample-extrapolation projection ("recovers all 3 of the
  Task #34 residual") was too optimistic: real-world recovery on the
  Task #34 residual is **33% (1/3)**, not the projected 100%. The
  remaining 2/3 require Playwright on the fallback probe.

## Production-impact estimate

The Task #34 residual is itself only ~10% of D1 schools (3/28 in the
probe sample). PR-9 recovers ~33% of that residual:

```
Task #34 inline-only baseline:                 ~82% hit rate
+ PR-9 fallback (33% of the 18% residual):     ~88% projected hit rate
Net production lift from PR-9:                 ~+6 pp
```

Task #37 ("Measure how many head coaches we now find in a real
crawl") is now unblocked by both PR-7 (Task #34) and PR-9 (this
change). It should be run next to confirm the projected ~88% lands
in production and to scope whether PR-10 (Playwright on the fallback)
is justified for the remaining ~12%.

## PR-10 — Playwright on the fallback (Task #55)

PR-10 closes the JS-only-coaches-page residual (the George Mason /
Virginia Tech bucket above) by extending `probe_coaches_pages` to
re-fetch a candidate via `_render_with_playwright` when the static
fetch returned HTML but `extract_head_coach_from_html` found
nothing. Same env flag (`NCAA_PLAYWRIGHT_FALLBACK`) gates it, so
CI / sandbox keeps working unchanged. Hits get tagged
`coaches-page-fallback:rendered:<inline-strategy>` so the end-of-run
strategy breakdown can tell rendered hits apart from static-fetch
hits at the same fallback bucket. Negative results (rendered DOM
also missed) flow through the existing per-host probe cache, so a
school with neither a server-rendered nor a JS-rendered staff page
pays the ~3-5 s render once and then no-ops on subsequent program
passes within the same run.
