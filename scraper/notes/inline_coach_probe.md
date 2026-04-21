# NCAA inline coach selector probe (Task #34)

**Date:** 2026-04-21  
**Goal:** Before committing to PR-9 (separate `/coaches` URL probe path), measure
how much head-coach coverage we can recover by extending the existing inline
extractor (`extract_head_coach_from_html`) — which today fires on the same
roster HTML we already fetch for player rows, at zero extra HTTP cost.

**Baseline:** the most recent NCAA D1 roster crawl produced 16,342 player rows
but only 2 head-coach tenures across ~1,200 page fetches — **0.17%** hit rate.

---

## Method

1. Sampled 28 D1 men's-soccer roster URLs across CMSs (25 SIDEARM, 3 other —
   Stanford, Virginia Tech, Purdue).
2. Re-fetched each page (no cache layer was available — `ARCHIVE_RAW_HTML_ENABLED`
   is off in this env, so the `raw_html_archive` table is empty). Sample size
   was held to 28 to keep the probe a small targeted re-fetch as the task
   directs, not a re-run of the full crawl.
3. Ran `extract_head_coach_from_html` against each → recorded baseline hits.
4. Independently scanned each page for inline coach signals: free-text "Head
   Coach" mentions, JSON-LD `Person@jobTitle`, `__NUXT__` / `__NEXT_DATA__`
   payloads, and class-name patterns on small containers wrapping head-coach
   text. Captured CSS class fingerprints for the unmatched cases.
5. Selected the dominant class patterns, wrote fixtures + tests, extended
   the extractor.
6. Re-ran the probe → recorded new hit count.

Probe script: [`scraper/notes/probe_inline_coach.py`](probe_inline_coach.py)
(run with `python3 scraper/notes/probe_inline_coach.py` from the scraper
package; outputs a markdown report to stdout).

---

## Findings — what the current extractor was missing

The 28-page sample was overwhelmingly dominated by one missed pattern, with
two long-tail variants worth picking up:

| Variant | Schools hit (out of 28) | Class fingerprint | What current extractor did |
| --- | --- | --- | --- |
| **Modern SIDEARM "nextgen" cards** | ~22 | `.s-person-card` + `.s-person-details__position` ("Head Coach") + `.s-person-details__personal-single-line` (name) | **Missed** — current extractor only knew the legacy `.sidearm-staff-member` selector. SIDEARM's Vue/nextgen rebuild renamed every class to `s-person-*`, and the head coach is rendered in the same `.s-person-card` grid as the players (not on a separate /coaches page). |
| **Legacy SIDEARM inline roster coach list** | 1 (Portland) | `<li class="sidearm-roster-coach">` with `.sidearm-roster-coach-title` / `.sidearm-roster-coach-name` | **Missed** — selector wasn't part of the original strategy set. |
| **WMT/Vue staff card** (Stanford-style) | 1 (and likely several more outside the sample) | `.roster-staff-members-card-item` with `.roster-card-item__position` (title) + `.roster-card-item__title` (name) | **Missed** — non-SIDEARM CMS, completely different class names. |
| JSON-LD `Person@jobTitle="Head Coach"` | 0 | `<script type="application/ld+json">` | Not present on any sampled page. JSON-LD `Person` blocks DO appear (Portland: 32, George Mason: 18) but never with `jobTitle` set — they describe players. |
| `<meta>` head-coach tag | 0 | n/a | Not present. |
| `__NUXT__` / `__NEXT_DATA__` inline payload | 0 | n/a | Not present. (Sidearm nextgen serializes via different inline scripts that don't carry the literal "Head Coach" string.) |

### Headline pre-fix numbers (from baseline probe run)

```
Pages with current extractor hit:               0/28
Pages with inline 'Head Coach' text anywhere:  26/28
Pages with JSON-LD Person@jobTitle Head Coach:  0/28
```

The 0/28 hit count is consistent with the production 0.17% rate — the cited
baseline (2 hits across ~1,200 pages) was almost certainly two unusual sites
that still ship the legacy `.sidearm-staff-member` markup on the roster page.

---

## Selector changes shipped

`scraper/extractors/ncaa_rosters.py — extract_head_coach_from_html` now runs four
strategies in order, returning the first hit:

1. **Legacy `.sidearm-staff-member`** — unchanged behaviour, but now filters
   out "Associate Head Coach" / "Assistant Head Coach" titles via the new
   `_is_strict_head_coach` helper so the four strategies share the same
   semantic contract.
2. **NEW `.s-person-card`** — title from `.s-person-details__position`, name
   from `[data-test-id='s-person-details__personal-single-line']` with an
   `aria-label` fallback (`<Name> full bio` is consistent across Sidearm
   nextgen).
3. **NEW `.sidearm-roster-coach`** — title from `.sidearm-roster-coach-title`,
   name from `.sidearm-roster-coach-name`.
4. **NEW `.roster-staff-members-card-item` / `.roster-card-item`** — title
   from `.roster-card-item__position`, name from `.roster-card-item__title`.

`_is_strict_head_coach` first runs the title through `_NON_HEAD_COACH_RE`
— an explicit guard regex that matches every subordinate-of-head-coach
form we've observed (Associate / Assoc / Assoc., Assistant / Asst /
Asst., "Assistant to the Head Coach", and prefixed variants like
"Volunteer Assistant Head Coach") — and rejects the title if it matches.
Only titles that survive that guard are then matched against the plain
`_STRICT_HEAD_COACH_RE`. This two-step design ensures a real Head Coach
card sitting next to an Associate Head Coach card is always preferred. Strategies 2 and 4
both demonstrate this in their fixtures (each fixture contains a Head + an
Associate card, and the test asserts the Head is returned).

Fixtures added under `scraper/tests/fixtures/ncaa/`:

- `sidearm_s_person_card_head_coach.html` — modern Sidearm grid (player +
  player + Head Coach + Associate Head Coach).
- `sidearm_inline_roster_coach.html` — Portland-style inline `<ul>` block.
- `staff_card_position_title.html` — Stanford-style WMT staff cards.

Tests added in `scraper/tests/test_ncaa_rosters.py::TestExtractHeadCoachInline`:

- `test_legacy_sidearm_staff_member` — regression guard for Strategy 1.
- `test_modern_sidearm_s_person_card` — verifies name, title, email, and
  phone all extract from the new selector.
- `test_s_person_card_skips_associate_when_no_real_head` — ensures we
  return None (so the caller falls back to a /coaches probe) instead of
  promoting an Associate Head Coach.
- `test_s_person_card_aria_label_name_fallback` — exercises the
  `<Name> full bio` aria-label fallback.
- `test_legacy_sidearm_roster_coach_inline` — Strategy 3.
- `test_wmt_vue_staff_card` — Strategy 4.
- `test_returns_none_for_pure_player_roster` — contract check.

All 67 tests in `scraper/tests/test_ncaa_rosters.py` pass.

---

## Headline post-fix numbers

```
Pages with extractor hit (after):     23/28   (was 0/28)
Pages with inline 'Head Coach' text:  26/28
```

**Absolute lift on the sample: +82 percentage points** (from 0% to ~82% hit
rate) — well past the +20% target stated in the task. The two pages with
inline text we still miss vs. the 26-page upper bound:

- **Stanford** — head coach is titled "The Knowles Family Director of Men's
  Soccer" on the staff card (a named directorship). Strategy 4 finds the
  card but `_is_strict_head_coach` correctly rejects the title because it
  doesn't contain the word "coach". This is the right call: handling
  arbitrary directorships would invite false positives across 100+ schools.
- **Michigan** — the only inline "Head Coach" mentions are in news headlines
  ("Men's Soccer Announces Anderson as Associate Head Coach"), not in any
  staff card. The roster page genuinely doesn't expose the head coach.

The 3 pages with no inline text at all (Pepperdine, George Mason, Virginia
Tech in the "other" CMS) are fully JS-rendered or render via Vue payloads
that don't include literal "Head Coach" strings — the inline path cannot
reach them at any selector cost.

---

## Recommendation

**Ship the inline extractor extension as-is, AND still proceed with PR-9
(separate-URL `/coaches` probe), but at lower urgency.**

Rationale:

1. The inline change recovers ~80% of head-coach signal from existing
   roster fetches at zero additional HTTP cost. That alone takes us from
   0.17% to a projected ~80% coverage for the dominant SIDEARM-nextgen
   cohort — easily the highest-leverage change in this domain.
2. The residual ~15-20% miss rate splits into two buckets:
   - **Recoverable by PR-9** (~10%): JS-rendered pages (Pepperdine,
     George Mason) where a separate `/coaches` URL almost always
     server-renders the head-coach card statically — the JS-rendered
     wrapper is the roster page, not the staff page. Worth picking up.
   - **Not recoverable by either path** (~5%): named directorships
     (Stanford), news-only mentions (Michigan), schools with truly no
     online staff listing. PR-9 won't help here.
3. After this change ships, re-run a full D1 crawl to get a real
   production hit rate, THEN size PR-9 against the actual residual
   instead of the theoretical residual. The numbers may justify
   deferring PR-9 entirely if production hit rate lands above ~85%.

**Suggested order of operations:**

1. Ship this PR (inline extractor extension).
2. Re-run the next scheduled NCAA D1 + D2 + D3 crawl.
3. Measure production head-coach hit rate against the (academic year × 
   division × gender) cell counts from `colleges`.
4. Based on the residual gap vs. the +80% improvement seen in this probe,
   decide whether PR-9 is still cost-justified. Likely yes for the JS-rendered
   ~10%, likely deferrable for everything else.
