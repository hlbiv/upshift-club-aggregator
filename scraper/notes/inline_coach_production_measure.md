# NCAA inline coach extractor — production hit rate (Task #37)

**Date:** 2026-04-22
**Goal:** Validate the +82pp lift seen in the 28-page diagnostic probe
([`inline_coach_probe.md`](inline_coach_probe.md), Task #34) against an
actual full NCAA D1+D2+D3 men's & women's soccer crawl, so we can size
the next investment (separate `/coaches`-URL probe — task "Render JS-only
coaches pages so we can find their head coaches too").

## Method

The full NCAA roster crawl was re-run between 2026-04-21 and
2026-04-22 (after the inline-extractor changes shipped). Source of
truth: `colleges`, `college_coaches`, and `scrape_run_logs` in the
production Postgres.

For each (division × gender) cell we report:

- **Schools** — distinct `colleges` rows for that cell.
- **Has URL** — schools where `colleges.soccer_program_url` is populated
  (i.e. a roster page is even available to fetch). This is the
  fetchable denominator.
- **With head coach** — schools where at least one
  `college_coaches.is_head_coach = true` row exists.
- **% of-URL** — head-coach hit rate restricted to schools we could
  actually fetch (`with_head / has_url`). This is the apples-to-apples
  measure of extractor quality.
- **% of-all** — head-coach hit rate against the full universe of
  `colleges` rows (`with_head / schools`). This folds in URL-seeding
  gaps and is the number operators care about.

A "Scraped" column is omitted from the headline table on purpose —
`colleges.last_scraped_at` is touched by both the directory/seed jobs
(which write a `colleges` row even when no roster URL is discovered)
and the roster-fetch jobs, so it conflates discovery with fetching and
is not a clean denominator for hit-rate analysis. The fetchable
denominator is `Has URL`. If you do want to see "scraped at all
recently" alongside, the query is:

```sql
SELECT division, gender_program,
  COUNT(*) FILTER (WHERE last_scraped_at IS NOT NULL) AS ever_touched,
  COUNT(*) FILTER (WHERE last_scraped_at >= '2026-04-20') AS touched_in_window
FROM colleges
GROUP BY 1,2 ORDER BY 1,2;
```

For reference (touched-in-window during this crawl): D1 m 225 / w 458,
D2 m 222 / w 280, D3 m 103 / w 101, NAIA m 212 / w 205. These exceed
the `Has URL` counts because the seed/directory jobs stamp
`last_scraped_at` even when no `soccer_program_url` is written —
which is exactly why this column is not used in the hit-rate table.

## Results

| Division | Gender | Schools | Has URL | With head | % of-URL | % of-all |
| -------- | ------ | ------: | ------: | --------: | -------: | -------: |
| D1       | mens   |     235 |      56 |        45 | **80.4%** |    19.1% |
| D1       | womens |     468 |     112 |        93 | **83.0%** |    19.9% |
| D2       | mens   |     260 |      57 |        53 | **93.0%** |    20.4% |
| D2       | womens |     307 |      67 |        63 | **94.0%** |    20.5% |
| D3       | mens   |     156 |     101 |        91 | **90.1%** |    58.3% |
| D3       | womens |     155 |     100 |        95 | **95.0%** |    61.3% |
| NAIA     | mens   |     212 |       1 |         1 |   100.0% |     0.5% |
| NAIA     | womens |     205 |       0 |         0 |        — |     0.0% |

(Pre-fix baseline for context: 0.17% — 2 head-coach rows across ~1,200
fetches across the entire D1 cohort.)

## Headline reading

The probe predicted ~80% inline hit rate; the production crawl confirms
**80–95% of-URL hit rate across every NCAA division and gender** —
exactly what the 28-page sample projected. Strategy 4 (WMT/Vue staff
cards) over-delivers on D2/D3, where non-Sidearm CMSs are more common.

The of-all numbers look low because the **URL-seeding gap is now the
binding constraint, not the extractor**:

- Only 56/235 (24%) D1 mens programs have `soccer_program_url` populated;
  the same is true for D1 womens (24%), D2 mens (22%), and D2 womens
  (22%). D3 is healthier (~65%) because Wikipedia category seeding
  filled it in.
- NAIA effectively has no roster URLs (1/417 across both genders).

In other words, the extractor improvement landed cleanly — every cell
we can actually fetch a page for now exceeds the 70% bar. The remaining
gap is upstream of the extractor and should be tracked as a seeding
problem, not a parsing one.

## Cells that miss the 70% bar (vs. all schools)

All four NCAA cells (D1 m/w, D2 m/w) miss the 70% of-all threshold.
**The miss is entirely attributable to URL coverage, not extractor
miss-rate** — every of-URL number for those four cells is above 80%.
NAIA misses on both axes (no URLs, no extractor signal).

D3 m/w sit at 58% / 61% of-all — also short of 70%, but again because
~35% of D3 schools have no `soccer_program_url`. The of-URL rates
(90% / 95%) are the highest of any cell.

## Residual schools — fetched but no head coach extracted

53 schools across D1+D2+D3 have a roster URL but produced no head-coach
row. This is exactly the population the next investment (JS-rendered
`/coaches` page probe) needs to target. Listed in full so the next task
can target them directly:

### D1 mens (11)
George Mason, Pepperdine, Stanford, Michigan, Minnesota, Nebraska,
Notre Dame, Oregon, Richmond, USC, Virginia Tech.

### D1 womens (19)
BYU, George Mason, Mississippi State, San Diego State, Stanford,
Tulane, UCF, Kentucky, Michigan, Nebraska, New Mexico, Notre Dame,
Oklahoma, South Carolina, Texas, Wyoming, Utah State, Vanderbilt,
Virginia Tech.

### D2 mens (4)
Assumption, Colorado Mesa, Colorado School of Mines, Maryville (St. Louis).

### D2 womens (4)
Colorado Mesa, Maryville (St. Louis), Saint Leo, Tiffin.

### D3 mens (10)
Adrian, Carthage, Lehman, MIT, Medgar Evers, Nazareth, SUNY Morrisville,
SUNY Oswego, Trinity (TX), Union.

### D3 womens (5)
Lehman, Medgar Evers, SUNY Oswego, Trinity (TX), Union.

The D1 list is dominated by exactly the CMSs the probe flagged as
JS-rendered or non-standard: Pepperdine, George Mason, Virginia Tech
(JS-rendered Vue payloads), Stanford (named directorship — "The Knowles
Family Director of Men's Soccer", correctly rejected by
`_is_strict_head_coach`), Michigan (no inline staff card on roster
page), and Notre Dame / USC / BYU (custom CMSs not yet covered by any
of the four selector strategies).

## NAIA caveat

NAIA shows essentially zero coverage on every axis. Root cause is
**not** the extractor — it's that the NAIA seed runs are failing
upstream: the Wikipedia seed pages 404 (`scrape_run_logs` shows
`ncaa-seed-wikipedia-naia-{mens,womens}` failing with `404 Client
Error: Not Found` for both
`/wiki/List_of_NAIA_men's_soccer_programs` and the womens equivalent
URL pattern), and the existing `naia_directory.py` populates `colleges`
rows without `soccer_program_url`. NAIA needs its own URL-discovery
pass before the inline extractor has anything to bite on.

## Recommendation — sizing the next move

1. **The inline-extractor change is doing what it promised.** Of-URL
   hit rate is 80–95% across every NCAA cell. No further selector
   strategies are warranted on the current SIDEARM/WMT cohort —
   diminishing returns now.

2. **The JS-rendered `/coaches`-URL probe (PR-9 / "Render JS-only
   coaches pages") should be scoped tightly to the 53 residual
   schools above**, not run as a blanket fallback. The list is
   small enough to validate per-school. Realistic ceiling: probably
   recovers the ~8 JS-rendered D1 schools (Pepperdine, George Mason,
   Virginia Tech, Notre Dame, USC, Michigan, Minnesota, Oregon)
   — call it +8 schools, taking D1 mens from 80.4% → ~94.6%
   of-URL.

3. **The bigger lever is now URL seeding, not parsing.** D1 mens at
   24% URL coverage means even a perfect extractor cannot exceed 24%
   of-all. The two pre-existing tasks already cover this:
   - "Show operators which schools still have no head coach so they
     can fix them" — surfaces the gap.
   - The NAIA seeding regression above should be folded into existing
     NAIA work rather than spun out separately.

## Reproducibility

Coverage table query (re-runnable as the crawl progresses):

```sql
WITH per_school AS (
  SELECT c.id, c.division, c.gender_program, c.soccer_program_url,
         BOOL_OR(cc.is_head_coach) AS has_head
  FROM colleges c
  LEFT JOIN college_coaches cc ON cc.college_id = c.id
  GROUP BY c.id
)
SELECT division, gender_program,
  COUNT(*) AS schools,
  COUNT(*) FILTER (WHERE soccer_program_url IS NOT NULL) AS has_url,
  COUNT(*) FILTER (WHERE has_head)                       AS with_head,
  ROUND(100.0*COUNT(*) FILTER (WHERE has_head)
        / NULLIF(COUNT(*) FILTER (WHERE soccer_program_url IS NOT NULL), 0), 1)
    AS pct_of_url,
  ROUND(100.0*COUNT(*) FILTER (WHERE has_head) / COUNT(*), 1) AS pct_of_all
FROM per_school
GROUP BY 1,2 ORDER BY 1,2;
```

Residual-school list query:

```sql
WITH per_school AS (
  SELECT c.id, c.name, c.division, c.gender_program, c.soccer_program_url,
         BOOL_OR(cc.is_head_coach) AS has_head
  FROM colleges c
  LEFT JOIN college_coaches cc ON cc.college_id = c.id
  GROUP BY c.id
)
SELECT division, gender_program, name, soccer_program_url
FROM per_school
WHERE soccer_program_url IS NOT NULL AND NOT COALESCE(has_head, false)
ORDER BY division, gender_program, name;
```
