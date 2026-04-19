# youthsoccersports.com — source investigation

**Date:** 2026-04-18
**Outcome:** **Skip.** Do not build a dedicated `--source youthsoccersports`
extractor. Documented here so the next reviewer doesn't repeat the
investigation.

This file lives under `docs/source-investigations/` as a graveyard for
candidate scrape sources we evaluated and rejected, with the data we
saw and the reasoning. If a future operator wants to revisit (e.g. the
site adds JSON-LD, or our needs change), the inputs are already here.

---

## What the site is

A consumer-facing **club directory** at `https://youthsoccersports.com`.
Homepage reports **"596 clubs indexed"** across **"2,054 cities plus
all 50 states"**.

Navigation: Find Teams, Soccer Pitch (equipment guides), Training,
Reviews (gear), Recruiting (college info). The actionable content is
the club directory; everything else is SEO content / affiliate-style
articles.

The site is itself an aggregator — it appears to assemble club
listings from primary sources we already scrape (state associations,
GotSport, etc.) and present them as a consumer search portal.

## robots.txt

Permissive for our use:

```
User-agent: *
Allow: /
Disallow: /admin/
Disallow: /api/
Disallow: /search?
Disallow: /contact
Disallow: /es/contact

Host: https://youthsoccersports.com
Sitemap: https://youthsoccersports.com/sitemap.xml
```

No `Crawl-delay`. The paths we'd want (`/soccer-clubs/*`,
`/find-youth-soccer-team-in-*`) are all explicitly allowed.

## Sitemap structure

Index at `/sitemap.xml` → 6 sub-sitemaps (`/sitemap/0.xml` …
`/sitemap/5.xml`). URL patterns:

| Pattern                                                | Page type            |
| ------------------------------------------------------ | -------------------- |
| `/find-youth-soccer-team-in-<state>`                   | State directory page |
| `/find-youth-soccer-team-in-<city>-<state>`            | City directory page  |
| `/soccer-clubs/<slug-with-city-state>`                 | Individual club page |
| `/es/...` mirrors of all of the above                  | Spanish translations |

Discovery is straightforward. ~596 club detail URLs.

## What's on a club page

Mixed across pages — fields are NOT consistently present:

| Field                  | Presence                                           |
| ---------------------- | -------------------------------------------------- |
| Club name              | Always                                             |
| City + state           | Always                                             |
| Logo URL (Supabase CDN)| Often                                              |
| Narrative description  | Always — multi-paragraph free text                 |
| Official website link  | **Often, but not always** (the most useful field)  |
| Phone                  | Sometimes (~half the spot-checked pages)           |
| Email                  | Sometimes                                          |
| Address                | Rarely                                             |
| Founded year           | Sometimes (often only mentioned in prose)          |
| Age groups             | Sometimes — usually only in prose                  |
| League affiliations    | Sometimes — only in prose ("plays in MLS NEXT...") |

**Crucially absent from every page sampled:**

- **No JSON-LD** (`<script type="application/ld+json">` blocks). Verified
  on 4 spot-checked pages including the homepage.
- **No coach names**
- **No rosters / players**
- **No tryout dates**
- **No tournament / event listings**
- **No social media URLs** (Facebook / Instagram / Twitter)
- **No structured league/age tables** — only prose mentions

The closest thing to structured data is the description, which is
prose. Parsing leagues / age groups / founded year from it would be a
brittle NLP exercise.

## Why we're skipping

Three reasons, in order of importance:

### 1. We already cover the same data better, at higher fidelity

`canonical_clubs` already has **~13,000 club records** scraped from
the primary sources this site aggregates from (state associations,
GotSport, SincSports, ECNL, MLS NEXT, etc.). 596 entries vs ~13k is
not the value-add — and most of the 596 are likely already in our
graph under their primary-source listing.

Where this site has a field we don't (the official club website URL),
we already have an `enrichment_runner` pipeline that can backfill
website URLs from cleaner sources. See `scraper/enrichment_runner.py`
and the `tryouts-wordpress` 861-entry seed list, which is itself
backfilled from `canonical_clubs.website`.

### 2. There are no Path-A "high-value" fields here

The Path A roadmap prioritizes coaches, rosters, tryouts, events,
matches. youthsoccersports.com has **none** of these. There is no
sink we'd be writing to that we couldn't write to better from a
primary source.

### 3. The data shape is hostile

Everything useful is buried in prose. No JSON-LD, no schema.org
markup, no consistent infobox/sidebar with `<dl>`-style key-value
pairs. Building per-club extractors would be regex-and-pray against
free-form descriptions written by the site's own staff. The
maintenance cost would be significant relative to the unique-data
yield.

## What we'd build if we changed our minds

If a future reviewer decides this is worth doing — likely as a
**one-shot enrichment pass**, not a recurring scraper:

1. Walk the 6 sitemap files; collect the ~596 `/soccer-clubs/*` URLs.
2. For each: fetch HTML, run a tolerant parser that pulls the
   "Visit Website" anchor + any phone/email regex hits.
3. Match each to an existing `canonical_clubs` row by (name, city,
   state); enrich the missing `website` / `phone` / `logo_url`
   fields. Skip if we already have the field.
4. Run as a one-off via `scraper/enrich_clubs.py` — no `--source` key,
   no recurring cron. The data doesn't change frequently enough to
   justify periodic re-scraping.

That work belongs in the enrichment runner if it ever happens, not
as a standalone Path-A extractor.

## References

- [PR #55 — Squarespace clubs investigation skip](https://github.com/hlbiv/upshift-data/pull/55)
- [PR #58 — Squarespace clubs follow-up](https://github.com/hlbiv/upshift-data/pull/58)
