# Upshift Data — Backlog

Tasks tracked for the Data repo. Items migrated from Player Platform during April 2026 cleanup.

---

## Scraper / Data Quality

1. **Verify 40 new club-discovery state URLs** — `STATE_ASSOC_SITEMAPS` covers all 50 states with sitemap.xml endpoints. Most new URLs are best-guess. First scrape run will reveal 404s to prune.
2. **Resolve 269 fuzzy near-duplicate organization pairs** — flagged at >=0.85 similarity by `dedup-organizations-fuzzy.ts`. Needs manual review or auto-merge at high confidence threshold.
3. **Wipe GA Premier orphan rows** — nav/facility text scraped as player names. Cleanup helper exists in Player Platform (`cleanupGaPremierOrphans()`). Needs equivalent in Data or run from Player admin UI.
4. **Verify GA Premier scraper clean** — re-run GA Premier scraper against 1-2 known clubs, spot-check output after extractor was hardened (Player Platform PR #198).

---

## Coach-pollution remediation — follow-ups (April 2026)

Deferred items from the April 2026 pollution remediation arc (PRs #188/#191/#194/#196/#197/#201/#202). All non-blocking.

5. **Document drizzle-kit CHECK-constraint blind spot in CLAUDE.md runbook.** When a CHECK constraint's expression changes but its name stays the same, `drizzle-kit push` reports `[✓] Changes applied` without actually emitting the ALTER. Hit during PR #201 rollout — operator had to run a manual `DROP CONSTRAINT` + `ADD CONSTRAINT` via psql. Add a preflight `pg_get_constraintdef` diagnostic + manual-ALTER fallback to the `Purge polluted coach_discoveries` runbook section in `CLAUDE.md`.
6. **Serialize `Promise.all` in `scripts/src/sweep-orphan-coaches.ts` for pg 9.0 forward-compat.** The four audit-fetch queries inside the per-batch loop use `Promise.all([client.query(...), client.query(...), ...])` which emits `DeprecationWarning: Calling client.query() when the client is already executing a query is deprecated and will be removed in pg@9.0`. Seen during the PR #202 commit pass on Replit (2026-04-22). Non-blocking — transaction committed cleanly — but will fail outright under pg 9.0. Swap for sequential `await`; the reads are inside one txn and the latency cost is negligible. Apply the same fix to the sibling `scripts/src/purge-polluted-coach-discoveries.ts` which has the same pattern.
7. **Annotate sibling-repo PR #485 decision doc with post-sweep Q6 re-measurement.** `docs/research/coach-dedup-framework-decision.md` (upshift-studio) cites Q6 = 0.08% against a 2,603-row `coaches` denominator. PR #202's orphan sweep on 2026-04-22 dropped 1,758 polluted masters, leaving 845. Re-run Q1/Q3/Q5/Q6 against the cleaned data and append a "Post-sweep re-measurement, 2026-04-22" section to the decision doc. Expected: numerator unchanged (orphans had zero club associations → couldn't appear in the same_club_pairs CTE), denominator 2,603 → 845, Q6 ≈ 0.24% — still 8× below the 2% port threshold, decision unchanged.

---

## NCAA enumeration — multi-sport arc (April 2026)

Planned in the 2026-04-23 session. Full schema/CLI contract captured in `docs/multi-sport-schema-contract.md`. All items below reference that doc for decision rationale. Ship order is top-down within each section; each PR lists dependencies. **Kickoff: 4pm 2026-04-23** with Path A/B decision gated on demo calendar.

**Goal:** close the kid URL workload (1,504 rows from the 2026-04-23 sweep: D1 535, D2 443, D3 110, NAIA 416) and reach demo-ready coverage across D1/D2/D3/NAIA while paying the multi-sport schema cost once, up front, so future basketball/football handlers don't relitigate foundational decisions.

### Pre-ship (independent of Path A/B choice)

8. **Ship the per-strategy coach-hit instrumentation commit.** ~10 LOC in `scraper/extractors/ncaa_rosters.py` — adds `_strategy` to the coach dict + per-run counter + end-of-run logline (`coach extraction hits: ...`). Decoupled from everything below; produces the data the Tier 1/2/3/4 decision depends on regardless of which path is picked. Operator can land tonight; bounded re-run ~1 hour on Replit.

### Path A / Path B fork (URL-fill strategy)

9. **Path A — Kid-fill workflow (no new engineering).** Hand the 12 exported CSVs at `~/workspace/exports/missing_*_20260423.csv` to a human worker. 1,504 URL rows at ~3 min/row = ~75 hours one-time. See `docs/operator/manual-ncaa-data-entry.md`. **Pick when demos ≤ 2 weeks out.**
10. **Path B — PR-19: Google CSE URL auto-resolver.** New `--source ncaa-discover-urls-google` handler. Queries `"<school> <state> athletics soccer"` per NULL-URL college; takes first `.edu`/`.com` athletics hit. Expected 80-90% hit rate → kid queue shrinks 1,504 → ~200 rows. Cost: ~$10 in CSE API calls + 1 day engineering. **Pick when demos > 3 weeks out.**
11. **Path B backstop — PR-20: ncsasports.org / productiverecruit.com curated seed.** One-time scrape of their consolidated D1/D2/D3/NAIA athletics URL lists; commit as checked-in CSVs under `scraper/seeds/ncaa_*_<date>.csv`; new `--source ncaa-seed-ncsa --division X` reads them. 90%+ coverage for the residue Google CSE misses. ~1 day engineering; yearly refresh.

### Multi-sport foundation (PRECONDITION for PR-23/24)

12. **PR-29 — Schema: add `sport TEXT NOT NULL` to `colleges`.** Back-fills existing rows as `'soccer'`, **then DROPs the DEFAULT** so every future INSERT must supply sport explicitly (belt-and-suspenders against silent cross-sport contamination). Replaces `colleges_name_division_gender_uq` with `(name, division, gender_program, sport)` composite key. Sport and gender are **separate axes** — football is `gender_program='mens', sport='football'`. See `docs/multi-sport-schema-contract.md` for SQL + rollback plan. ~3 hrs.
13. **PR-30 — Add `--sport` flag to CLI handlers.** Two-tier back-compat: existing handlers (`ncaa-seed-d1`, `ncaa-rosters`, etc.) accept `--sport SPORT` optional-with-default `soccer`; **new handlers shipped post-PR-30 are required (`required=True`)** — forcing function for future basketball PRs. Operator runbooks for existing handlers updated to show `--sport soccer` explicitly. Also in scope: rename `scraper/extractors/ncaa_rosters.py` → `ncaa_soccer_rosters.py` via `git mv` so soccer doesn't become the sport-in-filename asymmetry. ~4 hrs.

### Quality-flag surface + smart guard

14. **PR-24 — `college_roster_quality_flags` table + scraper writes + admin API.** New table (FK `college_id`, `academic_year`, `flag_type`, `metadata` jsonb, resolution columns mirroring `roster_quality_flags`). Three flag types: `historical_no_data`, `partial_parse`, `url_needs_review`. `url_needs_review.reason` is a 6-value enum: `no_url_at_all` / `static_404` / `playwright_exhausted` / `partial_parse` / `historical_no_data` / `current_zero_parse`. Scraper writes flags at two decision points in `scrape_college_rosters()`. Admin API mirrors existing roster-quality-flags endpoints at `/api/v1/admin/data-quality/college-roster-quality-flags`. Atomic SQL-level `attempts` counter (jsonb merge) safe under future `--parallel N`. See `docs/multi-sport-schema-contract.md` for full write-SQL spec. ~2 days.
15. **PR-23 — Smart scrape guard (`should_scrape()` + CLI flags).** In `scraper/extractors/ncaa_soccer_rosters.py`, add gating helper: current season → freshness check (`--skip-fresh-days N`, default 30); historical season with ≥10 existing players → NEVER re-scrape; historical with unresolved `url_needs_review` flag → skip until operator triages; historical with no data → retry up to 3 attempts, then permanently flag. Tolerates PR-24's missing flag table via `psycopg2.errors.UndefinedTable` catch so PR-23 is safe to merge before PR-24's `db push`. New flags: `--force-rescrape`, `--force-historical YYYY-YY`. **Expected: full sweep 5 hrs → 30 min (10× speedup).** ~1 day.

### Durable memory

16. **Commit `docs/multi-sport-schema-contract.md` alongside PR-29.** Schema contract lives in source control so future-you writing the basketball handler reads it there, not in chat history. Covers DROP DEFAULT pattern, sport-vs-gender axis separation, `--sport` required-on-new-handlers rule, 6-value `url_needs_review.reason` enum, PR-29 rollback SQL + window-closes-after-first-non-soccer-write forcing function.

### Deferred (not yet scheduled)

17. **PR-22 — COVID 2020-21 short-circuit.** If 2020-21 first-hit 404, skip Playwright fallback entirely; emit `SKIP reason: likely_covid_cancelled`. Mostly subsumed by PR-23's retry cap; separate PR only if operator wants 2020-21-specific smarter handling. ~2 hrs.
18. **PR-25 — Colleges dedup script.** One-shot cleanup: D1 has 468 womens rows vs. expected ~205 because both `"Alabama"` (Wikipedia form) and `"University of Alabama"` (stats.ncaa.org form) exist separately. Walk `colleges` for short-name/full-name pairs, migrate FKs, delete dups. JSONL audit artifact. ~4 hrs.
19. **PR-27 — Season-aware scheduler.** Modify cron wrappers (commit `bcafe2a`) to use month-based cadence: weekly Aug/Sep, monthly Oct-Jan, quarterly Feb-Jul. ~60-70% reduction in scheduled scrapes. Depends on PR-23. ~4 hrs.
20. **PR-28 — "Resolve with URL" admin action.** PATCH endpoint variant on `college-roster-quality-flags/:id/resolve` accepting `new_soccer_program_url` + triggering targeted re-scrape. Depends on PR-24. Defer until operator usage shows this is common. ~3 hrs.
