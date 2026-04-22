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
