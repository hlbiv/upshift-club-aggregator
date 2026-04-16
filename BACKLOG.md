# Upshift Data — Backlog

Tasks tracked for the Data repo. Items migrated from Player Platform during April 2026 cleanup.

---

## Scraper / Data Quality

1. **Verify 40 new club-discovery state URLs** — `STATE_ASSOC_SITEMAPS` covers all 50 states with sitemap.xml endpoints. Most new URLs are best-guess. First scrape run will reveal 404s to prune.
2. **Resolve 269 fuzzy near-duplicate organization pairs** — flagged at >=0.85 similarity by `dedup-organizations-fuzzy.ts`. Needs manual review or auto-merge at high confidence threshold.
3. **Wipe GA Premier orphan rows** — nav/facility text scraped as player names. Cleanup helper exists in Player Platform (`cleanupGaPremierOrphans()`). Needs equivalent in Data or run from Player admin UI.
4. **Verify GA Premier scraper clean** — re-run GA Premier scraper against 1-2 known clubs, spot-check output after extractor was hardened (Player Platform PR #198).
