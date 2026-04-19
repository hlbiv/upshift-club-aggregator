# Rejected ADRs

Architecture decisions we've actively considered and rejected. Each entry
records what was considered, why we said no, and under what conditions to
reconsider. Purpose: prevent the same proposals from being reopened without
new information.

## Redis cache (rejected 2026-04-19)

**Considered:** adding Redis as a read-side cache in front of the REST API
(`artifacts/api-server`). Most obvious candidates: `/api/clubs` listings,
`/api/analytics/*` rollups, and the canonical-club search endpoint.

**Rejected because:**

- Current scale doesn't justify the cache-invalidation complexity. The API
  is single-instance on Replit; cold Postgres reads are fast enough for
  current traffic.
- Adding a cache requires reasoning about invalidation (per-route, on
  write, TTL edge cases). That reasoning cost exceeds the latency win
  today.
- Scraper writes happen on a schedule (not in the request path), so the
  usual "write triggers cache invalidation" pattern isn't clean — cache
  would go stale between scheduled runs regardless.

**Revisit when any of these become true:**

- p95 latency on `/api/clubs` exceeds 200ms (measured via the rate-limit
  middleware's timing metadata or an added APM hook).
- Analytics queries on `scrape_health` or `canonical_clubs` bottleneck the
  dashboard, and a 10–60s staleness window would be acceptable.
- Horizontal scaling becomes necessary (multi-instance API with shared
  cache state).

**Related:** see `docs/runbooks/wave-2-post-merge.md` §4 for the current
rate-limit middleware; once p95 is observable there, we'll have the data
to re-open this.
