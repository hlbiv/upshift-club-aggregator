# NAIA proxy provider — recommendation

## Status: AWAITING PROCUREMENT

`scraper/proxy_config.yaml` is empty. Replit egress IPs hit naia.org's
WAF (HTTP 403/405 verified 2026-04-22). The NAIA URL resolver
(`scraper/run.py::_handle_naia_resolve_urls`) aborts with "no slugs
available" until proxies are configured. There are 416 NAIA programs
waiting on `soccer_program_url`; once proxies land, the resolver fills
them in one batch.

## What we actually need

- A small **residential or ISP proxy pool** (datacenter IPs are usually
  the ones already on the WAF blocklist).
- US-based egress (naia.org's WAF is friendlier to US IPs).
- Per-request URL of the form `http://USER:PASS@gateway.example.com:PORT`
  — that's exactly what `scraper/utils/http.py` accepts in
  `domains.<host>.proxies[]`.
- Volume estimate: ~5,000 requests/month total. Each NAIA resolve cycle
  is ~430 GETs (1 index per gender + ~417 detail pages); planned cadence
  is monthly. Headroom for spot-checks + retries.

## Provider shortlist (cheapest viable first)

1. **ScraperAPI** — `https://www.scraperapi.com/`
   - Hobby plan: $49/mo, 100k API credits, residential routing built in.
   - Single endpoint (`http://scraperapi:KEY@proxy-server.scraperapi.com:8001`)
     so the YAML entry stays one line.
   - Best fit if we want zero session-management work.

2. **ZenRows** — `https://www.zenrows.com/`
   - Developer plan: $69/mo, 250k requests, includes residential pool
     and JS-rendering fallback we don't currently need.
   - Same single-endpoint model as ScraperAPI.

3. **Bright Data** — `https://brightdata.com/`
   - Pay-as-you-go residential: ~$8.40/GB. NAIA traffic is HTML-light
     (~50 KB/page), so ~22 MB/cycle ≈ $0.20/cycle. Cheapest at this
     volume but the dashboard / contracting overhead is heaviest.
   - Use the "Web Unlocker" zone to skip session/sticky-IP plumbing.

4. **Oxylabs / Smartproxy** — comparable to Bright Data on price; use
   only if procurement already prefers one of them.

**Recommendation: ScraperAPI hobby plan.** Lowest setup friction,
flat monthly bill (easy budget), and the `http://user:pass@host:port`
URL drops straight into the existing YAML schema with no code changes.

## What to do once a plan is purchased

1. Add the proxy URL as a Replit secret named `NAIA_PROXY_URL`
   (env-vars skill — never commit credentials).
2. Update `scraper/proxy_config.yaml`:
   ```yaml
   domains:
     www.naia.org:
       proxies:
         - ${NAIA_PROXY_URL}
       cooldown_seconds: 300
   ```
   (Verify `scraper/utils/http.py` does env-var interpolation; if not,
   add a 2-line tweak in `_load_config` to expand `${VAR}` tokens
   before returning the dict.)
3. Smoke:
   ```bash
   python -c "from scraper.utils import http; \
     print(http.get('https://www.naia.org/sports/msoc/2021-22/teams', \
                    timeout=20).status_code)"
   ```
   Expected: 200.
4. Dry-run resolver (5 rows):
   ```bash
   python -m scraper.run --source naia-resolve-urls \
       --gender mens --limit 5 --dry-run
   ```
5. Then full backfill (no `--dry-run`, no `--limit`).
6. Mention the new proxy entry in `replit.md` under the scraper section.

## Scope

This file is procurement-only. Once a provider is chosen, the wiring
in step 2–6 is ~10 minutes of work and can be picked up by any agent.
