# NAIA roster URL extractor — live spot-check

## Status: BLOCKED on Task #69 (proxy configuration)

The `parse_naia_team_page` extractor in `scraper/extractors/naia_directory.py`
was built and unit-tested against a synthesized HTML fixture
(`scraper/tests/fixtures/ncaa/naia_team_detail_baker.html`) because Replit
egress IPs are blocked by naia.org's WAF (HTTP 403/405 on direct GETs;
verified 2026-04-22 via `curl https://www.naia.org/sports/msoc/2021-22/teams`).

A live spot-check against ~20 real NAIA detail pages — the validation
this task asks for — cannot run until `scraper/proxy_config.yaml` is
populated with a working naia.org proxy pool (Task #69).

## Current selector strategies (what the spot-check will validate)

`parse_naia_team_page` walks every `<a>` and accepts the first href that:

1. Is an absolute `http(s)://` URL,
2. Is NOT in `_NAIA_LINK_BLOCKLIST` (mailto/tel/javascript/relative,
   plus naia.org self-links and major social-media domains),
3. Has visible text OR `title` OR `aria-label` matching one of:
   - "official (athletics) site"
   - "athletic(s) website|home(page)|site"
   - "team website"
   - "visit (athletics) site"
   - "school website"

Returns scheme://host (path/query/fragment dropped). The downstream
SIDEARM resolver re-composes the roster path on top.

## Playbook for when proxies are configured

1. Confirm proxies work:
   ```bash
   python -c "from utils import http; print(http.get('https://www.naia.org/sports/msoc/2021-22/teams', timeout=20).status_code)"
   ```
   Expected: `200`. If 4xx/5xx, the proxy pool is misconfigured — fix
   that before running the spot-check.

2. Pick ~20 NAIA programs whose athletics website you can verify
   independently (Google + check the school's about page). A starter
   set known to have detail pages on naia.org's 2021-22 index:
   - mens: Baker, Aquinas, Antelope Valley, Arizona Christian,
     Avila, Bellevue, Benedictine, Briar Cliff, Brescia, Wayland
     Baptist, William Woods, Tougaloo, Ave Maria
   - womens (2020-21 index): same school slugs typically work

3. Run the resolver in dry-run mode against those programs:
   ```bash
   cd scraper
   python -m run --source naia-resolve-urls --gender mens --limit 20 --dry-run 2>&1 | tee /tmp/naia_spotcheck.log
   ```

4. Compare each `[dry-run] <NAME>: website=<URL> program_url=<URL>`
   line against your manually verified URL. Record results in this
   doc under "Run history" below.

5. For any miss (extractor returned wrong URL or None when a real
   athletics link exists on the page):
   - Save the offending HTML to
     `scraper/tests/fixtures/ncaa/naia_team_detail_<slug>.html`
     (strip any tracking params / inlined cookies first).
   - Add a parametrized test in `scraper/tests/test_naia_directory.py`
     under `TestParseNaiaTeamPage` that fails on the new fixture.
   - Tweak `_NAIA_OFFICIAL_SITE_LABELS` or `_NAIA_LINK_BLOCKLIST`
     until the test passes.

6. Target hit rate: ≥ 90% of the 20 spot-check programs should
   produce a non-None `website`. SIDEARM `program_url` hit rate will
   be lower (~70%) since not every NAIA school is on SIDEARM —
   that's expected and not an extractor bug.

## Run history

_None yet — awaiting Task #69._

| Date | Programs tested | Website hits | Program-URL hits | Selector tweaks |
|------|-----------------|--------------|------------------|-----------------|
| TBD  | -               | -            | -                | -               |
