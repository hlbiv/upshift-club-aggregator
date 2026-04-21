# scraper/tests

Fixture-based pytest suite covering the 27 registered extractors in
`scraper/extractors/registry.py` plus the surrounding scraper infrastructure
(writers, rollups, canonical-club linker, archive tooling, etc.).

Run everything:

```bash
python3 -m pytest scraper/tests/ -v
```

## Fixtures are synthetic, not scraped

Every file under `scraper/tests/fixtures/` is either (a) a minimal
hand-crafted HTML sample shaped to exercise one extractor's parse path, or
(b) a trimmed capture of live HTML whose layout the extractor depends on.
**No fixture is ever refreshed from a live scrape run as part of test
maintenance.** Live scraping is Replit-only (see `CLAUDE.md`), and replaying
a scrape locally would couple the test suite to site availability +
network. When a site's HTML changes, update the fixture deliberately — the
test failure is the signal, and the fixture diff is the spec.

## Adding a new extractor + test

1. Drop the extractor module at `scraper/extractors/<name>.py` and register
   it via `@register(...)` + `from extractors import <name>` in
   `scraper/extractors/registry.py`.

2. Add a fixture at `scraper/tests/fixtures/<name>_sample.html` containing
   the smallest HTML that exercises the extractor's parse path. Include at
   least one row the parser must accept plus one edge case it must filter
   (e.g. a placeholder row, a malformed entry) — the delta between those
   two is what the test pins down.

3. Add `scraper/tests/test_<name>_parse.py` using the shared helper. The
   canonical shape (see `test_socal_parse.py`, `test_edp_parse.py`,
   `test_mspsp_parse.py` as reference implementations):

   ```python
   from _fixture_helpers import parse_fixture

   FIXTURE = "<name>_sample.html"
   SOURCE_URL = "https://example.com/<path>"
   LEAGUE = "<League>"

   def _rows() -> list[dict]:
       return parse_fixture("<name>", FIXTURE,
                            source_url=SOURCE_URL, league_name=LEAGUE)

   def test_parse_html_returns_clubs():
       assert len(_rows()) >= 1

   def test_parse_html_stamps_source_url_and_league():
       rows = _rows()
       assert rows
       for r in rows:
           assert r["source_url"] == SOURCE_URL
           assert r["league_name"] == LEAGUE
   ```

4. Run the suite. The new test must pass and
   `test_extractor_coverage.py` must stay green (it will — the module name
   is in the test file's filename, which is the coverage heuristic).

### When the helper isn't a fit

`parse_fixture` assumes the extractor exposes a pure-function
`parse_html(html, *, source_url=..., league_name=...)`. Composite
orchestrators (e.g. `sincsports`) walk multiple pages and have no single
`parse_html` entry point — they get bespoke tests exercising the next-best
parse function (`parse_sincsports_teamlist`, etc.). If your extractor is in
that camp, write the bespoke test; do not force-fit the helper.

## Coverage gate

`test_extractor_coverage.py` parametrizes over every `from extractors
import X` line in `registry.py` and asserts that `X` appears in at least
one `test_*.py` filename. When a new extractor is added to the registry
without a matching test file, CI fails on that row. Explicit opt-outs
live in `COVERAGE_ALLOWLIST` at the top of the test — each entry must carry
a reason string future-you can act on.

The filename heuristic is deliberately loose: multiple test files per
extractor count as coverage (e.g. `test_sincsports_events.py` +
`test_sincsports_rosters.py` both count for `sincsports`), and the test
doesn't inspect what the test file actually does. The intent is to catch
"forgot to write any test at all," not to police test contents.
