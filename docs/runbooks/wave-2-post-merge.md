# Replit Runbook — Upshift Data Waves 1 + 2 Post-Merge

Run these in order from the Replit shell. Assumes `DATABASE_URL` and `$REPL_HOME` are already set. 19 PRs merged since last pull — expect `git pull` to have a lot of commits.

---

## Current state snapshot (verified 2026-04-22 against live DB)

The schema portion of this runbook is **already applied** in this environment.
Use this snapshot to skip to the still-outstanding sections instead of running
every step from scratch.

| Item | State | Notes |
|---|---|---|
| §1 git pull / pnpm install | ✅ done | Branch is on `master`, working tree clean. |
| §2 schema push (5 new tables + `triggered_by`) | ✅ done | All five tables present (`raw_html_archive`, `commitments`, `ynt_call_ups`, `odp_roster_entries`, `hs_rosters`); `scrape_run_logs.triggered_by` exists with default `'manual'`. |
| §3 Replit Secrets (auth/ratelimit/docs flags) | ⚠️ prod-only | API server boots with `[api-key-auth] DISABLED in development mode` — flags only take effect in prod. Verify after deploy. |
| §4 API smoke tests | ⏸️ prod-only | Auth gate / rate-limit headers / `/api/docs` all expect prod behavior; dev returns 200 unguarded by design. Re-run against the deployed `.replit.app` URL. |
| §5 Object Storage bucket `upshift-raw-html` | ❓ user-side | `raw_html_archive` table exists but has 0 rows despite 276 scrape runs in the last 7 days — bucket likely not yet created or `ARCHIVE_RAW_HTML_ENABLED` not set. Create bucket in Replit console + flip the secret. |
| §6 Scheduled Deployments (3 cron jobs) | ❓ user-side | Scripts exist at `scraper/scheduled/{nightly_tier1,weekly_state,hourly_linker}.sh`. Wire in Replit console per `docs/replit-scheduled-deployments.md`. |
| §7 Mint API key + sync sibling repo | ⏸️ partial | 4 `api_keys` rows already exist; `UPSHIFT_DATA_API_KEY` already in this env. Sibling repo (`hlbiv/upshift-studio`) lives on a separate Replit — sync there. |
| §8 Linker sanity | ⏸️ outstanding | 2,607 `event_teams` rows still have NULL `canonical_club_id`; 0 commitments with NULL `club_id`. Run `python3 scraper/run.py --source link-canonical-clubs --dry-run --limit 100` once scrapers have populated fresh data. |

### Known gap — RESOLVED 2026-04-22

`events_source_enum` was widened to 6 values and a sibling `roster_source_enum`
was introduced by `scripts/src/migrations/0002_split_events_source_enum.sql`.
Apply with:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f \
  scripts/src/migrations/0002_split_events_source_enum.sql
```

**Forward-only.** Postgres has no `DROP VALUE` for enums — adding values is
irreversible without recreating the type.

**`events_source_enum` (6 values):**
`gotsport, sincsports, manual, other, totalglobalsports, usclub_sanctioned`

**`roster_source_enum` (12 values, brand-new type):**
`gotsport, sincsports, maxpreps, ncaa, naia, njcaa, odp, soccerwire,
club_website, duda_360player, manual, other`. The
`club_roster_snapshots.source` column is retyped from `events_source_enum`
to `roster_source_enum` in the same migration (table was empty at retype time).

**Events backfill — host-pattern → enum-value mapping:**

| Host pattern (case-insensitive)              | New `events.source` |
|----------------------------------------------|---------------------|
| `public.totalglobalsports.com`               | `totalglobalsports` |
| `*.usclubsoccer.org` (any subdomain or root) | `usclub_sanctioned` |
| `system.gotsport.com` / `events.gotsport.com`| `gotsport`          |
| `*.sincsports.com`                           | `sincsports`        |

Anything else stays `'other'`. The original task spec targeted a residual
of <30 `'other'` rows, but the actual host distribution shows ~170 of the
176 `'other'` rows are legitimate misc-club tournament sites
(`socalelitefc.com`, `slsgsoccer.org`, `rebelssoccerclub.com`, …) that
don't correspond to any platform-level runner. **Actual residual after
backfill: 168 rows of legitimate `'other'`** (not a bug).

**Before/after counts (2026-04-22 dev DB):**
- Before: `other` 176, `gotsport` 24, `sincsports` 17, `manual` 0, total 217
- After:  `other` 168, `gotsport` 27 (+3), `sincsports` 17, `totalglobalsports` 4, `usclub_sanctioned` 1, total 217

**Follow-ups not covered by this migration** (separate PRs, called out in §9):
- The three roster scrapers (`soccerwire`, `club_website`, `duda_360player`)
  currently leave `club_roster_snapshots.source` NULL — emitting their
  enum values is a per-scraper change, not a schema change.
- A dashboard filter on the new enum values is a separate dashboard task.
- `usclub_sanctioned` is intentionally absent from `roster_source_enum` —
  that handler seeds events, not rosters.

---

## 1. Pull and install

```bash
cd "$REPL_HOME"
git fetch origin master && git checkout master && git pull
pnpm install
```

Check that workspace packages resolve:

```bash
pnpm list -r --depth=-1 | grep '@workspace/'
# or: pnpm -r exec pwd
```

If either walks all workspace packages, resolution is fine.

---

## 2. Apply schema changes

Adds `scrape_run_logs.triggered_by` column + 5 new tables: `raw_html_archive`, `commitments`, `ynt_call_ups`, `odp_roster_entries`, `hs_rosters`.

**Pre-flight: dump the current schema for rollback insurance.**

```bash
pg_dump "$DATABASE_URL" --schema-only > /tmp/pre-wave2-schema.sql
ls -lh /tmp/pre-wave2-schema.sql  # confirm non-empty
```

30 seconds of insurance against a misclicked drizzle-kit "rename" prompt.

**Push the schema:**

```bash
pnpm --filter @workspace/db run push
```

**Expected prompts from `drizzle-kit`:**
- "Is `<table>` created or renamed from another table?" for each new table → answer **No**.
- `triggered_by` column → auto-applies with `'manual'` default, no prompt.

**Verify:**

```bash
psql "$DATABASE_URL" -c "\dt raw_html_archive commitments ynt_call_ups odp_roster_entries hs_rosters"
psql "$DATABASE_URL" -c "SELECT column_name, column_default FROM information_schema.columns WHERE table_name='scrape_run_logs' AND column_name='triggered_by';"
```

Both should return rows. If they don't, stop and investigate before continuing — nothing else in this playbook is safe without the schema applied.

---

## 3. Set environment variables (Replit Secrets)

| Secret | Value | Purpose |
|---|---|---|
| `API_KEY_AUTH_ENABLED` | `true` | apiKeyAuth middleware (#62) |
| `API_RATE_LIMIT_ENABLED` | `true` | Rate limit: 100 req/min per key (#62) |
| `API_DOCS_ENABLED` | `true` | Serves swagger-ui at `/api/docs` |
| `ARCHIVE_RAW_HTML_ENABLED` | leave unset for now | Turn on after step 5 completes |

Restart the api-server:

```bash
pnpm --filter @workspace/api-server run dev  # or your prod start script
```

**Expected boot log:**

```
[api-key-auth] enabled
[rate-limit] enabled
[api-docs] serving at /api/docs
```

---

## 4. Smoke-test the API

With `UPSHIFT_DATA_API_KEY` exported:

```bash
# Docs site
curl -sSI http://localhost:8080/api/docs | head -1
# Expect: HTTP/1.1 200 OK

# Auth gate
curl -sS -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/clubs?page_size=1
# Expect: 401
curl -sS -o /dev/null -w "%{http_code}\n" -H "X-API-Key: $UPSHIFT_DATA_API_KEY" \
  http://localhost:8080/api/clubs?page_size=1
# Expect: 200

# Rate limit header (quick sanity)
curl -sSI -H "X-API-Key: $UPSHIFT_DATA_API_KEY" \
  http://localhost:8080/api/clubs?page_size=1 | grep -i '^ratelimit'
# Expect: RateLimit-Remaining and RateLimit-Limit headers present
```

**Optional parallel burst** (the sequential-curl burst from an earlier draft doesn't trip a 100/min limiter because 101 sequential localhost curls take >2 seconds):

```bash
seq 1 120 | xargs -P 20 -I{} curl -sS -o /dev/null -w "%{http_code}\n" \
  -H "X-API-Key: $UPSHIFT_DATA_API_KEY" \
  http://localhost:8080/api/clubs?page_size=1 \
  | sort | uniq -c
# Expect: most responses 200, some 429s after the window fills
```

The middleware tests that shipped with #62 already cover correctness — the smoke above is belt-and-suspenders.

---

## 5. Replit Object Storage for raw HTML archival

1. Replit console → Object Storage → create bucket `upshift-raw-html`.
2. Confirm SDK available:

   ```bash
   python3 -c "import replit.object_storage; print('ok')"
   ```

3. In Secrets: set `ARCHIVE_RAW_HTML_ENABLED=true`.
4. Smoke with a concrete named key so you can clean up deterministically:

   ```bash
   SMOKE_KEY="smoke-tests/runbook-check-$(date +%s).html"
   export SMOKE_URL="https://example.com/$SMOKE_KEY"
   python3 -c "
   from scraper.utils.html_archive import archive_raw_html
   import os
   result = archive_raw_html(os.environ['SMOKE_URL'], '<html>hi</html>')
   print(result)
   "
   # Cleanup (same shell session, before leaving §5):
   psql "$DATABASE_URL" -c "DELETE FROM raw_html_archive WHERE bucket_path LIKE 'smoke-tests/%';"
   # Then manually remove the object from Replit Object Storage console.
   ```

   Expect `{sha256, bucket_path, content_bytes}`. If `None`: check stderr for the one-time init warning. The `smoke-tests/` prefix keeps every runbook smoke in its own bucket namespace — the row-level DELETE above clears the DB side; the console click clears the bucket side.

5. **Real smoke** (after any scrape run):

   ```sql
   SELECT count(*) FROM raw_html_archive
   WHERE archived_at > now() - interval '10 min';
   ```

   Should be > 0. Both `requests`-based (`scraper_static.py`) and Playwright-based (`scraper_js.py`) paths archive through the same flag.

---

## 6. Wire scheduled deployments

Per `docs/replit-scheduled-deployments.md`. Three jobs in Replit console:

| Name | Command (cwd = repo root) | Cron (UTC — currently EDT) |
|---|---|---|
| `upshift-nightly-tier1` | `scraper/scheduled/nightly_tier1.sh` | `0 6 * * *` (= 02:00 ET) |
| `upshift-weekly-state` | `scraper/scheduled/weekly_state.sh` | `0 7 * * 0` (= 03:00 ET Sun) |
| `upshift-hourly-linker` | `scraper/scheduled/hourly_linker.sh` | `5 * * * *` |

**DST note:** Add +1 to each UTC hour after Nov 2, 2026 (EST). The docs file has both sets of crons.

**First-scheduled-run warning:** The console scheduler doesn't fire on save. It waits for the next cron window. If you set up the jobs at 10pm and check `scrape_run_logs` five minutes later, you will see nothing — don't start debugging.

To verify immediately, manually run the script once:

```bash
bash scraper/scheduled/nightly_tier1.sh
```

Then confirm a row appears:

```sql
SELECT source_name, triggered_by, started_at, status
FROM scrape_run_logs
WHERE started_at > now() - interval '10 min'
ORDER BY started_at DESC LIMIT 5;
```

For this manual run, `triggered_by` will be `'manual'` (the env var isn't set by your shell). The `'scheduler'` verification happens the morning after, when the cron actually fires.

**Next-morning check:**

```sql
SELECT source_name, triggered_by, started_at, status
FROM scrape_run_logs
WHERE triggered_by = 'scheduler'
ORDER BY started_at DESC LIMIT 5;
```

Should show rows from overnight / the scheduled window.

---

## 7. Mint API keys + sync sibling repo

### 7a. Mint a key for the sibling

```bash
pnpm --filter @workspace/scripts run create-api-key -- --name "upshift-studio"
# Prints plaintext key ONCE. Copy immediately.
```

### 7b. Sync the sibling repo (`hlbiv/upshift-studio`)

The sibling's GitHub repo is `hlbiv/upshift-studio` (local dir is `upshift-player-platform`, but remote + PRs live at `upshift-studio`).

**Protect any in-progress work before pulling.** A recent agent run stashed uncommitted edits on master as `"WIP before api-zod sync"` (CLAUDE.md, combineStations.ts, CombinesPage.tsx + 3 planning docs).

```bash
cd /path/to/upshift-studio
git stash list
# If you see: stash@{0}: On master: WIP before api-zod sync — back it up before anything else:
git stash show -p stash@{0} > /tmp/sibling-wip.patch
ls -lh /tmp/sibling-wip.patch  # confirm non-empty
```

**Then pull:**

```bash
git fetch origin master && git checkout master && git pull
pnpm install
```

upshift-studio#445 re-vendored `@hlbiv/api-zod@0.2.0` into `lib/upshift-data-client/vendor/hlbiv-api-zod/` and replaced 14 `z.any()` stubs with real imports. No npm publish — api-zod stays vendored.

**Decide what to do with the stash:**
- Recover it: `git stash pop` (or `git stash apply stash@{0}` to keep the stash around).
- Drop it intentionally: `git stash drop stash@{0}` — only if you've confirmed the content is no longer wanted.
- Defer: leave it in `git stash list` and move on.

**Smoke the sibling:**

```bash
pnpm --filter @workspace/upshift-data-client run test
# Should pass — now exercises real Zod validation end-to-end
```

### 7c. Wire the sibling's Replit Secrets

- `UPSHIFT_DATA_API_KEY` → plaintext from step 7a
- `UPSHIFT_DATA_API_URL` → this service's base URL (e.g., `https://upshift-data.<you>.repl.co`)

Restart the sibling's api-server.

### 7d. Validate read-swap is actually flowing through Data (not silent fallback)

Read-swap env is already live on the sibling (`UPSHIFT_DATA_READS=orgs,events,coaches,schools`, `UPSHIFT_DATA_API_URL`, `UPSHIFT_DATA_API_KEY` all set). But the sibling's `dataReads.ts` wrapper **fails silently** when a call to Data errors out — it logs a warning and falls back to the local Player tables. The runbook's earlier sections prove Data is reachable from a shell curl; they don't prove the sibling is *actually* routing reads through Data end-to-end.

**Check 1 — direct curl from the sibling environment:**

```bash
# On the sibling's Replit shell, with env already set:
curl -sS -H "X-API-Key: $UPSHIFT_DATA_API_KEY" \
  "$UPSHIFT_DATA_API_URL/api/clubs?page_size=1" | jq '.clubs[0].id'
# Expect: an integer club id from Data. 401 / empty / local UUID = read-swap broken.
```

**Check 2 — grep sibling logs for silent fallbacks over the last 24h:**

```bash
# Adjust path to wherever your sibling's logs land (Replit console tail, log file, Datadog, etc.)
grep -c '\[dataReads\] falling back to local' <sibling-log-source>
# Expect: 0. Any non-zero count means at least one call has silently degraded.
```

If check 2 returns > 0, the next step is NOT more runbook work — it's investigating why Data returned an error for that resource. Look for rate-limit 429s, auth 401s, or timeouts in this service's logs around the same timestamps.

---

## 8. Sanity check canonical-club linker + data counts

**Precondition — prove scrapers have actually run recently:**

Scrapers live on Replit now (the `artifacts/scraper/` directory was removed in the Phase 4+5 consolidation). If you run the linker cold on a freshly-pulled repo without §6's schedulers having fired at least once, the linker will be a no-op and the NULL-count queries below will show "zero NULLs" — not because linking succeeded, but because nothing has been scraped.

```sql
SELECT source_name, count(*) AS rows, max(completed_at) AS last_success
FROM scrape_run_logs
WHERE status = 'success' AND completed_at > now() - interval '7 days'
GROUP BY source_name
ORDER BY last_success DESC;
```

Zero rows = no scrapes have run in the last week. Fire one manually first (e.g., `bash scraper/scheduled/nightly_tier1.sh`) before proceeding with the linker checks below.

---

After any scraper runs, linker must backfill FKs:

```bash
python3 scraper/run.py --source link-canonical-clubs --dry-run --limit 100
# If dry-run numbers look right:
python3 scraper/run.py --source link-canonical-clubs
```

**Verify linker progress:**

```sql
SELECT count(*) FROM event_teams WHERE canonical_club_id IS NULL;
-- Should shrink each run.

SELECT count(*) FROM commitments WHERE club_id IS NULL;
-- Same pattern.

SELECT count(*) FROM hs_rosters WHERE school_name_raw IS NOT NULL;
-- School linker is a follow-up; expect raw-only today.
```

**Table counts + known-parser-gap labels** (run this *in the same session* as the linker check — avoids "why is commitments empty" panic at 11pm):

```sql
-- TDS commitments + YNT call-ups will show 0 rows today.
-- Known parser gaps — see §9.5 for the 1–5 line fixes needed.
SELECT 'commitments'       AS t, count(*) AS rows FROM commitments
UNION ALL SELECT 'ynt_call_ups',      count(*) FROM ynt_call_ups
UNION ALL SELECT 'odp_roster_entries', count(*) FROM odp_roster_entries
UNION ALL SELECT 'hs_rosters',         count(*) FROM hs_rosters
UNION ALL SELECT 'raw_html_archive',   count(*) FROM raw_html_archive;
```

The two zero-row tables are expected. Everything else should be non-zero after running their respective scrapers at least once.

---

## 9. What's working vs. what's still in flight

### ✅ Working now

- `--source replay-html --run-id <uuid>` replays archived HTML for extractors with pure-function parsers. ~14 extractors covered today: `gotsport_events`, `sincsports_events`, `totalglobalsports_events`, `maxpreps_rosters`, `ussoccer_ynt`, `topdrawer_commitments`, `tryouts_wordpress`, `gotsport_tryouts`, `gotsport_rosters`, `ncaa_rosters`, `naia_rosters`, `njcaa_rosters`, `college_coaches`, `youth_club_coaches`.
- Playwright path archives post-render DOM via `page.content()` — same `ARCHIVE_RAW_HTML_ENABLED` flag.
- Scheduler + `triggered_by` recorded in `scrape_run_logs`.

### 🚧 In flight / deferred

- TDS and MaxPreps at full volume — proxy pool is empty. `--limit 20` only. Add creds to `scraper/proxy_config.yaml` when ready (see #68 for format).
- 26 per-league extractors need a `(html, url)` entry-point refactor before replay covers them. Currently they fetch internally and can't be replayed from archive.

---

## 9.5. Known extractor gaps (produce 0 rows today)

Two Wave 2 extractors ship with verified-live seed URLs but parse 0 rows at runtime. Fixes are 1–5 line parser PRs, not Replit tasks — mentioning here so you don't panic when smokes return 0.

| Extractor | Issue | Fix |
|---|---|---|
| TDS commitments | `_HEADER_ALIASES` in `scraper/extractors/topdrawer_commitments.py` missing `"commitment" → "college_name_raw"`. TDS's actual column header is "Commitment". | Add one alias entry. |
| US Soccer YNT | Regex in `scraper/extractors/ussoccer_ynt.py` handles em-dash/pipe rosters but not the `Name (Club; Hometown)` inline format on ~2/3 of articles. | Extend regex. |

Both extractors work correctly against their fixtures — parsers just need a handful of new lines. Tracked as GitHub issues so this stops being institutional knowledge.

---

## 10. Rollback switches

If anything misbehaves, flip one of these in Secrets and restart the api-server / scraper:

| Flag | Off behavior |
|---|---|
| `API_KEY_AUTH_ENABLED=false` | Disables auth gate (pre-#62 behavior). |
| `API_RATE_LIMIT_ENABLED=false` | Disables rate limiting. |
| `API_DOCS_ENABLED=false` | Hides `/api/docs`. |
| `ARCHIVE_RAW_HTML_ENABLED=false` | Scraper stops archiving; DB and bucket untouched. |

**Schema changes are non-destructive:** the 5 new tables are empty on creation and unused by existing routes. `triggered_by` defaults to `'manual'` — existing logger calls unchanged. If something catastrophic happens at the schema level, restore from `/tmp/pre-wave2-schema.sql` (captured in §2).

---

## Post-run checklist

- [ ] `/tmp/pre-wave2-schema.sql` exists and is non-empty
- [ ] All 5 new tables exist and have row counts
- [ ] `scrape_run_logs.triggered_by` column exists
- [ ] API docs reachable at `/api/docs` (auth not required for docs page itself)
- [ ] Rate-limit headers present on authed requests (or parallel burst trips 429s)
- [ ] `raw_html_archive` has rows after first scrape with flag on
- [ ] Smoke-test bucket object cleaned up (§5 step 4 cleanup block)
- [ ] One manual scheduler-script run logged as `triggered_by='manual'`
- [ ] Next-morning: at least one `triggered_by='scheduler'` row in `scrape_run_logs`
- [ ] Sibling repo (`upshift-studio`) builds clean + tests pass
- [ ] One new API key minted, plaintext stored in sibling's `UPSHIFT_DATA_API_KEY`
- [ ] Linker pass run once; NULL FK counts verified shrinking
- [ ] WIP stash in sibling recovered or intentionally dropped
- [ ] At least one `replay-html` run succeeded end-to-end — validates §5 archival + §9 replay loop closes:

  ```bash
  run_id=$(psql "$DATABASE_URL" -tAc "SELECT run_id FROM scrape_run_logs WHERE status='success' ORDER BY completed_at DESC LIMIT 1")
  python3 scraper/run.py --source replay-html --run-id "$run_id" --limit 5
  ```

If any box stays unchecked after a full pass, file an issue tagged `post-wave-2` with the failing command + output.
