# Manual NCAA data entry — operator + kid runbook

This is the workflow for filling the gaps left by the automated scraper:

1. **Missing current-season head coaches** (`college_coaches`) — programs where the roster scraped but the coach couldn't be extracted (~11% of D1 programs as of the first replay).
2. **Missing historical coaches** (`college_coach_tenures`) — per-season coach entries we don't have from the backfill.
3. **Missing `soccer_program_url`** (`colleges.soccer_program_url`) — programs on non-SIDEARM athletics sites where the URL resolver couldn't find the roster page automatically.
4. **Missing rosters** (`college_roster_history`) — per-season roster rows where the scraper couldn't parse the page or the page didn't exist.

Two scripts do the heavy lifting; the kid never touches SQL or runs scripts directly.

---

## Operator side (you)

You need Replit shell access. Run these two commands.

### 1. Export what's missing

```bash
cd ~/workspace
pnpm --filter @workspace/scripts run export-missing-ncaa-data
```

Flags (all optional):
- `--division D1` (or D2, D3, NAIA, NJCAA) — only dump this division's gaps. Default: all divisions.
- `--out /tmp/ncaa-manual-entry` — output directory. Default: `/tmp`.
- `--backfill-seasons 3` — how many prior seasons to include in coach + roster gap exports. Default: 3.

Produces three CSVs named `missing_*_<division>_<YYYYMMDD>.csv`:

| File | Rows | What the kid fills |
|---|---|---|
| `missing_coaches_*.csv` | One per (college, season) with no coach data | `name`, `title`, `email`, `phone`, `source_url` |
| `missing_urls_*.csv` | One per college with NULL `soccer_program_url` | `soccer_program_url` |
| `missing_rosters_*.csv` | One **template** per (college, season) with no roster rows | Kid duplicates row per player + fills `player_name`, `position`, `year`, `hometown`, `prev_club`, `jersey_number` |

The export is **read-only** — running it never changes the DB. Safe to re-run at any point to get a fresh picture of what's still missing.

### 2. Upload CSVs to Google Drive

Share a folder with the kid. Drop each CSV in there.

For Google Sheets, open each CSV and save as a Sheet (**File → Save as Google Sheets**). Kid fills the Sheet. When done, **File → Download → CSV** back out.

### 3. Import filled CSVs

When the kid hands back a filled CSV:

```bash
# Always dry-run first — validates every row without touching the DB
pnpm --filter @workspace/scripts run import-manual-ncaa-data -- --input /path/to/filled.csv --dry-run

# When the dry-run looks right, commit for real
pnpm --filter @workspace/scripts run import-manual-ncaa-data -- --input /path/to/filled.csv
```

Auto-detects CSV type from the header row — you don't need to tell it which kind of data it is.

Output example:
```
[import-manual-ncaa-data] type=coaches rows=50 dry_run=false
  validated: 47 / 50 rows (3 errors)
  coach_tenures: inserted=40 updated=7 skipped=0
  college_coaches (current season only): inserted=12 updated=0 skipped=0
```

Validation errors print to stderr with a line number. Fix the offending cells in the Sheet, re-download, re-run. Idempotent — previously-imported rows just update `scraped_at`, they don't double-write.

---

## Kid side (the person doing data entry)

You'll get three CSVs or Google Sheets from the operator. Each one has a different job.

### Coaches sheet

For each row:
1. Open the school's roster page (column `website` in the sheet shows the athletics site).
2. Find the head coach — look for "Coaches" / "Coaching Staff" / "Staff Directory" link.
3. Copy into the blank columns:
   - `name` — full name, e.g. "Jay Boyd"
   - `title` — the literal title on the page, e.g. "Head Coach" or "Men's Soccer Head Coach"
   - `email` — leave blank if you can't find one; that's OK
   - `phone` — same, optional
   - `source_url` — the page URL where you found the info
4. `is_head_coach` is pre-set to `true`; don't change it.
5. The `academic_year` column tells you which season — **each row is one season**. Some coaches will appear in multiple rows (e.g. Jane Doe was head coach 2022-23, 2023-24, 2024-25, 2025-26 — that's 4 rows).

**Don't guess.** If the title isn't exactly "Head Coach" (e.g. "Interim Head Coach", "Director of Soccer"), enter what the page says and flag it to the operator — the scraper's filter may or may not let it through.

### URLs sheet

For each row:
1. Open the school's `website`.
2. Navigate to the men's or women's soccer roster page (the gender is in the `gender_program` column).
3. Copy the full URL of the roster page into `soccer_program_url`. Must start with `http://` or `https://`.
4. Skip rows where the program doesn't exist — operator will clean those up separately.

**NAIA rows have an empty `website` cell.** The NAIA seed source doesn't carry each school's athletics-site URL, so you need to find it yourself:

1. Google the school + "athletics" — e.g., `"Saint Leo athletics"`.
2. The top hit is almost always the athletics homepage. Open it.
3. Paste the athletics homepage into the `website` cell (must start with `http://` or `https://`).
4. Navigate to the men's or women's soccer roster page from there.
5. Paste the roster URL into `soccer_program_url` as usual.

If you fill `website` but can't find a roster page (rare — usually means the program was discontinued), leave `soccer_program_url` blank. The importer will accept `website`-only rows. Just flag the school to the operator so they can investigate.

### Rosters sheet (the big one)

Each row in the export is a **blank template** for a (college, season) that's missing its roster. You need to duplicate it once per player.

For each template row:
1. Open `soccer_program_url` in the row (should work — if it 404s, skip and flag).
2. Look at each player on the page.
3. In the Sheet, duplicate the template row as many times as there are players (~25 usually).
4. In each copy, fill:
   - `player_name` — full name
   - `position` — "GK", "M", "F", "D", "Forward", etc. whatever the page says
   - `year` — one of `freshman`, `sophomore`, `junior`, `senior`, `grad`. If the page says "Redshirt Junior" or "Graduate Student", use `junior` or `grad` respectively. If unsure, leave blank.
   - `hometown` — e.g. "Boston, MA" or "Madrid, Spain"
   - `prev_club` — previous high school or club team
   - `jersey_number` — just the number, no #
5. Leave `college_id`, `college_name`, `division`, `gender_program`, `academic_year`, `soccer_program_url` — those are pre-filled and identify the program/season.

**Don't worry about perfect data.** Blank cells are fine for optional fields. The importer uses `COALESCE` — any field you leave blank keeps whatever was there previously (usually NULL, sometimes a partial import from a prior run).

**Flag anything weird to the operator before continuing a section.** Examples:
- A roster page that shows a different school's players
- A season dropdown that goes back only to 2020
- A program that was just discontinued (no current roster)

---

## Common gotchas

- **Duplicate players.** Same-name players on the same roster won't double-insert (natural key is `(college_id, player_name, academic_year)`). But if you have "Jack Smith" and "Jack Smith Jr." on the same roster, enter both literally — they'll de-dup correctly.
- **Rosters that span multiple pages.** Some sites paginate 25-at-a-time. Don't miss page 2.
- **Interim coaches.** If a program has "Interim Head Coach" with no non-interim entry, enter that title verbatim. The operator's importer may filter on strict "Head Coach" — clarify which path they want.
- **Extra columns added by Google Sheets.** If Sheets auto-adds a column (like row numbers), the CSV export will include them. Delete extra columns before downloading, or the importer's error messages will help you spot the issue.

---

## Re-running

Everything is idempotent. If you run the export, fill, and import once:

1. `export-missing-ncaa-data` shows the new set of gaps (smaller, since you've filled some).
2. If the kid corrects their earlier work (e.g. "I had the wrong email for Coach Smith"), re-export won't include that program anymore — but they can just edit the original CSV and re-import. The importer's `ON CONFLICT UPDATE` handles this.

Running the import twice with identical input is harmless — the second pass reports `updated=<all the rows>, inserted=0`.

---

## Tracking progress

Over time, these counts should trend toward zero per division:

```bash
psql "$DATABASE_URL" -c "
  SELECT 'missing_coaches_2025_26' AS metric, count(*) FROM (
    SELECT rh.college_id FROM college_roster_history rh
    WHERE rh.academic_year = '2025-26'
    AND NOT EXISTS (SELECT 1 FROM college_coaches cc WHERE cc.college_id = rh.college_id)
  ) x
  UNION ALL
  SELECT 'missing_urls', count(*) FROM colleges
    WHERE division IN ('D1','D2','D3','NAIA') AND soccer_program_url IS NULL AND website IS NOT NULL;
"
```

Re-run the export+import cycle weekly or whenever new gaps show up (e.g. after a new season's enumeration).
