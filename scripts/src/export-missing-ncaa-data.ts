/**
 * Export "what's missing" CSVs for manual NCAA data entry.
 *
 * Usage (on Replit):
 *   pnpm --filter @workspace/scripts run export-missing-ncaa-data
 *   pnpm --filter @workspace/scripts run export-missing-ncaa-data -- --division D1
 *   pnpm --filter @workspace/scripts run export-missing-ncaa-data -- --division D2 --out /tmp/d2
 *
 * Produces three files in the chosen output directory:
 *
 *   missing_coaches_<division>_<YYYYMMDD>.csv
 *     One row per (college, 2025-26) where a roster was scraped but
 *     no coach row exists in college_coaches. Kid fills name/title/
 *     email/phone/source_url.
 *
 *   missing_urls_<division>_<YYYYMMDD>.csv
 *     One row per college with NULL soccer_program_url. Kid finds
 *     the correct /sports/.../roster URL and fills soccer_program_url.
 *
 *   missing_rosters_<division>_<YYYYMMDD>.csv
 *     One "template" row per (college, season) with no roster rows.
 *     Kid duplicates the row per player and fills player_name +
 *     position/year/hometown/etc. Each row is a single player.
 *
 * Companion: scripts/src/import-manual-ncaa-data.ts reads these CSVs
 * back (any of the three shapes; auto-detected by columns) and writes
 * to the DB via the existing Python-side writers' SQL contracts.
 *
 * Deliberately a READ-ONLY script — never mutates the DB. Safe to run
 * at any time, any number of times. Re-running regenerates fresh
 * CSVs with the current gap state.
 */
import { writeFileSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { pool, db } from "@workspace/db";
import { sql } from "drizzle-orm";

type RawRow = Record<string, string | number | null>;

// ---------------------------------------------------------------------------
// CLI args — minimal bespoke parser to stay consistent with other scripts
// ---------------------------------------------------------------------------

interface Args {
  division: string | null; // null = all divisions
  out: string;
  currentSeason: string;
  backfillSeasons: number;
}

function parseArgs(argv: string[]): Args {
  const args: Args = {
    division: null,
    out: "/tmp",
    currentSeason: deriveCurrentAcademicYear(),
    backfillSeasons: 3,
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    switch (a) {
      case "--division":
        args.division = (argv[++i] ?? "").toUpperCase();
        break;
      case "--out":
        args.out = argv[++i] ?? args.out;
        break;
      case "--current-season":
        args.currentSeason = argv[++i] ?? args.currentSeason;
        break;
      case "--backfill-seasons":
        args.backfillSeasons = Number(argv[++i] ?? args.backfillSeasons);
        break;
      case "-h":
      case "--help":
        printHelp();
        process.exit(0);
    }
  }
  if (args.division !== null) {
    const valid = ["D1", "D2", "D3", "NAIA", "NJCAA"];
    if (!valid.includes(args.division)) {
      console.error(`--division must be one of ${valid.join(", ")} (got ${args.division})`);
      process.exit(2);
    }
  }
  if (!Number.isInteger(args.backfillSeasons) || args.backfillSeasons < 0) {
    console.error(`--backfill-seasons must be a non-negative integer`);
    process.exit(2);
  }
  return args;
}

function printHelp() {
  console.error(`
Export "what's missing" CSVs for manual NCAA data entry.

Flags:
  --division D1|D2|D3|NAIA|NJCAA   Only this division (default: all divisions)
  --out <path>                      Output directory (default: /tmp)
  --current-season <YYYY-YY>        Override current season (default: derived from today's date)
  --backfill-seasons <N>            Include N prior seasons in coach + roster gap exports (default: 3)
  -h, --help                        Print this help
`.trim());
}

// ---------------------------------------------------------------------------
// Date helpers (mirror Python scrape_college_rosters.current_academic_year)
// ---------------------------------------------------------------------------

export function deriveCurrentAcademicYear(date: Date = new Date()): string {
  const y = date.getUTCFullYear();
  const m = date.getUTCMonth() + 1;
  if (m >= 8) {
    return `${y}-${String(y + 1).slice(-2)}`;
  }
  return `${y - 1}-${String(y).slice(-2)}`;
}

export function priorSeasons(current: string, n: number): string[] {
  const [startStr] = current.split("-");
  const start = Number(startStr);
  const out: string[] = [];
  for (let i = 0; i <= n; i++) {
    const s = start - i;
    out.push(`${s}-${String(s + 1).slice(-2)}`);
  }
  return out;
}

// ---------------------------------------------------------------------------
// CSV writer — minimal; escapes double-quotes + wraps any value containing
// comma, quote, or newline in double quotes.
// ---------------------------------------------------------------------------

export function escapeCsvCell(value: string | number | null | undefined): string {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (/[",\n\r]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

export function rowsToCsv(columns: string[], rows: RawRow[]): string {
  const header = columns.map(escapeCsvCell).join(",");
  const body = rows.map((r) => columns.map((c) => escapeCsvCell(r[c])).join(","));
  return [header, ...body].join("\n") + "\n";
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

const COACH_COLUMNS = [
  "college_id",
  "college_name",
  "division",
  "gender_program",
  "website",
  "soccer_program_url",
  "academic_year",
  "name",
  "title",
  "email",
  "phone",
  "source_url",
  "is_head_coach",
];

async function fetchMissingCoaches(
  division: string | null,
  seasons: string[],
): Promise<RawRow[]> {
  // One row per (college_id, academic_year) where:
  //   - the college has a roster row for that season (we only care
  //     about seasons we actually have data for)
  //   - AND there is no college_coach_tenures row for that season
  // This catches both "no current coach" and "historical season missing
  // coach" in one query. Filtered by division if requested.
  //
  // For the current season, we also want college_coaches gaps — but
  // those are a strict subset of college_coach_tenures gaps (the caller
  // only writes college_coaches when the season is current), so the
  // tenures gap is the right query.
  const divisionPredicate = division ? sql`AND c.division = ${division}` : sql``;
  const seasonList = seasons.length
    ? sql.raw(seasons.map((s) => `'${s.replace(/'/g, "''")}'`).join(", "))
    : sql.raw("NULL");
  const res = await db.execute(sql`
    SELECT
      c.id AS college_id,
      c.name AS college_name,
      c.division AS division,
      c.gender_program AS gender_program,
      c.website AS website,
      c.soccer_program_url AS soccer_program_url,
      rh.academic_year AS academic_year
    FROM colleges c
    JOIN (
      SELECT DISTINCT college_id, academic_year
      FROM college_roster_history
      WHERE academic_year IN (${seasonList})
    ) rh ON rh.college_id = c.id
    WHERE NOT EXISTS (
      SELECT 1
      FROM college_coach_tenures t
      WHERE t.college_id = c.id
        AND t.academic_year = rh.academic_year
    )
    ${divisionPredicate}
    ORDER BY c.division, c.name, c.gender_program, rh.academic_year DESC
  `);
  return (res.rows as RawRow[]).map((r) => ({
    college_id: r.college_id,
    college_name: r.college_name,
    division: r.division,
    gender_program: r.gender_program,
    website: r.website,
    soccer_program_url: r.soccer_program_url,
    academic_year: r.academic_year,
    // Blank cells the kid fills in
    name: "",
    title: "",
    email: "",
    phone: "",
    source_url: "",
    is_head_coach: "true",
  }));
}

const URL_COLUMNS = [
  "college_id",
  "college_name",
  "division",
  "gender_program",
  "website",
  "soccer_program_url",
];

async function fetchMissingUrls(division: string | null): Promise<RawRow[]> {
  const divisionPredicate = division ? sql`AND c.division = ${division}` : sql``;
  const res = await db.execute(sql`
    SELECT
      c.id AS college_id,
      c.name AS college_name,
      c.division AS division,
      c.gender_program AS gender_program,
      c.website AS website
    FROM colleges c
    WHERE c.soccer_program_url IS NULL
      AND c.website IS NOT NULL
      ${divisionPredicate}
    ORDER BY c.division, c.name, c.gender_program
  `);
  return (res.rows as RawRow[]).map((r) => ({
    college_id: r.college_id,
    college_name: r.college_name,
    division: r.division,
    gender_program: r.gender_program,
    website: r.website,
    soccer_program_url: "",
  }));
}

const ROSTER_COLUMNS = [
  "college_id",
  "college_name",
  "division",
  "gender_program",
  "academic_year",
  "soccer_program_url",
  "player_name",
  "position",
  "year",
  "hometown",
  "prev_club",
  "jersey_number",
];

async function fetchMissingRosterSeasons(
  division: string | null,
  seasons: string[],
): Promise<RawRow[]> {
  // One row per (college_id, academic_year) we'd LIKE to have a roster
  // for but don't. "Like to have" = the college has SOME roster in any
  // of the target seasons (proves the program exists + we have a URL
  // that works) AND is missing this specific season.
  //
  // Kid then duplicates each template row per player they find.
  const divisionPredicate = division ? sql`AND c.division = ${division}` : sql``;
  const seasonList = seasons.length
    ? sql.raw(seasons.map((s) => `'${s.replace(/'/g, "''")}'`).join(", "))
    : sql.raw("NULL");

  // For each college that has ANY roster and each target season,
  // emit a template row if that (college, season) pair has no roster
  // rows.
  const res = await db.execute(sql`
    WITH target_seasons AS (
      SELECT unnest(ARRAY[${seasonList}]::text[]) AS academic_year
    ),
    colleges_with_any_roster AS (
      SELECT DISTINCT c.id AS college_id
      FROM colleges c
      JOIN college_roster_history rh ON rh.college_id = c.id
      WHERE rh.academic_year IN (${seasonList})
        ${divisionPredicate}
    )
    SELECT
      c.id AS college_id,
      c.name AS college_name,
      c.division AS division,
      c.gender_program AS gender_program,
      c.soccer_program_url AS soccer_program_url,
      ts.academic_year AS academic_year
    FROM colleges c
    CROSS JOIN target_seasons ts
    WHERE c.id IN (SELECT college_id FROM colleges_with_any_roster)
      AND NOT EXISTS (
        SELECT 1 FROM college_roster_history rh
        WHERE rh.college_id = c.id
          AND rh.academic_year = ts.academic_year
      )
      ${divisionPredicate}
    ORDER BY c.division, c.name, c.gender_program, ts.academic_year DESC
  `);
  return (res.rows as RawRow[]).map((r) => ({
    college_id: r.college_id,
    college_name: r.college_name,
    division: r.division,
    gender_program: r.gender_program,
    academic_year: r.academic_year,
    soccer_program_url: r.soccer_program_url,
    // Blank cells the kid fills in — one row per player they find
    player_name: "",
    position: "",
    year: "",
    hometown: "",
    prev_club: "",
    jersey_number: "",
  }));
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function formatDate(date: Date = new Date()): string {
  return date.toISOString().slice(0, 10).replace(/-/g, "");
}

function divisionSuffix(division: string | null): string {
  return division ? `_${division.toLowerCase()}` : "_all";
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  mkdirSync(args.out, { recursive: true });

  const seasons = priorSeasons(args.currentSeason, args.backfillSeasons);
  const datestamp = formatDate();
  const suffix = divisionSuffix(args.division);

  console.log(`[export-missing-ncaa-data] division=${args.division ?? "ALL"}`);
  console.log(`[export-missing-ncaa-data] seasons=${JSON.stringify(seasons)}`);
  console.log(`[export-missing-ncaa-data] out=${args.out}`);

  const coachRows = await fetchMissingCoaches(args.division, seasons);
  const coachPath = resolve(args.out, `missing_coaches${suffix}_${datestamp}.csv`);
  writeFileSync(coachPath, rowsToCsv(COACH_COLUMNS, coachRows), "utf-8");
  console.log(`  → coaches: ${coachRows.length} rows → ${coachPath}`);

  const urlRows = await fetchMissingUrls(args.division);
  const urlPath = resolve(args.out, `missing_urls${suffix}_${datestamp}.csv`);
  writeFileSync(urlPath, rowsToCsv(URL_COLUMNS, urlRows), "utf-8");
  console.log(`  → urls: ${urlRows.length} rows → ${urlPath}`);

  const rosterRows = await fetchMissingRosterSeasons(args.division, seasons);
  const rosterPath = resolve(args.out, `missing_rosters${suffix}_${datestamp}.csv`);
  writeFileSync(rosterPath, rowsToCsv(ROSTER_COLUMNS, rosterRows), "utf-8");
  console.log(`  → rosters: ${rosterRows.length} template rows → ${rosterPath}`);
  console.log(
    `[export-missing-ncaa-data] note: roster CSV has 1 row per (college, season) — `
    + `kid duplicates each row per player found (~25 per template).`,
  );

  await pool.end();
}

const isDirectRun =
  import.meta.url === `file://${process.argv[1]}` ||
  process.argv[1]?.endsWith("export-missing-ncaa-data.ts");
if (isDirectRun) {
  main().catch((err) => {
    console.error("[export-missing-ncaa-data] fatal:", err);
    process.exit(1);
  });
}
