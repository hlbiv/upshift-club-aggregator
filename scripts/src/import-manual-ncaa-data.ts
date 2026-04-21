/**
 * Import manually-entered NCAA data from a CSV (coaches, URLs, or
 * rosters) back into the DB.
 *
 * Usage (on Replit):
 *   pnpm --filter @workspace/scripts run import-manual-ncaa-data -- --input /tmp/missing_coaches_filled.csv
 *   pnpm --filter @workspace/scripts run import-manual-ncaa-data -- --input /tmp/missing_urls_filled.csv --dry-run
 *
 * Auto-detects CSV type by inspecting the header row:
 *
 *   coaches    — presence of ``name`` + ``academic_year`` + ``title`` columns
 *   urls       — presence of ``soccer_program_url`` + NO ``academic_year``
 *   rosters    — presence of ``player_name`` column
 *
 * Writes through the same natural-key upserts the scraper uses:
 *
 *   coaches  → college_coaches (when academic_year == current)
 *              + college_coach_tenures (always)
 *   urls     → direct UPDATE on colleges.soccer_program_url
 *   rosters  → college_roster_history
 *
 * All writes are idempotent via ON CONFLICT. Re-running with the same
 * CSV is safe. The ``--dry-run`` flag parses + validates every row
 * without issuing any DB writes — useful for sanity-checking the kid's
 * work before committing.
 *
 * Row-level validation: blank rows skip silently. Rows missing a
 * required field (college_id, the type-specific identifier) are
 * printed to stderr and skipped; the rest of the file continues
 * processing. End-of-run summary: inserted / updated / skipped /
 * errored.
 */
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parse } from "csv-parse/sync";
import { pool, db } from "@workspace/db";
import { sql } from "drizzle-orm";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

interface Args {
  input: string | null;
  dryRun: boolean;
  currentSeason: string;
}

function deriveCurrentAcademicYear(date: Date = new Date()): string {
  const y = date.getUTCFullYear();
  const m = date.getUTCMonth() + 1;
  if (m >= 8) return `${y}-${String(y + 1).slice(-2)}`;
  return `${y - 1}-${String(y).slice(-2)}`;
}

function parseArgs(argv: string[]): Args {
  const args: Args = {
    input: null,
    dryRun: false,
    currentSeason: deriveCurrentAcademicYear(),
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    switch (a) {
      case "--input":
        args.input = argv[++i] ?? null;
        break;
      case "--dry-run":
        args.dryRun = true;
        break;
      case "--current-season":
        args.currentSeason = argv[++i] ?? args.currentSeason;
        break;
      case "-h":
      case "--help":
        printHelp();
        process.exit(0);
    }
  }
  if (!args.input) {
    console.error("--input is required. Use --help for usage.");
    process.exit(2);
  }
  return args;
}

function printHelp() {
  console.error(`
Import manually-entered NCAA data from a CSV (auto-detected type).

Flags:
  --input <path>             CSV to read (required)
  --dry-run                  Validate + parse but skip DB writes
  --current-season <YYYY-YY> Override current season (default: derived from today's date)
  -h, --help                 Print this help
`.trim());
}

// ---------------------------------------------------------------------------
// Row-shape detection
// ---------------------------------------------------------------------------

type CsvType = "coaches" | "urls" | "rosters";

export function detectCsvType(headers: string[]): CsvType {
  const set = new Set(headers.map((h) => h.toLowerCase().trim()));
  if (set.has("player_name")) return "rosters";
  if (set.has("name") && set.has("academic_year") && set.has("title")) return "coaches";
  if (set.has("soccer_program_url") && !set.has("academic_year")) return "urls";
  throw new Error(
    `Cannot detect CSV type from headers: ${headers.join(", ")}. `
    + `Expected one of: coaches | urls | rosters (see export-missing-ncaa-data.ts).`,
  );
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

const ACADEMIC_YEAR_RE = /^\d{4}-\d{2}$/;
const YEAR_ENUM = new Set([
  "freshman",
  "sophomore",
  "junior",
  "senior",
  "grad",
]);

function toInt(val: unknown): number | null {
  if (val === null || val === undefined || val === "") return null;
  const n = Number(String(val).trim());
  return Number.isInteger(n) && n > 0 ? n : null;
}

function normalizeString(val: unknown): string | null {
  if (val === null || val === undefined) return null;
  const s = String(val).trim();
  return s === "" ? null : s;
}

function normalizeBool(val: unknown, defaultValue = false): boolean {
  if (val === null || val === undefined || val === "") return defaultValue;
  const s = String(val).trim().toLowerCase();
  return s === "true" || s === "t" || s === "1" || s === "yes" || s === "y";
}

function normalizeEmail(val: unknown): string | null {
  const s = normalizeString(val);
  return s ? s.toLowerCase() : null;
}

// ---------------------------------------------------------------------------
// Coach import
// ---------------------------------------------------------------------------

interface CoachRow {
  college_id: number;
  academic_year: string;
  name: string;
  title: string | null;
  email: string | null;
  phone: string | null;
  source_url: string | null;
  is_head_coach: boolean;
}

export function validateCoachRow(raw: Record<string, string>, lineNo: number): CoachRow | { error: string } {
  const collegeId = toInt(raw.college_id);
  const academicYear = normalizeString(raw.academic_year);
  const name = normalizeString(raw.name);

  if (collegeId === null) return { error: `line ${lineNo}: missing/invalid college_id` };
  if (!academicYear || !ACADEMIC_YEAR_RE.test(academicYear)) {
    return { error: `line ${lineNo}: academic_year must match YYYY-YY (got ${academicYear ?? "null"})` };
  }
  if (!name) return { error: `line ${lineNo}: missing name` };

  return {
    college_id: collegeId,
    academic_year: academicYear,
    name,
    title: normalizeString(raw.title),
    email: normalizeEmail(raw.email),
    phone: normalizeString(raw.phone),
    source_url: normalizeString(raw.source_url),
    is_head_coach: normalizeBool(raw.is_head_coach, true),
  };
}

async function importCoaches(
  rows: CoachRow[],
  currentSeason: string,
  dryRun: boolean,
): Promise<{ tenures: Counts; directory: Counts }> {
  const tenures: Counts = { inserted: 0, updated: 0, skipped: 0 };
  const directory: Counts = { inserted: 0, updated: 0, skipped: 0 };

  for (const row of rows) {
    if (dryRun) continue;

    // Tenure write — always (historical + current)
    try {
      const res = await db.execute(sql`
        INSERT INTO college_coach_tenures (
          college_id, name, title, academic_year,
          is_head_coach, source_url, scraped_at
        )
        VALUES (
          ${row.college_id}, ${row.name}, ${row.title}, ${row.academic_year},
          ${row.is_head_coach}, ${row.source_url}, now()
        )
        ON CONFLICT ON CONSTRAINT college_coach_tenures_college_name_title_year_uq
        DO UPDATE SET
          is_head_coach = EXCLUDED.is_head_coach,
          source_url    = EXCLUDED.source_url,
          scraped_at    = now()
        RETURNING (xmax = 0) AS inserted
      `);
      const inserted = Boolean((res.rows[0] as { inserted: boolean })?.inserted);
      if (inserted) tenures.inserted += 1;
      else tenures.updated += 1;
    } catch (err) {
      console.error(`  tenure upsert failed for college_id=${row.college_id} ${row.academic_year}: ${err}`);
      tenures.skipped += 1;
      continue;
    }

    // Current-directory write — only when the row is for the current season
    if (row.academic_year === currentSeason) {
      try {
        const res = await db.execute(sql`
          INSERT INTO college_coaches (
            college_id, name, title, email, phone,
            is_head_coach, source, source_url,
            scraped_at, confidence,
            first_seen_at, last_seen_at
          )
          VALUES (
            ${row.college_id}, ${row.name}, ${row.title}, ${row.email}, ${row.phone},
            ${row.is_head_coach}, 'manual_entry', ${row.source_url},
            now(), 1.0,
            now(), now()
          )
          ON CONFLICT ON CONSTRAINT college_coaches_college_name_title_uq
          DO UPDATE SET
            email         = COALESCE(EXCLUDED.email, college_coaches.email),
            phone         = COALESCE(EXCLUDED.phone, college_coaches.phone),
            is_head_coach = EXCLUDED.is_head_coach,
            source        = EXCLUDED.source,
            source_url    = EXCLUDED.source_url,
            scraped_at    = now(),
            confidence    = EXCLUDED.confidence,
            last_seen_at  = now()
          RETURNING (xmax = 0) AS inserted
        `);
        const inserted = Boolean((res.rows[0] as { inserted: boolean })?.inserted);
        if (inserted) directory.inserted += 1;
        else directory.updated += 1;
      } catch (err) {
        console.error(`  directory upsert failed for college_id=${row.college_id}: ${err}`);
        directory.skipped += 1;
      }
    }
  }

  return { tenures, directory };
}

// ---------------------------------------------------------------------------
// URL import
// ---------------------------------------------------------------------------

interface UrlRow {
  college_id: number;
  soccer_program_url: string;
}

export function validateUrlRow(raw: Record<string, string>, lineNo: number): UrlRow | { error: string } {
  const collegeId = toInt(raw.college_id);
  const url = normalizeString(raw.soccer_program_url);
  if (collegeId === null) return { error: `line ${lineNo}: missing/invalid college_id` };
  if (!url) return { error: `line ${lineNo}: missing soccer_program_url` };
  if (!/^https?:\/\//i.test(url)) {
    return { error: `line ${lineNo}: soccer_program_url must start with http:// or https:// (got ${url})` };
  }
  return { college_id: collegeId, soccer_program_url: url };
}

async function importUrls(rows: UrlRow[], dryRun: boolean): Promise<Counts> {
  const counts: Counts = { inserted: 0, updated: 0, skipped: 0 };
  for (const row of rows) {
    if (dryRun) continue;
    try {
      const res = await db.execute(sql`
        UPDATE colleges
        SET soccer_program_url = ${row.soccer_program_url},
            last_scraped_at = now()
        WHERE id = ${row.college_id}
        RETURNING id
      `);
      if (res.rows.length > 0) counts.updated += 1;
      else {
        console.error(`  no college row for college_id=${row.college_id}`);
        counts.skipped += 1;
      }
    } catch (err) {
      console.error(`  url update failed for college_id=${row.college_id}: ${err}`);
      counts.skipped += 1;
    }
  }
  return counts;
}

// ---------------------------------------------------------------------------
// Roster import
// ---------------------------------------------------------------------------

interface RosterRow {
  college_id: number;
  academic_year: string;
  player_name: string;
  position: string | null;
  year: string | null;
  hometown: string | null;
  prev_club: string | null;
  jersey_number: string | null;
}

export function validateRosterRow(raw: Record<string, string>, lineNo: number): RosterRow | { error: string } {
  const collegeId = toInt(raw.college_id);
  const academicYear = normalizeString(raw.academic_year);
  const playerName = normalizeString(raw.player_name);

  // Template rows (left blank by the kid) are skipped silently —
  // they're intentional in the export format.
  if (collegeId === null && academicYear === null && playerName === null) {
    return { error: "__BLANK__" };
  }

  if (collegeId === null) return { error: `line ${lineNo}: missing/invalid college_id` };
  if (!academicYear || !ACADEMIC_YEAR_RE.test(academicYear)) {
    return { error: `line ${lineNo}: academic_year must match YYYY-YY (got ${academicYear ?? "null"})` };
  }
  if (!playerName) return { error: `line ${lineNo}: missing player_name` };

  const yearRaw = normalizeString(raw.year);
  const year = yearRaw && YEAR_ENUM.has(yearRaw.toLowerCase()) ? yearRaw.toLowerCase() : null;

  return {
    college_id: collegeId,
    academic_year: academicYear,
    player_name: playerName,
    position: normalizeString(raw.position),
    year,
    hometown: normalizeString(raw.hometown),
    prev_club: normalizeString(raw.prev_club),
    jersey_number: normalizeString(raw.jersey_number),
  };
}

async function importRosters(rows: RosterRow[], dryRun: boolean): Promise<Counts> {
  const counts: Counts = { inserted: 0, updated: 0, skipped: 0 };
  for (const row of rows) {
    if (dryRun) continue;
    try {
      const res = await db.execute(sql`
        INSERT INTO college_roster_history (
          college_id, player_name, position, year,
          academic_year, hometown, prev_club, jersey_number,
          scraped_at
        )
        VALUES (
          ${row.college_id}, ${row.player_name}, ${row.position}, ${row.year},
          ${row.academic_year}, ${row.hometown}, ${row.prev_club}, ${row.jersey_number},
          now()
        )
        ON CONFLICT ON CONSTRAINT college_roster_history_college_player_year_uq
        DO UPDATE SET
          position      = COALESCE(EXCLUDED.position, college_roster_history.position),
          year          = COALESCE(EXCLUDED.year, college_roster_history.year),
          hometown      = COALESCE(EXCLUDED.hometown, college_roster_history.hometown),
          prev_club     = COALESCE(EXCLUDED.prev_club, college_roster_history.prev_club),
          jersey_number = COALESCE(EXCLUDED.jersey_number, college_roster_history.jersey_number),
          scraped_at    = now()
        RETURNING (xmax = 0) AS inserted
      `);
      const inserted = Boolean((res.rows[0] as { inserted: boolean })?.inserted);
      if (inserted) counts.inserted += 1;
      else counts.updated += 1;
    } catch (err) {
      console.error(`  roster upsert failed for college_id=${row.college_id} ${row.player_name}: ${err}`);
      counts.skipped += 1;
    }
  }
  return counts;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

interface Counts {
  inserted: number;
  updated: number;
  skipped: number;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const content = readFileSync(resolve(args.input!), "utf-8");
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true,
    relax_column_count: true,
  }) as Record<string, string>[];
  if (records.length === 0) {
    console.error("[import-manual-ncaa-data] CSV has no data rows.");
    await pool.end();
    return;
  }
  const headers = Object.keys(records[0]);
  const type = detectCsvType(headers);

  console.log(`[import-manual-ncaa-data] type=${type} rows=${records.length} dry_run=${args.dryRun}`);

  let validationErrors = 0;

  if (type === "coaches") {
    const valid: CoachRow[] = [];
    records.forEach((raw, idx) => {
      const result = validateCoachRow(raw, idx + 2); // +2 = header line + 1-indexed
      if ("error" in result) {
        if (result.error === "__BLANK__") return;
        console.error(result.error);
        validationErrors += 1;
      } else {
        valid.push(result);
      }
    });
    console.log(`  validated: ${valid.length} / ${records.length} rows (${validationErrors} errors)`);
    const { tenures, directory } = await importCoaches(valid, args.currentSeason, args.dryRun);
    console.log(
      `  coach_tenures: inserted=${tenures.inserted} updated=${tenures.updated} skipped=${tenures.skipped}`,
    );
    console.log(
      `  college_coaches (current season only): inserted=${directory.inserted} updated=${directory.updated} skipped=${directory.skipped}`,
    );
  } else if (type === "urls") {
    const valid: UrlRow[] = [];
    records.forEach((raw, idx) => {
      const result = validateUrlRow(raw, idx + 2);
      if ("error" in result) {
        if (result.error === "__BLANK__") return;
        console.error(result.error);
        validationErrors += 1;
      } else {
        valid.push(result);
      }
    });
    console.log(`  validated: ${valid.length} / ${records.length} rows (${validationErrors} errors)`);
    const counts = await importUrls(valid, args.dryRun);
    console.log(
      `  colleges.soccer_program_url: updated=${counts.updated} skipped=${counts.skipped}`,
    );
  } else {
    // rosters
    const valid: RosterRow[] = [];
    records.forEach((raw, idx) => {
      const result = validateRosterRow(raw, idx + 2);
      if ("error" in result) {
        if (result.error === "__BLANK__") return; // intentional template rows
        console.error(result.error);
        validationErrors += 1;
      } else {
        valid.push(result);
      }
    });
    console.log(`  validated: ${valid.length} / ${records.length} rows (${validationErrors} errors)`);
    const counts = await importRosters(valid, args.dryRun);
    console.log(
      `  college_roster_history: inserted=${counts.inserted} updated=${counts.updated} skipped=${counts.skipped}`,
    );
  }

  if (args.dryRun) {
    console.log("[import-manual-ncaa-data] DRY RUN — no DB writes.");
  }
  await pool.end();
}

const isDirectRun =
  import.meta.url === `file://${process.argv[1]}` ||
  process.argv[1]?.endsWith("import-manual-ncaa-data.ts");
if (isDirectRun) {
  main().catch((err) => {
    console.error("[import-manual-ncaa-data] fatal:", err);
    process.exit(1);
  });
}
