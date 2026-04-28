/**
 * Import USSF coaching licenses from a CSV export.
 *
 * Usage:
 *   pnpm --filter @workspace/scripts run import-coach-licenses -- --file /path/to/ussf-licenses.csv --dry-run
 *   pnpm --filter @workspace/scripts run import-coach-licenses -- --file /path/to/ussf-licenses.csv
 *
 * Expected CSV columns (case-insensitive, extra columns ignored):
 *   coach_name   — full name as it appears on the USSF directory
 *   license_tier — one of: grassroots_online, grassroots_in_person, D, C, B, A, Pro
 *   state        — 2-letter US state abbreviation (optional)
 *   issue_date   — ISO date or MM/DD/YYYY (optional)
 *   expires_at   — ISO date or MM/DD/YYYY (optional)
 *   source_url   — URL of the USSF directory page (optional)
 *
 * Matching strategy:
 *   1. Normalize coach_name (lowercase, collapse whitespace).
 *   2. Exact match against coaches.display_name (normalized). Takes the first
 *      result; logs a warning when multiple coaches share the same name.
 *   3. No match → coach_id stays NULL. The row is still inserted so it can
 *      be backfilled later when the coach master grows. Logged as "unmatched".
 *
 * Upsert semantics:
 *   - Matched rows (coach_id IS NOT NULL): ON CONFLICT on
 *     coach_licenses_coach_tier_state_uq → update last_seen_at, expires_at,
 *     and source_url; preserve existing issue_date when new one is absent.
 *   - Unmatched rows (coach_id IS NULL): plain INSERT — no partial index
 *     exists to dedup on, so each run appends unmatched rows. Use
 *     --skip-unmatched to suppress this behavior.
 *
 * Output: JSONL audit to stdout; summary counts to stderr.
 */

import fs from "node:fs";
import readline from "node:readline";
import { parse } from "csv-parse";
import { pool } from "@workspace/db";
import { normalizeName } from "./backfill-coaches-master.js";

const DRY = process.argv.includes("--dry-run");
const SKIP_UNMATCHED = process.argv.includes("--skip-unmatched");

// ---------------------------------------------------------------------------
// CLI arg parsing
// ---------------------------------------------------------------------------

function getArg(flag: string): string | undefined {
  const idx = process.argv.indexOf(flag);
  return idx !== -1 ? process.argv[idx + 1] : undefined;
}

const FILE = getArg("--file");
if (!FILE) {
  console.error("Error: --file <path> is required");
  process.exit(1);
}
if (!fs.existsSync(FILE)) {
  console.error(`Error: file not found: ${FILE}`);
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const VALID_TIERS = new Set([
  "grassroots_online",
  "grassroots_in_person",
  "D",
  "C",
  "B",
  "A",
  "Pro",
]);

// Column name aliases (lowercase).
const COL_ALIASES: Record<string, string> = {
  "coach_name": "coach_name",
  "name": "coach_name",
  "full_name": "coach_name",
  "license_tier": "license_tier",
  "tier": "license_tier",
  "license": "license_tier",
  "state": "state",
  "issue_date": "issue_date",
  "issued_date": "issue_date",
  "issued": "issue_date",
  "expires_at": "expires_at",
  "expiration_date": "expires_at",
  "expiry": "expires_at",
  "expires": "expires_at",
  "source_url": "source_url",
  "url": "source_url",
};

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

function parseDate(raw: string | undefined): Date | null {
  if (!raw?.trim()) return null;
  const s = raw.trim();
  // ISO format
  const d = new Date(s);
  if (!isNaN(d.getTime())) return d;
  // MM/DD/YYYY
  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return new Date(`${m[3]}-${m[1].padStart(2, "0")}-${m[2].padStart(2, "0")}`);
  return null;
}

// ---------------------------------------------------------------------------
// CSV parsing
// ---------------------------------------------------------------------------

type RawRow = Record<string, string>;

interface NormalizedRow {
  coach_name: string;
  license_tier: string;
  state: string | null;
  issue_date: Date | null;
  expires_at: Date | null;
  source_url: string | null;
}

function normalizeHeader(h: string): string {
  return COL_ALIASES[h.toLowerCase().trim()] ?? h.toLowerCase().trim();
}

async function readCsv(filePath: string): Promise<NormalizedRow[]> {
  const stream = fs.createReadStream(filePath);
  const parser = parse({ columns: (hdr: string[]) => hdr.map(normalizeHeader), trim: true, skip_empty_lines: true });
  stream.pipe(parser);

  const rows: NormalizedRow[] = [];
  let lineNum = 1;

  for await (const raw of parser as AsyncIterable<RawRow>) {
    lineNum++;
    const name = (raw["coach_name"] ?? "").trim();
    const tier = (raw["license_tier"] ?? "").trim();

    if (!name) {
      console.warn(`[import] line ${lineNum}: empty coach_name — skipping`);
      continue;
    }
    if (!VALID_TIERS.has(tier)) {
      console.warn(
        `[import] line ${lineNum}: invalid license_tier "${tier}" for ${name} — skipping`,
      );
      continue;
    }

    rows.push({
      coach_name: name,
      license_tier: tier,
      state: (raw["state"] ?? "").trim() || null,
      issue_date: parseDate(raw["issue_date"]),
      expires_at: parseDate(raw["expires_at"]),
      source_url: (raw["source_url"] ?? "").trim() || null,
    });
  }

  return rows;
}

// ---------------------------------------------------------------------------
// Coach name → coach_id resolution
// ---------------------------------------------------------------------------

type NameMap = Map<string, number[]>; // normalized_name → [coach_id, ...]

async function buildNameMap(): Promise<NameMap> {
  const { rows } = await pool.query<{ id: number; display_name: string }>(
    "SELECT id, display_name FROM coaches ORDER BY id",
  );
  const map: NameMap = new Map();
  for (const row of rows) {
    const key = normalizeName(row.display_name);
    const bucket = map.get(key) ?? [];
    bucket.push(row.id);
    map.set(key, bucket);
  }
  return map;
}

function resolveCoachId(
  name: string,
  nameMap: NameMap,
): { coachId: number | null; ambiguous: boolean } {
  const key = normalizeName(name);
  const bucket = nameMap.get(key) ?? [];
  if (bucket.length === 0) return { coachId: null, ambiguous: false };
  if (bucket.length === 1) return { coachId: bucket[0], ambiguous: false };
  return { coachId: bucket[0], ambiguous: true };
}

// ---------------------------------------------------------------------------
// Upsert
// ---------------------------------------------------------------------------

interface UpsertResult {
  action: "inserted" | "updated" | "skipped";
  coachId: number | null;
}

async function upsertLicense(
  row: NormalizedRow,
  coachId: number | null,
): Promise<UpsertResult> {
  if (coachId !== null) {
    // Partial unique index exists: ON CONFLICT DO UPDATE.
    const result = await pool.query<{ id: number; xmax: string }>(
      `INSERT INTO coach_licenses
         (coach_id, license_tier, state, issue_date, expires_at, source_url,
          first_seen_at, last_seen_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
       ON CONFLICT (coach_id, license_tier, COALESCE(state, ''))
         WHERE coach_id IS NOT NULL
       DO UPDATE SET
         last_seen_at = NOW(),
         expires_at   = COALESCE(EXCLUDED.expires_at, coach_licenses.expires_at),
         issue_date   = COALESCE(EXCLUDED.issue_date, coach_licenses.issue_date),
         source_url   = COALESCE(EXCLUDED.source_url, coach_licenses.source_url)
       RETURNING id, xmax::text`,
      [coachId, row.license_tier, row.state, row.issue_date, row.expires_at, row.source_url],
    );
    const r = result.rows[0];
    return { action: r.xmax === "0" ? "inserted" : "updated", coachId };
  }

  // No coach_id — plain insert (no dedup guard available).
  await pool.query(
    `INSERT INTO coach_licenses
       (coach_id, license_tier, state, issue_date, expires_at, source_url,
        first_seen_at, last_seen_at)
     VALUES (NULL, $1, $2, $3, $4, $5, NOW(), NOW())`,
    [row.license_tier, row.state, row.issue_date, row.expires_at, row.source_url],
  );
  return { action: "inserted", coachId: null };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.error(`[import-coach-licenses] file=${FILE} dry=${DRY} skip_unmatched=${SKIP_UNMATCHED}`);

  const csvRows = await readCsv(FILE!);
  console.error(`[import-coach-licenses] parsed ${csvRows.length} valid CSV row(s)`);

  if (csvRows.length === 0) {
    console.error("[import-coach-licenses] nothing to import");
    return;
  }

  console.error("[import-coach-licenses] building coach name map…");
  const nameMap = await buildNameMap();
  console.error(`[import-coach-licenses] loaded ${nameMap.size} distinct coach names`);

  const counts = { inserted: 0, updated: 0, skipped_unmatched: 0, ambiguous: 0, errors: 0 };

  for (const row of csvRows) {
    const { coachId, ambiguous } = resolveCoachId(row.coach_name, nameMap);

    if (ambiguous) {
      counts.ambiguous++;
      console.warn(
        `[import] ambiguous match for "${row.coach_name}" — using lowest coach_id=${coachId}`,
      );
    }

    if (coachId === null && SKIP_UNMATCHED) {
      counts.skipped_unmatched++;
      process.stdout.write(
        JSON.stringify({
          coach_name: row.coach_name,
          license_tier: row.license_tier,
          state: row.state,
          action: "skipped_unmatched",
          coachId: null,
        }) + "\n",
      );
      continue;
    }

    if (DRY) {
      const action = coachId !== null ? "would_upsert" : "would_insert_unmatched";
      process.stdout.write(
        JSON.stringify({
          coach_name: row.coach_name,
          license_tier: row.license_tier,
          state: row.state,
          action,
          coachId,
          ambiguous,
        }) + "\n",
      );
      counts.inserted++;
      continue;
    }

    try {
      const result = await upsertLicense(row, coachId);
      if (result.action === "inserted") counts.inserted++;
      else counts.updated++;

      process.stdout.write(
        JSON.stringify({
          coach_name: row.coach_name,
          license_tier: row.license_tier,
          state: row.state,
          action: result.action,
          coachId: result.coachId,
          ambiguous,
        }) + "\n",
      );
    } catch (err) {
      counts.errors++;
      console.warn(
        `[import] error for "${row.coach_name}" (${row.license_tier}): ${(err as Error).message}`,
      );
    }
  }

  console.error("---");
  console.error(`Inserted:         ${counts.inserted}`);
  console.error(`Updated:          ${counts.updated}`);
  console.error(`Skipped unmatched:${counts.skipped_unmatched}`);
  console.error(`Ambiguous matches: ${counts.ambiguous}`);
  console.error(`Errors:           ${counts.errors}`);
  console.error(DRY ? "[dry] no changes written" : "[done]");
}

const invokedAsScript =
  import.meta.url === `file://${process.argv[1]}` ||
  import.meta.url.endsWith(process.argv[1] ?? "");

if (invokedAsScript) {
  main()
    .catch((e) => {
      console.error(e);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
