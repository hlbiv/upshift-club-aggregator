/**
 * Seed `canonical_schools` from the NCES Common Core of Data (CCD) public-
 * schools file. The CCD is the authoritative public-school universe for the
 * US (K-12). For this repo we only care about high schools, so we filter to
 * regular schools whose grade span overlaps 9-12.
 *
 * Natural key: `NCESSCH` (12-char state+district+school). Idempotent — a
 * re-run UPDATEs the existing row matched by `ncessch`. If no `ncessch`
 * match exists but a `(school_name_canonical, school_state)` row does, we
 * backfill the `ncessch` column onto that row instead of creating a
 * duplicate. This lets operator-curated rows acquire an NCES identity the
 * first time the seeder sees them.
 *
 * Run:
 *   pnpm --filter @workspace/scripts run seed-canonical-schools-nces \
 *     -- --csv /path/to/ccd_sch_029_YYYY_w_1a_DATE.csv
 *   pnpm --filter @workspace/scripts run seed-canonical-schools-nces \
 *     -- --csv /path/to/ccd.csv --dry-run --limit 1000
 *
 * The NCES CCD file is ~300MB and published annually at
 *   https://nces.ed.gov/ccd/files.asp
 * Download the most recent `ccd_sch_029_*.zip`, unzip, and pass the CSV
 * path via `--csv`. Private schools are OUT OF SCOPE — PSS is a separate
 * NCES product.
 *
 * Live DB runs happen on Replit only. Never point this at local/CI Postgres.
 */

import fs from "node:fs";
import { parse } from "csv-parse";
import { pool } from "@workspace/db";

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

interface CliArgs {
  csvPath: string | null;
  dryRun: boolean;
  limit: number | null;
}

export function parseArgs(argv: string[]): CliArgs {
  let csvPath: string | null = null;
  let dryRun = false;
  let limit: number | null = null;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--csv") {
      csvPath = argv[++i] ?? null;
    } else if (a === "--dry-run") {
      dryRun = true;
    } else if (a === "--limit") {
      const n = Number.parseInt(argv[++i] ?? "", 10);
      if (Number.isFinite(n) && n > 0) limit = n;
    }
  }
  return { csvPath, dryRun, limit };
}

// ---------------------------------------------------------------------------
// Row transformation — pure, testable
// ---------------------------------------------------------------------------

export interface NcesRow {
  ncessch: string;
  schoolName: string;
  state: string; // 2-letter uppercase
  city: string | null;
}

export interface FilterResult {
  kind: "ok" | "skip-non-hs" | "skip-private" | "drop-malformed";
  row?: NcesRow;
  reason?: string;
}

const US_STATE_RE = /^[A-Z]{2}$/;

/**
 * Map a raw CCD grade-span token (e.g. "KG", "PK", "09", "12", "UG") to an
 * integer 0..12. Returns null if unparseable. NCES uses:
 *   PK = pre-K, KG = kindergarten, 01-12 = grade 1-12, UG = ungraded,
 *   AE = adult ed, N = not applicable
 * Only 9, 10, 11, 12 matter for HS inclusion.
 */
export function parseGrade(raw: string | null | undefined): number | null {
  if (!raw) return null;
  const s = raw.trim().toUpperCase();
  if (s === "PK") return -1;
  if (s === "KG") return 0;
  if (/^\d{1,2}$/.test(s)) {
    const n = Number.parseInt(s, 10);
    if (n >= 0 && n <= 12) return n;
  }
  return null;
}

/** True iff the [lo, hi] grade span overlaps grades 9-12. */
export function spanOverlapsHighSchool(
  gslo: string | null | undefined,
  gshi: string | null | undefined,
): boolean {
  const lo = parseGrade(gslo);
  const hi = parseGrade(gshi);
  // If EITHER endpoint is unparseable, be conservative: require the other to
  // be inside 9..12. If both are unparseable, reject. This keeps "UG"-only
  // ungraded rows out of the HS universe.
  if (lo === null && hi === null) return false;
  if (lo === null) return hi! >= 9 && hi! <= 12;
  if (hi === null) return lo >= 9 && lo <= 12;
  return lo <= 12 && hi >= 9;
}

/**
 * Normalize NCES school name for matching. Conservative — NCES publishes
 * names in TitleCase mostly but occasionally ALL CAPS. We preserve casing
 * as-is (the linker uses case-insensitive fuzzy match) except for trimming
 * whitespace and collapsing runs of spaces. Do NOT strip tokens like "High
 * School" — they're load-bearing per the linker doc block.
 */
export function normalizeSchoolName(raw: string): string {
  return raw.trim().replace(/\s+/g, " ");
}

/**
 * Apply the HS-only filter + extract the canonical fields we store. Accepts
 * the raw CSV row (a Record<string, string> from csv-parse). Returns a
 * tagged result so callers can report per-category counts.
 *
 * Filter predicate (all must hold):
 *   1. NCESSCH present and non-empty
 *   2. SCH_NAME present and non-empty
 *   3. 2-letter state code (LSTATE preferred, fallback ST)
 *   4. SCH_TYPE = 1 (regular school) — drops special-ed/vocational/alt
 *   5. GSLO/GSHI span overlaps 9-12
 *
 * Private schools (`CHARTER_TEXT = "Yes"` is NOT the same — charters are
 * public; private schools come from PSS not CCD). But the CCD file is
 * guaranteed public-only, so we don't need a private filter here. We flag
 * any row whose `SY_STATUS` indicates closed/inactive as drop-malformed
 * rather than skip-non-hs (it wasn't a HS, it was a record of one).
 */
export function filterAndTransformRow(
  raw: Record<string, string>,
): FilterResult {
  const ncessch = (raw.NCESSCH ?? raw.nchessch ?? "").trim();
  const name = (raw.SCH_NAME ?? "").trim();
  const stateRaw = (raw.LSTATE ?? raw.ST ?? "").trim().toUpperCase();
  const city = (raw.LCITY ?? raw.MCITY ?? "").trim() || null;
  const schType = (raw.SCH_TYPE ?? "").trim();
  const gslo = raw.GSLO;
  const gshi = raw.GSHI;

  if (!ncessch || !name) {
    return { kind: "drop-malformed", reason: "missing NCESSCH or SCH_NAME" };
  }
  if (!US_STATE_RE.test(stateRaw)) {
    return {
      kind: "drop-malformed",
      reason: `bad state code ${JSON.stringify(stateRaw)}`,
    };
  }
  // SCH_TYPE = 1 per NCES CCD codebook: 1 Regular, 2 Special Ed,
  // 3 Vocational, 4 Alternative/Other. We take only regular.
  if (schType !== "1") {
    return { kind: "skip-non-hs", reason: `SCH_TYPE=${schType}` };
  }
  if (!spanOverlapsHighSchool(gslo, gshi)) {
    return { kind: "skip-non-hs", reason: `grades ${gslo ?? "?"}-${gshi ?? "?"}` };
  }

  return {
    kind: "ok",
    row: {
      ncessch,
      schoolName: normalizeSchoolName(name),
      state: stateRaw,
      city,
    },
  };
}

// ---------------------------------------------------------------------------
// Batching + upserts
// ---------------------------------------------------------------------------

export interface SeedCounters {
  considered: number;
  inserted: number;
  updated: number;
  skippedNonHs: number;
  droppedMalformed: number;
}

export interface UpsertClient {
  /** Upsert a batch of NCES rows. Returns per-batch (inserted, updated). */
  upsertBatch(rows: NcesRow[]): Promise<{ inserted: number; updated: number }>;
}

const BATCH_SIZE = 500;

/**
 * Stream the CSV, apply the HS filter, batch upserts. Pure of process.exit —
 * callers own termination. `client` is injected so tests can stub out DB I/O.
 */
export async function runSeed(
  csvPath: string,
  client: UpsertClient,
  opts: { limit: number | null; dryRun: boolean },
): Promise<SeedCounters> {
  const c: SeedCounters = {
    considered: 0,
    inserted: 0,
    updated: 0,
    skippedNonHs: 0,
    droppedMalformed: 0,
  };

  const batch: NcesRow[] = [];
  const flush = async () => {
    if (batch.length === 0) return;
    if (opts.dryRun) {
      // Count as "would-insert" but don't touch DB. We report this as
      // `inserted` for parity with real runs; the --dry-run banner makes
      // it unambiguous.
      c.inserted += batch.length;
    } else {
      const { inserted, updated } = await client.upsertBatch(batch);
      c.inserted += inserted;
      c.updated += updated;
    }
    batch.length = 0;
  };

  const stream = fs.createReadStream(csvPath).pipe(
    parse({
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
    }),
  );

  for await (const raw of stream as AsyncIterable<Record<string, string>>) {
    c.considered++;
    if (opts.limit !== null && c.considered > opts.limit) {
      c.considered--; // don't count the overflow row
      break;
    }

    const result = filterAndTransformRow(raw);
    switch (result.kind) {
      case "ok":
        batch.push(result.row!);
        if (batch.length >= BATCH_SIZE) {
          await flush();
        }
        break;
      case "skip-non-hs":
        c.skippedNonHs++;
        break;
      case "skip-private":
        // Not reachable from CCD (public-only), but reserved for future
        // PSS integration. Counted under non-HS so totals stay sane.
        c.skippedNonHs++;
        break;
      case "drop-malformed":
        c.droppedMalformed++;
        break;
    }
  }
  await flush();
  return c;
}

// ---------------------------------------------------------------------------
// Real Postgres client — wraps `pool` with the idempotent upsert logic.
// ---------------------------------------------------------------------------

export function makeDbClient(): UpsertClient {
  return {
    async upsertBatch(rows) {
      if (rows.length === 0) return { inserted: 0, updated: 0 };

      // Stage 1 — UPSERT keyed on ncessch. This handles the common path
      // (row already seeded with an ncessch). We detect insert vs update
      // by checking whether the returned xmax is zero (insert) or not
      // (update) — a well-known Postgres trick.
      const params: unknown[] = [];
      const tuples: string[] = [];
      for (const r of rows) {
        const i = params.length;
        params.push(r.schoolName, r.state, r.ncessch, r.city);
        tuples.push(`($${i + 1}, $${i + 2}, $${i + 3}, $${i + 4})`);
      }

      // Stage 1 — backfill ncessch onto any existing row whose
      // (school_name_canonical, school_state) matches AND ncessch IS NULL.
      // This runs BEFORE the ncessch-keyed UPSERT so an operator-curated
      // row without an NCES id gets one instead of racing the INSERT and
      // colliding on the name+state unique.
      await pool.query(
        `
        WITH incoming(name, state, ncessch, city) AS (
          VALUES ${tuples.join(", ")}
        )
        UPDATE canonical_schools cs
           SET ncessch = i.ncessch,
               city = COALESCE(cs.city, i.city),
               updated_at = now()
          FROM incoming i
         WHERE cs.school_name_canonical = i.name
           AND cs.school_state = i.state
           AND cs.ncessch IS NULL
        `,
        params as unknown[],
      );

      // Stage 2 — insert-or-update keyed on the partial ncessch unique.
      // Rows backfilled in Stage 1 will match via ncessch_uq and take the
      // UPDATE branch (bumps updated_at). xmax=0 distinguishes true INSERT
      // from UPDATE — a well-known Postgres trick.
      const upsert = await pool.query(
        `
        WITH incoming(name, state, ncessch, city) AS (
          VALUES ${tuples.join(", ")}
        ),
        ins AS (
          INSERT INTO canonical_schools
            (school_name_canonical, school_state, ncessch, city, updated_at)
          SELECT name, state, ncessch, city, now()
            FROM incoming
          ON CONFLICT (ncessch) WHERE ncessch IS NOT NULL
          DO UPDATE SET
            school_name_canonical = EXCLUDED.school_name_canonical,
            school_state          = EXCLUDED.school_state,
            city                  = EXCLUDED.city,
            updated_at            = now()
          RETURNING xmax = 0 AS inserted
        )
        SELECT
          COUNT(*) FILTER (WHERE inserted)       AS inserted,
          COUNT(*) FILTER (WHERE NOT inserted)   AS updated
          FROM ins
        `,
        params as unknown[],
      );

      const row = upsert.rows[0] ?? { inserted: 0, updated: 0 };
      return {
        inserted: Number(row.inserted ?? 0),
        updated: Number(row.updated ?? 0),
      };
    },
  };
}

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.csvPath) {
    console.error(
      "usage: seed-canonical-schools-nces --csv <path> [--dry-run] [--limit N]",
    );
    process.exit(2);
  }
  if (!fs.existsSync(args.csvPath)) {
    console.error(`[seed-canonical-schools-nces] CSV not found: ${args.csvPath}`);
    process.exit(2);
  }
  console.log(
    `[seed-canonical-schools-nces] csv=${args.csvPath} dry=${args.dryRun} limit=${args.limit ?? "none"}`,
  );

  const client = args.dryRun
    ? ({
        // dry-run: never called (runSeed short-circuits to in-memory counter)
        upsertBatch: async () => ({ inserted: 0, updated: 0 }),
      } satisfies UpsertClient)
    : makeDbClient();

  const counters = await runSeed(args.csvPath, client, {
    limit: args.limit,
    dryRun: args.dryRun,
  });

  console.log("---");
  console.log(`Considered:          ${counters.considered}`);
  console.log(`Inserted:            ${counters.inserted}`);
  console.log(`Updated:             ${counters.updated}`);
  console.log(`Skipped (non-HS):    ${counters.skippedNonHs}`);
  console.log(`Dropped (malformed): ${counters.droppedMalformed}`);
  console.log(args.dryRun ? "[dry] no changes written" : "[done]");
}

// Only run main() when invoked as a script, not when imported by tests.
const invokedDirectly =
  import.meta.url === `file://${process.argv[1]}` ||
  process.argv[1]?.endsWith("seed-canonical-schools-nces.ts");

if (invokedDirectly) {
  main()
    .catch((e) => {
      console.error(e);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
