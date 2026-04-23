/**
 * Seed `canonical_clubs.is_pro_academy` from a curated allow-list of
 * pro-pathway academies. See task-79.
 *
 *   pnpm --filter @workspace/scripts exec tsx src/seed-pro-academies.ts --dry-run
 *   pnpm --filter @workspace/scripts exec tsx src/seed-pro-academies.ts
 *
 * Why this exists
 * ---------------
 * `competitive_tier` originally flipped any club with an MLS NEXT /
 * NWSL Academy / USL Academy affiliation to `'academy'`. Those leagues
 * are youth-pathway leagues with hundreds of independent member clubs,
 * so the override over-tagged ~170 non-pro clubs. The fix is a curated
 * list of clubs that ACTUALLY operate a senior pro academy (MLS team,
 * NWSL team, USL Championship / USL L1 affiliate). The backfill script
 * then ANDs `is_pro_academy = TRUE` into the academy decision.
 *
 * Matching strategy
 * -----------------
 * Match by `club_name_canonical` (UNIQUE column). IDs are unstable
 * across DB resets — names are the durable identifier in this repo.
 * Names that don't resolve to exactly one row are reported as
 * `[unmatched]` and skipped without aborting; operators add aliases
 * via `club_aliases` or rename the entry in PRO_ACADEMY_NAMES.
 *
 * Idempotent: re-runs converge to the same set of TRUE flags. Clubs
 * not in the list are reset to FALSE so removing an entry from the
 * list propagates on the next run.
 *
 * @deprecated as the day-to-day source of truth — task #82.
 *
 * The pro-academy allow-list is now operator-editable from the admin
 * dashboard at `/data-quality/pro-academies`, which PATCHes
 * `canonical_clubs.is_pro_academy` and re-runs the per-club tier rollup
 * inline. PRO_ACADEMY_NAMES below is kept as the audit-trail bootstrap
 * seed (used to repopulate the flag after a DB reset, and as the
 * historical record of the curated names that originally seeded the
 * column). DO NOT add new academies here without also flipping them in
 * the dashboard — the dashboard write is what production reads. This
 * script remains the recommended way to seed a fresh database.
 */

import { pool } from "@workspace/db";

const DRY = process.argv.includes("--dry-run");
const FORCE = process.argv.includes("--force");

// Safety floor: if fewer than this fraction of curated names resolve,
// abort the destructive reset step. Protects against canonical-name
// drift (rename / delete / dedup merge) silently clearing the entire
// allow-list. Override with --force when intentionally shrinking the
// list. Threshold chosen so a 56-name list tolerates up to ~5 misses.
const MIN_RESOLVE_RATIO = 0.9;

// Curated pro-academy allow-list. Names must match
// `canonical_clubs.club_name_canonical` exactly. Grouped for review.
//
// Sourcing rule: a club is included iff it is the youth academy of a
// senior team currently playing in MLS, NWSL, USL Championship, or USL
// League One. Not USL League Two (amateur) and not USL W (separate
// women's pro league not feeding the same youth pipeline).
//
// Initial seed derived from the would-flip set produced by the existing
// rollup logic against the live DB on 2026-04-23. New entries should
// cite a public source (team page, league roster) in PR review. Removals
// should also be PR-justified — false negatives silently downgrade
// otherwise-correct academy classifications to 'elite'.
export const PRO_ACADEMY_NAMES: string[] = [
  // MLS academies (28). "Inter Miami Academy" is Inter Miami CF; LA
  // Galaxy + Los Angeles Academy are LAFC, etc. Charlotte FC, Miami CF
  // and any other current MLS clubs missing here are intentional gaps —
  // add them when their academy enters the affiliations table.
  "Atlanta Academy",
  "Austin Academy",
  "Chicago Fire",
  "Cincinnati Academy",
  "Colorado Rapids Academy",
  "Columbus Crew Academy",
  "D.C. Academy",
  "Dallas Academy",
  "Houston Dynamo Academy",
  "Inter Miami Academy",
  "La Galaxy Academy",
  "Los Angeles Academy",
  "Minnesota",
  "Montreal",
  "Nashville Academy",
  "New England Revolution Academy",
  "New York Red Bulls Academy",
  "Nycfc Academy",
  "Orlando Academy",
  "Philadelphia Union Academy",
  "Portland Timbers Academy",
  "Real Salt Lake Academy",
  "San Jose Earthquakes Academy",
  "Seattle Sounders Academy",
  "Sporting Kansas Academy",
  "St. Louis",
  "Toronto Academy",
  "Vancouver Whitecaps Academy",

  // NWSL academies (14, all current NWSL franchises with an academy
  // arm at time of seeding).
  "Angel Academy",
  "Bay Academy",
  "Boston Legacy Academy",
  "Chicago Red Stars Academy",
  "Denver Aurora Academy",
  "Houston Dash Academy",
  "Kansas Current Academy",
  "Nj/Ny Gotham Academy",
  "North Carolina Courage Academy",
  "Orlando Pride Academy",
  "Portland Thorns Academy",
  "San Diego Wave Academy",
  "Utah Royals Academy",
  "Washington Spirit Academy",

  // USL Championship / USL L1 academies + reserve sides (14).
  // "II" / "2" rows are USL pro reserve teams (e.g. Tampa Bay Rowdies
  // 2, Colorado Springs Switchbacks II) that double as the senior
  // club's pathway. Sacramento Republic + Tampa Bay Rowdies (the non-
  // "II" rows) are also pro USL Championship clubs whose youth setups
  // sit under the same org — included to cover both name variants.
  "Colorado Springs Switchbacks Ii",
  "El Paso Locomotive Ii",
  "Forward Madison",
  "Greenville Triumph",
  "Lexington",
  "Monterey Bay",
  "New Mexico",
  "North Carolina",
  "Oakland Roots",
  "Orange County Ii",
  "Sacramento Republic",
  "South Georgia Tormenta",
  "Tampa Bay Rowdies",
  "Tampa Bay Rowdies 2",
];

async function main() {
  console.log(`[seed-pro-academies] dry=${DRY}`);
  console.log(`  curated list size: ${PRO_ACADEMY_NAMES.length}`);

  // Resolve every name to a canonical_clubs.id. Report mismatches.
  const resolved = await pool.query<{ id: number; name: string }>(
    `SELECT id, club_name_canonical AS name
     FROM canonical_clubs
     WHERE club_name_canonical = ANY($1::text[])`,
    [PRO_ACADEMY_NAMES],
  );
  const foundNames = new Set(resolved.rows.map((r) => r.name));
  const missing = PRO_ACADEMY_NAMES.filter((n) => !foundNames.has(n));
  if (missing.length > 0) {
    console.warn(`\n[warn] ${missing.length} curated name(s) did not match any canonical_clubs row:`);
    for (const n of missing) console.warn(`       [unmatched] "${n}"`);
    console.warn("       Add an alias, rename the list entry, or wait for the affiliation scrape to land.");
  }
  console.log(`  resolved ${resolved.rows.length}/${PRO_ACADEMY_NAMES.length} names to club ids`);

  // Refuse to run the destructive reset+set if too few names resolved.
  // Without this guard, a wave of canonical_clubs renames or merges
  // would silently clear the entire allow-list on the next backfill.
  const ratio = resolved.rows.length / PRO_ACADEMY_NAMES.length;
  if (ratio < MIN_RESOLVE_RATIO && !FORCE) {
    console.error(
      `\n[abort] only ${(ratio * 100).toFixed(1)}% of curated names resolved (floor: ${(MIN_RESOLVE_RATIO * 100).toFixed(0)}%). ` +
        `This usually means canonical_clubs names drifted (rename / dedup merge). ` +
        `Fix the unmatched names above, or rerun with --force if you intend to shrink the list.`,
    );
    process.exitCode = 1;
    return;
  }

  if (DRY) {
    console.log("\n[dry] would set is_pro_academy=TRUE on the resolved set and FALSE on every other row");
    return;
  }

  const ids = resolved.rows.map((r) => r.id);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Reset everything so removals from the list propagate.
    const cleared = await client.query(
      "UPDATE canonical_clubs SET is_pro_academy = FALSE WHERE is_pro_academy = TRUE AND id <> ALL($1::int[])",
      [ids],
    );
    const set = await client.query(
      "UPDATE canonical_clubs SET is_pro_academy = TRUE WHERE id = ANY($1::int[]) AND is_pro_academy = FALSE",
      [ids],
    );
    await client.query("COMMIT");
    console.log(`  cleared is_pro_academy on ${cleared.rowCount ?? 0} stale row(s)`);
    console.log(`  set is_pro_academy=TRUE on ${set.rowCount ?? 0} row(s)`);
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }

  const total = await pool.query<{ n: string }>(
    "SELECT COUNT(*)::text AS n FROM canonical_clubs WHERE is_pro_academy = TRUE",
  );
  console.log(`\n[done] is_pro_academy=TRUE on ${total.rows[0].n} club(s)`);
}

main()
  .catch((e) => {
    console.error(e);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
