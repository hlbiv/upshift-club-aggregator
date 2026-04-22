/**
 * One-shot backfill: populate `club_affiliations.league_id` for every row
 * whose `league_id` is NULL but whose `source_name` exactly matches a
 * `leagues_master.league_name`.
 *
 * Why
 * ---
 * `club_affiliations.league_id` was added so the per-league coverage
 * rollups join through a stable id instead of `source_name`. Existing
 * rows that predate the column have `league_id = NULL` and would be
 * dropped from the rollup until we fill them in.
 *
 * The match is exact on the original league name — names that have
 * already drifted on the `leagues_master` side will not match here and
 * stay NULL, which is the correct outcome (operators need to either
 * rename them back or add an alias row before they can be linked).
 *
 * Run from workspace root:
 *   pnpm --filter @workspace/db exec tsx src/backfill-affiliations-league-id.ts
 *
 * Idempotent — re-running after a successful pass updates 0 rows.
 */
import { db, pool } from "./index.js";
import { sql } from "drizzle-orm";

async function main() {
  const result = await db.execute<{ updated: string }>(sql`
    WITH updated AS (
      UPDATE club_affiliations ca
      SET league_id = lm.id
      FROM leagues_master lm
      WHERE ca.league_id IS NULL
        AND ca.source_name = lm.league_name
      RETURNING ca.id
    )
    SELECT COUNT(*)::text AS updated FROM updated
  `);

  const list = Array.from(result as unknown as Array<{ updated: string }>);
  const updated = Number(list[0]?.updated ?? 0);
  console.log(`[backfill-affiliations-league-id] updated ${updated} rows`);

  const remaining = await db.execute<{ remaining: string }>(sql`
    SELECT COUNT(*)::text AS remaining
    FROM club_affiliations
    WHERE league_id IS NULL
  `);
  const remainingList = Array.from(
    remaining as unknown as Array<{ remaining: string }>,
  );
  const stillNull = Number(remainingList[0]?.remaining ?? 0);
  if (stillNull > 0) {
    console.warn(
      `[backfill-affiliations-league-id] ${stillNull} rows remain NULL — ` +
        `their source_name does not match any leagues_master.league_name. ` +
        `Inspect with: SELECT DISTINCT source_name FROM club_affiliations ` +
        `WHERE league_id IS NULL ORDER BY source_name;`,
    );
  } else {
    console.log("[backfill-affiliations-league-id] all rows linked");
  }

  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
