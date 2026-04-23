/**
 * Backfill `canonical_clubs.competitive_tier` by rolling up each club's
 * affiliations to the highest-tier league it plays in, normalized via
 * the user-confirmed TIER_LABEL_TO_ENUM map below. See task-78.
 *
 *   pnpm --filter @workspace/scripts exec tsx src/backfill-competitive-tier.ts --dry-run
 *   pnpm --filter @workspace/scripts exec tsx src/backfill-competitive-tier.ts
 *
 * Algorithm (single SQL UPDATE ... FROM rollup, per task-78 step 4):
 * ------------------------------------------------------------------
 * Inside a transaction:
 *   1. Reset every row to 'competitive' so deletions of high-tier
 *      affiliations between runs cause a club to drop back to default
 *      rather than stay stale.
 *   2. UPDATE canonical_clubs FROM a CTE that:
 *      a. Normalizes leagues_master.tier_label → competitive_tier via
 *         a CASE expression matching TIER_LABEL_TO_ENUM. Unrecognized
 *         labels become NULL and don't contribute to the rollup.
 *      b. Per club, picks the row(s) at MIN(tier_numeric) (= most elite).
 *      c. Applies the academy override ONLY when top tier_numeric = 1
 *         AND every top-tier-1 affiliation is in ACADEMY_FAMILIES. If
 *         the top-tier-1 set is mixed (e.g. MLS NEXT + Elite 64), the
 *         club is NOT flipped — task spec explicitly says log for human
 *         review instead of silently flipping. Those clubs fall through
 *         to the normal rollup (and end up at 'elite').
 *      d. Picks 'elite' over 'competitive' when both appear at the same
 *         top tier (tie-break — 'elite' is the higher signal).
 *
 * Before the UPDATE, runs two read-only SELECTs to surface diagnostics:
 *   - the per-club ambiguity report (clubs that WOULD have been academy
 *     but were left at 'elite' because of mixed top-tier affiliations).
 *   - the post-decision distribution + competitive-bucket breakdown
 *     (rollup vs. schema default vs. tournament-only).
 *
 * Note: club_affiliations.league_id was rolled out after most rows
 * were already written; lib/db/src/backfill-affiliations-league-id.ts
 * fills it in via source_name = league_name. To stay self-contained
 * (don't make this script depend on that one having run), the join is
 * `ca.league_id = lm.id OR (ca.league_id IS NULL AND ca.source_name = lm.league_name)`.
 *
 * Idempotent. Re-running converges to the same tier per club. Exits
 * non-zero on any update error.
 */

import { pool } from "@workspace/db";

const DRY = process.argv.includes("--dry-run");

// User-confirmed map from leagues_master.tier_label → competitive_tier.
// Mirrored as a SQL CASE expression in NORMALIZED_CASE_SQL below — keep
// the two in sync.
const TIER_LABEL_TO_ENUM: Record<string, "elite" | "competitive"> = {
  "National Elite": "elite",
  "National Elite / High National": "elite",
  "National Elite / Pro Pathway": "elite",
  "National / Regional High Performance": "elite",
  "Pre-Elite Development": "elite",
  "NPL Member League": "competitive",
  "Regional Power League": "competitive",
  "Regional Tournament": "competitive",
  "State Association / League Hub": "competitive",
};

// User-approved academy override: a club's tier flips to 'academy' when
// its top-tier-numeric=1 affiliation set is exclusively in this family
// list. Girls Academy is intentionally excluded (college-pathway elite,
// not pro-pathway academy). USL W is excluded (women's pro league).
// Mirrored in ACADEMY_FAMILIES_SQL below.
const ACADEMY_FAMILIES = ["MLS NEXT", "NWSL Academy", "USL Academy"];

const ACADEMY_FAMILIES_SQL = `('${ACADEMY_FAMILIES.join("','")}')`;

// SQL CASE expression matching TIER_LABEL_TO_ENUM. Built from the TS
// constant so adding/removing a tier_label only needs to be done once.
const NORMALIZED_CASE_SQL = (() => {
  const lines = Object.entries(TIER_LABEL_TO_ENUM)
    .map(([label, tier]) => `      WHEN ${asLit(label)} THEN '${tier}'`)
    .join("\n");
  return `CASE lm.tier_label\n${lines}\n      ELSE NULL\n    END`;
})();

function asLit(s: string): string {
  return `'${s.replace(/'/g, "''")}'`;
}

// Shared `normalized` CTE used by every diagnostic + the UPDATE.
const NORMALIZED_CTE_SQL = `
  normalized AS (
    SELECT
      ca.club_id,
      lm.tier_numeric,
      lm.tier_label,
      lm.league_family,
      ${NORMALIZED_CASE_SQL} AS norm_tier
    FROM club_affiliations ca
    JOIN leagues_master lm
      ON ca.league_id = lm.id
      OR (ca.league_id IS NULL AND ca.source_name = lm.league_name)
    WHERE ca.club_id IS NOT NULL
  )
`;

async function main() {
  console.log(`[backfill-competitive-tier] dry=${DRY}`);

  // --- Diagnostic 1: unmapped tier_labels (informational) ----------------
  const unmapped = await pool.query<{ tier_label: string; n: string }>(`
    WITH ${NORMALIZED_CTE_SQL}
    SELECT tier_label, COUNT(*)::text AS n
    FROM normalized
    WHERE tier_label IS NOT NULL AND norm_tier IS NULL
    GROUP BY tier_label
    ORDER BY 2 DESC
  `);
  if (unmapped.rows.length > 0) {
    console.log("\n[note] tier_label rows present in DB but not in TIER_LABEL_TO_ENUM:");
    for (const r of unmapped.rows) {
      console.log(`       ${r.tier_label} (${r.n} affiliation rows)`);
    }
  }

  // --- Diagnostic 2: ambiguous academy clubs (NOT flipped) ----------------
  // Clubs whose top-tier-1 affiliation set contains BOTH an academy family
  // AND a non-academy family. Per task spec these are explicitly NOT
  // flipped to academy — they log for human review and stay at 'elite'.
  const ambiguous = await pool.query<{
    club_id: number;
    club_name: string;
    families: string;
  }>(`
    WITH ${NORMALIZED_CTE_SQL},
    top_tier AS (
      SELECT club_id, MIN(tier_numeric) AS min_tn
      FROM normalized
      WHERE norm_tier IS NOT NULL
      GROUP BY club_id
    )
    SELECT
      n.club_id,
      cc.club_name_canonical AS club_name,
      string_agg(DISTINCT COALESCE(n.league_family, '(null)'), ', ' ORDER BY COALESCE(n.league_family, '(null)')) AS families
    FROM normalized n
    JOIN top_tier t ON t.club_id = n.club_id AND n.tier_numeric = t.min_tn
    JOIN canonical_clubs cc ON cc.id = n.club_id
    WHERE t.min_tn = 1
    GROUP BY n.club_id, cc.club_name_canonical
    HAVING bool_or(n.league_family = ANY(ARRAY['MLS NEXT','NWSL Academy','USL Academy'])) = TRUE
       AND bool_or(n.league_family IS NULL OR NOT (n.league_family = ANY(ARRAY['MLS NEXT','NWSL Academy','USL Academy']))) = TRUE
    ORDER BY n.club_id
  `);
  if (ambiguous.rows.length > 0) {
    console.warn(
      `\n[warn] ${ambiguous.rows.length} club(s) have BOTH academy + non-academy tier-1 affiliations — NOT flipping to 'academy'; left at rollup tier (elite). Review manually:`,
    );
    for (const r of ambiguous.rows.slice(0, 30)) {
      console.warn(
        `       club_id=${r.club_id} (${r.club_name}) tier1_families=[${r.families}]`,
      );
    }
    if (ambiguous.rows.length > 30) {
      console.warn(`       ...and ${ambiguous.rows.length - 30} more`);
    }
  }

  // --- The actual rollup UPDATE -----------------------------------------
  // Wrapped in a transaction. Step 1 resets to default so re-runs
  // converge; step 2 applies the rollup decision per club.
  if (!DRY) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const reset = await client.query(
        "UPDATE canonical_clubs SET competitive_tier = 'competitive' WHERE competitive_tier <> 'competitive'",
      );
      console.log(`\n  reset ${reset.rowCount ?? 0} rows to default`);

      // Single UPDATE ... FROM rollup. Decision rule per club:
      //   - top_tn = MIN(tier_numeric) over rows with a recognized norm_tier
      //   - academy iff top_tn = 1 AND every top-tier-1 affiliation has
      //     league_family in ACADEMY_FAMILIES (no mixed academy/non-academy)
      //   - otherwise: pick the most-elite normalized tier among the
      //     top-tier rows ('elite' beats 'competitive' on tie)
      const upd = await client.query(`
        WITH ${NORMALIZED_CTE_SQL},
        top_tier AS (
          SELECT club_id, MIN(tier_numeric) AS min_tn
          FROM normalized
          WHERE norm_tier IS NOT NULL
          GROUP BY club_id
        ),
        top_tier_families AS (
          SELECT
            n.club_id,
            bool_and(n.league_family = ANY(ARRAY[${ACADEMY_FAMILIES.map(asLit).join(",")}])) AS all_academy,
            bool_or(n.norm_tier = 'elite') AS has_elite,
            bool_or(n.norm_tier = 'competitive') AS has_competitive
          FROM normalized n
          JOIN top_tier t
            ON t.club_id = n.club_id AND n.tier_numeric = t.min_tn
          WHERE n.norm_tier IS NOT NULL
          GROUP BY n.club_id
        ),
        decisions AS (
          SELECT
            t.club_id,
            CASE
              WHEN t.min_tn = 1 AND f.all_academy = TRUE
                THEN 'academy'::competitive_tier
              WHEN f.has_elite
                THEN 'elite'::competitive_tier
              ELSE 'competitive'::competitive_tier
            END AS final_tier
          FROM top_tier t
          JOIN top_tier_families f ON f.club_id = t.club_id
        )
        UPDATE canonical_clubs cc
        SET competitive_tier = d.final_tier
        FROM decisions d
        WHERE cc.id = d.club_id
          AND cc.competitive_tier IS DISTINCT FROM d.final_tier
      `);
      console.log(`  applied rollup: ${upd.rowCount ?? 0} rows raised above default`);
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  } else {
    console.log("\n  [dry] skipping reset + rollup UPDATE");
  }

  // --- Diagnostic 3: post-run distribution ------------------------------
  // In dry mode, project the would-be distribution using the same CTE.
  const dist = DRY
    ? await pool.query<{ tier: string; n: string }>(`
        WITH ${NORMALIZED_CTE_SQL},
        top_tier AS (
          SELECT club_id, MIN(tier_numeric) AS min_tn
          FROM normalized
          WHERE norm_tier IS NOT NULL
          GROUP BY club_id
        ),
        top_tier_families AS (
          SELECT
            n.club_id,
            bool_and(n.league_family = ANY(ARRAY[${ACADEMY_FAMILIES.map(asLit).join(",")}])) AS all_academy,
            bool_or(n.norm_tier = 'elite') AS has_elite
          FROM normalized n
          JOIN top_tier t ON t.club_id = n.club_id AND n.tier_numeric = t.min_tn
          WHERE n.norm_tier IS NOT NULL
          GROUP BY n.club_id
        ),
        decisions AS (
          SELECT
            cc.id AS club_id,
            CASE
              WHEN t.min_tn IS NULL THEN 'competitive'
              WHEN t.min_tn = 1 AND f.all_academy = TRUE THEN 'academy'
              WHEN f.has_elite THEN 'elite'
              ELSE 'competitive'
            END AS final_tier
          FROM canonical_clubs cc
          LEFT JOIN top_tier t ON t.club_id = cc.id
          LEFT JOIN top_tier_families f ON f.club_id = cc.id
        )
        SELECT final_tier AS tier, COUNT(*)::text AS n
        FROM decisions
        GROUP BY 1 ORDER BY 1
      `)
    : await pool.query<{ tier: string; n: string }>(
        "SELECT competitive_tier::text AS tier, COUNT(*)::text AS n FROM canonical_clubs GROUP BY 1 ORDER BY 1",
      );

  console.log("\n--- Distribution ---");
  for (const r of dist.rows) {
    console.log(`  ${r.tier.padEnd(20)} ${r.n}`);
  }

  // Competitive-bucket breakdown: rollup vs default vs tournament-only.
  const buckets = await pool.query<{
    rollup_competitive: string;
    default_no_aff: string;
    default_unmapped_only: string;
    tournament_only: string;
  }>(`
    WITH ${NORMALIZED_CTE_SQL},
    by_club AS (
      SELECT cc.id AS club_id,
             COUNT(*) FILTER (WHERE n.club_id IS NOT NULL) AS aff_rows,
             COUNT(*) FILTER (WHERE n.norm_tier IS NOT NULL) AS mapped_rows,
             bool_and(n.tier_label = 'Regional Tournament') FILTER (WHERE n.tier_label IS NOT NULL) AS only_tournament
      FROM canonical_clubs cc
      LEFT JOIN normalized n ON n.club_id = cc.id
      GROUP BY cc.id
    )
    SELECT
      COUNT(*) FILTER (WHERE mapped_rows > 0)::text AS rollup_competitive,
      COUNT(*) FILTER (WHERE aff_rows = 0)::text AS default_no_aff,
      COUNT(*) FILTER (WHERE aff_rows > 0 AND mapped_rows = 0)::text AS default_unmapped_only,
      COUNT(*) FILTER (WHERE only_tournament = TRUE)::text AS tournament_only
    FROM by_club
  `);
  const b = buckets.rows[0];
  console.log("\n--- competitive bucket breakdown (orthogonal counts) ---");
  console.log(`  clubs w/ at least one mapped affiliation : ${b.rollup_competitive}`);
  console.log(`  clubs at default — zero affiliations     : ${b.default_no_aff}`);
  console.log(`  clubs at default — only unmapped labels  : ${b.default_unmapped_only}`);
  console.log(`  clubs whose only mapped tier is tournament: ${b.tournament_only}`);

  // Surface tournament-only clubs by name so operators can scan for
  // obviously-elite clubs that defaulted because we have no league signal.
  const tournamentOnly = await pool.query<{ id: number; name: string }>(`
    WITH ${NORMALIZED_CTE_SQL},
    by_club AS (
      SELECT n.club_id,
             bool_and(n.tier_label = 'Regional Tournament') AS only_t
      FROM normalized n
      WHERE n.tier_label IS NOT NULL
      GROUP BY n.club_id
    )
    SELECT cc.id, cc.club_name_canonical AS name
    FROM by_club b
    JOIN canonical_clubs cc ON cc.id = b.club_id
    WHERE b.only_t = TRUE
    ORDER BY cc.id
    LIMIT 10
  `);
  if (tournamentOnly.rows.length > 0) {
    console.log("\n  sample tournament-only clubs (review for obviously-elite misses):");
    for (const r of tournamentOnly.rows) {
      console.log(`       club_id=${r.id} (${r.name})`);
    }
  }

  console.log(
    "\n[note] 'Pre-Elite Development' (ECNL RL / pre-ECNL) collapses with 'National Elite' → 'elite'. " +
      "May warrant its own enum value later if scout-filter UX needs the distinction; not a blocker for this migration.",
  );

  console.log(DRY ? "\n[dry] no changes written" : "\n[done]");
}

main()
  .catch((e) => {
    console.error(e);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
