/**
 * Postgres regression test for the `club_affiliations.league_id` join.
 *
 * Why
 * ---
 * The `/admin/coverage/leagues*` rollups used to join
 * `leagues_master.league_name = club_affiliations.source_name`. The
 * moment a scraper wrote a slightly different label ("ECNL Boys" vs
 * "ECNL — Boys, 2025") the affected league silently dropped out of the
 * per-league rollup and the global "With roster / With coach" counters
 * under-counted. The fix is to join through the stable
 * `club_affiliations.league_id`. This test pins that contract.
 *
 * Scenario
 * --------
 *   1. Insert league "ECNL Boys" + a club affiliated to it, with the
 *      affiliation's `league_id` set to that league's id and
 *      `source_name = "ECNL Boys"`.
 *   2. Rename `leagues_master.league_name` to "ECNL — Boys, 2025"
 *      (simulating scraper drift). The affiliation's `source_name` is
 *      intentionally NOT updated — that's the whole point of the bug.
 *   3. Run the same join shape the prod query uses. Assert the club
 *      still counts under the renamed league.
 *   4. As a negative control, run the OLD source_name-based join shape
 *      and confirm it would have lost the row.
 *
 * Run (skips if no DB):
 *   DATABASE_URL=postgres://... pnpm --filter @workspace/db exec tsx \
 *     src/schema/__tests__/coverage-league-id.ts
 */
import pg from "pg";

const { Client } = pg;

if (!process.env.DATABASE_URL) {
  console.error(
    "[coverage-league-id] DATABASE_URL not set — skipping integration test.",
  );
  process.exit(0);
}

const SCHEMA = `upshift_cov_test_${process.pid}`;
const client = new Client({ connectionString: process.env.DATABASE_URL });

async function run() {
  await client.connect();
  await client.query(`CREATE SCHEMA ${SCHEMA}`);
  await client.query(`SET search_path TO ${SCHEMA}`);

  let pass = 0;
  let fail = 0;
  const log = (ok: boolean, name: string, msg = "") => {
    if (ok) {
      pass++;
      console.log(`  ok   ${name}`);
    } else {
      fail++;
      console.error(`  FAIL ${name}${msg ? `\n       ${msg}` : ""}`);
    }
  };

  try {
    await client.query(`
      CREATE TABLE leagues_master (
        id SERIAL PRIMARY KEY,
        league_name TEXT NOT NULL UNIQUE,
        league_family TEXT NOT NULL DEFAULT ''
      );

      CREATE TABLE canonical_clubs (
        id SERIAL PRIMARY KEY,
        club_name_canonical TEXT NOT NULL UNIQUE
      );

      CREATE TABLE club_affiliations (
        id SERIAL PRIMARY KEY,
        club_id INTEGER REFERENCES canonical_clubs(id) ON DELETE CASCADE,
        league_id INTEGER REFERENCES leagues_master(id) ON DELETE SET NULL,
        source_name TEXT,
        UNIQUE (club_id, source_name)
      );

      CREATE TABLE club_roster_snapshots (
        id SERIAL PRIMARY KEY,
        club_id INTEGER REFERENCES canonical_clubs(id) ON DELETE CASCADE
      );

      CREATE TABLE coach_discoveries (
        id SERIAL PRIMARY KEY,
        club_id INTEGER REFERENCES canonical_clubs(id) ON DELETE CASCADE
      );

      CREATE TABLE scrape_health (
        id SERIAL PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        last_scraped_at TIMESTAMP,
        UNIQUE (entity_type, entity_id)
      );
    `);

    // --- Fixture --------------------------------------------------------
    const {
      rows: [league],
    } = await client.query(
      `INSERT INTO leagues_master (league_name) VALUES ('ECNL Boys') RETURNING id`,
    );
    const {
      rows: [club],
    } = await client.query(
      `INSERT INTO canonical_clubs (club_name_canonical) VALUES ('FC Cascade') RETURNING id`,
    );
    await client.query(
      `INSERT INTO club_affiliations (club_id, league_id, source_name)
       VALUES ($1, $2, $3)`,
      [club.id, league.id, "ECNL Boys"],
    );
    await client.query(
      `INSERT INTO club_roster_snapshots (club_id) VALUES ($1)`,
      [club.id],
    );

    // --- Drift: rename the league. source_name on the affiliation is
    //     intentionally left at the old value.
    await client.query(
      `UPDATE leagues_master SET league_name = 'ECNL — Boys, 2025' WHERE id = $1`,
      [league.id],
    );

    // --- New (id-based) join: the club must still count -----------------
    const { rows: idJoinRows } = await client.query(
      `SELECT
         lm.league_name,
         COUNT(DISTINCT cc.id)::int AS clubs_total,
         COUNT(DISTINCT cc.id) FILTER (
           WHERE EXISTS (
             SELECT 1 FROM club_roster_snapshots crs WHERE crs.club_id = cc.id
           )
         )::int AS clubs_with_roster_snapshot
       FROM leagues_master lm
       LEFT JOIN club_affiliations ca ON ca.league_id = lm.id
       LEFT JOIN canonical_clubs cc   ON cc.id = ca.club_id
       WHERE lm.id = $1
       GROUP BY lm.league_name`,
      [league.id],
    );
    log(
      idJoinRows.length === 1 &&
        idJoinRows[0].league_name === "ECNL — Boys, 2025" &&
        idJoinRows[0].clubs_total === 1 &&
        idJoinRows[0].clubs_with_roster_snapshot === 1,
      "renamed league still counts via league_id join",
      `got ${JSON.stringify(idJoinRows)}`,
    );

    // --- Negative control: the OLD name-based join would have lost it ---
    const { rows: nameJoinRows } = await client.query(
      `SELECT COUNT(DISTINCT cc.id)::int AS clubs_total
       FROM leagues_master lm
       LEFT JOIN club_affiliations ca ON ca.source_name = lm.league_name
       LEFT JOIN canonical_clubs cc   ON cc.id = ca.club_id
       WHERE lm.id = $1`,
      [league.id],
    );
    log(
      nameJoinRows[0]?.clubs_total === 0,
      "name-based join would have lost the renamed league (regression guard)",
      `got ${JSON.stringify(nameJoinRows)}`,
    );

    // --- Per-league detail filter must also use league_id ---------------
    const { rows: detailRows } = await client.query(
      `SELECT cc.id AS club_id
       FROM club_affiliations ca
       JOIN canonical_clubs cc ON cc.id = ca.club_id
       WHERE ca.league_id = $1`,
      [league.id],
    );
    log(
      detailRows.length === 1 && detailRows[0].club_id === club.id,
      "league-detail clubs list is reachable by league_id after rename",
      `got ${JSON.stringify(detailRows)}`,
    );

    // --- Ingestion-path guard: a writer that resolves league_id at
    //     insert time (the seed.ts pattern) must produce a non-null
    //     league_id, so the row shows up in coverage immediately. ---
    const {
      rows: [freshLeague],
    } = await client.query(
      `INSERT INTO leagues_master (league_name) VALUES ('NPSL Pro') RETURNING id`,
    );
    const {
      rows: [freshClub],
    } = await client.query(
      `INSERT INTO canonical_clubs (club_name_canonical) VALUES ('Pro FC') RETURNING id`,
    );
    // Mirror seed.ts: build a name -> id map, then pass leagueId in.
    const leagueIdMap = new Map<string, number>([
      ["NPSL Pro", freshLeague.id],
    ]);
    await client.query(
      `INSERT INTO club_affiliations (club_id, league_id, source_name)
       VALUES ($1, $2, $3)`,
      [freshClub.id, leagueIdMap.get("NPSL Pro") ?? null, "NPSL Pro"],
    );
    const {
      rows: [freshRow],
    } = await client.query(
      `SELECT league_id FROM club_affiliations WHERE club_id = $1`,
      [freshClub.id],
    );
    log(
      freshRow?.league_id === freshLeague.id,
      "ingestion path persists a non-null league_id when leagueIdMap resolves",
      `got league_id=${freshRow?.league_id}`,
    );

    // --- Backfill semantics: a NULL-league_id row whose source_name
    //     still matches the (un-drifted) name should get linked. -------
    const {
      rows: [legacyLeague],
    } = await client.query(
      `INSERT INTO leagues_master (league_name) VALUES ('MLS NEXT') RETURNING id`,
    );
    const {
      rows: [legacyClub],
    } = await client.query(
      `INSERT INTO canonical_clubs (club_name_canonical) VALUES ('Legacy FC') RETURNING id`,
    );
    await client.query(
      `INSERT INTO club_affiliations (club_id, league_id, source_name)
       VALUES ($1, NULL, 'MLS NEXT')`,
      [legacyClub.id],
    );
    await client.query(`
      UPDATE club_affiliations ca
      SET league_id = lm.id
      FROM leagues_master lm
      WHERE ca.league_id IS NULL
        AND ca.source_name = lm.league_name
    `);
    const {
      rows: [legacyAfter],
    } = await client.query(
      `SELECT league_id FROM club_affiliations WHERE club_id = $1`,
      [legacyClub.id],
    );
    log(
      legacyAfter?.league_id === legacyLeague.id,
      "backfill links legacy NULL-league_id rows by exact source_name match",
      `got league_id=${legacyAfter?.league_id}`,
    );
  } finally {
    await client.query(`DROP SCHEMA ${SCHEMA} CASCADE`).catch(() => undefined);
    await client.end();
  }

  console.log(
    `\n[coverage-league-id] ${pass} passed, ${fail} failed (${pass + fail} total)`,
  );
  if (fail > 0) process.exit(1);
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
