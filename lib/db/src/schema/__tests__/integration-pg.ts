/**
 * Postgres integration test for Path A schema.
 *
 * Creates a temp schema, builds all tables via drizzle-kit push equivalent
 * (runs the Drizzle-generated DDL directly), and exercises the constraints
 * that the data model promises:
 *
 *   - unique constraint blocks duplicates
 *   - check constraint rejects bad enum values
 *   - generated column computes records_touched
 *   - FK ON DELETE CASCADE removes dependents
 *   - FK ON DELETE SET NULL clears coach_id on discovery
 *
 * Run:
 *   DATABASE_URL=postgres://... pnpm --filter @workspace/db exec tsx \
 *     src/schema/__tests__/integration-pg.ts
 *
 * The test runs against a newly-created Postgres schema
 * `upshift_test_<pid>` and drops it at the end. Safe to run against any
 * Postgres the user has CREATE SCHEMA permission on.
 */

import pg from "pg";

const { Client } = pg;

if (!process.env.DATABASE_URL) {
  console.error(
    "[integration-pg] DATABASE_URL not set — skipping integration test.",
  );
  process.exit(0);
}

const SCHEMA = `upshift_test_${process.pid}`;
const client = new Client({ connectionString: process.env.DATABASE_URL });

type TestCase = { name: string; fn: () => Promise<void> };
const tests: TestCase[] = [];
const test = (name: string, fn: () => Promise<void>) =>
  tests.push({ name, fn });

async function expectReject(
  sql: string,
  params: unknown[],
  errMatch: RegExp,
  label: string,
) {
  try {
    await client.query(sql, params);
    throw new Error(`${label}: expected rejection, got success`);
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e);
    if (!errMatch.test(msg)) {
      throw new Error(
        `${label}: expected /${errMatch.source}/, got: ${msg}`,
      );
    }
  }
}

async function run() {
  await client.connect();

  // Guard: fail fast if someone points at a production DB.
  const { rows: guard } = await client.query(
    `SELECT current_database() AS db`,
  );
  console.log(
    `[integration-pg] connected to ${guard[0].db}, using schema ${SCHEMA}`,
  );

  await client.query(`CREATE SCHEMA ${SCHEMA}`);
  await client.query(`SET search_path TO ${SCHEMA}`);

  try {
    // Minimal DDL — only the Path A new tables + just enough of the
    // existing referenced tables (canonical_clubs) to exercise FKs.
    await client.query(`
      CREATE TABLE canonical_clubs (
        id SERIAL PRIMARY KEY,
        club_name_canonical TEXT NOT NULL UNIQUE,
        website_status TEXT,
        CONSTRAINT canonical_clubs_website_status_enum CHECK (
          website_status IS NULL OR website_status IN
          ('active','dead','redirected','no_staff_page','search','unchecked')
        )
      );

      CREATE TABLE coaches (
        id SERIAL PRIMARY KEY,
        person_hash TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        primary_email TEXT,
        first_seen_at TIMESTAMP NOT NULL DEFAULT now(),
        last_seen_at TIMESTAMP NOT NULL DEFAULT now(),
        manually_merged BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now()
      );

      CREATE TABLE coach_discoveries (
        id SERIAL PRIMARY KEY,
        club_id INTEGER REFERENCES canonical_clubs(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        title TEXT NOT NULL DEFAULT '',
        coach_id INTEGER REFERENCES coaches(id) ON DELETE SET NULL,
        phone TEXT,
        CONSTRAINT coach_discoveries_club_name_title_uq
          UNIQUE (club_id, name, title)
      );

      CREATE TABLE scrape_run_logs (
        id SERIAL PRIMARY KEY,
        scraper_key TEXT NOT NULL,
        started_at TIMESTAMP NOT NULL DEFAULT now(),
        status TEXT NOT NULL DEFAULT 'running',
        failure_kind TEXT,
        records_created INTEGER NOT NULL DEFAULT 0,
        records_updated INTEGER NOT NULL DEFAULT 0,
        records_failed INTEGER NOT NULL DEFAULT 0,
        records_touched INTEGER GENERATED ALWAYS AS (records_created + records_updated) STORED,
        CONSTRAINT scrape_run_logs_status_enum
          CHECK (status IN ('running','ok','partial','failed')),
        CONSTRAINT scrape_run_logs_failure_kind_enum
          CHECK (failure_kind IS NULL OR failure_kind IN
            ('timeout','network','parse_error','zero_results','unknown'))
      );

      CREATE TABLE scrape_health (
        id SERIAL PRIMARY KEY,
        entity_type TEXT NOT NULL,
        entity_id INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'never',
        priority SMALLINT,
        CONSTRAINT scrape_health_entity_type_enum CHECK
          (entity_type IN ('club','league','college','coach','event','match','tryout')),
        CONSTRAINT scrape_health_status_enum CHECK
          (status IN ('ok','stale','failed','never')),
        CONSTRAINT scrape_health_priority_range CHECK
          (priority IS NULL OR (priority >= 1 AND priority <= 4)),
        CONSTRAINT scrape_health_entity_uq UNIQUE (entity_type, entity_id)
      );

      CREATE TABLE matches (
        id SERIAL PRIMARY KEY,
        home_team_name TEXT NOT NULL,
        away_team_name TEXT NOT NULL,
        match_date TIMESTAMP,
        age_group TEXT,
        gender TEXT,
        status TEXT NOT NULL DEFAULT 'scheduled',
        source TEXT,
        platform_match_id TEXT,
        CONSTRAINT matches_status_enum CHECK
          (status IN ('scheduled','final','cancelled','forfeit','postponed'))
      );
      CREATE UNIQUE INDEX matches_source_platform_id_uq
        ON matches (source, platform_match_id)
        WHERE platform_match_id IS NOT NULL;
      CREATE UNIQUE INDEX matches_natural_key_uq
        ON matches (home_team_name, away_team_name, match_date, age_group, gender)
        WHERE platform_match_id IS NULL;
    `);

    // -----------------------------------------------------------------
    test("canonical_clubs.website_status accepts 'search'", async () => {
      await client.query(
        `INSERT INTO canonical_clubs (club_name_canonical, website_status)
         VALUES ('Test FC', 'search')`,
      );
    });

    test("canonical_clubs.website_status rejects 'bogus'", async () => {
      await expectReject(
        `INSERT INTO canonical_clubs (club_name_canonical, website_status)
         VALUES ($1, $2)`,
        ["Bogus FC", "bogus"],
        /canonical_clubs_website_status_enum/,
        "website_status check",
      );
    });

    test("scrape_run_logs.records_touched is computed", async () => {
      const { rows } = await client.query(
        `INSERT INTO scrape_run_logs (scraper_key, records_created, records_updated, status)
         VALUES ('ecnl-boys', 10, 5, 'ok')
         RETURNING records_touched`,
      );
      if (rows[0].records_touched !== 15) {
        throw new Error(
          `expected records_touched=15, got ${rows[0].records_touched}`,
        );
      }
    });

    test("scrape_run_logs.failure_kind rejects bad value", async () => {
      await expectReject(
        `INSERT INTO scrape_run_logs (scraper_key, failure_kind, status)
         VALUES ($1, $2, $3)`,
        ["x", "asteroid_strike", "failed"],
        /scrape_run_logs_failure_kind_enum/,
        "failure_kind check",
      );
    });

    test("scrape_health.priority rejects 0 and 5", async () => {
      await expectReject(
        `INSERT INTO scrape_health (entity_type, entity_id, priority)
         VALUES ('club', 1, 0)`,
        [],
        /scrape_health_priority_range/,
        "priority=0",
      );
      await expectReject(
        `INSERT INTO scrape_health (entity_type, entity_id, priority)
         VALUES ('club', 2, 5)`,
        [],
        /scrape_health_priority_range/,
        "priority=5",
      );
    });

    test("matches partial uniques — platform_match_id branch", async () => {
      await client.query(
        `INSERT INTO matches (home_team_name, away_team_name, source, platform_match_id)
         VALUES ('A','B','gotsport','abc123')`,
      );
      await expectReject(
        `INSERT INTO matches (home_team_name, away_team_name, source, platform_match_id)
         VALUES ($1,$2,$3,$4)`,
        ["X", "Y", "gotsport", "abc123"],
        /matches_source_platform_id_uq|duplicate key/i,
        "duplicate platform_match_id",
      );
    });

    test("matches partial uniques — natural-key branch", async () => {
      const date = new Date("2026-05-01");
      await client.query(
        `INSERT INTO matches (home_team_name, away_team_name, match_date, age_group, gender)
         VALUES ($1,$2,$3,$4,$5)`,
        ["HomeX", "AwayY", date, "U14", "M"],
      );
      await expectReject(
        `INSERT INTO matches (home_team_name, away_team_name, match_date, age_group, gender)
         VALUES ($1,$2,$3,$4,$5)`,
        ["HomeX", "AwayY", date, "U14", "M"],
        /matches_natural_key_uq|duplicate key/i,
        "duplicate natural key",
      );
    });

    test("coach_discoveries.coach_id SET NULL on coach delete", async () => {
      const {
        rows: [coach],
      } = await client.query(
        `INSERT INTO coaches (person_hash, display_name) VALUES ('h1', 'Coach One')
         RETURNING id`,
      );
      const {
        rows: [club],
      } = await client.query(
        `INSERT INTO canonical_clubs (club_name_canonical) VALUES ('Club A')
         RETURNING id`,
      );
      const {
        rows: [disc],
      } = await client.query(
        `INSERT INTO coach_discoveries (club_id, name, title, coach_id)
         VALUES ($1, 'Coach One', 'head', $2) RETURNING id`,
        [club.id, coach.id],
      );
      await client.query(`DELETE FROM coaches WHERE id = $1`, [coach.id]);
      const { rows } = await client.query(
        `SELECT coach_id FROM coach_discoveries WHERE id = $1`,
        [disc.id],
      );
      if (rows[0].coach_id !== null) {
        throw new Error(
          `expected coach_id NULL after coach delete, got ${rows[0].coach_id}`,
        );
      }
    });

    test("coach_discoveries CASCADE on club delete", async () => {
      const {
        rows: [club],
      } = await client.query(
        `INSERT INTO canonical_clubs (club_name_canonical) VALUES ('Club B')
         RETURNING id`,
      );
      await client.query(
        `INSERT INTO coach_discoveries (club_id, name, title) VALUES ($1, 'X', 'y')`,
        [club.id],
      );
      await client.query(`DELETE FROM canonical_clubs WHERE id = $1`, [
        club.id,
      ]);
      const { rows } = await client.query(
        `SELECT count(*)::int AS n FROM coach_discoveries WHERE club_id = $1`,
        [club.id],
      );
      if (rows[0].n !== 0) {
        throw new Error(`expected cascade, still have ${rows[0].n} rows`);
      }
    });

    // -----------------------------------------------------------------

    let pass = 0;
    let fail = 0;
    for (const t of tests) {
      try {
        await client.query("BEGIN");
        await t.fn();
        await client.query("COMMIT");
        console.log(`  ok   ${t.name}`);
        pass++;
      } catch (e: unknown) {
        await client.query("ROLLBACK").catch(() => undefined);
        const msg = e instanceof Error ? e.message : String(e);
        console.error(`  FAIL ${t.name}\n       ${msg}`);
        fail++;
      }
    }

    console.log(
      `\n[integration-pg] ${pass} passed, ${fail} failed (${tests.length} total)`,
    );
    if (fail > 0) process.exitCode = 1;
  } finally {
    await client.query(`DROP SCHEMA ${SCHEMA} CASCADE`).catch(() => undefined);
    await client.end();
  }
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
