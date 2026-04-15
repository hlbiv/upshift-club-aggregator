/**
 * Backfill the `coaches` master table from existing `coach_discoveries` rows,
 * and absorb any legacy `club_coaches` rows (same schema shape) into
 * `coach_discoveries` along the way.
 *
 *   pnpm --filter @workspace/scripts exec tsx src/backfill-coaches-master.ts --dry-run
 *   pnpm --filter @workspace/scripts exec tsx src/backfill-coaches-master.ts
 *
 * The script is idempotent. Safe to re-run:
 *   - `coaches` is dedup'd by `person_hash` (sha256 of normalized name +
 *     lowercased email). INSERT ON CONFLICT DO NOTHING.
 *   - `coach_discoveries.coach_id` is updated when NULL or when the current
 *     coach_id's hash differs from the recomputed one.
 *   - Absorbed `club_coaches` rows use ON CONFLICT DO NOTHING on the
 *     existing `(club_id, name, title)` unique constraint, so re-runs are
 *     no-ops for rows that already landed.
 *
 * Ambiguity policy for `person_hash`:
 *   - Coaches WITH an email: hash(normalized_name + lowercased_email). Safe.
 *   - Coaches WITHOUT an email: hash(normalized_name + '|no-email|' + club_id).
 *     This deliberately makes "Mike Smith at Club A" and "Mike Smith at
 *     Club B" distinct rows until a manual merge upgrades `manually_merged`.
 *     Better to over-split than to collapse 8 distinct coaches into one.
 *
 * Output: a run summary of counts (coaches inserted, discoveries linked,
 * club_coaches absorbed, skipped).
 */

import { createHash } from "node:crypto";
import { pool, db } from "@workspace/db";
import { coaches, coachDiscoveries } from "@workspace/db/schema";
import { eq, isNull, sql } from "drizzle-orm";

const DRY = process.argv.includes("--dry-run");

function normalizeName(raw: string): string {
  return raw
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function personHash(name: string, email: string | null, clubId: number | null) {
  const n = normalizeName(name);
  const key = email
    ? `${n}|${email.trim().toLowerCase()}`
    : `${n}|no-email|${clubId ?? "null"}`;
  return createHash("sha256").update(key).digest("hex");
}

async function tableExists(name: string): Promise<boolean> {
  const { rows } = await pool.query(
    `SELECT to_regclass($1) AS oid`,
    [`public.${name}`],
  );
  return rows[0]?.oid !== null;
}

async function absorbClubCoaches(): Promise<number> {
  if (!(await tableExists("club_coaches"))) {
    console.log("  [skip] legacy club_coaches table not present");
    return 0;
  }
  if (DRY) {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM club_coaches`,
    );
    console.log(
      `  [dry] would absorb up to ${rows[0].n} club_coaches row(s)`,
    );
    return 0;
  }
  // platform_family is NOT NULL — set 'unknown' explicitly rather than
  // relying on the schema default. first_seen_at + last_seen_at mirror
  // the legacy scraped_at so historical coaches don't get today's date.
  const { rowCount } = await pool.query(`
    INSERT INTO coach_discoveries
      (club_id, name, title, email, phone, source_url, scraped_at,
       confidence, platform_family, first_seen_at, last_seen_at)
    SELECT
      cc.club_id,
      cc.name,
      COALESCE(cc.title, ''),
      cc.email,
      cc.phone,
      cc.source_url,
      COALESCE(cc.scraped_at, now()),
      COALESCE(cc.confidence_score, 1.0),
      'unknown',
      COALESCE(cc.scraped_at, now()),
      COALESCE(cc.scraped_at, now())
    FROM club_coaches cc
    ON CONFLICT ON CONSTRAINT coach_discoveries_club_name_title_uq
      DO NOTHING
  `);
  return rowCount ?? 0;
}

async function backfillCoachesMaster(): Promise<{
  inserted: number;
  linked: number;
  scanned: number;
  skippedMerged: number;
  errors: number;
}> {
  let inserted = 0;
  let linked = 0;
  let scanned = 0;
  let skippedMerged = 0;
  let errors = 0;

  const BATCH = 500;
  let lastId = 0;
  // paginate by id — stable, avoids OFFSET scan costs
  while (true) {
    const batch = await db
      .select({
        id: coachDiscoveries.id,
        clubId: coachDiscoveries.clubId,
        name: coachDiscoveries.name,
        email: coachDiscoveries.email,
        coachId: coachDiscoveries.coachId,
      })
      .from(coachDiscoveries)
      .where(sql`${coachDiscoveries.id} > ${lastId}`)
      .orderBy(coachDiscoveries.id)
      .limit(BATCH);

    if (batch.length === 0) break;

    for (const row of batch) {
      scanned++;
      lastId = row.id;
      const hash = personHash(row.name, row.email, row.clubId);

      if (DRY) {
        continue;
      }

      try {
        // Upsert master (ignore conflict on person_hash).
        const up = await db
          .insert(coaches)
          .values({
            personHash: hash,
            displayName: row.name,
            primaryEmail: row.email ?? null,
          })
          .onConflictDoNothing({ target: coaches.personHash })
          .returning({
            id: coaches.id,
            manuallyMerged: coaches.manuallyMerged,
          });

        let coachId: number;
        let manuallyMerged = false;
        if (up.length > 0) {
          inserted++;
          coachId = up[0].id;
          manuallyMerged = up[0].manuallyMerged ?? false;
        } else {
          const [existing] = await db
            .select({
              id: coaches.id,
              manuallyMerged: coaches.manuallyMerged,
            })
            .from(coaches)
            .where(eq(coaches.personHash, hash));
          if (!existing) {
            // Row vanished between INSERT and SELECT (delete race, schema
            // drift, etc). Skip rather than aborting the whole batch.
            errors++;
            console.warn(
              `  [warn] coaches row missing after ON CONFLICT; hash=${hash} discovery_id=${row.id}`,
            );
            continue;
          }
          coachId = existing.id;
          manuallyMerged = existing.manuallyMerged ?? false;
        }

        // Respect human-curated merges. If the master row is manually
        // merged, don't touch discoveries — operator decisions win over
        // hash-based backfill, both for already-linked AND unlinked
        // discoveries (an operator may have intentionally left a
        // discovery unlinked to route it elsewhere later).
        if (manuallyMerged && row.coachId !== coachId) {
          skippedMerged++;
          continue;
        }

        if (row.coachId !== coachId) {
          await db
            .update(coachDiscoveries)
            .set({ coachId, lastSeenAt: new Date() })
            .where(eq(coachDiscoveries.id, row.id));
          linked++;
        }
      } catch (err) {
        errors++;
        console.warn(
          `  [warn] row failed discovery_id=${row.id} name=${JSON.stringify(row.name)}: ${(err as Error).message}`,
        );
      }
    }
    process.stdout.write(`  scanned ${scanned} discovery rows…\r`);
  }
  process.stdout.write("\n");
  return { inserted, linked, scanned, skippedMerged, errors };
}

async function main() {
  console.log(`[backfill-coaches-master] dry=${DRY}`);

  console.log("Step 1 — absorb legacy club_coaches (if present)");
  const absorbed = await absorbClubCoaches();
  console.log(`  absorbed ${absorbed} rows`);

  console.log("Step 2 — populate coaches master from coach_discoveries");
  const { inserted, linked, scanned, skippedMerged, errors } =
    await backfillCoachesMaster();

  console.log("\n---");
  console.log(`Scanned:               ${scanned}`);
  console.log(`Coaches inserted:      ${inserted}`);
  console.log(`Discoveries linked:    ${linked}`);
  console.log(`Skipped (merged):      ${skippedMerged}`);
  console.log(`Row errors (logged):   ${errors}`);
  console.log(`Club_coaches absorbed: ${absorbed}`);
  console.log(DRY ? "[dry] no changes written" : "[done]");
}

main()
  .catch((e) => {
    console.error(e);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
