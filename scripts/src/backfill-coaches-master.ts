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
 *   - Coaches WITHOUT an email (default): hash(normalized_name + '|no-email|'
 *     + club_id). This deliberately makes "Mike Smith at Club A" and "Mike
 *     Smith at Club B" distinct rows until a manual merge upgrades
 *     `manually_merged`. Better to over-split than to collapse 8 distinct
 *     coaches into one.
 *   - Coaches WITHOUT an email AND `--allow-rehash`: hash(normalized_name +
 *     '|no-email'). Drops the clubId tail so the same name across multiple
 *     clubs collapses to a single master row. This is the one-shot
 *     irreversible cutover documented in CLAUDE.md ("Coach person_hash
 *     rehash cutover"). With this flag the script:
 *       1. Recomputes hashes under the new formula for email-less rows.
 *       2. Detects collisions where two existing `coaches` rows would now
 *          collapse to one (auto-merge candidates).
 *       3. Emits a JSONL audit of every would-be-merged pair to
 *          /tmp/coach-rehash-cutover-<timestamp>.jsonl BEFORE any mutation.
 *       4. In `--dry-run` mode: rolls back. In `--commit` mode: applies.
 *       5. NEVER touches `coaches.manually_merged = true` rows — operator
 *          curation always wins. Filtered out in the SELECT AND with a
 *          redundant `manually_merged = false` predicate on every UPDATE
 *          and DELETE statement.
 *
 * Output: a run summary of counts (coaches inserted, discoveries linked,
 * club_coaches absorbed, skipped).
 */

import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pool, db } from "@workspace/db";
import { coaches, coachDiscoveries } from "@workspace/db/schema";
import { eq, sql } from "drizzle-orm";

const DRY = process.argv.includes("--dry-run");
const ALLOW_REHASH = process.argv.includes("--allow-rehash");
const COMMIT = process.argv.includes("--commit");
const REHASH_AUDIT_DIR = "/tmp";

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./__tests__/backfill-coaches-master.test.ts)
// ---------------------------------------------------------------------------

export function normalizeName(raw: string): string {
  return raw
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s'-]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Compute the `person_hash` for a coach.
 *
 * Three cases:
 *   - email present: hash(normalized_name + '|' + lower(email))
 *   - email absent + allowRehash=false (default): hash(normalized_name +
 *     '|no-email|' + clubId) — current production behavior, splits a coach
 *     across clubs.
 *   - email absent + allowRehash=true: hash(normalized_name + '|no-email')
 *     — cutover behavior, collapses a coach across clubs into one master row.
 */
export function personHash(
  name: string,
  email: string | null,
  clubId: number | null,
  allowRehash = false,
): string {
  const n = normalizeName(name);
  let key: string;
  if (email) {
    key = `${n}|${email.trim().toLowerCase()}`;
  } else if (allowRehash) {
    key = `${n}|no-email`;
  } else {
    key = `${n}|no-email|${clubId ?? "null"}`;
  }
  return createHash("sha256").update(key).digest("hex");
}

/** Build the JSONL filename for a rehash cutover audit run. The timestamp
 * is second-precision but safe for filesystems — ":" and "." are
 * replaced with "-". */
export function buildRehashAuditPath(auditDir: string, now: Date): string {
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  return path.join(auditDir, `coach-rehash-cutover-${stamp}.jsonl`);
}

/** Materialize one JSONL record describing a planned merge. Stable key
 * order so operators can diff two runs lexically. `losers` are the coach
 * rows that will be deleted (their referencing discoveries re-pointed to
 * `winner`); `winner` is the row kept. */
export function formatRehashAuditRecord(
  newHash: string,
  winner: Record<string, unknown>,
  losers: readonly Record<string, unknown>[],
): string {
  return (
    JSON.stringify({
      newHash,
      winner,
      losers,
    }) + "\n"
  );
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
      const hash = personHash(row.name, row.email, row.clubId, ALLOW_REHASH);

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

/**
 * One-shot cutover: rehash email-less `coaches` master rows under the
 * club-id-less formula and merge collisions where two existing master
 * rows would now collapse to one.
 *
 * Behavior:
 *   - Selects every `coaches` row whose `primary_email` is NULL AND
 *     `manually_merged = false`.
 *   - For each row, the script asks the question "what is the most
 *     recently-seen display_name across this row's referencing
 *     coach_discoveries?" — that's the canonical name we'll re-derive
 *     a hash from. If the row has zero discoveries, fall back to
 *     `coaches.display_name`.
 *   - Recomputes person_hash with allowRehash=true.
 *   - Groups rows by recomputed hash. Singleton groups (size 1) are
 *     UPDATEd in place: just rewrite their person_hash. Multi-row
 *     groups become merge candidates: keep the lowest-id row as the
 *     "winner", the others become "losers" whose referencing
 *     coach_discoveries are re-pointed to the winner, and then the
 *     loser rows are DELETEd.
 *   - JSONL audit of every group (singleton + merge) is written
 *     BEFORE any mutation. In `--dry-run` the txn rolls back; in
 *     `--commit` it commits.
 *
 * SAFETY — `manually_merged = true` is sacred. Filtered out in the
 * SELECT and a redundant `manually_merged = false` predicate is added
 * to every UPDATE / DELETE statement.
 *
 * Runs INSIDE the `pool`'s default connection — uses an explicit
 * client checkout so the BEGIN / COMMIT scope is real.
 */
async function rehashCutover(): Promise<{
  scanned: number;
  rehashedInPlace: number;
  mergeGroups: number;
  losersDeleted: number;
  discoveriesRepointed: number;
  auditPath: string;
}> {
  const now = new Date();
  const auditPath = buildRehashAuditPath(REHASH_AUDIT_DIR, now);
  if (!fs.existsSync(REHASH_AUDIT_DIR)) {
    fs.mkdirSync(REHASH_AUDIT_DIR, { recursive: true });
  }

  const client = await pool.connect();
  let committed = false;
  try {
    await client.query("BEGIN");

    // Pull every email-less, non-manually-merged master row plus its
    // most-recent referencing discovery name (for canonical name
    // re-derivation). The LATERAL join falls back to coaches.display_name
    // when no discoveries reference the master row.
    const candidates = await client.query<{
      id: number;
      display_name: string;
      person_hash: string;
      canonical_name: string;
    }>(
      `SELECT
         c.id,
         c.display_name,
         c.person_hash,
         COALESCE(latest.name, c.display_name) AS canonical_name
       FROM coaches c
       LEFT JOIN LATERAL (
         SELECT cd.name
         FROM coach_discoveries cd
         WHERE cd.coach_id = c.id
         ORDER BY cd.last_seen_at DESC NULLS LAST, cd.id DESC
         LIMIT 1
       ) latest ON true
       WHERE c.primary_email IS NULL
         AND c.manually_merged = false
       ORDER BY c.id`,
    );

    const scanned = candidates.rows.length;
    console.log(`  rehash scan: ${scanned} email-less master row(s)`);

    if (scanned === 0) {
      // No email-less rows to rehash — close out the empty txn cleanly.
      await client.query(COMMIT && !DRY ? "COMMIT" : "ROLLBACK");
      committed = COMMIT && !DRY;
      return {
        scanned: 0,
        rehashedInPlace: 0,
        mergeGroups: 0,
        losersDeleted: 0,
        discoveriesRepointed: 0,
        auditPath,
      };
    }

    // Group candidates by recomputed hash.
    const byHash = new Map<
      string,
      Array<{ id: number; display_name: string; old_hash: string; canonical_name: string }>
    >();
    for (const row of candidates.rows) {
      const newHash = personHash(row.canonical_name, null, null, true);
      const bucket = byHash.get(newHash) ?? [];
      bucket.push({
        id: row.id,
        display_name: row.display_name,
        old_hash: row.person_hash,
        canonical_name: row.canonical_name,
      });
      byHash.set(newHash, bucket);
    }

    // Audit dump BEFORE any mutation.
    const auditStream = fs.createWriteStream(auditPath, { flags: "w" });
    let mergeGroups = 0;
    let plannedRehashedInPlace = 0;
    try {
      for (const [newHash, group] of byHash.entries()) {
        // Sort by id so the lowest-id row is always the winner — stable
        // and easy for an operator to audit.
        group.sort((a, b) => a.id - b.id);
        const winner = group[0];
        const losers = group.slice(1);
        if (losers.length === 0) {
          // Singleton — just an in-place rehash if the hash actually changed.
          if (winner.old_hash !== newHash) {
            plannedRehashedInPlace += 1;
            auditStream.write(
              formatRehashAuditRecord(newHash, winner, []),
            );
          }
          continue;
        }
        mergeGroups += 1;
        auditStream.write(formatRehashAuditRecord(newHash, winner, losers));
      }
    } finally {
      auditStream.end();
      await new Promise<void>((resolve, reject) => {
        auditStream.on("finish", () => resolve());
        auditStream.on("error", reject);
      });
    }
    console.log(
      `  audit: ${auditPath} (in-place=${plannedRehashedInPlace}, merge-groups=${mergeGroups})`,
    );

    // Apply mutations. Inside one transaction; any error rolls back.
    let rehashedInPlace = 0;
    let losersDeleted = 0;
    let discoveriesRepointed = 0;
    for (const [newHash, group] of byHash.entries()) {
      group.sort((a, b) => a.id - b.id);
      const winner = group[0];
      const losers = group.slice(1);

      if (losers.length === 0) {
        if (winner.old_hash === newHash) continue;
        // In-place rehash. Redundant manually_merged guard.
        const upd = await client.query(
          `UPDATE coaches
           SET person_hash = $1, updated_at = now()
           WHERE id = $2 AND manually_merged = false`,
          [newHash, winner.id],
        );
        rehashedInPlace += upd.rowCount ?? 0;
        continue;
      }

      // Re-point discoveries from losers to winner. Redundant
      // manually_merged guard via subquery so a concurrent flip
      // does NOT clobber a now-curated row.
      const loserIds = losers.map((l) => l.id);
      const repoint = await client.query(
        `UPDATE coach_discoveries
         SET coach_id = $1, last_seen_at = now()
         WHERE coach_id = ANY($2::int[])
           AND coach_id IN (
             SELECT id FROM coaches
             WHERE id = ANY($2::int[])
               AND manually_merged = false
           )`,
        [winner.id, loserIds],
      );
      discoveriesRepointed += repoint.rowCount ?? 0;

      // Bump the winner's hash to the new value (if changed).
      if (winner.old_hash !== newHash) {
        await client.query(
          `UPDATE coaches
           SET person_hash = $1, updated_at = now()
           WHERE id = $2 AND manually_merged = false`,
          [newHash, winner.id],
        );
      }

      // Drop the loser rows. Redundant manually_merged guard so a
      // concurrent flip cannot mass-delete a curated row.
      const del = await client.query(
        `DELETE FROM coaches
         WHERE id = ANY($1::int[])
           AND manually_merged = false`,
        [loserIds],
      );
      losersDeleted += del.rowCount ?? 0;
    }

    if (DRY) {
      console.log("  [rehash dry-run] rolling back");
      await client.query("ROLLBACK");
      return {
        scanned,
        rehashedInPlace,
        mergeGroups,
        losersDeleted,
        discoveriesRepointed,
        auditPath,
      };
    }

    if (!COMMIT) {
      throw new Error(
        "--allow-rehash requires either --dry-run or --commit (refusing to mutate without an explicit verb)",
      );
    }

    await client.query("COMMIT");
    committed = true;
    console.log(
      `  [rehash committed] in-place=${rehashedInPlace}, merge-groups=${mergeGroups}, ` +
        `discoveries-repointed=${discoveriesRepointed}, losers-deleted=${losersDeleted}`,
    );
    return {
      scanned,
      rehashedInPlace,
      mergeGroups,
      losersDeleted,
      discoveriesRepointed,
      auditPath,
    };
  } catch (err) {
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // best-effort
      }
    }
    throw err;
  } finally {
    client.release();
  }
}

async function main() {
  console.log(
    `[backfill-coaches-master] dry=${DRY} commit=${COMMIT} ` +
      `allow_rehash=${ALLOW_REHASH}`,
  );

  if (ALLOW_REHASH) {
    if (!DRY && !COMMIT) {
      throw new Error(
        "--allow-rehash requires either --dry-run or --commit",
      );
    }
    if (COMMIT) {
      // Auto-merge cutover is deliberately disabled. The name-only
      // hash collapses same-name strangers (e.g. two different "John
      // Smith" coaches at two different clubs), which is unsafe in
      // youth soccer where common names are common and email capture
      // is spotty. The proper fix is a candidate-pair review queue —
      // see docs/coach-merge-candidate-queue.md. Use --dry-run only
      // to generate the audit JSONL for cardinality analysis.
      throw new Error(
        "--commit --allow-rehash is locked. Auto-merge is unsafe; " +
          "see docs/coach-merge-candidate-queue.md. Use --dry-run " +
          "--allow-rehash to generate audit JSONL only.",
      );
    }
    console.log(
      "Step 0 — coach person_hash rehash cutover (--allow-rehash, dry-run only)",
    );
    const r = await rehashCutover();
    console.log(`  rehash scanned:           ${r.scanned}`);
    console.log(`  rehashed in place:        ${r.rehashedInPlace}`);
    console.log(`  merge groups:             ${r.mergeGroups}`);
    console.log(`  discoveries re-pointed:   ${r.discoveriesRepointed}`);
    console.log(`  loser coaches deleted:    ${r.losersDeleted}`);
    console.log(`  audit:                    ${r.auditPath}`);
  }

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

// Only run main() when invoked as a script — this lets the test file
// import the pure helpers without triggering the DB connection.
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
