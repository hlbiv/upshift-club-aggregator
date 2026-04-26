/**
 * Sweep orphan `coaches` master rows — rows with zero referencing
 * `coach_discoveries` entries AND `manually_merged = false`.
 *
 * After the April 2026 coach-pollution purge (PR #197) the DELETEs on
 * `coach_discoveries` ran through the `coach_id` FK with ON DELETE SET
 * NULL, which means the parent `coaches` master rows survived as
 * unreferenced husks. A Q2 audit counted ~200 such orphans alongside
 * the Cohort-A buckets we expect the second-wave detector (PR #201) to
 * also produce over time.
 *
 * This script is the cleanup hand. It:
 *   1. Selects `coaches` rows that (a) no `coach_discoveries` row
 *      currently points to and (b) are NOT `manually_merged`.
 *   2. Dumps each target plus any cascade-tied children
 *      (`coach_career_history`, `coach_movement_events`,
 *      `coach_effectiveness`) to a JSONL audit artifact.
 *   3. DELETEs from `coaches` inside a single transaction. The cascade
 *      FKs on the three children tables drop those rows automatically.
 *
 * Dry-run is the default — you MUST pass `--commit` to actually
 * delete. Matches the safety posture of
 * `purge-polluted-coach-discoveries.ts`; opposite of
 * `backfill-coaches-master.ts`.
 *
 * Usage on Replit:
 *
 *   # Preview — no writes, JSONL still produced for review.
 *   pnpm --filter @workspace/scripts run sweep-orphan-coaches
 *
 *   # Delete.
 *   pnpm --filter @workspace/scripts run sweep-orphan-coaches -- --commit
 *
 *   # Override audit dir (default /tmp).
 *   pnpm --filter @workspace/scripts run sweep-orphan-coaches -- \
 *       --commit --audit-dir /home/runner/workspace/artifacts/sweep
 *
 * SAFETY — `manually_merged = true` is sacred. The SELECT filter AND a
 * defensive `AND manually_merged = false` clause on the DELETE
 * statement itself guarantee operator-curated rows are never touched,
 * even if a concurrent writer flips the flag between SELECT and DELETE.
 *
 * Idempotency: a second `--commit` run against an already-swept DB
 * reports "0 targets" and exits 0.
 *
 * Rollback: audit → DELETE → post-cascade residual check → COMMIT, all
 * inside one transaction. Any error throws and rolls back. The JSONL
 * artifact is flushed to disk before the DELETE runs — keep it as the
 * reconstruction source of truth even on a rolled-back run.
 */
import fs from "node:fs";
import path from "node:path";
import { pool } from "@workspace/db";
import { personHash } from "./backfill-coaches-master.js";

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./__tests__/sweep-orphan-coaches.test.ts)
// ---------------------------------------------------------------------------

export type SweepArgs = {
  commit: boolean;
  auditDir: string;
  relink: boolean;
};

export const DEFAULT_AUDIT_DIR = "/tmp";
export const CHUNK_SIZE = 500;

/** Parse a minimal CLI — supports `--commit`, `--audit-dir <p>`,
 * `--audit-dir=<p>`, and `--relink`. Anything else is ignored so we
 * stay forward-compatible with wrapper scripts. */
export function parseArgs(argv: readonly string[]): SweepArgs {
  let commit = false;
  let auditDir = DEFAULT_AUDIT_DIR;
  let relink = false;

  for (let i = 0; i < argv.length; i++) {
    const tok = argv[i];
    if (tok === "--commit") {
      commit = true;
      continue;
    }
    if (tok === "--relink") {
      relink = true;
      continue;
    }
    if (tok === "--audit-dir" && i + 1 < argv.length) {
      auditDir = argv[++i];
      continue;
    }
    if (tok.startsWith("--audit-dir=")) {
      auditDir = tok.slice("--audit-dir=".length);
      continue;
    }
  }

  if (auditDir.length === 0) {
    throw new Error("--audit-dir must not be empty");
  }
  return { commit, auditDir, relink };
}

/** Build the JSONL filename for a run. The timestamp is second-precision
 * but safe for filesystems — ":" and "." are replaced with "-". */
export function buildAuditPath(auditDir: string, now: Date): string {
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  return path.join(auditDir, `orphan-coaches-sweep-${stamp}.jsonl`);
}

/** Split a list into fixed-size chunks. Used to cap the `id = ANY($1)`
 * parameter size on the per-batch fetch. */
export function chunk<T>(xs: readonly T[], size: number): T[][] {
  if (size <= 0) throw new Error("chunk size must be > 0");
  const out: T[][] = [];
  for (let i = 0; i < xs.length; i += size) {
    out.push(xs.slice(i, i + size));
  }
  return out;
}

/** Group a flat list of child rows by their `coach_id`, preserving
 * input order within each group. Defensive: silently drops rows with
 * a non-numeric `coach_id`. */
export function groupByCoachId<C extends { coach_id: unknown }>(
  rows: readonly C[],
): Map<number, C[]> {
  const out = new Map<number, C[]>();
  for (const r of rows) {
    const id = r.coach_id;
    if (typeof id !== "number" || !Number.isFinite(id)) continue;
    const arr = out.get(id) ?? [];
    arr.push(r);
    out.set(id, arr);
  }
  return out;
}

/** Materialize one JSONL record for the audit dump. Stable key order so
 * operators can diff two runs lexically. `careerHistory`,
 * `movementEvents`, `effectiveness` carry whatever the cascade would
 * drop on DELETE — operator can rebuild if something was miscategorized
 * as an orphan. */
export function formatAuditRecord(
  coach: Record<string, unknown>,
  careerHistory: readonly Record<string, unknown>[],
  movementEvents: readonly Record<string, unknown>[],
  effectiveness: readonly Record<string, unknown>[],
): string {
  return (
    JSON.stringify({
      coach,
      careerHistory,
      movementEvents,
      effectiveness,
    }) + "\n"
  );
}

/**
 * Minimal interface satisfied by both `pg.PoolClient` and the test
 * mock — covers the two methods runRelinkPass actually calls.
 */
export interface RelinkClient {
  query<R extends Record<string, unknown> = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ): Promise<{ rows: R[]; rowCount?: number | null }>;
}

/**
 * Scan `coach_discoveries` rows whose `coach_id IS NULL` and re-attach
 * each one to a still-existing `coaches` master row whose `person_hash`
 * matches the discovery's recomputed (post-cutover) hash.
 *
 * Idempotent: returns 0 if there are no NULL discoveries or no hash
 * matches. `manually_merged = true` masters are skipped — the operator
 * may have left a discovery unlinked on purpose to route it elsewhere.
 *
 * Exported for unit-testing — runs against a minimal `RelinkClient`
 * interface so the test can stub `query`.
 */
export async function runRelinkPass(client: RelinkClient): Promise<number> {
  const nullDiscoveries = await client.query<{
    id: number;
    name: string;
    email: string | null;
  }>(
    `SELECT id, name, email
     FROM coach_discoveries
     WHERE coach_id IS NULL
     ORDER BY id`,
  );
  let relinked = 0;
  for (const d of nullDiscoveries.rows) {
    // Compute the post-cutover hash. For email-having rows, the
    // formula is the same regardless of allowRehash; for email-less
    // rows we use the cutover (club-id-less) formula.
    const hash = personHash(d.name, d.email, null, true);
    const hit = await client.query<{ id: number }>(
      `SELECT id FROM coaches
       WHERE person_hash = $1 AND manually_merged = false
       LIMIT 1`,
      [hash],
    );
    if (hit.rows.length === 0) continue;
    await client.query(
      `UPDATE coach_discoveries
       SET coach_id = $1, last_seen_at = now()
       WHERE id = $2 AND coach_id IS NULL`,
      [hit.rows[0].id, d.id],
    );
    relinked += 1;
  }
  return relinked;
}

// ---------------------------------------------------------------------------
// Main (not unit-tested — verified via Replit smoke run)
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const now = new Date();
  const auditPath = buildAuditPath(args.auditDir, now);

  console.log(
    `[sweep-orphan-coaches] commit=${args.commit} relink=${args.relink} ` +
      `audit=${auditPath}`,
  );

  if (!fs.existsSync(args.auditDir)) {
    fs.mkdirSync(args.auditDir, { recursive: true });
  }

  const client = await pool.connect();
  let committed = false;
  try {
    await client.query("BEGIN");

    // Table-existence sanity. If someone runs this before the Path A
    // schema push has landed, fail loud rather than delete nothing
    // silently.
    const probe = await client.query<{ oid: string | null }>(
      `SELECT to_regclass($1) AS oid`,
      ["public.coaches"],
    );
    if (!probe.rows[0] || probe.rows[0].oid === null) {
      throw new Error(
        "coaches table does not exist — run " +
          "`pnpm --filter @workspace/db run push` on Replit first.",
      );
    }

    // Target set: coaches rows with zero referencing discoveries AND
    // manually_merged = false. ORDER BY id for deterministic audit +
    // deterministic DELETE ANY() plan.
    //
    // NOT EXISTS is the right filter here — `LEFT JOIN ... IS NULL`
    // would over-inflate the intermediate result on large tables. PG
    // short-circuits NOT EXISTS.
    const targetResult = await client.query<{ id: number }>(
      `SELECT c.id
       FROM coaches c
       WHERE c.manually_merged = false
         AND NOT EXISTS (
           SELECT 1 FROM coach_discoveries cd WHERE cd.coach_id = c.id
         )
       ORDER BY c.id`,
    );
    const targetIds = targetResult.rows.map((r) => r.id);
    console.log(`  targets: ${targetIds.length}`);

    if (targetIds.length === 0) {
      console.log("  nothing to sweep — no orphan coaches");
      // Even with zero orphans we want --relink to attempt repointing
      // dangling NULL discoveries against the current master set.
      if (args.relink && args.commit) {
        const relinked = await runRelinkPass(client);
        console.log(`  relink: ${relinked} discovery row(s) re-attached`);
      } else if (args.relink && !args.commit) {
        console.log(
          "  [dry-run] --relink skipped — pass --commit to actually relink",
        );
      }
      await client.query(args.commit ? "COMMIT" : "ROLLBACK");
      committed = args.commit;
      return;
    }

    // Audit dump BEFORE any mutation. Stream so we don't hold all rows
    // in Node memory — today this is ~200 rows but this script is the
    // pattern for future orphan passes and may scale.
    const auditStream = fs.createWriteStream(auditPath, { flags: "w" });
    let auditRowsWritten = 0;

    try {
      for (const idBatch of chunk(targetIds, CHUNK_SIZE)) {
        const [coaches, careerHistory, movementEvents, effectiveness] =
          await Promise.all([
            client.query(
              `SELECT * FROM coaches WHERE id = ANY($1::int[]) ORDER BY id`,
              [idBatch],
            ),
            client.query(
              `SELECT * FROM coach_career_history
               WHERE coach_id = ANY($1::int[])
               ORDER BY coach_id, id`,
              [idBatch],
            ),
            client.query(
              `SELECT * FROM coach_movement_events
               WHERE coach_id = ANY($1::int[])
               ORDER BY coach_id, id`,
              [idBatch],
            ),
            client.query(
              `SELECT * FROM coach_effectiveness
               WHERE coach_id = ANY($1::int[])
               ORDER BY coach_id, id`,
              [idBatch],
            ),
          ]);

        const careerByCoach = groupByCoachId(
          careerHistory.rows as Array<{ coach_id: unknown }>,
        );
        const movementByCoach = groupByCoachId(
          movementEvents.rows as Array<{ coach_id: unknown }>,
        );
        const effectivenessByCoach = groupByCoachId(
          effectiveness.rows as Array<{ coach_id: unknown }>,
        );

        for (const c of coaches.rows as Array<Record<string, unknown>>) {
          const cid = c.id;
          if (typeof cid !== "number") continue;
          auditStream.write(
            formatAuditRecord(
              c,
              (careerByCoach.get(cid) ?? []) as Array<Record<string, unknown>>,
              (movementByCoach.get(cid) ?? []) as Array<Record<string, unknown>>,
              (effectivenessByCoach.get(cid) ?? []) as Array<
                Record<string, unknown>
              >,
            ),
          );
          auditRowsWritten += 1;
        }
      }
    } finally {
      auditStream.end();
      await new Promise<void>((resolve, reject) => {
        auditStream.on("finish", () => resolve());
        auditStream.on("error", reject);
      });
    }
    console.log(`  audit rows written: ${auditRowsWritten}`);

    if (auditRowsWritten !== targetIds.length) {
      throw new Error(
        `audit row count mismatch: wrote ${auditRowsWritten} but ` +
          `expected ${targetIds.length} — refusing to proceed with DELETE`,
      );
    }

    if (!args.commit) {
      console.log(
        "  [dry-run] rolling back — pass --commit to actually delete",
      );
      await client.query("ROLLBACK");
      return;
    }

    // DELETE in ONE statement. `manually_merged = false` is redundant
    // with the SELECT filter but defensive against concurrent flips;
    // a row that flipped between SELECT and DELETE should survive, not
    // get mass-deleted. The cascade FKs on coach_career_history /
    // coach_movement_events / coach_effectiveness drop the child rows.
    const deleteResult = await client.query(
      `DELETE FROM coaches
       WHERE id = ANY($1::int[])
         AND manually_merged = false`,
      [targetIds],
    );
    console.log(`  coaches deleted: ${deleteResult.rowCount}`);

    // Row-count check — STRICT equality. Both directions are abort
    // conditions:
    //   - More-than-targeted deletions = a bug in our DELETE predicate
    //     (cannot happen given `id = ANY(...)` but defensive).
    //   - Fewer-than-targeted = something concurrently mutated the
    //     target set between SELECT and DELETE (most likely a flip of
    //     `manually_merged` to true). Better to abort + roll back so
    //     the operator sees the discrepancy than to commit a partial
    //     sweep that the audit JSONL no longer matches.
    if ((deleteResult.rowCount ?? 0) !== targetIds.length) {
      throw new Error(
        `DELETE row count mismatch: deleted ${deleteResult.rowCount ?? 0} ` +
          `but targeted ${targetIds.length} — concurrent mutation suspected, ` +
          "rolling back. Re-run after investigating.",
      );
    }

    // Post-cascade sanity: residual child rows for purged coaches MUST
    // be zero. If any of these fail the cascade FK definition drifted —
    // bail loud so the operator notices before committing.
    for (const [table, label] of [
      ["coach_career_history", "career_history"],
      ["coach_movement_events", "movement_events"],
      ["coach_effectiveness", "effectiveness"],
    ] as const) {
      const residual = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n FROM ${table}
         WHERE coach_id = ANY($1::int[])`,
        [targetIds],
      );
      const residualCount = Number(residual.rows[0]?.n ?? "0");
      console.log(
        `  ${label} residual after cascade: ${residualCount} (expected 0)`,
      );
      if (residualCount !== 0) {
        throw new Error(
          `cascade failed on ${table} — ${residualCount} rows still ` +
            "reference deleted coaches. Check FK ON DELETE behavior.",
        );
      }
    }

    // Extra belt-and-suspenders: ensure no manually_merged = true row
    // was somehow deleted. Re-SELECT by id with the "must survive"
    // predicate inverted — under strict-equality the row count check
    // already aborted on any concurrent flip, so this MUST be zero.
    const safetyCheck = await client.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM coaches
       WHERE id = ANY($1::int[]) AND manually_merged = true`,
      [targetIds],
    );
    const survivorCount = Number(safetyCheck.rows[0]?.n ?? "0");
    if (survivorCount > 0) {
      // Should be unreachable given strict-equality above; if we get
      // here a serializable-isolation violation occurred. Bail.
      throw new Error(
        `safety violation: ${survivorCount} manually_merged coach row(s) ` +
          "remain after a DELETE that supposedly skipped them — aborting.",
      );
    }

    // Optional relink pass: scan coach_discoveries with coach_id IS
    // NULL for rows whose recomputed person_hash matches a still-
    // existing master `coaches` row. This catches the case where the
    // orphan we just deleted shared a hash with a kept master — its
    // referencing discoveries (now NULL via ON DELETE SET NULL) can be
    // re-pointed at the survivor.
    let relinked = 0;
    if (args.relink) {
      relinked = await runRelinkPass(client);
      console.log(`  relink: ${relinked} discovery row(s) re-attached`);
    }

    await client.query("COMMIT");
    committed = true;
    console.log(
      `[sweep-orphan-coaches] done — committed ` +
        `(deleted=${deleteResult.rowCount ?? 0}, relinked=${relinked})`,
    );
  } catch (err) {
    if (!committed) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // best-effort — original error is more interesting
      }
    }
    throw err;
  } finally {
    client.release();
  }
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
