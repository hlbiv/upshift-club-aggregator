/**
 * Purge polluted `coach_discoveries` rows flagged by the
 * coach-pollution detector.
 *
 * Selects rows referenced by an UNRESOLVED `coach_quality_flags` entry
 * of the configured flag type (default `looks_like_name_reject`), dumps
 * every targeted row + its flags + its parent `coaches` row to a JSONL
 * audit artifact, then DELETEs from `coach_discoveries`. The CASCADE
 * FK on `coach_quality_flags.discovery_id` cleans the flag rows
 * automatically.
 *
 * Usage on Replit (dry-run default for safety — you MUST pass --commit
 * to actually delete):
 *
 *   # Preview — no writes, JSONL still produced for review.
 *   pnpm --filter @workspace/scripts exec tsx \
 *       src/purge-polluted-coach-discoveries.ts
 *
 *   # Delete.
 *   pnpm --filter @workspace/scripts exec tsx \
 *       src/purge-polluted-coach-discoveries.ts --commit
 *
 *   # Override audit dir (default /tmp) or target flag type.
 *   pnpm --filter @workspace/scripts exec tsx \
 *       src/purge-polluted-coach-discoveries.ts \
 *       --commit --audit-dir /home/runner/workspace/artifacts/purge \
 *       --flag-type looks_like_name_reject
 *
 * Idempotency: a second --commit run against an already-purged DB
 * reports "0 targets" and exits 0. Flags that an operator has
 * manually marked resolved (`resolved_at IS NOT NULL`) are NOT
 * purged — the operator's triage decision wins.
 *
 * Coaches master: `coach_discoveries.coach_id` is ON DELETE SET NULL,
 * so this script DOES NOT delete any `coaches` rows. A coach row
 * whose only discoveries were polluted becomes orphaned (zero linked
 * discoveries). Orphan-coach cleanup is a separate decision and lives
 * in a follow-up PR — we preserve the `coaches.manually_merged = true`
 * guarantee by touching only `coach_discoveries`.
 *
 * Rollback: the DELETE is inside one transaction with the pre-delete
 * audit. On any error the transaction rolls back. The JSONL artifact
 * is already flushed to disk at that point — keep it as your source of
 * truth for reconstruction, even on a rolled-back run.
 */
import fs from "node:fs";
import path from "node:path";
import { pool } from "@workspace/db";

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./__tests__/purge-polluted-coach-discoveries.test.ts)
// ---------------------------------------------------------------------------

export type PurgeArgs = {
  commit: boolean;
  auditDir: string;
  flagType: string;
};

export const DEFAULT_FLAG_TYPE = "looks_like_name_reject";
export const DEFAULT_AUDIT_DIR = "/tmp";
export const CHUNK_SIZE = 500;

/** Parse a minimal CLI — supports `--commit`, `--audit-dir <p>`,
 * `--audit-dir=<p>`, `--flag-type <t>`, `--flag-type=<t>`. Anything
 * else is ignored so we stay forward-compatible with wrapper scripts
 * that pass extra tokens. */
export function parseArgs(argv: readonly string[]): PurgeArgs {
  let commit = false;
  let auditDir = DEFAULT_AUDIT_DIR;
  let flagType = DEFAULT_FLAG_TYPE;

  for (let i = 0; i < argv.length; i++) {
    const tok = argv[i];
    if (tok === "--commit") {
      commit = true;
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
    if (tok === "--flag-type" && i + 1 < argv.length) {
      flagType = argv[++i];
      continue;
    }
    if (tok.startsWith("--flag-type=")) {
      flagType = tok.slice("--flag-type=".length);
      continue;
    }
  }

  if (flagType.length === 0) {
    throw new Error("--flag-type must not be empty");
  }
  if (auditDir.length === 0) {
    throw new Error("--audit-dir must not be empty");
  }
  return { commit, auditDir, flagType };
}

/** Build the JSONL filename for a run. The timestamp is second-precision
 * but safe for filesystems — ":" and "." are replaced with "-". */
export function buildAuditPath(auditDir: string, now: Date): string {
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  return path.join(auditDir, `coach-discoveries-purge-${stamp}.jsonl`);
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

/** Group a flat list of flag rows by their `discovery_id`, preserving
 * input order within each group. Defensive: silently drops rows with
 * a non-numeric `discovery_id`. */
export function groupFlagsByDiscoveryId<
  F extends { discovery_id: unknown },
>(flags: readonly F[]): Map<number, F[]> {
  const out = new Map<number, F[]>();
  for (const f of flags) {
    const id = f.discovery_id;
    if (typeof id !== "number" || !Number.isFinite(id)) continue;
    const arr = out.get(id) ?? [];
    arr.push(f);
    out.set(id, arr);
  }
  return out;
}

/** Materialize one JSONL record for the audit dump. Stable key order
 * so operators can diff two runs lexically. */
export function formatAuditRecord(
  discovery: Record<string, unknown>,
  flags: readonly Record<string, unknown>[],
  coach: Record<string, unknown> | null,
): string {
  return (
    JSON.stringify({
      discovery,
      flags,
      coach,
    }) + "\n"
  );
}

// ---------------------------------------------------------------------------
// Main (not unit-tested — verified via Replit smoke run)
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const now = new Date();
  const auditPath = buildAuditPath(args.auditDir, now);

  console.log(
    `[purge-polluted-coach-discoveries] commit=${args.commit} ` +
      `flag_type=${args.flagType} audit=${auditPath}`,
  );

  if (!fs.existsSync(args.auditDir)) {
    fs.mkdirSync(args.auditDir, { recursive: true });
  }

  const client = await pool.connect();
  let committed = false;
  try {
    await client.query("BEGIN");

    // Table-existence sanity. `coach_quality_flags` lands via a
    // separate `db push`; if someone runs this script before the
    // flagger PR is applied, fail loud rather than delete nothing
    // silently.
    const probe = await client.query<{ oid: string | null }>(
      `SELECT to_regclass($1) AS oid`,
      ["public.coach_quality_flags"],
    );
    if (!probe.rows[0] || probe.rows[0].oid === null) {
      throw new Error(
        "coach_quality_flags table does not exist — run " +
          "`pnpm --filter @workspace/db run push` on Replit first.",
      );
    }

    // Target set: distinct discovery_ids with an unresolved flag of
    // the configured type. Sorted for deterministic audit ordering +
    // deterministic DELETE ANY() plan.
    const targetResult = await client.query<{ id: number }>(
      `SELECT DISTINCT discovery_id AS id
       FROM coach_quality_flags
       WHERE flag_type = $1
         AND resolved_at IS NULL
       ORDER BY discovery_id`,
      [args.flagType],
    );
    const targetIds = targetResult.rows.map((r) => r.id);
    console.log(`  targets: ${targetIds.length}`);

    if (targetIds.length === 0) {
      console.log("  nothing to purge — all polluted rows already gone");
      await client.query("COMMIT");
      committed = true;
      return;
    }

    // Audit dump, BEFORE any mutation. Stream so we don't hold all
    // rows in Node memory — 1,740 rows is small today but this script
    // is the pattern for future pollution passes and may scale.
    const auditStream = fs.createWriteStream(auditPath, { flags: "w" });
    let auditRowsWritten = 0;

    try {
      for (const idBatch of chunk(targetIds, CHUNK_SIZE)) {
        const [discoveries, flags] = await Promise.all([
          client.query(
            `SELECT * FROM coach_discoveries WHERE id = ANY($1::int[]) ORDER BY id`,
            [idBatch],
          ),
          client.query(
            `SELECT * FROM coach_quality_flags
             WHERE discovery_id = ANY($1::int[])
             ORDER BY discovery_id, flag_type`,
            [idBatch],
          ),
        ]);
        const flagsByDiscovery = groupFlagsByDiscoveryId(
          flags.rows as Array<{ discovery_id: unknown }>,
        );

        // Parent coaches lookup — only for rows where coach_id is set.
        const coachIds = Array.from(
          new Set(
            discoveries.rows
              .map((d) => (d as { coach_id: unknown }).coach_id)
              .filter(
                (id): id is number => typeof id === "number" && Number.isFinite(id),
              ),
          ),
        );
        const coachesResult =
          coachIds.length > 0
            ? await client.query(
                `SELECT * FROM coaches WHERE id = ANY($1::int[])`,
                [coachIds],
              )
            : { rows: [] as Array<Record<string, unknown>> };
        const coachesById = new Map<number, Record<string, unknown>>();
        for (const c of coachesResult.rows as Array<Record<string, unknown>>) {
          const cid = c.id;
          if (typeof cid === "number") coachesById.set(cid, c);
        }

        for (const d of discoveries.rows as Array<Record<string, unknown>>) {
          const did = d.id;
          if (typeof did !== "number") continue;
          const rowFlags = (flagsByDiscovery.get(did) ?? []) as Array<
            Record<string, unknown>
          >;
          const coachId = d.coach_id;
          const coach =
            typeof coachId === "number" ? coachesById.get(coachId) ?? null : null;
          auditStream.write(formatAuditRecord(d, rowFlags, coach));
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

    // Re-SELECT the target ids under the same predicate IMMEDIATELY
    // before the DELETE. Between the original SELECT and now, an
    // operator could have triaged a flag in the admin UI (setting
    // `resolved_at` to a non-null value), turning a previously
    // targeted discovery into one that should NOT be purged. If the
    // refreshed set differs from the original target set, abort with
    // a clear message — the operator's triage wins and the JSONL
    // audit no longer matches the would-be DELETE.
    const recheck = await client.query<{ id: number }>(
      `SELECT DISTINCT discovery_id AS id
       FROM coach_quality_flags
       WHERE flag_type = $1
         AND resolved_at IS NULL
       ORDER BY discovery_id`,
      [args.flagType],
    );
    const recheckIds = recheck.rows.map((r) => r.id);
    const originalSet = new Set(targetIds);
    const recheckSet = new Set(recheckIds);
    let added = 0;
    let removed = 0;
    for (const id of recheckIds) if (!originalSet.has(id)) added += 1;
    for (const id of targetIds) if (!recheckSet.has(id)) removed += 1;
    if (added !== 0 || removed !== 0) {
      throw new Error(
        `target set changed between SELECT and DELETE: ` +
          `${removed} id(s) removed (likely operator-resolved), ` +
          `${added} id(s) added (likely new flags landed) — ` +
          `aborting transaction. Re-run to pick up the new target set.`,
      );
    }

    // DELETE in ONE statement — pg planner handles the ANY() list well
    // for up to low-tens-of-thousands of int4 values. The CASCADE on
    // coach_quality_flags.discovery_id drops the flag rows for us.
    const deleteResult = await client.query(
      `DELETE FROM coach_discoveries WHERE id = ANY($1::int[])`,
      [targetIds],
    );
    console.log(`  coach_discoveries deleted: ${deleteResult.rowCount}`);

    if (deleteResult.rowCount !== targetIds.length) {
      throw new Error(
        `DELETE row count mismatch: deleted ${deleteResult.rowCount} but ` +
          `expected ${targetIds.length}`,
      );
    }

    // Post-cascade sanity: residual flag rows for purged discoveries
    // MUST be zero. If this fails, the FK definition drifted — bail
    // loud so the operator notices before committing.
    const residual = await client.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM coach_quality_flags
       WHERE discovery_id = ANY($1::int[])`,
      [targetIds],
    );
    const residualCount = Number(residual.rows[0]?.n ?? "0");
    console.log(
      `  coach_quality_flags residual after cascade: ${residualCount} (expected 0)`,
    );
    if (residualCount !== 0) {
      throw new Error(
        `cascade failed — ${residualCount} flag rows still reference ` +
          "deleted discoveries. Check FK ON DELETE behavior.",
      );
    }

    await client.query("COMMIT");
    committed = true;
    console.log(`[purge-polluted-coach-discoveries] done — committed`);
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
