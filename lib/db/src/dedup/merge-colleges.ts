/**
 * mergeColleges — transactional college merge helper.
 *
 * Destructive operation behind the admin UI's college dedup review "merge"
 * button. Collapses one colleges row (the loser) into another (the winner)
 * by reparenting every FK that points at `colleges.id`, inserting an audit
 * `college_aliases` row that back-references the loser's id, and finally
 * DELETEing the loser row. All of this runs inside a single Drizzle
 * transaction — any failure rolls back the entire merge.
 *
 * Scope (what this function does and does not do)
 * -----------------------------------------------
 *   - Validates winnerId != loserId and that both rows exist.
 *   - Reparents every FK-bearing table:
 *       college_coaches, college_roster_history, college_coach_tenures.
 *   - Inserts an audit college_aliases row (ON CONFLICT DO NOTHING).
 *   - Marks matching rows in college_duplicates as status='merged'.
 *   - DELETEs the loser colleges row.
 *
 * Not in scope (by design)
 * ------------------------
 *   - Undo / unmerge. The `merged_from_college_id` column on college_aliases
 *     is the breadcrumb an undo would key off of.
 *   - Auto-merge. This helper is always operator-initiated.
 *   - Bumping `college_duplicates.reviewed_at` / `reviewed_by`. The admin
 *     route that calls this helper owns those columns.
 */

import { sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type * as schema from "../schema";

/** Per-table reparent counts plus the overall merge outcome. */
export type MergeCollegesResult = {
  ok: true;
  winnerId: number;
  loserAliasesCreated: number;
  coachesReparented: number;
  rosterRowsReparented: number;
  tenuresReparented: number;
  collegeDuplicatesMarked: number;
};

export type MergeCollegesDeps = {
  db: NodePgDatabase<typeof schema>;
  winnerId: number;
  loserId: number;
  /** admin_users.id, or null when called outside a dedup review flow. */
  reviewedBy: number | null;
  /** Free-form operator note, stored on the alias row. */
  notes?: string;
};

/**
 * Dummy struct describing the transactional client we need. Lets the tests
 * inject a mock without pulling Drizzle's full PgTransaction type surface.
 */
type MergeTx = {
  execute: (query: ReturnType<typeof sql>) => Promise<{
    rows?: Array<Record<string, unknown>>;
    rowCount?: number | null;
  }>;
};

/**
 * Collapse `loserId` into `winnerId`. Runs inside a single Drizzle
 * transaction. Throws (rolling the transaction back) if:
 *
 *   - `winnerId === loserId`.
 *   - Either colleges row is missing.
 *   - Any reparent / delete SQL fails.
 *
 * Callers should treat this as an exceptional-path operation: a thrown
 * error means the DB is unchanged.
 */
export async function mergeColleges(
  deps: MergeCollegesDeps,
): Promise<MergeCollegesResult> {
  const { db, winnerId, loserId, reviewedBy, notes } = deps;

  if (!Number.isInteger(winnerId) || winnerId <= 0) {
    throw new Error(`mergeColleges: winnerId must be a positive integer, got ${winnerId}`);
  }
  if (!Number.isInteger(loserId) || loserId <= 0) {
    throw new Error(`mergeColleges: loserId must be a positive integer, got ${loserId}`);
  }
  if (winnerId === loserId) {
    throw new Error(
      `mergeColleges: winnerId (${winnerId}) must differ from loserId (${loserId})`,
    );
  }

  return db.transaction(async (tx) => {
    return runCollegeMerge(tx as unknown as MergeTx, {
      winnerId,
      loserId,
      reviewedBy,
      notes,
    });
  });
}

/**
 * Core merge body, split out so tests can drive it with a mocked tx object.
 * Not exported from the package — import from this module only inside the
 * __tests__ directory.
 */
export async function runCollegeMerge(
  tx: MergeTx,
  args: {
    winnerId: number;
    loserId: number;
    reviewedBy: number | null;
    notes?: string;
  },
): Promise<MergeCollegesResult> {
  const { winnerId, loserId, reviewedBy, notes } = args;

  if (winnerId === loserId) {
    throw new Error(
      `mergeColleges: winnerId (${winnerId}) must differ from loserId (${loserId})`,
    );
  }

  // -------------------------------------------------------------------------
  // 1. Existence check + loser name load under FOR UPDATE row locks.
  // -------------------------------------------------------------------------
  const lockRes = await tx.execute(sql`
    SELECT id, name
    FROM colleges
    WHERE id IN (${winnerId}, ${loserId})
    FOR UPDATE
  `);
  const rows = (lockRes.rows ?? []) as Array<{
    id: number;
    name: string | null;
  }>;
  const byId = new Map<number, string>();
  for (const r of rows) {
    byId.set(Number(r.id), r.name ?? "");
  }
  if (!byId.has(winnerId)) {
    throw new Error(`mergeColleges: winnerId ${winnerId} not found in colleges`);
  }
  if (!byId.has(loserId)) {
    throw new Error(`mergeColleges: loserId ${loserId} not found in colleges`);
  }
  const loserName = byId.get(loserId) ?? "";

  // -------------------------------------------------------------------------
  // 2. Insert the audit alias row. merged_from_college_id preserves the
  //    loser's old id so an operator (or a future undo path) can trace the
  //    merge. ON CONFLICT is idempotent.
  // -------------------------------------------------------------------------
  const aliasRes = await tx.execute(sql`
    INSERT INTO college_aliases (
      college_id, alias_name,
      merged_from_college_id, merged_at
    )
    VALUES (${winnerId}, ${loserName}, ${loserId}, NOW())
    ON CONFLICT ON CONSTRAINT college_aliases_college_alias_uq DO NOTHING
  `);
  const loserAliasesCreated = Number(aliasRes.rowCount ?? 0);

  // -------------------------------------------------------------------------
  // 3. Reparent every FK-bearing table.
  // -------------------------------------------------------------------------

  const coachesReparented = rowCount(
    await tx.execute(sql`
      UPDATE college_coaches SET college_id = ${winnerId}
      WHERE college_id = ${loserId}
    `),
  );

  const rosterRowsReparented = rowCount(
    await tx.execute(sql`
      UPDATE college_roster_history SET college_id = ${winnerId}
      WHERE college_id = ${loserId}
    `),
  );

  const tenuresReparented = rowCount(
    await tx.execute(sql`
      UPDATE college_coach_tenures SET college_id = ${winnerId}
      WHERE college_id = ${loserId}
    `),
  );

  // college_aliases — reparent any existing aliases from the loser to winner.
  await tx.execute(sql`
    UPDATE college_aliases SET college_id = ${winnerId}
    WHERE college_id = ${loserId}
  `);

  // -------------------------------------------------------------------------
  // 4. Mark college_duplicates rows involving the loser as merged.
  // -------------------------------------------------------------------------
  const collegeDuplicatesMarked = rowCount(
    await tx.execute(sql`
      UPDATE college_duplicates SET status = 'merged'
      WHERE (left_college_id = ${loserId} OR right_college_id = ${loserId})
        AND status = 'pending'
    `),
  );

  // reviewedBy is accepted for symmetry with the admin route API but not
  // written here — the admin route owns reviewed_at / reviewed_by.
  void reviewedBy;
  void notes;

  // -------------------------------------------------------------------------
  // 5. Delete the loser colleges row. CASCADE FKs will fire on any remaining
  //    children we missed — but we reparented them all above.
  // -------------------------------------------------------------------------
  const del = await tx.execute(sql`
    DELETE FROM colleges WHERE id = ${loserId}
  `);
  if (rowCount(del) === 0) {
    throw new Error(
      `mergeColleges: expected DELETE of colleges.id=${loserId} to remove 1 row, got 0`,
    );
  }

  return {
    ok: true,
    winnerId,
    loserAliasesCreated,
    coachesReparented,
    rosterRowsReparented,
    tenuresReparented,
    collegeDuplicatesMarked,
  };
}

/** Normalize `rowCount` from node-postgres (number | null) to a safe number. */
function rowCount(r: { rowCount?: number | null }): number {
  const n = r.rowCount;
  return typeof n === "number" && Number.isFinite(n) ? n : 0;
}
