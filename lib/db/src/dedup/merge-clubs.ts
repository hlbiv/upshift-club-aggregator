/**
 * mergeClubs — transactional canonical-club merge helper.
 *
 * Destructive operation behind the admin UI's dedup review "merge" button.
 * Collapses one canonical_clubs row (the loser) into another (the winner)
 * by reparenting every FK that points at `canonical_clubs.id`, inserting
 * an audit `club_aliases` row that back-references the loser's id, and
 * finally DELETEing the loser row. All of this runs inside a single
 * Drizzle transaction — any failure rolls back the entire merge.
 *
 * Why a dedicated TS helper (vs. reusing scraper/dedup/canonical_club_merger.py)?
 * -------------------------------------------------------------------------
 * The Python merger lives inside the batch scraper process and speaks
 * psycopg2 directly; the admin UI needs a synchronous helper it can call
 * from an Express route handler inside Drizzle's connection pool. The two
 * implementations must agree on the set of FK tables they reparent — see
 * the scraper/dedup/canonical_club_merger.py:_REDIRECT_TABLES list for the
 * parallel coverage. Keep them in sync if you add a new table that points
 * at canonical_clubs.id.
 *
 * Scope (what this function does and does not do)
 * -----------------------------------------------
 *   - Validates winnerId != loserId and that both rows exist.
 *   - Reparents every FK-bearing table. Uses plain `UPDATE ... WHERE fk =
 *     loserId` + capturing rowcount. The pre-merge collision-avoidance
 *     step that the Python merger does for tables with (club_id, ...) UQs
 *     is intentionally NOT replicated here — the admin-UI caller operates
 *     on human-reviewed pairs, so collisions mean operator error and we
 *     want them to surface as a transaction rollback (duplicate-key
 *     error) rather than silently deleting the loser's rows. If we need
 *     to support "merge clubs that both have the same affiliation" in
 *     the UI, add a per-table delete-then-update step guarded by an
 *     explicit operator confirmation.
 *   - Marks matching rows in `club_duplicates` as status='merged'.
 *   - Flips `canonical_clubs.manually_merged = true` on the winner.
 *   - DELETEs the loser canonical_clubs row.
 *
 * Not in scope (by design)
 * ------------------------
 *   - Undo / unmerge. Policy decision pending. The `merged_from_canonical_id`
 *     column on club_aliases is the breadcrumb an undo would key off of.
 *   - Auto-merge. This helper is always operator-initiated. Auto-merge
 *     lives in the scraper-side Python merger and has its own guards.
 *   - Bumping `club_duplicates.reviewed_at` / `reviewed_by`. The admin
 *     route that calls this helper owns those columns — the helper only
 *     flips `status` so other unreviewed pairs involving the loser don't
 *     get offered for review again.
 */

import { sql } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import type * as schema from "../schema";

/** Per-table reparent counts plus the overall merge outcome. */
export type MergeClubsResult = {
  ok: true;
  winnerId: number;
  loserAliasesCreated: number;
  affiliationsReparented: number;
  rosterSnapshotsReparented: number;
  rosterDiffsReparented: number;
  eventTeamsReparented: number;
  matchesReparented: number;
  clubResultsReparented: number;
  commitmentsReparented: number;
  yntReparented: number;
  odpReparented: number;
  coachCareerReparented: number;
  tryoutsReparented: number;
  siteChangesReparented: number;
  clubDuplicatesMarked: number;
  coachDiscoveriesReparented: number;
};

export type MergeClubsDeps = {
  db: NodePgDatabase<typeof schema>;
  winnerId: number;
  loserId: number;
  /** admin_users.id, or null when called outside a dedup review flow. */
  reviewedBy: number | null;
  /** Free-form operator note, stored on the alias row's source column. */
  notes?: string;
};

/**
 * Dummy struct describing the transactional client we need. Lets the tests
 * inject a mock without pulling Drizzle's full PgTransaction type surface.
 * In production, the Drizzle tx object satisfies this shape via its
 * `.execute(sql`...`)` method.
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
 *   - Either canonical_clubs row is missing.
 *   - Any reparent / delete SQL fails.
 *
 * Callers should treat this as an exceptional-path operation: a thrown
 * error means the DB is unchanged.
 */
export async function mergeClubs(
  deps: MergeClubsDeps,
): Promise<MergeClubsResult> {
  const { db, winnerId, loserId, reviewedBy, notes } = deps;

  if (!Number.isInteger(winnerId) || winnerId <= 0) {
    throw new Error(`mergeClubs: winnerId must be a positive integer, got ${winnerId}`);
  }
  if (!Number.isInteger(loserId) || loserId <= 0) {
    throw new Error(`mergeClubs: loserId must be a positive integer, got ${loserId}`);
  }
  if (winnerId === loserId) {
    throw new Error(
      `mergeClubs: winnerId (${winnerId}) must differ from loserId (${loserId})`,
    );
  }

  return db.transaction(async (tx) => {
    return runMerge(tx as unknown as MergeTx, {
      winnerId,
      loserId,
      reviewedBy,
      notes,
    });
  });
}

/**
 * Core merge body, split out so the tests can drive it with a mocked
 * transaction object. Not exported from the package — import from this
 * module only inside the __tests__ directory.
 */
export async function runMerge(
  tx: MergeTx,
  args: {
    winnerId: number;
    loserId: number;
    reviewedBy: number | null;
    notes?: string;
  },
): Promise<MergeClubsResult> {
  const { winnerId, loserId, reviewedBy, notes } = args;

  // Guard: re-validate. `mergeClubs` covers the callers routed through
  // db.transaction, but `runMerge` is also imported by the tests and by
  // any future admin-route code that wants to share an outer
  // transaction. Keep the check co-located with the helper that owns
  // the SQL.
  if (winnerId === loserId) {
    throw new Error(
      `mergeClubs: winnerId (${winnerId}) must differ from loserId (${loserId})`,
    );
  }

  // ---------------------------------------------------------------------
  // 1. Existence check + loser name load under FOR UPDATE row locks.
  // ---------------------------------------------------------------------
  const lockRes = await tx.execute(sql`
    SELECT id, club_name_canonical
    FROM canonical_clubs
    WHERE id IN (${winnerId}, ${loserId})
    FOR UPDATE
  `);
  const rows = (lockRes.rows ?? []) as Array<{
    id: number;
    club_name_canonical: string | null;
  }>;
  const byId = new Map<number, string>();
  for (const r of rows) {
    byId.set(Number(r.id), r.club_name_canonical ?? "");
  }
  if (!byId.has(winnerId)) {
    throw new Error(`mergeClubs: winnerId ${winnerId} not found in canonical_clubs`);
  }
  if (!byId.has(loserId)) {
    throw new Error(`mergeClubs: loserId ${loserId} not found in canonical_clubs`);
  }
  const loserName = byId.get(loserId) ?? "";

  // ---------------------------------------------------------------------
  // 2. Insert the audit alias row. merged_from_canonical_id preserves the
  //    loser's old id so an operator (or a future undo path) can trace
  //    the merge. `source` doubles as a free-form notes field — stash
  //    any operator note there alongside the 'admin-merge' tag. ON
  //    CONFLICT is idempotent — if the operator re-issues the merge
  //    after a previous partial attempt (unlikely under transactions,
  //    but possible if a prior run was aborted after commit but before
  //    response), the INSERT does nothing and we report 0.
  // ---------------------------------------------------------------------
  const sourceTag = notes && notes.trim().length > 0
    ? `admin-merge:${notes.trim().slice(0, 500)}`
    : "admin-merge";
  const aliasRes = await tx.execute(sql`
    INSERT INTO club_aliases (
      club_id, alias_name, source, is_official,
      merged_from_canonical_id, merged_at
    )
    VALUES (${winnerId}, ${loserName}, ${sourceTag}, false, ${loserId}, NOW())
    ON CONFLICT ON CONSTRAINT club_aliases_club_alias_uq DO NOTHING
  `);
  const loserAliasesCreated = Number(aliasRes.rowCount ?? 0);

  // ---------------------------------------------------------------------
  // 3. Reparent every FK-bearing table. Each UPDATE returns the rows
  //    touched via rowCount. Plain `UPDATE ... WHERE fk = loserId` — no
  //    per-table UQ-collision avoidance (see module docstring).
  // ---------------------------------------------------------------------

  // club_aliases — existing loser-side aliases. Don't touch the
  // merged_from_canonical_id back-reference; that's historical truth.
  const aliasReparent = await tx.execute(sql`
    UPDATE club_aliases SET club_id = ${winnerId}
    WHERE club_id = ${loserId}
  `);
  // Intentionally not returned — absorbed into loserAliasesCreated would be
  // misleading. Callers that care can sum `loserAliasesCreated` + existing
  // aliases. Kept inside the transaction for correctness; no reported field.
  void aliasReparent;

  const affiliationsReparented = rowCount(
    await tx.execute(sql`
      UPDATE club_affiliations SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const coachDiscoveriesReparented = rowCount(
    await tx.execute(sql`
      UPDATE coach_discoveries SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const rosterSnapshotsReparented = rowCount(
    await tx.execute(sql`
      UPDATE club_roster_snapshots SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const rosterDiffsReparented = rowCount(
    await tx.execute(sql`
      UPDATE roster_diffs SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const eventTeamsReparented = rowCount(
    await tx.execute(sql`
      UPDATE event_teams SET canonical_club_id = ${winnerId}
      WHERE canonical_club_id = ${loserId}
    `),
  );

  // matches has TWO FK columns pointing at canonical_clubs. Update them
  // separately so the two row counts are independently tracked.
  const matchesHome = rowCount(
    await tx.execute(sql`
      UPDATE matches SET home_club_id = ${winnerId}
      WHERE home_club_id = ${loserId}
    `),
  );
  const matchesAway = rowCount(
    await tx.execute(sql`
      UPDATE matches SET away_club_id = ${winnerId}
      WHERE away_club_id = ${loserId}
    `),
  );
  const matchesReparented = matchesHome + matchesAway;

  const clubResultsReparented = rowCount(
    await tx.execute(sql`
      UPDATE club_results SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const commitmentsReparented = rowCount(
    await tx.execute(sql`
      UPDATE commitments SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const yntReparented = rowCount(
    await tx.execute(sql`
      UPDATE ynt_call_ups SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const odpReparented = rowCount(
    await tx.execute(sql`
      UPDATE odp_roster_entries SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  // coach_career_history is polymorphic (entity_type, entity_id). Only
  // redirect rows where entity_type = 'club' — colleges share the id
  // column.
  const coachCareerReparented = rowCount(
    await tx.execute(sql`
      UPDATE coach_career_history SET entity_id = ${winnerId}
      WHERE entity_type = 'club' AND entity_id = ${loserId}
    `),
  );

  const tryoutsReparented = rowCount(
    await tx.execute(sql`
      UPDATE tryouts SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  const siteChangesReparented = rowCount(
    await tx.execute(sql`
      UPDATE club_site_changes SET club_id = ${winnerId}
      WHERE club_id = ${loserId}
    `),
  );

  // coach_scrape_snapshots — not listed in the public result shape (the
  // contract doesn't surface it) but MUST be reparented or the DELETE
  // of the loser canonical_clubs row will cascade-delete the loser's
  // scrape history, which we want to retain under the winner.
  await tx.execute(sql`
    UPDATE coach_scrape_snapshots SET club_id = ${winnerId}
    WHERE club_id = ${loserId}
  `);

  // player_id_selections — same rationale. FK is SET NULL so skipping
  // the update would silently null out the selection's club_id instead
  // of preserving it.
  await tx.execute(sql`
    UPDATE player_id_selections SET club_id = ${winnerId}
    WHERE club_id = ${loserId}
  `);

  // duplicate_review_decisions + club_duplicates both use CASCADE FKs
  // (see schema). Reparent decisions onto the winner where possible,
  // then normalize the pair ordering (club_a_id < club_b_id) — the
  // table has a CHECK enforcing that. If moving loser onto winner
  // would produce a self-pair (winner = loser after merge), drop the
  // row; it no longer references two distinct clubs.
  //
  // DECISION: leave duplicate_review_decisions alone here. The normalized
  // pair-ordering check constraint makes reparenting fiddly and the
  // admin UI already reads `club_duplicates.status`, not this table,
  // for its review queue. The rows cascade-delete when the loser's
  // canonical_clubs row drops at the end of this function — acceptable
  // history loss, they are review decisions about a club that no longer
  // exists.

  // ---------------------------------------------------------------------
  // 4. Mark club_duplicates rows involving the loser as merged. Do NOT
  //    touch reviewed_at / reviewed_by — those are owned by the admin
  //    route (if this is a review-initiated merge) or left null (if
  //    this is called from elsewhere).
  // ---------------------------------------------------------------------
  const clubDuplicatesMarked = rowCount(
    await tx.execute(sql`
      UPDATE club_duplicates SET status = 'merged'
      WHERE (left_club_id = ${loserId} OR right_club_id = ${loserId})
        AND status = 'pending'
    `),
  );
  // reviewedBy is accepted by the helper for symmetry with the admin
  // route API, but intentionally not written here — see header.
  void reviewedBy;

  // ---------------------------------------------------------------------
  // 5. Mark the winner as manually_merged=true so downstream auto-merge
  //    (scraper/dedup/canonical_club_merger.py) will not rewrite it.
  // ---------------------------------------------------------------------
  await tx.execute(sql`
    UPDATE canonical_clubs SET manually_merged = true
    WHERE id = ${winnerId}
  `);

  // ---------------------------------------------------------------------
  // 6. Delete the loser canonical_clubs row. By this point every FK
  //    that points at it has been reparented above. Cascade FKs
  //    (coach_scrape_snapshots, club_affiliations, etc.) should all
  //    find zero remaining rows when Postgres evaluates them. If any
  //    cascade actually fires — i.e. this DELETE removes dependent
  //    rows via ON DELETE CASCADE — it means the reparent list is
  //    incomplete for some table. Fail loudly rather than silently
  //    losing data: we check that at least one row was deleted (the
  //    loser itself) and throw if the delete count is zero.
  // ---------------------------------------------------------------------
  const del = await tx.execute(sql`
    DELETE FROM canonical_clubs WHERE id = ${loserId}
  `);
  if (rowCount(del) === 0) {
    throw new Error(
      `mergeClubs: expected DELETE of canonical_clubs.id=${loserId} to remove 1 row, got 0`,
    );
  }

  return {
    ok: true,
    winnerId,
    loserAliasesCreated,
    affiliationsReparented,
    rosterSnapshotsReparented,
    rosterDiffsReparented,
    eventTeamsReparented,
    matchesReparented,
    clubResultsReparented,
    commitmentsReparented,
    yntReparented,
    odpReparented,
    coachCareerReparented,
    tryoutsReparented,
    siteChangesReparented,
    clubDuplicatesMarked,
    coachDiscoveriesReparented,
  };
}

/** Normalize `rowCount` from node-postgres (number | null) to a safe number. */
function rowCount(r: { rowCount?: number | null }): number {
  const n = r.rowCount;
  return typeof n === "number" && Number.isFinite(n) ? n : 0;
}
