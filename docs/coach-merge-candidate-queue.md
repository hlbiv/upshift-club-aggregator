# Coach merge candidate queue — design note

**Status:** Deferred design. Not yet implemented.

## Why this exists

PR 13 ("coach person_hash rehash cutover") proposed auto-merging
email-less coaches across clubs by dropping `clubId` from the hash. The
auto-merge approach is unsafe and the cutover path is locked (see the
"Coach person_hash rehash — RESEARCH DRY-RUN ONLY" section in
`CLAUDE.md`).

This document is the design for the proper fix.

## The problem

Today, `coaches.person_hash` is computed two ways:

| Coach has email | Hash formula                                        |
| --------------- | --------------------------------------------------- |
| Yes             | `sha256(normalized_name + '\|' + lower(email))`     |
| No              | `sha256(normalized_name + '\|no-email\|' + clubId)` |

The email-less branch is club-scoped on purpose — it prevents false
merges of two different "John Smith" coaches at two different clubs.
But it also prevents a *real* coach who moves from Club A to Club B
from being recognized as the same person across both clubs. Their
`coach_career_history` gets fragmented into two records.

Both error modes are real:
- **Under-merge** (current code): real Mike Smith moves Club A → Club B,
  ends up as two `coaches` rows. Career history split.
- **Over-merge** (PR 13's proposed fix): two different John Smiths at
  Club A and Club B with no email captured collapse to one row. Their
  kids get attributed to the wrong coach. Dashboard shows one person
  who somehow coached two simultaneous teams.

A single deterministic hash cannot distinguish the two cases.

## The proper design

Mirror the existing `club_duplicates` pattern (clubs already use a
candidate-pair queue with operator review at `/dedup`).

### Schema additions

```ts
// lib/db/src/schema/coaches.ts (or a new file)
export const coachMergeCandidates = pgTable("coach_merge_candidates", {
  id: serial("id").primaryKey(),

  // The two coaches the heuristic flagged as possibly the same person.
  coachAId: integer("coach_a_id").notNull().references(() => coaches.id, {
    onDelete: "cascade",
  }),
  coachBId: integer("coach_b_id").notNull().references(() => coaches.id, {
    onDelete: "cascade",
  }),

  // Why this pair was suggested. JSON because we'll iterate on heuristics:
  //   { reason: 'name_match_no_email', confidence: 0.85, ... }
  metadata: jsonb("metadata").notNull(),

  // Operator triage state.
  status: text("status")
    .notNull()
    .default("pending")
    .$type<"pending" | "merged" | "rejected">(),
  reviewedBy: integer("reviewed_by").references(() => adminUsers.id),
  reviewedAt: timestamp("reviewed_at"),

  detectedAt: timestamp("detected_at").defaultNow().notNull(),
}, (t) => [
  // Ordered-pair uniqueness — collapse (a,b) and (b,a) via LEAST/GREATEST.
  uniqueIndex("coach_merge_candidates_pair_uq").on(
    sql`LEAST(${t.coachAId}, ${t.coachBId})`,
    sql`GREATEST(${t.coachAId}, ${t.coachBId})`,
  ),
]);
```

### Candidate-detection heuristic

A scheduled job (similar to `nav-leaked-names-detect`) scans `coaches`
for pairs that share:

1. Identical `normalized_name`.
2. Either both `email IS NULL`, OR both have email but at different
   `(club_id)` and the names match.
3. Neither is `manually_merged = true` (those are operator-confirmed and
   off-limits).

For each match the job inserts a candidate row with:
- `metadata.reason` = `"name_match_no_email"` /
  `"name_match_different_emails"` / etc.
- `metadata.confidence` = a heuristic score (e.g., 1.0 for exact name
  + same title + same age group; lower otherwise).
- `metadata.same_normalized_name` = the shared name.
- `metadata.club_a_name`, `metadata.club_b_name` for fast UI display.

### Admin UI

Mirror `/dedup` for clubs:

- `GET /api/v1/admin/coach-merge-candidates` — paginated pending queue.
- `GET /api/v1/admin/coach-merge-candidates/:id` — pair + both coaches'
  full profile (career, discoveries, recent rosters).
- `POST /api/v1/admin/coach-merge-candidates/:id/merge` — operator
  confirms; re-points discoveries from loser to winner, deletes loser,
  sets winner's `manually_merged = true`, marks candidate `merged`.
- `POST /api/v1/admin/coach-merge-candidates/:id/reject` — operator
  marks the pair as definitely-not-the-same; candidate goes to
  `rejected` and won't be re-suggested by the detector.

### Detector idempotency

The detector must not re-suggest pairs that have been resolved. Easiest
mechanism: when querying for new candidates, filter out pairs whose
`(LEAST(a,b), GREATEST(a,b))` already exists in
`coach_merge_candidates` with status `merged` or `rejected`.

### Migration / rollout

This is *additive* — no existing data changes. The current
`person_hash` formula stays as-is. The candidate queue accumulates over
time as the detector runs nightly. Operators triage at their own pace.

## What changes about PR 13's existing code

PR 13 shipped three things; only one needs to change for this plan:

1. **`scripts/src/backfill-coaches-master.ts`** — auto-merge code path
   stays in place but the `--commit --allow-rehash` entry is locked.
   When the candidate-queue infrastructure lands, the flag is
   repurposed to *write candidate rows* instead of merging. The lock
   comes out as part of that PR.
2. **`scripts/src/sweep-orphan-coaches.ts`** strict row-count check —
   keep, useful regardless.
3. **`scripts/src/sweep-orphan-coaches.ts`** `--relink` flag — keep,
   useful as an operator escape hatch even without auto-merge.
4. **`scripts/src/purge-polluted-coach-discoveries.ts`** re-SELECT
   guard — keep, an unrelated correctness improvement.

## Open questions

1. **Detector confidence threshold.** Should low-confidence candidates
   still be surfaced to the queue, or filtered out so the queue stays
   actionable? (Easier to start strict and loosen later than the
   reverse.)
2. **Bulk-action UI.** If the dry-run JSONL from PR 13 reveals
   thousands of candidates, single-pair review is too slow. Consider a
   "merge all with confidence > 0.95 and same title" bulk action,
   gated to super_admin.
3. **Email-having coaches with different emails but same name + same
   club.** Are those plausibly the same person who changed contact
   info, or definitely two different people with a common name? The
   detector should probably NOT flag these by default — but worth
   revisiting after the basic flow lands.
4. **`coach_career_history` lineage.** When a merge happens, does the
   loser's history get appended chronologically to the winner's? Or
   stay as-is (loser deleted, history rows cascade-delete)? The latter
   is simpler but loses the historical record. Probably worth
   preserving via cascade-rewrite.

## Trigger to start work

Run the dry-run procedure in `CLAUDE.md` ("Coach person_hash rehash —
RESEARCH DRY-RUN ONLY") to count the candidate cardinality. If it's
small (low hundreds), do this work as filler. If it's large (thousands),
prioritize accordingly.
