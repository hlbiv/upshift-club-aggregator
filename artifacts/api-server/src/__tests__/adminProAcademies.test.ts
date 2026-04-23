/**
 * admin/pro-academies — unit tests for the request-validation surface
 * and the result-shape adapter (no DB).
 *
 * Run: tsx src/__tests__/adminProAcademies.test.ts
 *
 * Why these specific assertions
 * -----------------------------
 *   1. `rowsOf` is the adapter that bridges the difference between
 *      drizzle returning a `{rows: [...]}` `pg.QueryResult` (current
 *      behaviour with `drizzle-orm/node-postgres`) and a bare row array
 *      (the shape some other admin handlers in this repo assume). The
 *      handler stays correct under either shape; the test pins both.
 *
 *   2. The Zod request schema (`ProAcademiesRequest`) is the single
 *      sanity check between the dashboard and the SQL — covering the
 *      defaulting + bound-checks here keeps a malformed query string
 *      from reaching Postgres.
 *
 *   3. `ACADEMY_FAMILIES` and `TIER_LABEL_TO_ENUM` are mirrored from
 *      `scripts/src/backfill-competitive-tier.ts`. The constants test
 *      pins the exact set so a drift between the two surfaces is
 *      caught at CI rather than at "operator flips toggle, club ends
 *      up at the wrong tier".
 */
import { ProAcademiesRequest } from "@hlbiv/api-zod/admin";
import {
  ACADEMY_FAMILIES,
  TIER_LABEL_TO_ENUM,
  rowsOf,
} from "../routes/admin/pro-academies";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];
function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

// -------------------------------------------------------------------
// rowsOf — handles both QueryResult and bare-array shapes.
// -------------------------------------------------------------------
{
  const qr = { rows: [{ id: 1 }, { id: 2 }], rowCount: 2 };
  const out = rowsOf<{ id: number }>(qr);
  assert(
    out.length === 2 && out[0].id === 1 && out[1].id === 2,
    "rowsOf-queryresult",
    `expected 2 rows from QueryResult, got ${JSON.stringify(out)}`,
  );
}
{
  const bare = [{ id: 7 }];
  const out = rowsOf<{ id: number }>(bare);
  assert(
    out.length === 1 && out[0].id === 7,
    "rowsOf-bare-array",
    `expected pass-through of bare array, got ${JSON.stringify(out)}`,
  );
}
{
  // Defensive: anything else (null, undefined, strings, malformed objects)
  // returns [] rather than throwing — the handler treats `[]` as "no
  // rows" which is the safe default for read paths.
  for (const odd of [null, undefined, "rows", 42, { foo: 1 }, { rows: "x" }]) {
    const out = rowsOf<unknown>(odd);
    assert(
      Array.isArray(out) && out.length === 0,
      "rowsOf-fallback",
      `expected [] for ${JSON.stringify(odd)}, got ${JSON.stringify(out)}`,
    );
  }
}

// -------------------------------------------------------------------
// ProAcademiesRequest — defaults + validation bounds.
// -------------------------------------------------------------------
{
  const parsed = ProAcademiesRequest.safeParse({});
  assert(
    parsed.success &&
      parsed.data.flag === "all" &&
      parsed.data.page === 1 &&
      parsed.data.pageSize === 50,
    "request-defaults",
    `expected defaults, got ${JSON.stringify(parsed)}`,
  );
}
{
  const bad = ProAcademiesRequest.safeParse({ flag: "garbage" });
  assert(
    !bad.success,
    "request-rejects-bad-flag",
    "expected unknown flag value to fail Zod parse",
  );
}
{
  const tooBig = ProAcademiesRequest.safeParse({ pageSize: 9999 });
  assert(
    !tooBig.success,
    "request-rejects-pagesize-cap",
    "expected pageSize > 200 to fail (DOS protection)",
  );
}
{
  const negative = ProAcademiesRequest.safeParse({ page: 0 });
  assert(
    !negative.success,
    "request-rejects-zero-page",
    "expected page=0 to fail (1-indexed)",
  );
}

// -------------------------------------------------------------------
// Constants drift — must match scripts/src/backfill-competitive-tier.ts.
// -------------------------------------------------------------------
{
  // ACADEMY_FAMILIES: the three tier-1 academy-family labels that flip
  // a club to 'academy' (when is_pro_academy = TRUE).
  const expected = ["MLS NEXT", "NWSL Academy", "USL Academy"];
  assert(
    ACADEMY_FAMILIES.length === expected.length &&
      expected.every((f, i) => ACADEMY_FAMILIES[i] === f),
    "academy-families-pinned",
    `ACADEMY_FAMILIES drifted; got ${JSON.stringify(ACADEMY_FAMILIES)}`,
  );
}
{
  // TIER_LABEL_TO_ENUM: pinned to the same 9 entries as the backfill
  // script. Adding a new tier_label requires updating both files.
  const expectedKeys = [
    "National Elite",
    "National Elite / High National",
    "National Elite / Pro Pathway",
    "National / Regional High Performance",
    "Pre-Elite Development",
    "NPL Member League",
    "Regional Power League",
    "Regional Tournament",
    "State Association / League Hub",
  ];
  const keys = Object.keys(TIER_LABEL_TO_ENUM);
  assert(
    keys.length === expectedKeys.length &&
      expectedKeys.every((k) => k in TIER_LABEL_TO_ENUM),
    "tier-label-keys-pinned",
    `TIER_LABEL_TO_ENUM keys drifted; got ${JSON.stringify(keys)}`,
  );
  for (const k of expectedKeys.slice(0, 5)) {
    assert(
      TIER_LABEL_TO_ENUM[k] === "elite",
      "tier-label-elite",
      `expected ${k} -> elite, got ${TIER_LABEL_TO_ENUM[k]}`,
    );
  }
  for (const k of expectedKeys.slice(5)) {
    assert(
      TIER_LABEL_TO_ENUM[k] === "competitive",
      "tier-label-competitive",
      `expected ${k} -> competitive, got ${TIER_LABEL_TO_ENUM[k]}`,
    );
  }
}

if (failures.length > 0) {
  for (const f of failures) console.error(`FAIL ${f.name}: ${f.issue}`);
  console.error(`\n${failures.length} test(s) failed`);
  process.exit(1);
}
console.log("admin/pro-academies tests OK");
