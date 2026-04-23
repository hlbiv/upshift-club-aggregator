/**
 * Purge junk `canonical_clubs` rows produced by SincSports nav-string
 * extraction failures.
 *
 * Background
 * ----------
 * Before the SincSports extractor was hardened (`scraper/extractors/sincsports.py`),
 * `_parse_clubs_from_html` walked every <table> on `TTTeamList.aspx` and
 * read td[1] as the "Club" name. Several of those tables are settings/nav
 * UI panels (Display Settings, Sort By, Divisions selector, ...), which
 * caused config strings to be ingested as canonical clubs. The originally
 * documented offenders (April 2026, dev DB) were ids 15479-15483:
 *
 *   15479  "Display Settings...Sort By..."   (494-char Display Settings blob)
 *   15480  "SINC Content Manager"
 *   15481  "Merge Tourneys"
 *   15482  "USYS"
 *   15483  "US Club" (canonicalized to "Us")
 *
 * These rows distort tier coverage metrics and any tier-based filter,
 * because their only `club_affiliations` rows are SincSports
 * tournament-derived "Independent / Regional Tournament" entries.
 *
 * What this script does
 * ---------------------
 * 1. Selects `canonical_clubs` rows whose `club_name_canonical` matches
 *    a known nav-string pattern (exact-match against a curated list, or
 *    longer than a sensible max length).
 * 2. Filters out anything with downstream references in tables we DON'T
 *    own cleanup for (coach_discoveries, tryouts, commitments, rosters,
 *    event_teams, etc.) — the script will refuse to delete a row that
 *    has any such reference and log it for manual review instead.
 * 3. Dumps every targeted canonical row + its child `club_affiliations`
 *    + `club_aliases` rows to a JSONL audit artifact.
 * 4. Inside ONE transaction: deletes the child `club_aliases` and
 *    `club_affiliations` rows, then deletes the `canonical_clubs` row.
 *
 * Dry-run by default — pass `--commit` to actually delete.
 *
 * Usage
 * -----
 *   # Preview (no writes; JSONL still produced):
 *   pnpm --filter @workspace/scripts exec tsx \
 *     src/purge-junk-canonical-clubs.ts
 *
 *   # Delete:
 *   pnpm --filter @workspace/scripts exec tsx \
 *     src/purge-junk-canonical-clubs.ts --commit
 *
 *   # Override audit dir:
 *   pnpm --filter @workspace/scripts exec tsx \
 *     src/purge-junk-canonical-clubs.ts --commit \
 *     --audit-dir /home/runner/workspace/artifacts/purge
 *
 * Idempotent: a second --commit run after a successful one reports
 * "0 targets" and exits 0.
 */
import fs from "node:fs";
import path from "node:path";
import { pool } from "@workspace/db";

// ---------------------------------------------------------------------------
// Pure helpers (unit-testable without a DB)
// ---------------------------------------------------------------------------

export type Args = {
  commit: boolean;
  auditDir: string;
};

export function parseArgs(argv: readonly string[]): Args {
  let commit = false;
  let auditDir = "/tmp";
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
    if (tok === "--help" || tok === "-h") {
      console.log(
        "Usage: purge-junk-canonical-clubs.ts [--commit] [--audit-dir DIR]",
      );
      process.exit(0);
    }
  }
  return { commit, auditDir };
}

/**
 * Lower-cased exact-match nav strings that have leaked into
 * canonical_clubs from SincSports settings/nav UI panels. Mirrors
 * `_NAV_STRING_EXACT` in `scraper/extractors/sincsports.py` but is the
 * authoritative cleanup target on the DB side. Keep these two lists in
 * sync if either grows — and prefer adding entries on both sides.
 */
export const JUNK_NAME_EXACT: ReadonlySet<string> = new Set([
  "sinc content manager",
  "merge tourneys",
  "tourneys",
  "display settings",
  "usys",
  "us club",
  "us",
  "usa rank",
  "team link",
  "default division",
  "advanced sort",
  "edit selection",
]);

/**
 * Real club names are short. Anything longer than this is almost
 * certainly a concatenated nav blob (worst observed: 494 chars). Matches
 * `_MAX_CLUB_NAME_LEN` in `scraper/extractors/sincsports.py`.
 */
export const MAX_CLUB_NAME_LEN = 80;

export function isJunkName(name: string | null | undefined): boolean {
  if (!name) return true;
  if (name.length > MAX_CLUB_NAME_LEN) return true;
  return JUNK_NAME_EXACT.has(name.trim().toLowerCase());
}

// ---------------------------------------------------------------------------
// DB cleanup
// ---------------------------------------------------------------------------

type CanonicalRow = {
  id: number;
  club_name_canonical: string;
  status: string | null;
};

type Counts = {
  affs: number;
  aliases: number;
  coaches: number;
  tryouts: number;
  commitments: number;
  results: number;
  rosters: number;
  site_changes: number;
  coach_snaps: number;
  odp: number;
  pid: number;
  rdiffs: number;
  videos: number;
  ynt: number;
  event_teams: number;
};

async function findCandidates(
  client: import("pg").PoolClient,
): Promise<CanonicalRow[]> {
  const exactList = Array.from(JUNK_NAME_EXACT);
  const r = await client.query<CanonicalRow>(
    `
    SELECT id, club_name_canonical, status
    FROM canonical_clubs
    WHERE
      length(club_name_canonical) > $1
      OR lower(trim(club_name_canonical)) = ANY($2::text[])
    ORDER BY id;
    `,
    [MAX_CLUB_NAME_LEN, exactList],
  );
  return r.rows;
}

async function countReferences(
  client: import("pg").PoolClient,
  id: number,
): Promise<Counts> {
  const r = await client.query(
    `
    SELECT
      (SELECT COUNT(*)::int FROM club_affiliations      WHERE club_id           = $1) AS affs,
      (SELECT COUNT(*)::int FROM club_aliases           WHERE club_id           = $1) AS aliases,
      (SELECT COUNT(*)::int FROM coach_discoveries      WHERE club_id           = $1) AS coaches,
      (SELECT COUNT(*)::int FROM tryouts                WHERE club_id           = $1) AS tryouts,
      (SELECT COUNT(*)::int FROM commitments            WHERE club_id           = $1) AS commitments,
      (SELECT COUNT(*)::int FROM club_results           WHERE club_id           = $1) AS results,
      (SELECT COUNT(*)::int FROM club_roster_snapshots  WHERE club_id           = $1) AS rosters,
      (SELECT COUNT(*)::int FROM club_site_changes      WHERE club_id           = $1) AS site_changes,
      (SELECT COUNT(*)::int FROM coach_scrape_snapshots WHERE club_id           = $1) AS coach_snaps,
      (SELECT COUNT(*)::int FROM odp_roster_entries     WHERE club_id           = $1) AS odp,
      (SELECT COUNT(*)::int FROM player_id_selections   WHERE club_id           = $1) AS pid,
      (SELECT COUNT(*)::int FROM roster_diffs           WHERE club_id           = $1) AS rdiffs,
      (SELECT COUNT(*)::int FROM video_sources          WHERE club_id           = $1) AS videos,
      (SELECT COUNT(*)::int FROM ynt_call_ups           WHERE club_id           = $1) AS ynt,
      (SELECT COUNT(*)::int FROM event_teams            WHERE canonical_club_id = $1) AS event_teams;
    `,
    [id],
  );
  return r.rows[0] as Counts;
}

/**
 * Tables we are willing to cascade-delete from. Any reference outside
 * this set blocks the cleanup for that row — operator review wins.
 */
const SAFE_CASCADE_KEYS: ReadonlyArray<keyof Counts> = ["affs", "aliases"];

export function isSafeToDelete(counts: Counts): boolean {
  for (const k of Object.keys(counts) as (keyof Counts)[]) {
    if (SAFE_CASCADE_KEYS.includes(k)) continue;
    if (counts[k] > 0) return false;
  }
  return true;
}

async function dumpAudit(
  client: import("pg").PoolClient,
  rows: CanonicalRow[],
  auditPath: string,
): Promise<void> {
  const stream = fs.createWriteStream(auditPath, { flags: "w" });
  for (const row of rows) {
    const affs = await client.query(
      `SELECT * FROM club_affiliations WHERE club_id = $1`,
      [row.id],
    );
    const aliases = await client.query(
      `SELECT * FROM club_aliases WHERE club_id = $1`,
      [row.id],
    );
    const counts = await countReferences(client, row.id);
    stream.write(
      JSON.stringify({
        canonical_club: row,
        counts,
        affiliations: affs.rows,
        aliases: aliases.rows,
        safe_to_delete: isSafeToDelete(counts),
      }) + "\n",
    );
  }
  await new Promise<void>((resolve, reject) => {
    stream.end((err: unknown) => (err ? reject(err) : resolve()));
  });
}

export async function main(args: Args): Promise<number> {
  const client = await pool.connect();
  try {
    const candidates = await findCandidates(client);
    console.log(`[purge-junk] Found ${candidates.length} junk-name candidate(s).`);
    if (candidates.length === 0) {
      console.log("[purge-junk] Nothing to do.");
      return 0;
    }

    fs.mkdirSync(args.auditDir, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    const auditPath = path.join(
      args.auditDir,
      `purge-junk-canonical-clubs-${stamp}.jsonl`,
    );
    await dumpAudit(client, candidates, auditPath);
    console.log(`[purge-junk] Audit dump: ${auditPath}`);

    const safe: CanonicalRow[] = [];
    const blocked: { row: CanonicalRow; counts: Counts }[] = [];
    for (const row of candidates) {
      const counts = await countReferences(client, row.id);
      if (isSafeToDelete(counts)) {
        safe.push(row);
      } else {
        blocked.push({ row, counts });
      }
    }

    for (const row of safe) {
      console.log(
        `[purge-junk]   SAFE  id=${row.id}  name=${JSON.stringify(
          row.club_name_canonical.slice(0, 80),
        )}`,
      );
    }
    for (const { row, counts } of blocked) {
      console.warn(
        `[purge-junk]   BLOCKED  id=${row.id}  name=${JSON.stringify(
          row.club_name_canonical.slice(0, 80),
        )}  refs=${JSON.stringify(counts)}`,
      );
    }

    if (!args.commit) {
      console.log(
        `[purge-junk] Dry-run — no writes. Pass --commit to delete ${safe.length} row(s).`,
      );
      return 0;
    }

    if (safe.length === 0) {
      console.log("[purge-junk] No safe-to-delete rows after reference check.");
      return 0;
    }

    const ids = safe.map((r) => r.id);
    await client.query("BEGIN");
    try {
      const aliasRes = await client.query(
        `DELETE FROM club_aliases WHERE club_id = ANY($1::int[])`,
        [ids],
      );
      const affRes = await client.query(
        `DELETE FROM club_affiliations WHERE club_id = ANY($1::int[])`,
        [ids],
      );
      const canonicalRes = await client.query(
        `DELETE FROM canonical_clubs WHERE id = ANY($1::int[])`,
        [ids],
      );
      await client.query("COMMIT");
      console.log(
        `[purge-junk] Deleted ${aliasRes.rowCount} club_aliases, ` +
          `${affRes.rowCount} club_affiliations, ` +
          `${canonicalRes.rowCount} canonical_clubs.`,
      );
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    }

    return 0;
  } finally {
    client.release();
  }
}

const isMain =
  import.meta.url === `file://${process.argv[1]}` ||
  import.meta.url.endsWith(process.argv[1] ?? "");

if (isMain) {
  const args = parseArgs(process.argv.slice(2));
  main(args)
    .then((code) => process.exit(code))
    .catch((err) => {
      console.error("[purge-junk] FAILED:", err);
      process.exit(1);
    })
    .finally(() => pool.end());
}
