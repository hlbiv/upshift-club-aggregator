/**
 * `/api/v1/admin/linker/*` — canonical-club linker resolution UI backend.
 *
 *   GET  /api/v1/admin/linker/unmatched
 *   POST /api/v1/admin/linker/resolve
 *   POST /api/v1/admin/linker/ignore
 *
 * These endpoints power the Linker Resolution UI that lets operators:
 *   - See raw team names that have not yet resolved to a canonical_clubs row
 *     (ordered by how frequently they appear across all source tables)
 *   - Manually resolve a raw name by mapping it to a canonical_clubs.id
 *     (writes a club_aliases row with source='manual')
 *   - Mark a raw name as permanently ignorable (writes to linker_ignores
 *     so the Python linker skips it on future runs)
 *
 * Auth: mounted under authedAdminRouter — requireAdmin + 120/min rate
 * limiter already applied upstream.
 */
import { Router, type IRouter } from "express";
import { sql } from "drizzle-orm";
import { db } from "@workspace/db";
import { z } from "zod";

const router: IRouter = Router();

// ---------------------------------------------------------------------------
// GET /unmatched — paginated list of raw names with no canonical resolution
// ---------------------------------------------------------------------------

router.get("/unmatched", async (req, res, next): Promise<void> => {
  try {
    const page = Math.max(1, Number(req.query.page) || 1);
    const pageSize = Math.min(200, Math.max(1, Number(req.query.page_size) || 50));
    const offset = (page - 1) * pageSize;

    // Aggregate unresolved raw names across all source tables, excluding
    // entries that an operator has already marked to ignore.
    const itemsResult = await db.execute<{ raw_name: string; total_count: number }>(
      sql`
        SELECT raw_name, SUM(cnt)::int AS total_count FROM (
          SELECT team_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM event_teams         WHERE canonical_club_id IS NULL AND team_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT home_team_name  AS raw_name, COUNT(*)::int AS cnt FROM matches              WHERE home_club_id       IS NULL AND home_team_name  <> '' GROUP BY 1
          UNION ALL
          SELECT away_team_name  AS raw_name, COUNT(*)::int AS cnt FROM matches              WHERE away_club_id       IS NULL AND away_team_name  <> '' GROUP BY 1
          UNION ALL
          SELECT home_team_name  AS raw_name, COUNT(*)::int AS cnt FROM tournament_matches   WHERE home_club_id       IS NULL AND home_team_name  <> '' GROUP BY 1
          UNION ALL
          SELECT away_team_name  AS raw_name, COUNT(*)::int AS cnt FROM tournament_matches   WHERE away_club_id       IS NULL AND away_team_name  <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM club_roster_snapshots WHERE club_id           IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM roster_diffs         WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM tryouts              WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM commitments          WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM ynt_call_ups         WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM odp_roster_entries   WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
          UNION ALL
          SELECT club_name_raw   AS raw_name, COUNT(*)::int AS cnt FROM player_id_selections WHERE club_id            IS NULL AND club_name_raw   <> '' GROUP BY 1
        ) sub
        WHERE raw_name NOT IN (SELECT raw_team_name FROM linker_ignores)
        GROUP BY raw_name
        ORDER BY total_count DESC
        LIMIT ${pageSize} OFFSET ${offset}
      `,
    );

    const countResult = await db.execute<{ total: number }>(
      sql`
        SELECT COUNT(*)::int AS total FROM (
          SELECT raw_name FROM (
            SELECT team_name_raw   AS raw_name FROM event_teams         WHERE canonical_club_id IS NULL AND team_name_raw   <> ''
            UNION ALL
            SELECT home_team_name  AS raw_name FROM matches              WHERE home_club_id       IS NULL AND home_team_name  <> ''
            UNION ALL
            SELECT away_team_name  AS raw_name FROM matches              WHERE away_club_id       IS NULL AND away_team_name  <> ''
            UNION ALL
            SELECT home_team_name  AS raw_name FROM tournament_matches   WHERE home_club_id       IS NULL AND home_team_name  <> ''
            UNION ALL
            SELECT away_team_name  AS raw_name FROM tournament_matches   WHERE away_club_id       IS NULL AND away_team_name  <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM club_roster_snapshots WHERE club_id           IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM roster_diffs         WHERE club_id            IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM tryouts              WHERE club_id            IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM commitments          WHERE club_id            IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM ynt_call_ups         WHERE club_id            IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM odp_roster_entries   WHERE club_id            IS NULL AND club_name_raw   <> ''
            UNION ALL
            SELECT club_name_raw   AS raw_name FROM player_id_selections WHERE club_id            IS NULL AND club_name_raw   <> ''
          ) sub
          WHERE raw_name NOT IN (SELECT raw_team_name FROM linker_ignores)
          GROUP BY raw_name
        ) counted
      `,
    );

    const total = (countResult.rows[0]?.total as number) ?? 0;
    const items = itemsResult.rows.map((r) => ({
      raw_name: r.raw_name,
      total_count: r.total_count,
    }));

    res.json({ items, total, page, page_size: pageSize });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /resolve — manually map a raw name to a canonical club
// ---------------------------------------------------------------------------

const ResolveBody = z.object({
  raw_name: z.string().min(1),
  canonical_club_id: z.number().int().positive(),
});

router.post("/resolve", async (req, res, next): Promise<void> => {
  try {
    const parsed = ResolveBody.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid request body" });
      return;
    }
    const { raw_name, canonical_club_id } = parsed.data;

    // Check the club exists.
    const clubCheck = await db.execute<{ id: number }>(
      sql`SELECT id FROM canonical_clubs WHERE id = ${canonical_club_id} LIMIT 1`,
    );
    if (clubCheck.rows.length === 0) {
      res.status(404).json({ error: "canonical_club not found" });
      return;
    }

    // Check if alias already exists before upsert (to report already_existed).
    const existing = await db.execute<{ id: number }>(
      sql`SELECT id FROM club_aliases WHERE club_id = ${canonical_club_id} AND alias_name = ${raw_name} LIMIT 1`,
    );
    const alreadyExisted = existing.rows.length > 0;

    // Upsert the alias.
    const upsert = await db.execute<{ id: number }>(
      sql`
        INSERT INTO club_aliases (club_id, alias_name, source)
        VALUES (${canonical_club_id}, ${raw_name}, 'manual')
        ON CONFLICT (club_id, alias_name) DO UPDATE SET source = EXCLUDED.source
        RETURNING id
      `,
    );

    const aliasId = upsert.rows[0]?.id as number;
    res.json({ ok: true, alias_id: aliasId, already_existed: alreadyExisted });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /ignore — mark a raw name as permanently ignorable
// ---------------------------------------------------------------------------

const IgnoreBody = z.object({
  raw_name: z.string().min(1),
  reason: z.string().optional(),
});

router.post("/ignore", async (req, res, next): Promise<void> => {
  try {
    const parsed = IgnoreBody.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid request body" });
      return;
    }
    const { raw_name, reason } = parsed.data;

    const adminUserId =
      req.adminAuth?.kind === "session" ? req.adminAuth.userId : null;

    await db.execute(
      sql`
        INSERT INTO linker_ignores (raw_team_name, reason, created_by)
        VALUES (${raw_name}, ${reason ?? null}, ${adminUserId})
        ON CONFLICT (raw_team_name) DO NOTHING
      `,
    );

    res.json({ ok: true });
  } catch (err) {
    next(err);
  }
});

export default router;
