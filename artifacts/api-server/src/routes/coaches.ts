import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { coachDiscoveries } from "@workspace/db/schema";
import { eq, ilike, gte, sql, asc } from "drizzle-orm";
import { CoachSearchResponse } from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

/**
 * Escape LIKE/ILIKE metacharacters so user input is treated literally.
 * Without this, a search for "50%" matches everything.
 */
function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

const router: IRouter = Router();

/**
 * Coach search.
 *
 * Reads from `coach_discoveries` (not `club_coaches`). The old `club_coaches`
 * table is being dropped after the coaches-master backfill ships — see
 * docs/path-a-data-model.md. `coach_discoveries` has a 1:1 superset of the
 * fields the API returns (name, title, email, phone, confidence, source_url).
 *
 * Once the coaches master table is backfilled this endpoint will also filter
 * by `coach_id` and project `coaches.display_name`, but for now the contract
 * matches the pre-migration shape exactly.
 */
router.get("/coaches/search", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const name = req.query.name as string | undefined;
    const title = req.query.title as string | undefined;
    const minConfidenceRaw = req.query.min_confidence as string | undefined;
    const minConfidence =
      minConfidenceRaw !== undefined ? Number(minConfidenceRaw) : undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(coachDiscoveries.clubId, clubId)
        : undefined,
      name ? ilike(coachDiscoveries.name, `%${escapeLike(name)}%`) : undefined,
      title ? ilike(coachDiscoveries.title, `%${escapeLike(title)}%`) : undefined,
      minConfidence !== undefined && !isNaN(minConfidence)
        ? gte(coachDiscoveries.confidence, minConfidence)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(coachDiscoveries)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(coachDiscoveries)
      .where(where)
      .orderBy(asc(coachDiscoveries.name))
      .limit(pageSize)
      .offset(offset);

    res.json(
      CoachSearchResponse.parse({
        coaches: rows.map((r) => ({
          id: r.id,
          club_id: r.clubId ?? null,
          name: r.name,
          title: r.title ?? null,
          email: r.email ?? null,
          phone: r.phone ?? null,
          confidence_score: r.confidence ?? null,
          source_url: r.sourceUrl ?? null,
        })),
        total,
        page,
        page_size: pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
