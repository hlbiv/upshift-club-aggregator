import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { clubCoaches } from "@workspace/db/schema";
import { eq, ilike, gte, sql, asc } from "drizzle-orm";
import { CoachSearchResponse } from "@workspace/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

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
        ? eq(clubCoaches.clubId, clubId)
        : undefined,
      name ? ilike(clubCoaches.name, `%${name}%`) : undefined,
      title ? ilike(clubCoaches.title, `%${title}%`) : undefined,
      minConfidence !== undefined && !isNaN(minConfidence)
        ? gte(clubCoaches.confidenceScore, minConfidence)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(clubCoaches)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(clubCoaches)
      .where(where)
      .orderBy(asc(clubCoaches.name))
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
          confidence_score: r.confidenceScore ?? null,
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
