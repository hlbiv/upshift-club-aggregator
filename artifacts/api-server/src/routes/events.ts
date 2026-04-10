import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { clubEvents } from "@workspace/db/schema";
import { eq, ilike, gte, lte, sql, asc } from "drizzle-orm";
import { EventSearchResponse } from "@workspace/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

router.get("/events/search", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const league = req.query.league as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;
    const season = req.query.season as string | undefined;
    const source = req.query.source as string | undefined;
    const startDateFrom = req.query.start_date_from as string | undefined;
    const startDateTo = req.query.start_date_to as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(clubEvents.clubId, clubId)
        : undefined,
      league ? ilike(clubEvents.leagueName, `%${league}%`) : undefined,
      ageGroup ? ilike(clubEvents.ageGroup, `%${ageGroup}%`) : undefined,
      gender ? ilike(clubEvents.gender, `%${gender}%`) : undefined,
      season ? ilike(clubEvents.season, `%${season}%`) : undefined,
      source ? ilike(clubEvents.sourceUrl, `%${source}%`) : undefined,
      startDateFrom
        ? gte(clubEvents.startDate, new Date(startDateFrom))
        : undefined,
      startDateTo
        ? lte(clubEvents.startDate, new Date(startDateTo))
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(clubEvents)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(clubEvents)
      .where(where)
      .orderBy(asc(clubEvents.startDate), asc(clubEvents.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      EventSearchResponse.parse({
        events: rows.map((r) => ({
          id: r.id,
          club_id: r.clubId ?? null,
          league_name: r.leagueName ?? null,
          event_id: r.eventId ?? null,
          org_season_id: r.orgSeasonId ?? null,
          age_group: r.ageGroup ?? null,
          gender: r.gender ?? null,
          division: r.division ?? null,
          conference: r.conference ?? null,
          season: r.season ?? null,
          start_date: r.startDate ? r.startDate.toISOString() : null,
          end_date: r.endDate ? r.endDate.toISOString() : null,
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
