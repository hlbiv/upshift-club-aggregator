import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { canonicalClubs, clubAliases } from "@workspace/db/schema";
import { ilike, or, asc, sql } from "drizzle-orm";
import { SearchClubsResponse } from "@workspace/api-zod";

const router: IRouter = Router();

router.get("/search", async (req, res, next) => {
  try {
    const q = ((req.query.q as string) || "").trim();
    if (!q) {
      return res.json(SearchClubsResponse.parse({ results: [], query: q }));
    }

    const pattern = `%${q}%`;

    const aliasMatches = await db
      .select({ clubId: clubAliases.clubId })
      .from(clubAliases)
      .where(ilike(clubAliases.aliasName, pattern));

    const aliasClubIds = [
      ...new Set(aliasMatches.map((r) => r.clubId!).filter(Boolean)),
    ];

    const rows = await db
      .select({
        id: canonicalClubs.id,
        clubNameCanonical: canonicalClubs.clubNameCanonical,
        clubSlug: canonicalClubs.clubSlug,
        city: canonicalClubs.city,
        state: canonicalClubs.state,
        country: canonicalClubs.country,
        status: canonicalClubs.status,
        rank: sql<number>`CASE WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${`${q}%`} THEN 0
             WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${pattern} THEN 1
             ELSE 2 END`,
      })
      .from(canonicalClubs)
      .where(
        or(
          ilike(canonicalClubs.clubNameCanonical, pattern),
          ilike(canonicalClubs.clubSlug, pattern),
          aliasClubIds.length > 0
            ? sql`${canonicalClubs.id} = ANY(ARRAY[${sql.raw(aliasClubIds.join(","))}]::int[])`
            : sql`false`,
        ),
      )
      .orderBy(
        sql`CASE WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${`${q}%`} THEN 0
             WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${pattern} THEN 1
             ELSE 2 END`,
        asc(canonicalClubs.clubNameCanonical),
      )
      .limit(25);

    const response = SearchClubsResponse.parse({
      query: q,
      results: rows.map((r) => ({
        id: r.id,
        club_name_canonical: r.clubNameCanonical,
        club_slug: r.clubSlug ?? "",
        city: r.city ?? "",
        state: r.state ?? "",
        country: r.country ?? "USA",
        status: r.status ?? "active",
      })),
    });

    res.json(response);
  } catch (err) {
    next(err);
  }
});

export default router;
