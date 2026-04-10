import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { canonicalClubs, clubAliases } from "@workspace/db/schema";
import { ilike, or, asc, sql } from "drizzle-orm";
import { SearchClubsResponse } from "@workspace/api-zod";

const router: IRouter = Router();

router.get("/search", async (req, res, next): Promise<void> => {
  try {
    const q = ((req.query.q as string) || "").trim();
    if (!q) {
      res.json(SearchClubsResponse.parse({ results: [], query: q }));
      return;
    }

    const pattern = `%${q}%`;
    const prefixPattern = `${q}%`;

    const aliasMatches = await db
      .select({ clubId: clubAliases.clubId })
      .from(clubAliases)
      .where(ilike(clubAliases.aliasName, pattern));

    const aliasClubIds = [
      ...new Set(aliasMatches.map((r) => r.clubId!).filter(Boolean)),
    ];

    const orConditions = [
      ilike(canonicalClubs.clubNameCanonical, pattern),
      ilike(canonicalClubs.clubSlug, pattern),
    ];
    if (aliasClubIds.length > 0) {
      orConditions.push(
        sql`${canonicalClubs.id} = ANY(ARRAY[${sql.raw(aliasClubIds.join(","))}]::int[])`,
      );
    }

    const rows = await db
      .select({
        id: canonicalClubs.id,
        clubNameCanonical: canonicalClubs.clubNameCanonical,
        clubSlug: canonicalClubs.clubSlug,
        city: canonicalClubs.city,
        state: canonicalClubs.state,
        country: canonicalClubs.country,
        status: canonicalClubs.status,
      })
      .from(canonicalClubs)
      .where(or(...orConditions))
      .orderBy(
        sql`CASE WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${prefixPattern} THEN 0
             WHEN ${canonicalClubs.clubNameCanonical} ILIKE ${pattern} THEN 1
             ELSE 2 END`,
        asc(canonicalClubs.clubNameCanonical),
      )
      .limit(25);

    res.json(
      SearchClubsResponse.parse({
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
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
