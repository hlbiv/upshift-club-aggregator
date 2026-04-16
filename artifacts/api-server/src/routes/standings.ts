import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { clubResults, canonicalClubs } from "@workspace/db/schema";
import { eq, ilike, sql, asc, desc } from "drizzle-orm";
import { StandingsSearchResponse } from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// ---------------------------------------------------------------------------
// Shared row mapper
// ---------------------------------------------------------------------------

function mapStandingRow(row: {
  standing: typeof clubResults.$inferSelect;
  clubName: string | null;
}) {
  const s = row.standing;
  return {
    id: s.id,
    club_id: s.clubId,
    club_name: row.clubName ?? null,
    season: s.season,
    league: s.league ?? null,
    division: s.division ?? null,
    age_group: s.ageGroup ?? null,
    gender: s.gender ?? null,
    wins: s.wins,
    losses: s.losses,
    draws: s.draws,
    goals_for: s.goalsFor,
    goals_against: s.goalsAgainst,
    matches_played: s.matchesPlayed,
    last_calculated_at: s.lastCalculatedAt.toISOString(),
  };
}

// ---------------------------------------------------------------------------
// GET /api/standings — Search standings
// ---------------------------------------------------------------------------

router.get("/standings", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const season = req.query.season as string | undefined;
    const league = req.query.league as string | undefined;
    const division = req.query.division as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(clubResults.clubId, clubId)
        : undefined,
      season ? eq(clubResults.season, season) : undefined,
      league ? ilike(clubResults.league, `%${escapeLike(league)}%`) : undefined,
      division
        ? ilike(clubResults.division, `%${escapeLike(division)}%`)
        : undefined,
      ageGroup
        ? ilike(clubResults.ageGroup, `%${escapeLike(ageGroup)}%`)
        : undefined,
      gender
        ? ilike(clubResults.gender, `%${escapeLike(gender)}%`)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(clubResults)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select({
        standing: clubResults,
        clubName: canonicalClubs.clubNameCanonical,
      })
      .from(clubResults)
      .leftJoin(canonicalClubs, eq(canonicalClubs.id, clubResults.clubId))
      .where(where)
      .orderBy(desc(clubResults.season), asc(clubResults.clubId))
      .limit(pageSize)
      .offset(offset);

    res.json(
      StandingsSearchResponse.parse({
        standings: rows.map(mapStandingRow),
        total,
        page,
        page_size: pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/standings/by-club/:clubId
// ---------------------------------------------------------------------------

router.get(
  "/standings/by-club/:clubId",
  async (req, res, next): Promise<void> => {
    try {
      const clubId = Number(req.params.clubId);
      if (isNaN(clubId)) {
        res.status(400).json({ error: "Invalid clubId" });
        return;
      }

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const where = eq(clubResults.clubId, clubId);

      const [countRow] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(clubResults)
        .where(where);

      const total = countRow?.count ?? 0;

      const rows = await db
        .select({
          standing: clubResults,
          clubName: canonicalClubs.clubNameCanonical,
        })
        .from(clubResults)
        .leftJoin(canonicalClubs, eq(canonicalClubs.id, clubResults.clubId))
        .where(where)
        .orderBy(desc(clubResults.season))
        .limit(pageSize)
        .offset(offset);

      res.json(
        StandingsSearchResponse.parse({
          standings: rows.map(mapStandingRow),
          total,
          page,
          page_size: pageSize,
        }),
      );
    } catch (err) {
      next(err);
    }
  },
);

export default router;
