import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { matches, canonicalClubs, clubResults } from "@workspace/db/schema";
import { eq, sql, asc, desc, inArray, ilike, gte, lte } from "drizzle-orm";
import {
  MatchSearchResponse,
  MatchBatchResponse,
  MatchDetailResponse,
  ClubResultsResponse,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// ---------------------------------------------------------------------------
// Shared row-mapper — maps a joined match row to the API response shape
// ---------------------------------------------------------------------------

function mapMatchRow(row: {
  match: typeof matches.$inferSelect;
  homeClubName: string | null;
  awayClubName: string | null;
}) {
  const m = row.match;
  return {
    id: m.id,
    event_id: m.eventId ?? null,
    home_club_id: m.homeClubId ?? null,
    away_club_id: m.awayClubId ?? null,
    home_team_name: m.homeTeamName,
    away_team_name: m.awayTeamName,
    home_club_name: row.homeClubName ?? null,
    away_club_name: row.awayClubName ?? null,
    home_score: m.homeScore ?? null,
    away_score: m.awayScore ?? null,
    match_date: m.matchDate ? m.matchDate.toISOString() : null,
    age_group: m.ageGroup ?? null,
    gender: m.gender ?? null,
    division: m.division ?? null,
    season: m.season ?? null,
    league: m.league ?? null,
    status: m.status,
    source: m.source ?? null,
    source_url: m.sourceUrl ?? null,
    platform_match_id: m.platformMatchId ?? null,
    scraped_at: m.scrapedAt.toISOString(),
  };
}

// ---------------------------------------------------------------------------
// Shared query helper — select matches with home/away club name joins
// ---------------------------------------------------------------------------

function matchSelectWithClubNames() {
  return db
    .select({
      match: matches,
      homeClubName:
        sql<string | null>`home_c."club_name_canonical"`.as("home_club_name"),
      awayClubName:
        sql<string | null>`away_c."club_name_canonical"`.as("away_club_name"),
    })
    .from(matches)
    .leftJoin(
      sql`${canonicalClubs} AS home_c`,
      sql`home_c."id" = ${matches.homeClubId}`,
    )
    .leftJoin(
      sql`${canonicalClubs} AS away_c`,
      sql`away_c."id" = ${matches.awayClubId}`,
    );
}

function matchCountFrom() {
  return db
    .select({ count: sql<number>`count(*)::int` })
    .from(matches);
}

// ---------------------------------------------------------------------------
// GET /api/matches/search
// ---------------------------------------------------------------------------

router.get("/matches/search", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const eventIdRaw = req.query.event_id as string | undefined;
    const eventId = eventIdRaw ? Number(eventIdRaw) : undefined;
    const league = req.query.league as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;
    const season = req.query.season as string | undefined;
    const status = req.query.status as string | undefined;
    const source = req.query.source as string | undefined;
    const startDateFromRaw = req.query.start_date_from as string | undefined;
    const startDateToRaw = req.query.start_date_to as string | undefined;

    const parseDate = (raw: string | undefined): Date | undefined | null => {
      if (!raw) return undefined;
      const d = new Date(raw);
      if (isNaN(d.getTime())) return null;
      return d;
    };

    const startDateFrom = parseDate(startDateFromRaw);
    const startDateTo = parseDate(startDateToRaw);

    if (startDateFrom === null || startDateTo === null) {
      res.status(400).json({
        error: "Invalid date format. Use ISO 8601 (e.g. 2024-08-01).",
      });
      return;
    }

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const clubIdFilter =
      clubId !== undefined && !isNaN(clubId)
        ? sql`(${matches.homeClubId} = ${clubId} OR ${matches.awayClubId} = ${clubId})`
        : undefined;

    const where = buildWhere([
      clubIdFilter,
      eventId !== undefined && !isNaN(eventId)
        ? eq(matches.eventId, eventId)
        : undefined,
      league ? ilike(matches.league, `%${escapeLike(league)}%`) : undefined,
      ageGroup
        ? ilike(matches.ageGroup, `%${escapeLike(ageGroup)}%`)
        : undefined,
      gender ? ilike(matches.gender, `%${escapeLike(gender)}%`) : undefined,
      season ? ilike(matches.season, `%${escapeLike(season)}%`) : undefined,
      status ? eq(matches.status, status) : undefined,
      source ? ilike(matches.source, `%${escapeLike(source)}%`) : undefined,
      startDateFrom ? gte(matches.matchDate, startDateFrom) : undefined,
      startDateTo ? lte(matches.matchDate, startDateTo) : undefined,
    ]);

    const [countRow] = await matchCountFrom().where(where);
    const total = countRow?.count ?? 0;

    const rows = await matchSelectWithClubNames()
      .where(where)
      .orderBy(desc(matches.matchDate), desc(matches.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      MatchSearchResponse.parse({
        matches: rows.map(mapMatchRow),
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
// GET /api/matches/batch?ids=1,2,3
// ---------------------------------------------------------------------------

const MAX_BATCH_SIZE = 100;

router.get("/matches/batch", async (req, res, next): Promise<void> => {
  try {
    const idsRaw = req.query.ids as string | undefined;
    if (!idsRaw || idsRaw.trim().length === 0) {
      res.status(400).json({ error: "ids query parameter is required" });
      return;
    }

    const parts = idsRaw.split(",").map((s) => s.trim());
    const parsed: number[] = [];
    for (const part of parts) {
      const n = Number(part);
      if (!Number.isInteger(n) || n < 0) {
        res.status(400).json({
          error: `Invalid match id: "${part}". All IDs must be non-negative integers.`,
        });
        return;
      }
      parsed.push(n);
    }

    if (parsed.length > MAX_BATCH_SIZE) {
      res.status(400).json({
        error: `Too many IDs. Maximum is ${MAX_BATCH_SIZE}, got ${parsed.length}.`,
      });
      return;
    }

    const rows = await matchSelectWithClubNames()
      .where(inArray(matches.id, parsed))
      .orderBy(asc(matches.id));

    res.json(
      MatchBatchResponse.parse({
        matches: rows.map(mapMatchRow),
        total: rows.length,
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/matches/:id
// ---------------------------------------------------------------------------

router.get("/matches/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [row] = await matchSelectWithClubNames().where(eq(matches.id, id));

    if (!row) {
      res.status(404).json({ error: "Match not found" });
      return;
    }

    res.json(MatchDetailResponse.parse(mapMatchRow(row)));
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/matches/by-event/:eventId
// ---------------------------------------------------------------------------

router.get(
  "/matches/by-event/:eventId",
  async (req, res, next): Promise<void> => {
    try {
      const eventId = Number(req.params.eventId);
      if (isNaN(eventId)) {
        res.status(400).json({ error: "Invalid eventId" });
        return;
      }

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const where = eq(matches.eventId, eventId);

      const [countRow] = await matchCountFrom().where(where);
      const total = countRow?.count ?? 0;

      const rows = await matchSelectWithClubNames()
        .where(where)
        .orderBy(asc(matches.matchDate), asc(matches.id))
        .limit(pageSize)
        .offset(offset);

      res.json(
        MatchSearchResponse.parse({
          matches: rows.map(mapMatchRow),
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

// ---------------------------------------------------------------------------
// GET /api/matches/by-club/:clubId
// ---------------------------------------------------------------------------

router.get(
  "/matches/by-club/:clubId",
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

      const where = sql`(${matches.homeClubId} = ${clubId} OR ${matches.awayClubId} = ${clubId})`;

      const [countRow] = await matchCountFrom().where(where);
      const total = countRow?.count ?? 0;

      const rows = await matchSelectWithClubNames()
        .where(where)
        .orderBy(desc(matches.matchDate), desc(matches.id))
        .limit(pageSize)
        .offset(offset);

      res.json(
        MatchSearchResponse.parse({
          matches: rows.map(mapMatchRow),
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

// ---------------------------------------------------------------------------
// GET /api/matches — simple paginated match listing with optional filters
// ---------------------------------------------------------------------------

router.get("/matches", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const season = req.query.season as string | undefined;
    const source = req.query.source as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const clubIdFilter =
      clubId !== undefined && !isNaN(clubId)
        ? sql`(${matches.homeClubId} = ${clubId} OR ${matches.awayClubId} = ${clubId})`
        : undefined;

    const where = buildWhere([
      clubIdFilter,
      season ? ilike(matches.season, `%${escapeLike(season)}%`) : undefined,
      source ? ilike(matches.source, `%${escapeLike(source)}%`) : undefined,
    ]);

    const [countRow] = await matchCountFrom().where(where);
    const total = countRow?.count ?? 0;

    const rows = await matchSelectWithClubNames()
      .where(where)
      .orderBy(desc(matches.matchDate), desc(matches.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      MatchSearchResponse.parse({
        matches: rows.map(mapMatchRow),
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
// GET /api/clubs/:id/results — club win/loss/draw record
// ---------------------------------------------------------------------------

router.get("/clubs/:id/results", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const rows = await db
      .select()
      .from(clubResults)
      .where(eq(clubResults.clubId, id))
      .orderBy(desc(clubResults.season), asc(clubResults.league));

    res.json(
      ClubResultsResponse.parse({
        club_id: id,
        results: rows.map((r) => ({
          id: r.id,
          season: r.season,
          league: r.league ?? null,
          division: r.division ?? null,
          age_group: r.ageGroup ?? null,
          gender: r.gender ?? null,
          wins: r.wins,
          losses: r.losses,
          draws: r.draws,
          goals_for: r.goalsFor,
          goals_against: r.goalsAgainst,
          matches_played: r.matchesPlayed,
          last_calculated_at: r.lastCalculatedAt.toISOString(),
        })),
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
