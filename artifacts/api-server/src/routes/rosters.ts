import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { clubRosterSnapshots, rosterDiffs, canonicalClubs } from "@workspace/db/schema";
import { eq, ilike, sql, asc, desc } from "drizzle-orm";
import {
  RosterSnapshotSearchResponse,
  RosterDiffSearchResponse,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// ---------------------------------------------------------------------------
// GET /api/rosters/snapshots — Search roster snapshots
// ---------------------------------------------------------------------------

router.get("/rosters/snapshots", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const clubName = req.query.club_name as string | undefined;
    const season = req.query.season as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;
    const playerName = req.query.player_name as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(clubRosterSnapshots.clubId, clubId)
        : undefined,
      clubName
        ? ilike(
            clubRosterSnapshots.clubNameRaw,
            `%${escapeLike(clubName)}%`,
          )
        : undefined,
      season ? eq(clubRosterSnapshots.season, season) : undefined,
      ageGroup ? eq(clubRosterSnapshots.ageGroup, ageGroup) : undefined,
      gender ? eq(clubRosterSnapshots.gender, gender) : undefined,
      playerName
        ? ilike(
            clubRosterSnapshots.playerName,
            `%${escapeLike(playerName)}%`,
          )
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(clubRosterSnapshots)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select({
        id: clubRosterSnapshots.id,
        clubId: clubRosterSnapshots.clubId,
        clubNameRaw: clubRosterSnapshots.clubNameRaw,
        season: clubRosterSnapshots.season,
        ageGroup: clubRosterSnapshots.ageGroup,
        gender: clubRosterSnapshots.gender,
        division: clubRosterSnapshots.division,
        playerName: clubRosterSnapshots.playerName,
        jerseyNumber: clubRosterSnapshots.jerseyNumber,
        position: clubRosterSnapshots.position,
        sourceUrl: clubRosterSnapshots.sourceUrl,
        snapshotDate: clubRosterSnapshots.snapshotDate,
        scrapedAt: clubRosterSnapshots.scrapedAt,
        source: clubRosterSnapshots.source,
        eventId: clubRosterSnapshots.eventId,
      })
      .from(clubRosterSnapshots)
      .where(where)
      .orderBy(
        asc(clubRosterSnapshots.clubNameRaw),
        asc(clubRosterSnapshots.playerName),
      )
      .limit(pageSize)
      .offset(offset);

    res.json(
      RosterSnapshotSearchResponse.parse({
        snapshots: rows.map((r) => ({
          id: r.id,
          club_id: r.clubId ?? null,
          club_name_raw: r.clubNameRaw,
          season: r.season,
          age_group: r.ageGroup,
          gender: r.gender,
          division: r.division ?? null,
          player_name: r.playerName,
          jersey_number: r.jerseyNumber ?? null,
          position: r.position ?? null,
          source_url: r.sourceUrl ?? null,
          snapshot_date: r.snapshotDate ? r.snapshotDate.toISOString() : null,
          scraped_at: r.scrapedAt.toISOString(),
          source: r.source ?? null,
          event_id: r.eventId ?? null,
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

// ---------------------------------------------------------------------------
// GET /api/rosters/snapshots/by-club/:clubId
// ---------------------------------------------------------------------------

router.get(
  "/rosters/snapshots/by-club/:clubId",
  async (req, res, next): Promise<void> => {
    try {
      const clubId = Number(req.params.clubId);
      if (isNaN(clubId)) {
        res.status(400).json({ error: "Invalid clubId" });
        return;
      }

      const season = req.query.season as string | undefined;
      const ageGroup = req.query.age_group as string | undefined;
      const gender = req.query.gender as string | undefined;

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const where = buildWhere([
        eq(clubRosterSnapshots.clubId, clubId),
        season ? eq(clubRosterSnapshots.season, season) : undefined,
        ageGroup ? eq(clubRosterSnapshots.ageGroup, ageGroup) : undefined,
        gender ? eq(clubRosterSnapshots.gender, gender) : undefined,
      ]);

      const [countRow] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(clubRosterSnapshots)
        .where(where);

      const total = countRow?.count ?? 0;

      const rows = await db
        .select({
          id: clubRosterSnapshots.id,
          clubId: clubRosterSnapshots.clubId,
          clubNameRaw: clubRosterSnapshots.clubNameRaw,
          season: clubRosterSnapshots.season,
          ageGroup: clubRosterSnapshots.ageGroup,
          gender: clubRosterSnapshots.gender,
          division: clubRosterSnapshots.division,
          playerName: clubRosterSnapshots.playerName,
          jerseyNumber: clubRosterSnapshots.jerseyNumber,
          position: clubRosterSnapshots.position,
          sourceUrl: clubRosterSnapshots.sourceUrl,
          snapshotDate: clubRosterSnapshots.snapshotDate,
          scrapedAt: clubRosterSnapshots.scrapedAt,
          source: clubRosterSnapshots.source,
          eventId: clubRosterSnapshots.eventId,
        })
        .from(clubRosterSnapshots)
        .where(where)
        .orderBy(asc(clubRosterSnapshots.playerName))
        .limit(pageSize)
        .offset(offset);

      res.json(
        RosterSnapshotSearchResponse.parse({
          snapshots: rows.map((r) => ({
            id: r.id,
            club_id: r.clubId ?? null,
            club_name_raw: r.clubNameRaw,
            season: r.season,
            age_group: r.ageGroup,
            gender: r.gender,
            division: r.division ?? null,
            player_name: r.playerName,
            jersey_number: r.jerseyNumber ?? null,
            position: r.position ?? null,
            source_url: r.sourceUrl ?? null,
            snapshot_date: r.snapshotDate
              ? r.snapshotDate.toISOString()
              : null,
            scraped_at: r.scrapedAt.toISOString(),
            source: r.source ?? null,
            event_id: r.eventId ?? null,
          })),
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
// GET /api/rosters/diffs — Search roster diffs
// ---------------------------------------------------------------------------

router.get("/rosters/diffs", async (req, res, next): Promise<void> => {
  try {
    const clubIdRaw = req.query.club_id as string | undefined;
    const clubId = clubIdRaw ? Number(clubIdRaw) : undefined;
    const clubName = req.query.club_name as string | undefined;
    const season = req.query.season as string | undefined;
    const diffType = req.query.diff_type as string | undefined;
    const playerName = req.query.player_name as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      clubId !== undefined && !isNaN(clubId)
        ? eq(rosterDiffs.clubId, clubId)
        : undefined,
      clubName
        ? ilike(rosterDiffs.clubNameRaw, `%${escapeLike(clubName)}%`)
        : undefined,
      season ? eq(rosterDiffs.season, season) : undefined,
      diffType ? eq(rosterDiffs.diffType, diffType) : undefined,
      playerName
        ? ilike(rosterDiffs.playerName, `%${escapeLike(playerName)}%`)
        : undefined,
    ]);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(rosterDiffs)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(rosterDiffs)
      .where(where)
      .orderBy(desc(rosterDiffs.detectedAt), desc(rosterDiffs.id))
      .limit(pageSize)
      .offset(offset);

    res.json(
      RosterDiffSearchResponse.parse({
        diffs: rows.map((r) => ({
          id: r.id,
          club_id: r.clubId ?? null,
          club_name_raw: r.clubNameRaw,
          season: r.season ?? null,
          age_group: r.ageGroup ?? null,
          gender: r.gender ?? null,
          player_name: r.playerName,
          diff_type: r.diffType,
          from_jersey_number: r.fromJerseyNumber ?? null,
          to_jersey_number: r.toJerseyNumber ?? null,
          from_position: r.fromPosition ?? null,
          to_position: r.toPosition ?? null,
          detected_at: r.detectedAt.toISOString(),
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

// ---------------------------------------------------------------------------
// GET /api/rosters/diffs/by-club/:clubId
// ---------------------------------------------------------------------------

router.get(
  "/rosters/diffs/by-club/:clubId",
  async (req, res, next): Promise<void> => {
    try {
      const clubId = Number(req.params.clubId);
      if (isNaN(clubId)) {
        res.status(400).json({ error: "Invalid clubId" });
        return;
      }

      const season = req.query.season as string | undefined;
      const diffType = req.query.diff_type as string | undefined;

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const where = buildWhere([
        eq(rosterDiffs.clubId, clubId),
        season ? eq(rosterDiffs.season, season) : undefined,
        diffType ? eq(rosterDiffs.diffType, diffType) : undefined,
      ]);

      const [countRow] = await db
        .select({ count: sql<number>`count(*)::int` })
        .from(rosterDiffs)
        .where(where);

      const total = countRow?.count ?? 0;

      const rows = await db
        .select()
        .from(rosterDiffs)
        .where(where)
        .orderBy(desc(rosterDiffs.detectedAt), desc(rosterDiffs.id))
        .limit(pageSize)
        .offset(offset);

      res.json(
        RosterDiffSearchResponse.parse({
          diffs: rows.map((r) => ({
            id: r.id,
            club_id: r.clubId ?? null,
            club_name_raw: r.clubNameRaw,
            season: r.season ?? null,
            age_group: r.ageGroup ?? null,
            gender: r.gender ?? null,
            player_name: r.playerName,
            diff_type: r.diffType,
            from_jersey_number: r.fromJerseyNumber ?? null,
            to_jersey_number: r.toJerseyNumber ?? null,
            from_position: r.fromPosition ?? null,
            to_position: r.toPosition ?? null,
            detected_at: r.detectedAt.toISOString(),
          })),
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
