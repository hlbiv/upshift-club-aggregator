import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import {
  coachDiscoveries,
  coaches,
  coachCareerHistory,
  coachMovementEvents,
  coachEffectiveness,
  canonicalClubs,
  colleges,
} from "@workspace/db/schema";
import { eq, ilike, gte, desc, sql, asc } from "drizzle-orm";
import {
  CoachSearchResponse,
  CoachDetailResponse,
  CoachCareerResponse,
  CoachMovementsResponse,
  CoachEffectivenessResponse,
  CoachLeaderboardResponse,
} from "@hlbiv/api-zod";
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

// ---------------------------------------------------------------------------
// Coach leaderboard (must be registered BEFORE :id to avoid capture)
// ---------------------------------------------------------------------------

/**
 * GET /api/coaches/top — Top coaches by total placements.
 */
router.get("/coaches/top", async (req, res, next): Promise<void> => {
  try {
    const minPlacementsRaw = req.query.min_placements as string | undefined;
    const minPlacements =
      minPlacementsRaw !== undefined ? Number(minPlacementsRaw) : undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = buildWhere([
      minPlacements !== undefined && !isNaN(minPlacements)
        ? gte(coachEffectiveness.playersPlacedTotal, minPlacements)
        : undefined,
    ]);

    const [totalRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(coachEffectiveness)
      .where(where);

    const total = totalRow?.count ?? 0;

    const rows = await db
      .select({
        coachId: coachEffectiveness.coachId,
        playersPlacedTotal: coachEffectiveness.playersPlacedTotal,
        playersPlacedD1: coachEffectiveness.playersPlacedD1,
        playersPlacedD2: coachEffectiveness.playersPlacedD2,
        playersPlacedD3: coachEffectiveness.playersPlacedD3,
        playersPlacedNaia: coachEffectiveness.playersPlacedNaia,
        playersPlacedNjcaa: coachEffectiveness.playersPlacedNjcaa,
        clubsCoached: coachEffectiveness.clubsCoached,
        seasonsTracked: coachEffectiveness.seasonsTracked,
        displayName: coaches.displayName,
        id: coaches.id,
      })
      .from(coachEffectiveness)
      .innerJoin(coaches, eq(coaches.id, coachEffectiveness.coachId))
      .where(where)
      .orderBy(desc(coachEffectiveness.playersPlacedTotal))
      .limit(pageSize)
      .offset(offset);

    res.json(
      CoachLeaderboardResponse.parse({
        coaches: rows.map((r) => ({
          id: r.id,
          display_name: r.displayName,
          players_placed_total: r.playersPlacedTotal,
          players_placed_d1: r.playersPlacedD1,
          players_placed_d2: r.playersPlacedD2,
          players_placed_d3: r.playersPlacedD3,
          players_placed_naia: r.playersPlacedNaia,
          players_placed_njcaa: r.playersPlacedNjcaa,
          clubs_coached: r.clubsCoached,
          seasons_tracked: r.seasonsTracked,
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
// Coach detail
// ---------------------------------------------------------------------------

/**
 * Resolve entity name for a career history or movement event row.
 * Returns the club or college name, or null if not found.
 */
async function resolveEntityName(
  entityType: string | null,
  entityId: number | null,
): Promise<string | null> {
  if (!entityType || entityId == null) return null;
  if (entityType === "club") {
    const [row] = await db
      .select({ name: canonicalClubs.clubNameCanonical })
      .from(canonicalClubs)
      .where(eq(canonicalClubs.id, entityId));
    return row?.name ?? null;
  }
  if (entityType === "college") {
    const [row] = await db
      .select({ name: colleges.name })
      .from(colleges)
      .where(eq(colleges.id, entityId));
    return row?.name ?? null;
  }
  return null;
}

/**
 * GET /api/coaches/:id — Single coach detail with career + effectiveness.
 */
router.get("/coaches/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [coach] = await db
      .select()
      .from(coaches)
      .where(eq(coaches.id, id));

    if (!coach) {
      res.status(404).json({ error: "Coach not found" });
      return;
    }

    const career = await db
      .select()
      .from(coachCareerHistory)
      .where(eq(coachCareerHistory.coachId, id))
      .orderBy(desc(coachCareerHistory.startYear));

    const careerWithNames = await Promise.all(
      career.map(async (c) => ({
        id: c.id,
        coach_id: c.coachId,
        entity_type: c.entityType,
        entity_id: c.entityId,
        entity_name: await resolveEntityName(c.entityType, c.entityId),
        role: c.role,
        start_year: c.startYear ?? null,
        end_year: c.endYear ?? null,
        is_current: c.isCurrent,
        source: c.source ?? null,
        source_url: c.sourceUrl ?? null,
        confidence: c.confidence ?? null,
      })),
    );

    const [eff] = await db
      .select()
      .from(coachEffectiveness)
      .where(eq(coachEffectiveness.coachId, id));

    res.json(
      CoachDetailResponse.parse({
        id: coach.id,
        person_hash: coach.personHash,
        display_name: coach.displayName,
        primary_email: coach.primaryEmail ?? null,
        first_seen_at: coach.firstSeenAt?.toISOString() ?? null,
        last_seen_at: coach.lastSeenAt?.toISOString() ?? null,
        career: careerWithNames,
        effectiveness: eff
          ? {
              id: eff.id,
              coach_id: eff.coachId,
              players_placed_d1: eff.playersPlacedD1,
              players_placed_d2: eff.playersPlacedD2,
              players_placed_d3: eff.playersPlacedD3,
              players_placed_naia: eff.playersPlacedNaia,
              players_placed_njcaa: eff.playersPlacedNjcaa,
              players_placed_total: eff.playersPlacedTotal,
              clubs_coached: eff.clubsCoached,
              seasons_tracked: eff.seasonsTracked,
              last_calculated_at: eff.lastCalculatedAt?.toISOString() ?? null,
            }
          : null,
      }),
    );
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// Coach career history
// ---------------------------------------------------------------------------

/**
 * GET /api/coaches/:id/career — Paginated career history.
 */
router.get("/coaches/:id/career", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [coach] = await db
      .select({ id: coaches.id })
      .from(coaches)
      .where(eq(coaches.id, id));

    if (!coach) {
      res.status(404).json({ error: "Coach not found" });
      return;
    }

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = eq(coachCareerHistory.coachId, id);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(coachCareerHistory)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(coachCareerHistory)
      .where(where)
      .orderBy(desc(coachCareerHistory.startYear))
      .limit(pageSize)
      .offset(offset);

    const careerWithNames = await Promise.all(
      rows.map(async (c) => ({
        id: c.id,
        coach_id: c.coachId,
        entity_type: c.entityType,
        entity_id: c.entityId,
        entity_name: await resolveEntityName(c.entityType, c.entityId),
        role: c.role,
        start_year: c.startYear ?? null,
        end_year: c.endYear ?? null,
        is_current: c.isCurrent,
        source: c.source ?? null,
        source_url: c.sourceUrl ?? null,
        confidence: c.confidence ?? null,
      })),
    );

    res.json(
      CoachCareerResponse.parse({
        career: careerWithNames,
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
// Coach movement events
// ---------------------------------------------------------------------------

/**
 * GET /api/coaches/:id/movements — Paginated movement feed.
 */
router.get("/coaches/:id/movements", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [coach] = await db
      .select({ id: coaches.id })
      .from(coaches)
      .where(eq(coaches.id, id));

    if (!coach) {
      res.status(404).json({ error: "Coach not found" });
      return;
    }

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = eq(coachMovementEvents.coachId, id);

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(coachMovementEvents)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(coachMovementEvents)
      .where(where)
      .orderBy(desc(coachMovementEvents.detectedAt))
      .limit(pageSize)
      .offset(offset);

    const movementsWithNames = await Promise.all(
      rows.map(async (m) => ({
        id: m.id,
        coach_id: m.coachId,
        event_type: m.eventType,
        from_entity_type: m.fromEntityType ?? null,
        from_entity_id: m.fromEntityId ?? null,
        from_entity_name: await resolveEntityName(m.fromEntityType, m.fromEntityId),
        to_entity_type: m.toEntityType ?? null,
        to_entity_id: m.toEntityId ?? null,
        to_entity_name: await resolveEntityName(m.toEntityType, m.toEntityId),
        from_role: m.fromRole ?? null,
        to_role: m.toRole ?? null,
        detected_at: m.detectedAt.toISOString(),
        confidence: m.confidence ?? null,
      })),
    );

    res.json(
      CoachMovementsResponse.parse({
        movements: movementsWithNames,
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
// Coach effectiveness
// ---------------------------------------------------------------------------

/**
 * GET /api/coaches/:id/effectiveness — Single effectiveness record.
 */
router.get("/coaches/:id/effectiveness", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (isNaN(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [eff] = await db
      .select()
      .from(coachEffectiveness)
      .where(eq(coachEffectiveness.coachId, id));

    if (!eff) {
      res.status(404).json({ error: "Effectiveness not found for this coach" });
      return;
    }

    res.json(
      CoachEffectivenessResponse.parse({
        id: eff.id,
        coach_id: eff.coachId,
        players_placed_d1: eff.playersPlacedD1,
        players_placed_d2: eff.playersPlacedD2,
        players_placed_d3: eff.playersPlacedD3,
        players_placed_naia: eff.playersPlacedNaia,
        players_placed_njcaa: eff.playersPlacedNjcaa,
        players_placed_total: eff.playersPlacedTotal,
        clubs_coached: eff.clubsCoached,
        seasons_tracked: eff.seasonsTracked,
        last_calculated_at: eff.lastCalculatedAt?.toISOString() ?? null,
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
