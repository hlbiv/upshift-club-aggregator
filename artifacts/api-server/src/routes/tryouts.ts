import { Router, type IRouter } from "express";
import { db } from "@workspace/db";
import { tryouts } from "@workspace/db/schema";
import { eq, ilike, gte, lte, sql, asc, inArray } from "drizzle-orm";
import {
  TryoutSearchResponse,
  TryoutStatsResponse,
  TryoutSubmitBody,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

const router: IRouter = Router();

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// ---------------------------------------------------------------------------
// GET /api/tryouts/search
// ---------------------------------------------------------------------------
router.get("/tryouts/search", async (req, res, next): Promise<void> => {
  try {
    const clubName = req.query.club_name as string | undefined;
    const ageGroup = req.query.age_group as string | undefined;
    const gender = req.query.gender as string | undefined;
    const state = req.query.state as string | undefined;
    const status = req.query.status as string | undefined;
    const source = req.query.source as string | undefined;

    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const statusFilter = status
      ? eq(tryouts.status, status)
      : inArray(tryouts.status, ["upcoming", "active"]);

    const where = buildWhere([
      statusFilter,
      clubName
        ? ilike(tryouts.clubNameRaw, `%${escapeLike(clubName)}%`)
        : undefined,
      ageGroup
        ? ilike(tryouts.ageGroup, `%${escapeLike(ageGroup)}%`)
        : undefined,
      gender ? eq(tryouts.gender, gender) : undefined,
      state
        ? ilike(tryouts.locationState, `%${escapeLike(state)}%`)
        : undefined,
      source ? eq(tryouts.source, source) : undefined,
    ]);

    const [rows, countResult] = await Promise.all([
      db
        .select()
        .from(tryouts)
        .where(where)
        .orderBy(asc(tryouts.tryoutDate))
        .limit(pageSize)
        .offset(offset),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(tryouts)
        .where(where),
    ]);

    const total = countResult[0]?.count ?? 0;

    res.json({
      items: rows,
      total,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/tryouts/upcoming — convenience alias (status=upcoming, no filter)
// ---------------------------------------------------------------------------
router.get("/tryouts/upcoming", async (req, res, next): Promise<void> => {
  try {
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    const where = eq(tryouts.status, "upcoming");

    const [rows, countResult] = await Promise.all([
      db
        .select()
        .from(tryouts)
        .where(where)
        .orderBy(asc(tryouts.tryoutDate))
        .limit(pageSize)
        .offset(offset),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(tryouts)
        .where(where),
    ]);

    res.json({
      items: rows,
      total: countResult[0]?.count ?? 0,
      page,
      page_size: pageSize,
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/tryouts/stats — aggregate counts by status/source
// ---------------------------------------------------------------------------
router.get("/tryouts/stats", async (_req, res, next): Promise<void> => {
  try {
    const byStatus = await db
      .select({
        status: tryouts.status,
        count: sql<number>`count(*)::int`,
      })
      .from(tryouts)
      .groupBy(tryouts.status);

    const bySource = await db
      .select({
        source: tryouts.source,
        count: sql<number>`count(*)::int`,
      })
      .from(tryouts)
      .groupBy(tryouts.source);

    const totalResult = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(tryouts);

    res.json({
      total: totalResult[0]?.count ?? 0,
      by_status: Object.fromEntries(
        byStatus.map((r) => [r.status, r.count]),
      ),
      by_source: Object.fromEntries(
        bySource.map((r) => [r.source, r.count]),
      ),
    });
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// GET /api/tryouts/by-club/:clubId
// ---------------------------------------------------------------------------
router.get(
  "/tryouts/by-club/:clubId",
  async (req, res, next): Promise<void> => {
    try {
      const clubId = Number(req.params.clubId);
      if (!clubId || isNaN(clubId)) {
        res.status(400).json({ error: "Invalid clubId" });
        return;
      }

      const { page, pageSize, offset } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const where = eq(tryouts.clubId, clubId);

      const [rows, countResult] = await Promise.all([
        db
          .select()
          .from(tryouts)
          .where(where)
          .orderBy(asc(tryouts.tryoutDate))
          .limit(pageSize)
          .offset(offset),
        db
          .select({ count: sql<number>`count(*)::int` })
          .from(tryouts)
          .where(where),
      ]);

      res.json({
        items: rows,
        total: countResult[0]?.count ?? 0,
        page,
        page_size: pageSize,
      });
    } catch (err) {
      next(err);
    }
  },
);

// ---------------------------------------------------------------------------
// GET /api/tryouts/:id
// ---------------------------------------------------------------------------
router.get("/tryouts/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (!id || isNaN(id)) {
      res.status(400).json({ error: "Invalid tryout id" });
      return;
    }

    const [row] = await db
      .select()
      .from(tryouts)
      .where(eq(tryouts.id, id))
      .limit(1);

    if (!row) {
      res.status(404).json({ error: "Tryout not found" });
      return;
    }

    res.json(row);
  } catch (err) {
    next(err);
  }
});

// ---------------------------------------------------------------------------
// POST /api/tryouts/submit — manual tryout submission
// ---------------------------------------------------------------------------
router.post("/tryouts/submit", async (req, res, next): Promise<void> => {
  try {
    const parsed = TryoutSubmitBody.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({
        error: "Validation failed",
        details: parsed.error.flatten(),
      });
      return;
    }

    const data = parsed.data;

    const [inserted] = await db
      .insert(tryouts)
      .values({
        clubNameRaw: data.club_name_raw,
        ageGroup: data.age_group ?? null,
        gender: data.gender ?? null,
        tryoutDate: data.tryout_date ? new Date(data.tryout_date) : null,
        locationName: data.location_name ?? null,
        locationCity: data.location_city ?? null,
        locationState: data.location_state ?? null,
        url: data.url ?? null,
        notes: data.notes ?? null,
        source: "manual",
        status: "upcoming",
      })
      .returning();

    res.status(201).json(inserted);
  } catch (err) {
    next(err);
  }
});

export default router;
