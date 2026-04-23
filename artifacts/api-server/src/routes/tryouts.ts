/**
 * Tryouts router.
 *
 * GET /tryouts/search and GET /tryouts/upcoming are the Player
 * Platform's read contract:
 *   - read-only, anonymous (no API-key gating in v1);
 *   - upcoming-only — they unconditionally exclude rows whose
 *     `tryout_date` is in the past or NULL, regardless of `status`,
 *     because the status updater can lag;
 *   - return the stable `TryoutPublic` shape (no `site_change_id`,
 *     `scraped_at`, `detected_at`, `expires_at`);
 *   - cap `page_size` at 100, default 20;
 *   - sorted by `tryout_date` ascending.
 *
 * GET /tryouts/by-club/:clubId, GET /tryouts/:id, GET /tryouts/stats,
 * and POST /tryouts/submit are unchanged internal endpoints and may
 * include past-dated rows.
 *
 * The consumer handlers are factored as `makeSearchHandler(deps)` /
 * `makeUpcomingHandler(deps)` so unit tests can stand in an in-memory
 * fake for `deps.searchTryouts` without reaching Postgres. The default
 * router (`makeTryoutsRouter()` with no args) wires the real Drizzle
 * implementation. See adminCoverage.test.ts for the same pattern.
 */
import {
  Router,
  type IRouter,
  type Request,
  type Response,
  type NextFunction,
} from "express";
import { db } from "@workspace/db";
import { tryouts } from "@workspace/db/schema";
import {
  eq,
  ilike,
  gte,
  lte,
  sql,
  asc,
  inArray,
  isNotNull,
} from "drizzle-orm";
import {
  TryoutSubmitBody,
  TryoutPublic,
} from "@hlbiv/api-zod";
import { parsePagination, buildWhere } from "../lib/pagination";

function escapeLike(raw: string): string {
  return raw.replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

const ISO_DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;
const ISO_DATETIME_RE = /^\d{4}-\d{2}-\d{2}T.*$/;

/**
 * Parse an ISO-8601 date string. `endOfDay=true` is used for inclusive
 * upper bounds: a bare YYYY-MM-DD is normalized to 23:59:59.999 UTC of
 * that day so a tryout later on the same calendar day is included. Full
 * datetime strings are accepted as-is in both modes.
 *
 * Returns null on malformed input so the caller can return 400.
 */
export function parseIsoDate(
  raw: unknown,
  opts: { endOfDay?: boolean } = {},
): Date | null {
  if (typeof raw !== "string" || !raw) return null;
  if (ISO_DATE_ONLY_RE.test(raw)) {
    // Normalize to UTC start- or end-of-day so date-only input is
    // inclusive on both ends regardless of server timezone.
    const suffix = opts.endOfDay ? "T23:59:59.999Z" : "T00:00:00.000Z";
    const d = new Date(`${raw}${suffix}`);
    return isNaN(d.getTime()) ? null : d;
  }
  if (ISO_DATETIME_RE.test(raw)) {
    const d = new Date(raw);
    return isNaN(d.getTime()) ? null : d;
  }
  return null;
}

type TryoutRow = typeof tryouts.$inferSelect;
export type PublicTryout = ReturnType<typeof TryoutPublic.parse>;

/**
 * Project a raw `tryouts` row to the consumer-public shape. Drops
 * `siteChangeId`, `scrapedAt`, `detectedAt`, `expiresAt`. Dates are
 * serialized to ISO strings.
 */
export function toPublicTryout(row: TryoutRow): PublicTryout {
  return {
    id: row.id,
    club_id: row.clubId ?? null,
    club_name_raw: row.clubNameRaw,
    age_group: row.ageGroup ?? null,
    gender: row.gender ?? null,
    division: row.division ?? null,
    tryout_date: row.tryoutDate ? row.tryoutDate.toISOString() : null,
    registration_deadline: row.registrationDeadline
      ? row.registrationDeadline.toISOString()
      : null,
    location_name: row.locationName ?? null,
    location_address: row.locationAddress ?? null,
    location_city: row.locationCity ?? null,
    location_state: row.locationState ?? null,
    cost: row.cost ?? null,
    url: row.url ?? null,
    notes: row.notes ?? null,
    source: row.source,
    status: row.status,
  };
}

// ---------------------------------------------------------------------------
// Deps interface — lets unit tests stand in a fake searchTryouts.
// ---------------------------------------------------------------------------

export interface TryoutSearchFilters {
  clubName?: string;
  ageGroup?: string;
  gender?: string;
  state?: string;
  status?: string;
  source?: string;
  /** Inclusive lower bound on `tryout_date`. */
  dateFrom?: Date;
  /** Inclusive upper bound on `tryout_date`. */
  dateTo?: Date;
  /**
   * Hard floor on `tryout_date` — rows with `tryoutDate < upcomingFloor`
   * or `tryoutDate IS NULL` MUST be excluded. The route always sets
   * this to `new Date()` so past-dated rows can never leak into the
   * consumer response.
   */
  upcomingFloor: Date;
  page: number;
  pageSize: number;
}

export interface TryoutsDeps {
  searchTryouts: (
    filters: TryoutSearchFilters,
  ) => Promise<{ rows: TryoutRow[]; total: number }>;
}

// ---------------------------------------------------------------------------
// Production deps — real Drizzle queries.
// ---------------------------------------------------------------------------

export const prodTryoutsDeps: TryoutsDeps = {
  async searchTryouts(filters) {
    const statusFilter = filters.status
      ? eq(tryouts.status, filters.status)
      : inArray(tryouts.status, ["upcoming", "active"]);

    const where = buildWhere([
      // Consumer floor: tryout_date present AND >= now(). Load-bearing —
      // the Player Platform must never see past-dated rows even if the
      // status updater hasn't run yet.
      isNotNull(tryouts.tryoutDate),
      gte(tryouts.tryoutDate, filters.upcomingFloor),
      statusFilter,
      filters.clubName
        ? ilike(tryouts.clubNameRaw, `%${escapeLike(filters.clubName)}%`)
        : undefined,
      filters.ageGroup
        ? ilike(tryouts.ageGroup, `%${escapeLike(filters.ageGroup)}%`)
        : undefined,
      filters.gender ? eq(tryouts.gender, filters.gender) : undefined,
      filters.state
        ? ilike(tryouts.locationState, `%${escapeLike(filters.state)}%`)
        : undefined,
      filters.source ? eq(tryouts.source, filters.source) : undefined,
      filters.dateFrom ? gte(tryouts.tryoutDate, filters.dateFrom) : undefined,
      filters.dateTo ? lte(tryouts.tryoutDate, filters.dateTo) : undefined,
    ]);

    const offset = (filters.page - 1) * filters.pageSize;

    const [rows, countResult] = await Promise.all([
      db
        .select()
        .from(tryouts)
        .where(where)
        .orderBy(asc(tryouts.tryoutDate))
        .limit(filters.pageSize)
        .offset(offset),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(tryouts)
        .where(where),
    ]);

    return { rows, total: countResult[0]?.count ?? 0 };
  },
};

// ---------------------------------------------------------------------------
// Handler factories.
// ---------------------------------------------------------------------------

export function makeSearchHandler(deps: TryoutsDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const clubName = req.query.club_name as string | undefined;
      const ageGroup = req.query.age_group as string | undefined;
      const gender = req.query.gender as string | undefined;
      const state = req.query.state as string | undefined;
      const status = req.query.status as string | undefined;
      const source = req.query.source as string | undefined;

      let dateFrom: Date | undefined;
      let dateTo: Date | undefined;
      if (req.query.date_from !== undefined) {
        const parsed = parseIsoDate(req.query.date_from);
        if (parsed === null) {
          res
            .status(400)
            .json({ error: "Invalid date_from (expect ISO-8601)" });
          return;
        }
        dateFrom = parsed;
      }
      if (req.query.date_to !== undefined) {
        const parsed = parseIsoDate(req.query.date_to, { endOfDay: true });
        if (parsed === null) {
          res
            .status(400)
            .json({ error: "Invalid date_to (expect ISO-8601)" });
          return;
        }
        dateTo = parsed;
      }

      const { page, pageSize } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const { rows, total } = await deps.searchTryouts({
        clubName,
        ageGroup,
        gender,
        state,
        status,
        source,
        dateFrom,
        dateTo,
        upcomingFloor: new Date(),
        page,
        pageSize,
      });

      res.json({
        items: rows.map(toPublicTryout),
        total,
        page,
        page_size: pageSize,
      });
    } catch (err) {
      next(err);
    }
  };
}

export function makeUpcomingHandler(deps: TryoutsDeps) {
  return async (
    req: Request,
    res: Response,
    next: NextFunction,
  ): Promise<void> => {
    try {
      const { page, pageSize } = parsePagination(
        req.query.page,
        req.query.page_size,
      );

      const { rows, total } = await deps.searchTryouts({
        status: "upcoming",
        upcomingFloor: new Date(),
        page,
        pageSize,
      });

      res.json({
        items: rows.map(toPublicTryout),
        total,
        page,
        page_size: pageSize,
      });
    } catch (err) {
      next(err);
    }
  };
}

// ---------------------------------------------------------------------------
// Router factory.
// ---------------------------------------------------------------------------

export function makeTryoutsRouter(
  deps: TryoutsDeps = prodTryoutsDeps,
): IRouter {
  const router: IRouter = Router();

  router.get("/tryouts/search", makeSearchHandler(deps));
  router.get("/tryouts/upcoming", makeUpcomingHandler(deps));

  // Aggregate counts by status/source — internal/admin shape.
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

  // Internal/admin lookup. Past-dated rows intentionally included so
  // operators can browse history.
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

  return router;
}

const router = makeTryoutsRouter();
export default router;
