/**
 * `/v1/admin/scrape-health` — read-only admin view onto `scrape_health`.
 *
 *   GET /v1/admin/scrape-health
 *   GET /v1/admin/scrape-health/:entity_type/:entity_id
 *
 * The `scrape_health` table has an `(entity_type, entity_id)` composite key
 * — not a surrogate serial — so the detail route is 2-segment. Status
 * mapping is the same as scrape-runs.ts: DB `ok`/`stale`/`failed`/`never`
 * collapses to contract `success`/`failure`/`running`/null:
 *   ok → success, stale → success (it DID run), failed → failure,
 *   never → null (nothing has ever run for this entity).
 *
 * Pagination: `scrape_health` is bounded by real entity counts today, which
 * keeps the list well under the 1000-row cap even for the whole club graph.
 * The route still supports `?page=&page_size=` to future-proof against
 * eventual growth.
 */
import { Router, type IRouter } from "express";
import { and, desc, eq, sql } from "drizzle-orm";
import { db, scrapeHealth } from "@workspace/db";
import { ScrapeHealthList, ScrapeHealthRow } from "@hlbiv/api-zod/admin";
import { parsePagination } from "../../lib/pagination";

const router: IRouter = Router();

type ContractStatus = "success" | "failure" | "running" | null;

function mapStatus(db: string): ContractStatus {
  if (db === "never") return null;
  if (db === "failed") return "failure";
  // 'ok' or 'stale' — the entity has been scraped at some point.
  return "success";
}

const VALID_ENTITY_TYPES = new Set([
  "club",
  "event",
  "league",
  "college",
  "coach",
]);

function rowToContract(row: typeof scrapeHealth.$inferSelect): unknown {
  return {
    entityType: row.entityType,
    entityId: row.entityId,
    lastScrapedAt: row.lastScrapedAt ? row.lastScrapedAt.toISOString() : null,
    lastStatus: mapStatus(row.status),
    consecutiveFailures: row.consecutiveFailures,
    nextScheduledAt: row.nextScheduledAt
      ? row.nextScheduledAt.toISOString()
      : null,
    // No metadata jsonb column on scrape_health; surface auxiliary fields
    // as a synthetic bag so the contract shape is satisfied.
    metadata: {
      confidence: row.confidence ?? null,
      lastError: row.lastError ?? null,
      lastSuccessAt: row.lastSuccessAt
        ? row.lastSuccessAt.toISOString()
        : null,
      priority: row.priority ?? null,
    },
  };
}

router.get("/", async (req, res, next): Promise<void> => {
  try {
    const { page: _page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.page_size,
    );

    // The contract shape doesn't include page/pageSize — it's {rows, total}
    // — but we still respect the query params so a caller can page through
    // a large result. total counts the whole table regardless.
    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(scrapeHealth);
    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(scrapeHealth)
      .orderBy(desc(scrapeHealth.lastScrapedAt))
      .limit(pageSize)
      .offset(offset);

    // Filter rows that don't pass the contract's entity_type enum (defensive
    // — the DB's CHECK is wider than the contract's enum on e.g. 'match',
    // 'tryout'). The contract currently covers club/event/league/college/coach.
    const filtered = rows.filter((r) => VALID_ENTITY_TYPES.has(r.entityType));

    res.json(
      ScrapeHealthList.parse({
        rows: filtered.map(rowToContract),
        total,
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/:entity_type/:entity_id", async (req, res, next): Promise<void> => {
  try {
    const entityType = req.params.entity_type;
    const entityId = Number(req.params.entity_id);
    if (!entityType || !VALID_ENTITY_TYPES.has(entityType)) {
      res.status(400).json({ error: "Invalid entity_type" });
      return;
    }
    if (!Number.isFinite(entityId)) {
      res.status(400).json({ error: "Invalid entity_id" });
      return;
    }

    const [row] = await db
      .select()
      .from(scrapeHealth)
      .where(
        and(
          eq(scrapeHealth.entityType, entityType),
          eq(scrapeHealth.entityId, entityId),
        ),
      )
      .limit(1);

    if (!row) {
      res.status(404).json({ error: "ScrapeHealth row not found" });
      return;
    }

    res.json(ScrapeHealthRow.parse(rowToContract(row)));
  } catch (err) {
    next(err);
  }
});

export default router;
