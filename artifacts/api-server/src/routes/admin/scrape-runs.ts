/**
 * `/api/v1/admin/scrape-runs` — read-only admin view onto `scrape_run_logs`.
 *
 *   GET /api/v1/admin/scrape-runs?since=&source=&status=&limit=&page=
 *   GET /api/v1/admin/scrape-runs/:id
 *
 * The DB schema (see lib/db/src/schema/scrape-health.ts) and the API
 * contract (docs/planning/upshift-data-admin-api-contract.md) diverge on
 * field names — `scraper_key` vs. `source`, `completed_at` vs.
 * `finishedAt`, `records_created + records_updated` vs. `rowsIn/rowsOut`.
 * This route is the single place we translate between the two shapes so
 * the contract is stable even if the DB evolves.
 *
 * Status mapping:
 *   DB status is one of: 'running' | 'ok' | 'partial' | 'failed'
 *   Contract status is:  'running' | 'success' | 'failure'
 *   Mapping: ok → success, partial → success (it completed), failed → failure,
 *            running → running.
 */
import { Router, type IRouter } from "express";
import { and, desc, eq, gte, sql, type SQL } from "drizzle-orm";
import { db, scrapeRunLogs } from "@workspace/db";
import { ScrapeRunLog, ScrapeRunLogList } from "@hlbiv/api-zod/admin";
import { parsePagination } from "../../lib/pagination";

const router: IRouter = Router();

type ContractStatus = "success" | "failure" | "running";

function mapStatus(dbStatus: string): ContractStatus {
  if (dbStatus === "running") return "running";
  if (dbStatus === "failed") return "failure";
  // 'ok' and 'partial' both represent "run completed" — collapse to success.
  return "success";
}

/**
 * Inverse mapping: from a contract-status filter to a SQL predicate on the
 * DB column. Returns undefined for an unknown status (caller skips the
 * filter rather than returning a 400 — matches the other /api/* routes).
 */
function statusFilterSQL(value: string): SQL | undefined {
  if (value === "running") return eq(scrapeRunLogs.status, "running");
  if (value === "failure") return eq(scrapeRunLogs.status, "failed");
  if (value === "success") {
    return sql`${scrapeRunLogs.status} IN ('ok', 'partial')`;
  }
  return undefined;
}

function rowToContract(row: typeof scrapeRunLogs.$inferSelect): unknown {
  const rowsIn =
    row.recordsCreated != null || row.recordsUpdated != null
      ? (row.recordsCreated ?? 0) + (row.recordsUpdated ?? 0)
      : null;
  return {
    id: row.id,
    source: row.scraperKey,
    // No dedicated jobKey column yet — scheduler work will add one. Until
    // then, the best approximation is the scraper_key (scrape source).
    // Keep it explicitly null so clients don't think it's populated data.
    jobKey: null,
    status: mapStatus(row.status),
    startedAt: row.startedAt.toISOString(),
    finishedAt: row.completedAt ? row.completedAt.toISOString() : null,
    rowsIn,
    rowsOut: row.recordsTouched ?? null,
    errorMessage: row.errorMessage ?? null,
    // No metadata jsonb column on scrape_run_logs today; surface the
    // failure_kind + triggered_by as a synthetic metadata bag so admin UIs
    // don't have to special-case.
    metadata: {
      failureKind: row.failureKind ?? null,
      triggeredBy: row.triggeredBy,
      sourceUrl: row.sourceUrl ?? null,
      leagueName: row.leagueName ?? null,
    },
  };
}

router.get("/", async (req, res, next): Promise<void> => {
  try {
    const { page, pageSize, offset } = parsePagination(
      req.query.page,
      req.query.limit ?? req.query.page_size,
    );

    const conditions: SQL[] = [];

    const since = req.query.since;
    if (typeof since === "string" && since.length > 0) {
      const parsed = new Date(since);
      if (!Number.isNaN(parsed.getTime())) {
        conditions.push(gte(scrapeRunLogs.startedAt, parsed));
      }
    }

    const source = req.query.source;
    if (typeof source === "string" && source.length > 0) {
      conditions.push(eq(scrapeRunLogs.scraperKey, source));
    }

    const status = req.query.status;
    if (typeof status === "string" && status.length > 0) {
      const pred = statusFilterSQL(status);
      if (pred) conditions.push(pred);
    }

    const where = conditions.length > 0 ? and(...conditions) : undefined;

    const [countRow] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(scrapeRunLogs)
      .where(where);

    const total = countRow?.count ?? 0;

    const rows = await db
      .select()
      .from(scrapeRunLogs)
      .where(where)
      .orderBy(desc(scrapeRunLogs.startedAt))
      .limit(pageSize)
      .offset(offset);

    res.json(
      ScrapeRunLogList.parse({
        runs: rows.map(rowToContract),
        total,
        page,
        pageSize,
      }),
    );
  } catch (err) {
    next(err);
  }
});

router.get("/:id", async (req, res, next): Promise<void> => {
  try {
    const id = Number(req.params.id);
    if (!Number.isFinite(id)) {
      res.status(400).json({ error: "Invalid id" });
      return;
    }

    const [row] = await db
      .select()
      .from(scrapeRunLogs)
      .where(eq(scrapeRunLogs.id, id))
      .limit(1);

    if (!row) {
      res.status(404).json({ error: "ScrapeRunLog not found" });
      return;
    }

    res.json(ScrapeRunLog.parse(rowToContract(row)));
  } catch (err) {
    next(err);
  }
});

export default router;
