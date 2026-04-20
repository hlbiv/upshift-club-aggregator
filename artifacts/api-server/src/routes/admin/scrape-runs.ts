/**
 * `/api/v1/admin/scrape-runs` — read-only admin view onto `scrape_run_logs`.
 *
 *   GET /api/v1/admin/scrape-runs?since=&source=&status=&limit=&page=
 *   GET /api/v1/admin/scrape-runs/:id
 *
 * As of api-zod v0.4.0 the contract field names mirror the DB column names
 * (scraperKey, completedAt, recordsTouched, status enum
 * `running|ok|partial|failed`). We select the rows and let Zod's `.parse()`
 * strip unknown DB-only fields (records_created, error_message detail, etc.).
 *
 * `?source=` query param is preserved for backward compatibility — it maps
 * to a `scraperKey` filter. Likewise the `?status=` filter accepts either
 * the legacy contract enum (`success`/`failure`) or the new DB enum
 * (`ok`/`partial`/`failed`/`running`) so callers mid-migration continue to
 * work.
 */
import { Router, type IRouter } from "express";
import { and, desc, eq, gte, sql, type SQL } from "drizzle-orm";
import { db, scrapeRunLogs } from "@workspace/db";
import { ScrapeRunLog, ScrapeRunLogList } from "@hlbiv/api-zod/admin";
import { parsePagination } from "../../lib/pagination";

const router: IRouter = Router();

/**
 * Map a status query param to a SQL predicate on the DB column. Accepts
 * the DB enum directly (`running`/`ok`/`partial`/`failed`) plus the two
 * legacy contract aliases (`success` → `ok|partial`, `failure` → `failed`)
 * so callers mid-migration don't break.
 */
function statusFilterSQL(value: string): SQL | undefined {
  if (value === "running") return eq(scrapeRunLogs.status, "running");
  if (value === "ok") return eq(scrapeRunLogs.status, "ok");
  if (value === "partial") return eq(scrapeRunLogs.status, "partial");
  if (value === "failed" || value === "failure") {
    return eq(scrapeRunLogs.status, "failed");
  }
  if (value === "success") {
    return sql`${scrapeRunLogs.status} IN ('ok', 'partial')`;
  }
  return undefined;
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

    // No dedicated `jobKey` column yet — scheduler work will add one. The
    // contract permits null so we surface null explicitly. `metadata` is
    // synthesized from supplementary scrape_run_logs columns that don't
    // have their own contract fields (failure_kind, triggered_by, etc.).
    res.json(
      ScrapeRunLogList.parse({
        runs: rows.map((row) => ({
          id: row.id,
          scraperKey: row.scraperKey,
          jobKey: null,
          status: row.status,
          startedAt: row.startedAt.toISOString(),
          completedAt: row.completedAt ? row.completedAt.toISOString() : null,
          recordsTouched: row.recordsTouched ?? null,
          errorMessage: row.errorMessage ?? null,
          metadata: {
            failureKind: row.failureKind ?? null,
            triggeredBy: row.triggeredBy,
            sourceUrl: row.sourceUrl ?? null,
            leagueName: row.leagueName ?? null,
          },
        })),
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

    res.json(
      ScrapeRunLog.parse({
        id: row.id,
        scraperKey: row.scraperKey,
        jobKey: null,
        status: row.status,
        startedAt: row.startedAt.toISOString(),
        completedAt: row.completedAt ? row.completedAt.toISOString() : null,
        recordsTouched: row.recordsTouched ?? null,
        errorMessage: row.errorMessage ?? null,
        metadata: {
          failureKind: row.failureKind ?? null,
          triggeredBy: row.triggeredBy,
          sourceUrl: row.sourceUrl ?? null,
          leagueName: row.leagueName ?? null,
        },
      }),
    );
  } catch (err) {
    next(err);
  }
});

export default router;
