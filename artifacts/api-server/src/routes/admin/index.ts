/**
 * `/api/v1/admin` sub-app. Two exports:
 *
 *   - `unauthAdminRouter` — the single public route on the admin surface,
 *     `POST /auth/login`. Must NOT be behind requireAdmin (it IS the auth
 *     entry point). Mounted with its own 10/min rate limiter in app.ts.
 *
 *   - `authedAdminRouter` — everything else. Mounted behind requireAdmin +
 *     the 120/min read limiter. Contains:
 *       POST /auth/logout
 *       GET  /me
 *       GET  /scrape-runs, GET /scrape-runs/:id
 *       GET  /scrape-health, GET /scrape-health/:entity_type/:entity_id
 *       GET  /dedup/clubs, GET /dedup/clubs/:id
 *       POST /dedup/clubs/:id/merge, POST /dedup/clubs/:id/reject
 */
import { Router, type IRouter } from "express";
import { loginRouter, logoutRouter } from "./auth";
import meRouter from "./me";
import scrapeRunsRouter from "./scrape-runs";
import scrapeHealthRouter from "./scrape-health";
import { dedupRouter } from "./dedup";
import { dataQualityRouter } from "./data-quality";
import growthRouter from "./growth";

export const unauthAdminRouter: IRouter = Router();
unauthAdminRouter.use(loginRouter);

export const authedAdminRouter: IRouter = Router();
authedAdminRouter.use(logoutRouter);
authedAdminRouter.use(meRouter);
authedAdminRouter.use("/scrape-runs", scrapeRunsRouter);
authedAdminRouter.use("/scrape-health", scrapeHealthRouter);
authedAdminRouter.use("/dedup", dedupRouter);
authedAdminRouter.use("/data-quality", dataQualityRouter);
authedAdminRouter.use("/growth", growthRouter);
