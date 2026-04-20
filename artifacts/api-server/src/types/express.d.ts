/**
 * Express Request augmentation — `req.apiKey`.
 *
 * Populated by `middlewares/apiKeyAuth.ts` on a successful key match.
 * Consumed by downstream middlewares (`requireScope`, `rateLimit`) and by
 * any route that needs to know which caller made the request.
 *
 * Kept in a dedicated ambient-types file so that multiple middlewares can
 * rely on the shape without importing from `apiKeyAuth`, which would
 * create a circular-ish dependency the moment a type-only import widens
 * into a runtime one.
 */
import type { ApiKey } from "@workspace/db";

declare module "express" {
  interface Request {
    apiKey?: Pick<ApiKey, "id" | "name" | "keyPrefix" | "scopes" | "createdAt">;
  }
}

export {};
