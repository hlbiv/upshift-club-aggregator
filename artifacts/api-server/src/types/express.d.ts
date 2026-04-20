/**
 * Module augmentations for Express. Keeps `req.apiKey` (M2M) and
 * `req.adminAuth` (admin session or API key) strongly typed everywhere the
 * Express Request type is used, without relying on each route file to
 * import the specific middleware that "owns" the augmentation.
 *
 * Express's types use `declare module "express-serve-static-core"` under
 * the hood — mirroring the same incantation here so both apiKeyAuth and
 * requireAdmin see the augmented shape.
 */
import type { ApiKey } from "@workspace/db";
import type { AdminAuthContext } from "../middlewares/requireAdmin";

// Augment the global `Express.Request` interface — this is the single
// surface that express-serve-static-core, express, and route-handler
// inference all pull from. Adding to `declare module "express"` alone
// misses the inner `core.Request` that handlers registered via
// `router.get("/x", (req, res) => ...)` actually receive.
declare global {
  namespace Express {
    interface Request {
      apiKey?: Pick<
        ApiKey,
        "id" | "name" | "keyPrefix" | "scopes" | "createdAt"
      >;
      adminAuth?: AdminAuthContext;
    }
  }
}

// Without this export the file is not treated as a module, and the
// `declare module` augmentation above won't fire when the file is only
// picked up by `tsc --include`.
export {};
