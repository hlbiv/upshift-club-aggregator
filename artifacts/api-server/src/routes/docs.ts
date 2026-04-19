/**
 * docs — interactive OpenAPI documentation via swagger-ui-express.
 *
 * Serves the OpenAPI 3.1 spec at `/api/docs` (mounted from `app.ts`).
 * Gated behind `API_DOCS_ENABLED=true` — when disabled, the factory
 * returns null and the route is never mounted.
 *
 * Auth note: `/api/docs` itself is deliberately reachable WITHOUT an API
 * key so the docs page loads in a browser. The apiKeyAuth middleware is
 * mounted BEFORE the router in `app.ts`, so this is enforced via an
 * explicit exemption added in that file. "Try it out" buttons inside the
 * UI will correctly 401 when the user hasn't supplied a key — that's the
 * intended UX.
 *
 * Spec loading: we use `createRequire` to resolve the absolute path to
 * `@workspace/api-spec/package.json`, then read `openapi.yaml` from that
 * same directory. This works in three runtime scenarios:
 *   1. tsx (tests, vitest) — import.meta.url points into src/routes/.
 *   2. Built output (`dist/index.mjs`) — import.meta.url points into dist/.
 *   3. Any future bundler that preserves Node's resolution — same story.
 * The workspace symlink at `node_modules/@workspace/api-spec` resolves
 * back to `lib/api-spec/`, where `openapi.yaml` lives.
 */
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { Router, type IRouter } from "express";
import swaggerUi from "swagger-ui-express";
import YAML from "yaml";

function loadOpenApiSpec(): Record<string, unknown> {
  const requireFn = createRequire(import.meta.url);
  const pkgPath = requireFn.resolve("@workspace/api-spec/package.json");
  const specPath = path.join(path.dirname(pkgPath), "openapi.yaml");
  const raw = fs.readFileSync(specPath, "utf8");
  const parsed = YAML.parse(raw);
  if (parsed === null || typeof parsed !== "object") {
    throw new Error(`Invalid OpenAPI spec at ${specPath}: expected object`);
  }
  return parsed as Record<string, unknown>;
}

/**
 * Build the docs router if `API_DOCS_ENABLED` is "true"; otherwise return
 * null so the caller can skip mounting and the route naturally 404s.
 *
 * Exported as a factory (rather than a bare Router) so tests can call it
 * with the env flag toggled in-process.
 */
export function buildDocsRouter(): IRouter | null {
  if (process.env.API_DOCS_ENABLED !== "true") {
    return null;
  }

  const spec = loadOpenApiSpec();
  const router: IRouter = Router();

  // swaggerUi.serve is an array of handlers that serve the static assets
  // (swagger-ui-dist CSS/JS). setup(spec) serves index.html with the spec
  // inlined so it renders without an extra HTTP round-trip.
  router.use("/", swaggerUi.serve);
  router.get(
    "/",
    swaggerUi.setup(spec, {
      customSiteTitle: "Upshift Data API",
    }),
  );

  return router;
}
