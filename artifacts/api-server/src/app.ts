import path from "node:path";
import { fileURLToPath } from "node:url";
import express, { type Express } from "express";
import cors from "cors";
import pinoHttp from "pino-http";
import router from "./routes";
import { logger } from "./lib/logger";
import { apiKeyAuth } from "./middlewares/apiKeyAuth";
import { buildRateLimiter } from "./middlewares/rateLimit";
import { buildDocsRouter } from "./routes/docs";

const app: Express = express();

app.use(
  pinoHttp({
    logger,
    serializers: {
      req(req) {
        return {
          id: req.id,
          method: req.method,
          url: req.url?.split("?")[0],
        };
      },
      res(res) {
        return {
          statusCode: res.statusCode,
        };
      },
    },
  }),
);
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// M2M API-key auth. Runs before the router so every `/api/*` path except
// the liveness probe requires a valid key. See middlewares/apiKeyAuth.ts.
//
// Feature-flagged so a fresh deploy doesn't 401 every request before the
// operator has had a chance to create a key and distribute it to callers.
// Bootstrap sequence: run scripts/create-api-key → set API_KEY_AUTH_ENABLED=true
// in Replit Secrets → restart server.
//
// Auth is always skipped in development so the Replit preview pane works
// without needing a key. /api/healthz is always public regardless of mode.
const _authEnabled =
  process.env.API_KEY_AUTH_ENABLED === "true" &&
  process.env.NODE_ENV !== "development";

if (_authEnabled) {
  // Exempt /api/healthz (liveness) and /api/docs/* (OpenAPI UI) from auth.
  // Docs are deliberately public so a browser can load the page without a
  // key; "Try it out" calls from the UI hit authed routes and correctly
  // 401 without a key.
  app.use(/^\/api(?!\/healthz|\/docs)/, apiKeyAuth);
  // eslint-disable-next-line no-console
  console.log("[api-key-auth] enabled");
} else {
  // eslint-disable-next-line no-console
  console.log(
    process.env.NODE_ENV === "development"
      ? "[api-key-auth] DISABLED in development mode"
      : "[api-key-auth] DISABLED (set API_KEY_AUTH_ENABLED=true to enable)",
  );
}

// Rate limiting. Runs AFTER apiKeyAuth so authenticated requests can be
// keyed off req.apiKey.id (larger bucket), while unauthenticated routes
// fall back to per-IP limiting (smaller bucket). Feature-flagged so a
// fresh deploy doesn't throttle traffic before the operator has tuned the
// limits. See middlewares/rateLimit.ts.
const _rateLimitEnabled = process.env.API_RATE_LIMIT_ENABLED === "true";
if (_rateLimitEnabled) {
  app.use(/^\/api(?!\/docs)/, buildRateLimiter());
  // eslint-disable-next-line no-console
  console.log("[rate-limit] enabled");
} else {
  // eslint-disable-next-line no-console
  console.log(
    "[rate-limit] DISABLED (set API_RATE_LIMIT_ENABLED=true to enable)",
  );
}

// Interactive OpenAPI documentation. Mounted BEFORE the main router so
// `/api/docs` beats any accidental catch-all. Feature-flagged — if
// disabled the router is null and the path naturally 404s.
const docsRouter = buildDocsRouter();
if (docsRouter) {
  app.use("/api/docs", docsRouter);
  // eslint-disable-next-line no-console
  console.log("[api-docs] serving at /api/docs");
} else {
  // eslint-disable-next-line no-console
  console.log("[api-docs] DISABLED (set API_DOCS_ENABLED=true to enable)");
}

app.use("/api", router);

// --------------------------------------------------------------------------
// Serve the built mockup-sandbox frontend as static files.
// In production the Vite build output lives at ../../mockup-sandbox/dist.
// All non-API requests fall through to index.html (SPA client-side routing).
// --------------------------------------------------------------------------
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendDist = path.resolve(__dirname, "../../mockup-sandbox/dist");

app.use(express.static(frontendDist));
app.get("/{*path}", (_req, res, next) => {
  // Don't intercept /api routes that didn't match
  if (_req.path.startsWith("/api")) return next();
  res.sendFile(path.join(frontendDist, "index.html"), (err) => {
    if (err) next();
  });
});

export default app;
