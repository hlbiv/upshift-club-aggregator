import path from "node:path";
import { fileURLToPath } from "node:url";
import express, { type Express } from "express";
import cors from "cors";
import pinoHttp from "pino-http";
import router from "./routes";
import { logger } from "./lib/logger";
import { apiKeyAuth } from "./middlewares/apiKeyAuth";

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
  app.use(/^\/api(?!\/healthz)/, apiKeyAuth);
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

app.use("/api", router);

// --------------------------------------------------------------------------
// Serve the built mockup-sandbox frontend as static files.
// In production the Vite build output lives at ../../mockup-sandbox/dist.
// All non-API requests fall through to index.html (SPA client-side routing).
// --------------------------------------------------------------------------
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendDist = path.resolve(__dirname, "../../mockup-sandbox/dist");

app.use(express.static(frontendDist));
app.get("*", (_req, res, next) => {
  // Don't intercept /api routes that didn't match
  if (_req.path.startsWith("/api")) return next();
  res.sendFile(path.join(frontendDist, "index.html"), (err) => {
    if (err) next();
  });
});

export default app;
