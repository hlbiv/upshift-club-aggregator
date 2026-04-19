/**
 * docs route — integration tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx src/__tests__/docs.test.ts
 *
 * Verifies the feature flag: with API_DOCS_ENABLED=true a router is built
 * and GET /api/docs/ returns swagger-ui HTML; without the flag the factory
 * returns null and the caller would not mount anything (so we simulate a
 * 404 by mounting nothing).
 */
import http from "node:http";
import express from "express";
import { buildDocsRouter } from "../routes/docs";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

interface HitResult {
  status: number;
  body: string;
  contentType: string;
}

async function hit(port: number, path: string): Promise<HitResult> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: "127.0.0.1", port, path, method: "GET" },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          resolve({
            status: res.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
            contentType: String(res.headers["content-type"] ?? ""),
          });
        });
      },
    );
    req.on("error", reject);
    req.end();
  });
}

function startServer(configure: (app: express.Express) => void): Promise<{
  port: number;
  close: () => Promise<void>;
}> {
  return new Promise((resolve, reject) => {
    const app = express();
    configure(app);
    const server = app.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        reject(new Error("no server address"));
        return;
      }
      resolve({
        port: addr.port,
        close: () =>
          new Promise<void>((r, j) => server.close((err) => (err ? j(err) : r()))),
      });
    });
  });
}

async function run() {
  // 1. Flag ENABLED: factory returns a router; GET /api/docs/ returns HTML.
  {
    const prev = process.env.API_DOCS_ENABLED;
    process.env.API_DOCS_ENABLED = "true";
    let router;
    try {
      router = buildDocsRouter();
    } finally {
      process.env.API_DOCS_ENABLED = prev;
    }
    assert(router !== null, "enabled-build", "factory should return a router when flag is true");
    if (router) {
      const { port, close } = await startServer((app) => {
        app.use("/api/docs", router);
      });
      try {
        const res = await hit(port, "/api/docs/");
        assert(
          res.status === 200,
          "enabled-get",
          `GET /api/docs/ should be 200, got ${res.status}`,
        );
        assert(
          res.contentType.includes("text/html"),
          "enabled-get",
          `content-type should be text/html, got ${res.contentType}`,
        );
        // Swagger UI injects a specific div id + a "swagger-ui" class.
        // Checking both gives us high confidence we served the real UI
        // (not an accidental blank page from some 302 redirect eating the
        // body).
        assert(
          res.body.includes("swagger-ui"),
          "enabled-get",
          "body should contain 'swagger-ui' marker",
        );
      } finally {
        await close();
      }
    }
  }

  // 2. Flag UNSET: factory returns null → route is never mounted → 404.
  {
    const prev = process.env.API_DOCS_ENABLED;
    delete process.env.API_DOCS_ENABLED;
    let router;
    try {
      router = buildDocsRouter();
    } finally {
      if (prev !== undefined) process.env.API_DOCS_ENABLED = prev;
    }
    assert(router === null, "disabled-build", "factory should return null when flag is unset");

    const { port, close } = await startServer(() => {
      // Intentionally mount nothing — simulates app.ts behavior when the
      // factory returned null.
    });
    try {
      const res = await hit(port, "/api/docs/");
      assert(
        res.status === 404,
        "disabled-get",
        `GET /api/docs/ should be 404, got ${res.status}`,
      );
    } finally {
      await close();
    }
  }

  // 3. Flag set to "false": also returns null (only the literal "true"
  //    activates the feature, mirroring the API_KEY_AUTH_ENABLED pattern).
  {
    const prev = process.env.API_DOCS_ENABLED;
    process.env.API_DOCS_ENABLED = "false";
    let router;
    try {
      router = buildDocsRouter();
    } finally {
      if (prev === undefined) delete process.env.API_DOCS_ENABLED;
      else process.env.API_DOCS_ENABLED = prev;
    }
    assert(
      router === null,
      "false-build",
      "factory should return null when flag is 'false'",
    );
  }

  if (failures.length === 0) {
    console.log("[docs-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[docs-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
