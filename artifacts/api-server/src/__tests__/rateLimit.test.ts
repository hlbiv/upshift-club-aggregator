/**
 * rateLimit middleware — integration tests.
 *
 * Run: pnpm --filter @workspace/api-server exec tsx src/__tests__/rateLimit.test.ts
 *
 * Same harness style as apiKeyAuth.test.ts — no vitest, no supertest. We
 * mount the middleware on a real Express app and drive it via Node's
 * `http` client. The factory `buildRateLimiter` takes a tiny limit so a
 * few rapid requests exercise the 429 path deterministically.
 */
import http from "node:http";
import express, { type Request, type Response, type NextFunction } from "express";
import { buildRateLimiter } from "../middlewares/rateLimit";

type Failure = { name: string; issue: string };
const failures: Failure[] = [];

function assert(cond: unknown, name: string, issue: string) {
  if (!cond) failures.push({ name, issue });
}

interface HitResult {
  status: number;
  body: string;
  headers: http.IncomingHttpHeaders;
}

async function hit(port: number, headers: Record<string, string> = {}): Promise<HitResult> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        path: "/api/test",
        method: "GET",
        headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          resolve({
            status: res.statusCode ?? 0,
            body: Buffer.concat(chunks).toString("utf8"),
            headers: res.headers,
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
  // 1. Unauthenticated: IP bucket with limit=3 → 4th req is 429.
  {
    const { port, close } = await startServer((app) => {
      app.use(buildRateLimiter({ windowMs: 60_000, authLimit: 100, ipLimit: 3 }));
      app.get("/api/test", (_req, res) => res.json({ ok: true }));
    });
    try {
      const results: HitResult[] = [];
      for (let i = 0; i < 5; i++) {
        results.push(await hit(port));
      }
      assert(
        results[0]!.status === 200,
        "ip-limit",
        `first request should be 200, got ${results[0]!.status}`,
      );
      assert(
        results[1]!.status === 200,
        "ip-limit",
        `second request should be 200, got ${results[1]!.status}`,
      );
      assert(
        results[2]!.status === 200,
        "ip-limit",
        `third request should be 200, got ${results[2]!.status}`,
      );
      assert(
        results[3]!.status === 429,
        "ip-limit",
        `fourth request should be 429, got ${results[3]!.status}`,
      );
      assert(
        results[4]!.status === 429,
        "ip-limit",
        `fifth request should be 429, got ${results[4]!.status}`,
      );
      const body = JSON.parse(results[3]!.body) as {
        error?: string;
        retry_after_seconds?: number;
      };
      assert(
        body.error === "rate_limited",
        "ip-limit",
        `429 body should have error='rate_limited', got ${body.error}`,
      );
      assert(
        typeof body.retry_after_seconds === "number" && body.retry_after_seconds > 0,
        "ip-limit",
        `retry_after_seconds should be a positive number, got ${body.retry_after_seconds}`,
      );
      // Draft-7 standard header should be present on 200 responses.
      assert(
        typeof results[0]!.headers["ratelimit"] === "string" ||
          typeof results[0]!.headers["ratelimit-limit"] === "string",
        "ip-limit",
        "expected RateLimit-* draft-7 header on successful response",
      );
    } finally {
      await close();
    }
  }

  // 2. Authenticated keys get their own bucket; the IP bucket is not
  //    consumed, so a third authed key 1 request should be fine even
  //    after exhausting a hypothetical IP bucket of the same size.
  //    Here we set a larger authLimit so authed keys pass while IP is
  //    still 20 (default) — but the point is: different keys = different
  //    buckets. We test that with two fake apiKey ids.
  {
    const { port, close } = await startServer((app) => {
      // Fake an authenticated request by stamping req.apiKey in a
      // pre-middleware, using an X-Fake-Key-Id header.
      app.use((req: Request, _res: Response, next: NextFunction) => {
        const rawId = req.header("x-fake-key-id");
        if (rawId) {
          // Match the full shape of Request.apiKey so TS is happy in
          // strict mode. Only `id` is actually read by the rate limiter.
          req.apiKey = {
            id: Number(rawId),
            name: `fake-${rawId}`,
            keyPrefix: `fake-${rawId}`.slice(0, 8),
            scopes: [],
            createdAt: new Date(0),
          };
        }
        next();
      });
      app.use(
        buildRateLimiter({
          windowMs: 60_000,
          authLimit: 2,
          ipLimit: 1000,
        }),
      );
      app.get("/api/test", (_req, res) => res.json({ ok: true }));
    });
    try {
      const a1 = await hit(port, { "x-fake-key-id": "1" });
      const a2 = await hit(port, { "x-fake-key-id": "1" });
      const a3 = await hit(port, { "x-fake-key-id": "1" }); // key 1 over
      const b1 = await hit(port, { "x-fake-key-id": "2" }); // key 2 fresh
      const b2 = await hit(port, { "x-fake-key-id": "2" });
      const b3 = await hit(port, { "x-fake-key-id": "2" }); // key 2 over
      assert(a1.status === 200, "key-buckets", `a1 status ${a1.status}`);
      assert(a2.status === 200, "key-buckets", `a2 status ${a2.status}`);
      assert(a3.status === 429, "key-buckets", `a3 status ${a3.status}`);
      assert(b1.status === 200, "key-buckets", `b1 status ${b1.status}`);
      assert(b2.status === 200, "key-buckets", `b2 status ${b2.status}`);
      assert(b3.status === 429, "key-buckets", `b3 status ${b3.status}`);
    } finally {
      await close();
    }
  }

  if (failures.length === 0) {
    console.log("[rateLimit-test] OK — all scenarios passed");
    process.exit(0);
  } else {
    console.error(`[rateLimit-test] ${failures.length} failure(s):`);
    for (const f of failures) console.error(`  ${f.name}: ${f.issue}`);
    process.exit(1);
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
