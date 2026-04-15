/**
 * Smoke test for the X-API-Key middleware on the data API.
 *
 * Exercises four endpoints with and without the key and asserts the expected
 * status codes. Use this after flipping `API_KEY_AUTH_ENABLED=true` on Replit
 * to prove the cutover worked.
 *
 * Env:
 *   UPSHIFT_DATA_API_KEY   — plaintext key from `create-api-key`
 *   UPSHIFT_DATA_API_URL   — base URL (default: http://localhost:8080)
 *   API_KEY_AUTH_ENABLED   — mirrors the server flag. When "true" (default),
 *                            the without-key probes must return 401. Set to
 *                            "false" to only verify the with-key probes
 *                            (useful during pre-cutover staging).
 *
 * Usage:
 *   pnpm --filter @workspace/scripts run smoke-api-key
 *
 * Exits non-zero on any failed assertion.
 *
 * Note: `/api/healthz` is intentionally exempt from auth in the middleware
 * (EXEMPT_PATHS in apiKeyAuth.ts). It's smoke-tested without a key for
 * liveness only; we do NOT assert 401 against it.
 */

type Probe = {
  label: string;
  path: string;
  /** When true, exempt from auth — no 401 probe. */
  exempt?: boolean;
};

const PROBES: Probe[] = [
  { label: "healthz", path: "/api/healthz", exempt: true },
  { label: "clubs-list", path: "/api/clubs?page_size=1" },
  { label: "coaches-search", path: "/api/coaches/search?page_size=1" },
  { label: "events-search", path: "/api/events/search?page_size=1" },
];

type Result = {
  probe: string;
  variant: "with-key" | "without-key";
  expected: number | number[];
  actual: number;
  ok: boolean;
  note?: string;
};

function expectedMatches(
  expected: number | number[],
  actual: number,
): boolean {
  if (Array.isArray(expected)) return expected.includes(actual);
  return expected === actual;
}

async function probe(
  baseUrl: string,
  path: string,
  headers: Record<string, string>,
): Promise<number> {
  const res = await fetch(`${baseUrl}${path}`, { headers });
  // Drain body to free the socket; we only care about status.
  await res.text().catch(() => "");
  return res.status;
}

async function main(): Promise<void> {
  const apiKey = process.env.UPSHIFT_DATA_API_KEY;
  const baseUrl = (
    process.env.UPSHIFT_DATA_API_URL ?? "http://localhost:8080"
  ).replace(/\/+$/, "");
  const authEnabled =
    (process.env.API_KEY_AUTH_ENABLED ?? "true").toLowerCase() === "true";

  if (!apiKey) {
    console.error("Error: UPSHIFT_DATA_API_KEY is required.");
    console.error(
      "Hint: mint one with `pnpm --filter @workspace/scripts run create-api-key -- --name smoke-test`.",
    );
    process.exit(2);
  }

  console.log(`[smoke] base=${baseUrl} authEnabled=${authEnabled}`);
  console.log(`[smoke] key prefix=${apiKey.slice(0, 8)}…`);

  const results: Result[] = [];

  for (const p of PROBES) {
    // With-key probe: should always succeed with 200.
    try {
      const status = await probe(baseUrl, p.path, { "X-API-Key": apiKey });
      results.push({
        probe: p.label,
        variant: "with-key",
        expected: 200,
        actual: status,
        ok: status === 200,
      });
    } catch (err) {
      results.push({
        probe: p.label,
        variant: "with-key",
        expected: 200,
        actual: 0,
        ok: false,
        note: `fetch threw: ${(err as Error).message}`,
      });
    }

    // Without-key probe: 401 when auth is enabled, 200 when not (or for
    // exempt paths regardless).
    if (p.exempt) {
      try {
        const status = await probe(baseUrl, p.path, {});
        results.push({
          probe: p.label,
          variant: "without-key",
          expected: 200,
          actual: status,
          ok: status === 200,
          note: "exempt path (healthz)",
        });
      } catch (err) {
        results.push({
          probe: p.label,
          variant: "without-key",
          expected: 200,
          actual: 0,
          ok: false,
          note: `fetch threw: ${(err as Error).message}`,
        });
      }
      continue;
    }

    const expectedNoKey = authEnabled ? 401 : 200;
    try {
      const status = await probe(baseUrl, p.path, {});
      results.push({
        probe: p.label,
        variant: "without-key",
        expected: expectedNoKey,
        actual: status,
        ok: expectedMatches(expectedNoKey, status),
      });
    } catch (err) {
      results.push({
        probe: p.label,
        variant: "without-key",
        expected: expectedNoKey,
        actual: 0,
        ok: false,
        note: `fetch threw: ${(err as Error).message}`,
      });
    }
  }

  const pad = (s: string, n: number) => s.padEnd(n, " ");
  console.log("");
  console.log(
    `${pad("PROBE", 18)}${pad("VARIANT", 14)}${pad("EXPECT", 8)}${pad("GOT", 6)}RESULT`,
  );
  for (const r of results) {
    const expected = Array.isArray(r.expected)
      ? r.expected.join("/")
      : String(r.expected);
    const mark = r.ok ? "PASS" : "FAIL";
    const tail = r.note ? ` (${r.note})` : "";
    console.log(
      `${pad(r.probe, 18)}${pad(r.variant, 14)}${pad(expected, 8)}${pad(String(r.actual), 6)}${mark}${tail}`,
    );
  }

  const failed = results.filter((r) => !r.ok);
  console.log("");
  if (failed.length > 0) {
    console.log(`[smoke] ${failed.length} failure(s). Exiting non-zero.`);
    process.exit(1);
  }
  console.log(`[smoke] ${results.length} probes passed.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
