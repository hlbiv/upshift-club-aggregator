# Integrating with the Upshift Data API (player-platform handoff)

This doc is the handoff note for `upshift-player-platform`. Paste a copy into
that repo's `CLAUDE.md` (or a linked integration doc) so future agents know how
to call the reference-data API without scraping independently or poking the DB
directly.

## What you get

- Base URL: `https://<data-replit-host>/api` (or the internal Replit URL when
  both apps run on the same account).
- Auth: single M2M API key in the `X-API-Key` header (also accepts
  `Authorization: Bearer <key>`).
- Endpoints: see `lib/api-spec/openapi.yaml` in this repo for the full surface
  (`/clubs`, `/clubs/search`, `/clubs/:id`, `/clubs/:id/related`,
  `/clubs/:id/staff`, `/coaches/search`, `/events/search`, `/leagues`,
  `/leagues/:id/clubs`, `/search`, `/analytics/*`). Generated Zod schemas live
  in `@workspace/api-zod`.

## Environment variables (player-platform side)

| Name | Purpose |
|---|---|
| `UPSHIFT_DATA_API_URL` | Base URL of the data API, no trailing slash. E.g. `https://upshift-data.replit.app`. |
| `UPSHIFT_DATA_API_KEY` | Plaintext key minted via `create-api-key` on the data repo. Shown exactly once. |

## Minimal TypeScript client

Drop this into player-platform at `artifacts/api-server/src/lib/upshiftData.ts`
(or equivalent). Zero deps beyond the global `fetch`.

```ts
/**
 * Upshift Data API client.
 *
 * Auth: X-API-Key header, key sourced from UPSHIFT_DATA_API_KEY.
 * Base:  UPSHIFT_DATA_API_URL (no trailing slash).
 * Errors: throws UpshiftDataError with .status; callers can branch on it.
 * Retries: transient 5xx only, capped at 3 attempts with exponential backoff.
 *          401/403/429 are returned to the caller immediately — retrying a
 *          401 just wastes attempts, and 429 means the server is asking us
 *          to back off (respect Retry-After if present).
 */

export class UpshiftDataError extends Error {
  constructor(
    public readonly status: number,
    public readonly path: string,
    message: string,
    public readonly body?: unknown,
  ) {
    super(message);
    this.name = "UpshiftDataError";
  }
}

const BASE_URL = (process.env.UPSHIFT_DATA_API_URL ?? "").replace(/\/+$/, "");
const API_KEY = process.env.UPSHIFT_DATA_API_KEY ?? "";

const MAX_ATTEMPTS = 3;
const BASE_BACKOFF_MS = 250;

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export async function upshiftDataFetch<T = unknown>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  if (!BASE_URL) throw new Error("UPSHIFT_DATA_API_URL not set");
  if (!API_KEY) throw new Error("UPSHIFT_DATA_API_KEY not set");
  if (!path.startsWith("/")) path = `/${path}`;

  const url = `${BASE_URL}${path}`;
  const headers = new Headers(init.headers);
  headers.set("X-API-Key", API_KEY);
  headers.set("Accept", "application/json");

  let lastErr: unknown;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const res = await fetch(url, { ...init, headers });

      // 401/403 are auth problems. Never retry — fix the key.
      if (res.status === 401 || res.status === 403) {
        const body = await safeJson(res);
        throw new UpshiftDataError(
          res.status,
          path,
          `upshift-data auth failed (${res.status})`,
          body,
        );
      }

      // 429 — respect Retry-After if present, else exponential.
      if (res.status === 429) {
        const body = await safeJson(res);
        throw new UpshiftDataError(
          res.status,
          path,
          "upshift-data rate limited",
          body,
        );
      }

      if (res.status >= 500 && attempt < MAX_ATTEMPTS) {
        // Transient — back off and retry.
        await sleep(BASE_BACKOFF_MS * 2 ** (attempt - 1));
        continue;
      }

      if (!res.ok) {
        const body = await safeJson(res);
        throw new UpshiftDataError(
          res.status,
          path,
          `upshift-data ${res.status}`,
          body,
        );
      }

      return (await res.json()) as T;
    } catch (err) {
      lastErr = err;
      // Non-HTTP errors (network blip, DNS) — retry if we have attempts left.
      if (
        err instanceof UpshiftDataError ||
        attempt >= MAX_ATTEMPTS
      ) {
        throw err;
      }
      await sleep(BASE_BACKOFF_MS * 2 ** (attempt - 1));
    }
  }
  throw lastErr ?? new Error("upshift-data fetch failed");
}

async function safeJson(res: Response): Promise<unknown> {
  try {
    return await res.json();
  } catch {
    return undefined;
  }
}
```

## Usage examples

```ts
import { upshiftDataFetch, UpshiftDataError } from "./lib/upshiftData";

// Fuzzy club lookup — wire into the player claim flow.
const hits = await upshiftDataFetch<{
  query: string;
  results: Array<{ id: number; club_name_canonical: string; state: string }>;
}>(`/search?q=${encodeURIComponent(raw)}`);

// Coaches for a club (staff page).
try {
  const staff = await upshiftDataFetch<{ club_id: number; staff: unknown[] }>(
    `/clubs/${clubId}/staff`,
  );
} catch (err) {
  if (err instanceof UpshiftDataError && err.status === 404) {
    // club not in master — fall back to local directory or show empty state
  } else {
    throw err;
  }
}
```

## Types

Prefer the generated Zod schemas from `@workspace/api-zod` on the data repo
rather than hand-rolling types. If the player-platform repo can't import from
the data repo directly, copy the relevant generated TS interfaces under
`lib/api-zod/src/generated/types/index.ts` into a local file — they're plain
types, no runtime.

## Error-code cheat sheet

| Status | Meaning | Action |
|---|---|---|
| 200 | OK | Use the body. |
| 400 | Malformed query params | Fix the caller. |
| 401 | Missing / bad / revoked key | Check `UPSHIFT_DATA_API_KEY`. No retry. |
| 403 | Reserved for future scope checks | Don't retry. |
| 404 | Resource not found | Branch in caller. |
| 429 | Rate limited | Back off; future server may send `Retry-After`. |
| 5xx | Transient | Retried automatically (3 attempts, exponential). |

## Rotation playbook

1. On the data repo (Replit): `pnpm --filter @workspace/scripts run create-api-key -- --name "<caller> <date>"`.
2. Copy the plaintext into player-platform's `UPSHIFT_DATA_API_KEY` env.
3. Restart the player-platform server (or redeploy).
4. On the data repo (Replit): `pnpm --filter @workspace/scripts run revoke-api-key -- --prefix <old-8-char-prefix>`.

A revoked key becomes a 401 on next request — callers should redeploy with
the new key before step 4.
