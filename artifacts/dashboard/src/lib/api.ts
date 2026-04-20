/**
 * Thin fetch wrapper for the admin API.
 *
 * Base URL resolution:
 *   - VITE_ADMIN_API_BASE_URL (set at build time, e.g. "https://api.upshift-data.replit.app")
 *   - Falls back to "" so paths like "/api/v1/admin/me" resolve against the
 *     current origin. Useful in dev when the dashboard is served by the
 *     api-server (reverse-proxied) and in test environments where fetch
 *     is mocked and the URL is irrelevant.
 *
 * Every request sets `credentials: "include"` so the admin session cookie
 * (httpOnly, set by POST /api/v1/admin/auth/login) travels with the request.
 *
 * Phase B.1 wires the concrete routes. If B.1 isn't merged when this PR
 * lands, the fetch calls still resolve — they'll just 404 until the API
 * router is registered.
 */
const BASE_URL: string =
  (import.meta.env.VITE_ADMIN_API_BASE_URL as string | undefined) ?? "";

export function adminApiUrl(path: string): string {
  if (!path.startsWith("/")) {
    throw new Error(`adminApiUrl: path must start with "/", got: ${path}`);
  }
  return `${BASE_URL}${path}`;
}

export async function adminFetch(
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  return fetch(adminApiUrl(path), {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
    ...init,
  });
}
