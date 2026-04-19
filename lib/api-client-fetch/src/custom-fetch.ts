/**
 * Native-fetch mutator for Orval-generated client functions.
 *
 * Mirrors the ergonomics of the sibling `upshift-player-platform` repo's
 * hand-written `UpshiftDataClient`:
 *
 *   - `X-API-Key: <token>` attached on every request (token supplied by the
 *     caller via `setApiKey`, `setApiKeyGetter`, or an explicit per-request
 *     `Authorization`/`X-API-Key` header override).
 *   - `Accept: application/json` by default.
 *   - Configurable base URL (`setBaseUrl`) prepended to relative `/api/*`
 *     paths emitted by the generated client.
 *   - 15s default timeout via `AbortController`, overridable via
 *     `setDefaultTimeoutMs` or per-request `signal` (caller-supplied signals
 *     disable the internal timer — caller owns cancellation).
 *   - Non-2xx responses throw `UpshiftDataError` with `status`, `message`,
 *     `code`, `details` — matching the shape of the sibling's error class so
 *     the existing try/catch sites keep working.
 *   - JSON bodies parsed automatically; 204/205/304 and HEAD responses
 *     return `null` as `T`.
 *
 * Orval invokes `customFetch<T>(url, init)` for each request. The generated
 * functions take care of building the URL (query + path) and stringifying
 * the body; this mutator only handles transport, headers, auth, timeout,
 * and error normalization.
 */

export type CustomFetchOptions = RequestInit & {
  responseType?: "json" | "text" | "blob";
};

export type ErrorType<T = unknown> = UpshiftDataError<T>;

export type BodyType<T> = T;

export type ApiKeyGetter = () => Promise<string | null> | string | null;

const DEFAULT_TIMEOUT_MS = 15_000;
const NO_BODY_STATUS = new Set([204, 205, 304]);
const DEFAULT_JSON_ACCEPT = "application/json";

// ---------------------------------------------------------------------------
// Module-level configuration
// ---------------------------------------------------------------------------

let _baseUrl: string | null = null;
let _apiKeyGetter: ApiKeyGetter | null = null;
let _defaultTimeoutMs: number = DEFAULT_TIMEOUT_MS;
let _fetchImpl: typeof fetch | null = null;

/**
 * Set the base URL prepended to relative request paths (those starting with
 * `/`). Pass `null` to clear. Example: `setBaseUrl("https://upshiftdata.com")`.
 */
export function setBaseUrl(url: string | null): void {
  _baseUrl = url ? url.replace(/\/+$/, "") : null;
}

/**
 * Set a static API key that will be sent as `X-API-Key` on every request.
 * Shorthand for `setApiKeyGetter(() => apiKey)`. Pass `null` to clear.
 */
export function setApiKey(apiKey: string | null): void {
  _apiKeyGetter = apiKey == null ? null : () => apiKey;
}

/**
 * Register a getter that supplies the API key dynamically. Useful when the
 * key rotates or is fetched from a secrets store. Called before every
 * request; when it returns null the `X-API-Key` header is omitted.
 */
export function setApiKeyGetter(getter: ApiKeyGetter | null): void {
  _apiKeyGetter = getter;
}

/**
 * Set the default per-request timeout in milliseconds. Default is 15_000.
 */
export function setDefaultTimeoutMs(ms: number): void {
  _defaultTimeoutMs = ms;
}

/**
 * Override the global `fetch` implementation (for tests or non-standard
 * runtimes). Pass `null` to restore the default.
 */
export function setFetchImpl(impl: typeof fetch | null): void {
  _fetchImpl = impl;
}

// ---------------------------------------------------------------------------
// Error class (shape-compatible with the sibling repo's UpshiftDataError)
// ---------------------------------------------------------------------------

export class UpshiftDataError<T = unknown> extends Error {
  readonly name = "UpshiftDataError";
  readonly status: number;
  readonly code?: string;
  readonly details?: unknown;
  readonly data: T | null;
  readonly response: Response | null;

  constructor(
    status: number,
    message: string,
    opts: {
      code?: string;
      details?: unknown;
      data?: T | null;
      response?: Response | null;
    } = {},
  ) {
    super(`[upshift-data ${status}] ${message}`);
    Object.setPrototypeOf(this, new.target.prototype);
    this.status = status;
    this.code = opts.code;
    this.details = opts.details;
    this.data = opts.data ?? null;
    this.response = opts.response ?? null;
  }
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function isRequest(input: RequestInfo | URL): input is Request {
  return typeof Request !== "undefined" && input instanceof Request;
}

function isUrl(input: RequestInfo | URL): input is URL {
  return typeof URL !== "undefined" && input instanceof URL;
}

function resolveUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  if (isUrl(input)) return input.toString();
  return input.url;
}

function resolveMethod(input: RequestInfo | URL, explicitMethod?: string): string {
  if (explicitMethod) return explicitMethod.toUpperCase();
  if (isRequest(input)) return input.method.toUpperCase();
  return "GET";
}

function applyBaseUrl(input: RequestInfo | URL): RequestInfo | URL {
  if (!_baseUrl) return input;
  const url = resolveUrl(input);
  if (!url.startsWith("/")) return input;
  const absolute = `${_baseUrl}${url}`;
  if (typeof input === "string") return absolute;
  if (isUrl(input)) return new URL(absolute);
  return new Request(absolute, input as Request);
}

function mergeHeaders(...sources: Array<HeadersInit | undefined>): Headers {
  const headers = new Headers();
  for (const source of sources) {
    if (!source) continue;
    new Headers(source).forEach((value, key) => {
      headers.set(key, value);
    });
  }
  return headers;
}

function hasNoBody(response: Response, method: string): boolean {
  if (method === "HEAD") return true;
  if (NO_BODY_STATUS.has(response.status)) return true;
  if (response.headers.get("content-length") === "0") return true;
  if (response.body === null) return true;
  return false;
}

function getStringField(value: unknown, key: string): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const candidate = (value as Record<string, unknown>)[key];
  return typeof candidate === "string" && candidate.trim() !== "" ? candidate : undefined;
}

async function parseBody(response: Response): Promise<unknown> {
  const text = await response.text();
  if (text.trim() === "") return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------

export async function customFetch<T = unknown>(
  input: RequestInfo | URL,
  options: CustomFetchOptions = {},
): Promise<T> {
  input = applyBaseUrl(input);
  const { responseType = "json", headers: headersInit, signal, ...init } = options;
  const method = resolveMethod(input, init.method);

  const headers = mergeHeaders(isRequest(input) ? input.headers : undefined, headersInit);

  if (!headers.has("accept")) {
    headers.set("accept", DEFAULT_JSON_ACCEPT);
  }

  if (typeof init.body === "string" && !headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }

  // Attach X-API-Key unless the caller already provided one (or an explicit
  // Authorization header, for forward-compat with bearer setups).
  if (
    _apiKeyGetter &&
    !headers.has("x-api-key") &&
    !headers.has("authorization")
  ) {
    const token = await _apiKeyGetter();
    if (token) headers.set("x-api-key", token);
  }

  // Set up the timeout controller. If the caller passed their own signal we
  // respect it and skip our internal timer (the caller owns cancellation).
  let internalController: AbortController | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let effectiveSignal: AbortSignal | undefined = signal ?? undefined;

  if (!effectiveSignal) {
    internalController = new AbortController();
    effectiveSignal = internalController.signal;
    timer = setTimeout(() => internalController?.abort(), _defaultTimeoutMs);
  }

  const url = resolveUrl(input);
  const fetchImpl = _fetchImpl ?? fetch;

  let response: Response;
  try {
    response = await fetchImpl(input, {
      ...init,
      method,
      headers,
      signal: effectiveSignal,
    });
  } catch (err) {
    if (timer) clearTimeout(timer);
    if ((err as Error | undefined)?.name === "AbortError") {
      throw new UpshiftDataError(
        0,
        `Request to ${method} ${url} timed out after ${_defaultTimeoutMs}ms`,
      );
    }
    throw new UpshiftDataError(
      0,
      `Network error calling ${method} ${url}: ${(err as Error | undefined)?.message ?? String(err)}`,
    );
  }
  if (timer) clearTimeout(timer);

  if (!response.ok) {
    const payload = await parseBody(response);
    const errMessage =
      getStringField(payload, "error") ??
      getStringField(payload, "message") ??
      getStringField(payload, "detail") ??
      response.statusText ??
      `HTTP ${response.status}`;
    const code = getStringField(payload, "code");
    const details = (payload && typeof payload === "object" && "details" in payload)
      ? (payload as { details?: unknown }).details
      : undefined;
    throw new UpshiftDataError(response.status, errMessage, {
      code,
      details,
      data: payload as unknown,
      response,
    });
  }

  if (hasNoBody(response, method)) {
    return null as T;
  }

  if (responseType === "text") {
    return (await response.text()) as T;
  }
  if (responseType === "blob") {
    if (typeof response.blob !== "function") {
      throw new TypeError(
        "Blob responses are not supported in this runtime. Use responseType \"json\" or \"text\" instead.",
      );
    }
    return (await response.blob()) as T;
  }

  return (await parseBody(response)) as T;
}
