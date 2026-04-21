/**
 * Shared utilities for shell-level SPA reconnaissance probes.
 *
 * Used by `scripts/src/probe-*.ts`. Extracted from the duplication that
 * accreted across `probe-hudl-fan.ts`, `probe-usl-w-league.ts`, and
 * `probe-ussf-directory.ts` — same fetch loop, same UA, same regexes, same
 * `formatFetchBlock` boilerplate, three slightly-different copies.
 *
 * Scope: this is a *probe* helper library. It is NOT a generic HTTP client.
 *   - `fetchOnce` always uses GET, captures every response header, classifies
 *     the body, and returns a small fixed-shape result.
 *   - `fetchBundleLike` is the same primitive but for follow-up GETs of JS
 *     bundles / discovered API URLs, returning the (capped) full body so the
 *     caller can grep it.
 *   - Nothing here writes to a database, calls an integration, or executes
 *     JavaScript. Hard stop at the shell level.
 *
 * Design constraints:
 *   - Zero new dependencies. Native `fetch` only.
 *   - Caller controls the actual request headers (via `opts.headers`); the
 *     library only provides constants + a builder for the common header sets.
 *   - The classifier is unified: every probe gets the full `BodyClass` union.
 *     Probe-level code that wants to special-case a particular class can do
 *     so with a single switch.
 */

import { writeFileSync } from "node:fs";
import { join } from "node:path";

// ---- Constants -------------------------------------------------------------

/**
 * Per-request budget. If a bot-wall hangs us, don't wait forever. Picked to
 * be longer than any reasonable HTML response and shorter than any reasonable
 * "I am bored, did this script die" operator wait.
 */
export const REQUEST_TIMEOUT_MS = 15_000;

/**
 * How many leading bytes of each response body to keep in the JSON report.
 * Enough to eyeball "real HTML" vs "JS challenge" vs "empty SPA shell"
 * without bloating the output.
 */
export const BODY_SNIPPET_CHARS = 500;

/**
 * Hard cap on bytes we'll hold in memory from a single bundle / API GET.
 * Real Webpack chunks easily run 200–800 KB; we only need to grep them, not
 * archive them.
 */
export const BUNDLE_FETCH_BYTES_MAX = 2_000_000;

/**
 * A realistic, slightly-stale Chrome UA. Not trying to evade fingerprinting,
 * just avoiding being flagged as a trivially-identifiable bot during a
 * reconnaissance probe.
 */
export const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/**
 * Headers we always echo into the JSON report's `headersOfInterest` block.
 * Probe-specific reports may extend this list.
 */
export const DEFAULT_HEADERS_OF_INTEREST: readonly string[] = [
  "server",
  "via",
  "cf-ray",
  "cf-cache-status",
  "cf-mitigated",
  "x-akamai-transformed",
  "x-akamai-request-id",
  "x-cache",
  "x-amz-cf-id",
  "content-type",
  "content-length",
  "set-cookie",
  "x-content-type-options",
  "x-frame-options",
  "strict-transport-security",
  "retry-after",
  "location",
  "x-powered-by",
];

// ---- Body-classifier regex constants --------------------------------------

/** Cloudflare interactive-challenge markers (body + cookie tokens). */
export const CF_CHALLENGE_RE = /cf-chl|challenge-platform|__cf_chl_|cf_chl_opt/i;

/** Akamai / generic WAF block-page markers. */
export const WAF_BLOCK_RE =
  /access denied|pardon our interruption|request blocked|are you a robot|reference #[0-9a-f]+/i;

/**
 * Common SPA root-element markers — React (`#root`, `#app`), Next
 * (`#__next`), Nuxt (`#__nuxt`), Svelte (`#svelte`). Hitting one of these on
 * a sub-5KB body is a strong "this needs a browser" signal.
 */
export const SPA_SHELL_RE =
  /<div[^>]+id=["'](root|app|__next|__nuxt|svelte)["']/i;

/** Angular's bootstrap element. */
export const APP_ROOT_RE = /<app-root[\s>]/i;

// ---- Bundle / API discovery defaults --------------------------------------

/**
 * Hosts we drop from candidate API URLs because they're framework-doc links
 * accidentally inlined into vendor bundles, not real backends.
 */
export const DEFAULT_DOC_HOST_DENYLIST: readonly string[] = [
  "docs.angularjs.org",
  "angular.io",
  "angular.dev",
  "reactjs.org",
  "react.dev",
  "developer.mozilla.org",
  "github.com",
  "w3.org",
  "ietf.org",
  "schema.org",
];

/** SPA / framework state-hook markers we grep the HTML body for. */
export const DEFAULT_STATE_HOOKS: ReadonlyArray<{ name: string; re: RegExp }> = [
  { name: "__INITIAL_STATE__", re: /__INITIAL_STATE__/ },
  { name: "__PRELOADED_STATE__", re: /__PRELOADED_STATE__/ },
  { name: "__NEXT_DATA__", re: /__NEXT_DATA__/ },
  { name: "__NUXT__", re: /__NUXT__/ },
  { name: "window.__APP_STATE__", re: /window\.__APP_STATE__/ },
  { name: "window.__DATA__", re: /window\.__DATA__/ },
  { name: "window.__APOLLO_STATE__", re: /window\.__APOLLO_STATE__/ },
  { name: "self.__remixContext", re: /self\.__remixContext/ },
];

/** API-shape markers we grep bundle bodies for. */
export const DEFAULT_API_PATTERNS: ReadonlyArray<{ name: string; re: RegExp }> = [
  { name: "apiBaseUrl", re: /apiBaseUrl/g },
  { name: "API_BASE_URL", re: /API_BASE_URL/g },
  { name: "/api/", re: /["'`](\/api\/[A-Za-z0-9_\-\/]+)/g },
  { name: "graphql endpoint", re: /["'`]([^"'`]*graphql[^"'`]*)["'`]/g },
  {
    name: "absolute api URL",
    re: /https?:\/\/[a-z0-9.\-]+\/(?:api|graphql)\/[^"'`\s)]*/g,
  },
  { name: "algolia", re: /algolia(?:net|search)?\.(?:com|net)/g },
  { name: "typesense", re: /typesense\.(?:net|org|com)/g },
];

// ---- Header builders ------------------------------------------------------

/** "Naive" mode — minimal headers, no UA. Reproduces a bare-client baseline. */
export function naiveHeaders(): Record<string, string> {
  return {};
}

/**
 * "UA-shod" mode — Chrome UA + the Accept / Accept-Language pair a real
 * browser sends. Use when a probe needs to ask "does UA filtering alone
 * unblock this URL?".
 */
export function uaShodHeaders(): Record<string, string> {
  return {
    "User-Agent": USER_AGENT,
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
  };
}

/**
 * Lightweight UA-shod set — just UA + Accept + Accept-Language. Used by the
 * Hudl probe to keep the request shape close to what a curl-with-UA would
 * send (no Sec-Fetch-* hints that might affect SameSite cookie behavior).
 */
export function lightUaHeaders(): Record<string, string> {
  return {
    "User-Agent": USER_AGENT,
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
  };
}

// ---- Types ----------------------------------------------------------------

export type FetchMode = "naive" | "ua-shod" | "single";

export type BodyClass =
  | "real-html"
  | "json"
  | "js-challenge"
  | "bot-wall"
  | "empty-shell"
  | "redirect"
  | "error"
  | "blocked"
  | "unknown";

export interface SingleFetchResult {
  mode: FetchMode;
  status: number | null;
  statusText: string | null;
  elapsedMs: number;
  contentLength: number | null;
  headers: Record<string, string>;
  headersOfInterest: Record<string, string | null>;
  bodySnippet: string;
  bodySnippetBytes: number;
  error: string | null;
  bodyClass: BodyClass;
  /**
   * Full response body. Only populated when `FetchOnceOpts.keepFullBody` is
   * true. Capped at `BUNDLE_FETCH_BYTES_MAX`. Probe code that needs to grep
   * the body (e.g. USL's Modular11 scan) opts in; the JSON report excludes
   * this field via the `serializeFetchResult` helper.
   */
  body?: string;
}

export interface FetchOnceOpts {
  /** Request headers. Default: empty (`naiveHeaders()`). */
  headers?: Record<string, string>;
  /** Per-request abort timeout. Default: REQUEST_TIMEOUT_MS. */
  timeoutMs?: number;
  /** "manual" (default) or "follow". */
  redirect?: "manual" | "follow";
  /** Which header names to echo into `headersOfInterest`. */
  headersOfInterest?: readonly string[];
  /** Body snippet length. Default: BODY_SNIPPET_CHARS. */
  bodySnippetChars?: number;
  /**
   * If true, populate `result.body` with the full response body (capped at
   * `BUNDLE_FETCH_BYTES_MAX`). Defaults to false to keep memory bounded.
   */
  keepFullBody?: boolean;
}

// ---- Body classifier ------------------------------------------------------

/**
 * Classify a response body using purely static heuristics.
 *
 * Decision order (first match wins):
 *   1. 3xx → `redirect`.
 *   2. 4xx + Cloudflare challenge markers (body or `cf-mitigated: challenge`)
 *      → `js-challenge`.
 *   3. 4xx + Akamai header / server fingerprint → `bot-wall`.
 *   4. 4xx + WAF body markers → `bot-wall`.
 *   5. 401 / 403 / 451 with no challenge body → `blocked` (auth wall vs
 *      generic "go away"; caller decides which).
 *   6. Other 4xx/5xx → `error`.
 *   7. 2xx + challenge markers → `js-challenge` (rare but real).
 *   8. 2xx + WAF markers → `bot-wall`.
 *   9. 2xx + `application/json` content-type, OR a body that lexically looks
 *      like JSON → `json`.
 *  10. 2xx + sub-5KB body containing an SPA root marker → `empty-shell`.
 *  11. 2xx + `<html` somewhere and >2KB → `real-html`.
 *  12. Otherwise `unknown`.
 */
export function classifyBody(
  status: number,
  headers: Record<string, string>,
  body: string,
): BodyClass {
  if (status >= 300 && status < 400) return "redirect";

  const cfRay = headers["cf-ray"];
  const cfMitigated = headers["cf-mitigated"];
  const server = headers["server"] ?? "";
  const akamaiHeader = Object.keys(headers).some((h) =>
    h.startsWith("x-akamai"),
  );
  const contentType = (headers["content-type"] ?? "").toLowerCase();

  if (status >= 400) {
    if (
      status === 403 &&
      (CF_CHALLENGE_RE.test(body) ||
        cfMitigated === "challenge" ||
        (cfRay != null && CF_CHALLENGE_RE.test(body)))
    ) {
      return "js-challenge";
    }
    if (status === 403 && (akamaiHeader || /akamaighost/i.test(server))) {
      return "bot-wall";
    }
    if (status === 403 && WAF_BLOCK_RE.test(body)) return "bot-wall";
    if (status === 401 || status === 403 || status === 451) return "blocked";
    return "error";
  }

  if (CF_CHALLENGE_RE.test(body)) return "js-challenge";
  if (WAF_BLOCK_RE.test(body)) return "bot-wall";

  if (contentType.includes("json")) return "json";
  const trimmed = body.trimStart();
  if (
    !contentType.includes("html") &&
    (trimmed.startsWith("{") || trimmed.startsWith("[")) &&
    body.length > 1
  ) {
    return "json";
  }

  if (
    body.length < 5_120 &&
    (SPA_SHELL_RE.test(body) || APP_ROOT_RE.test(body))
  ) {
    return "empty-shell";
  }
  if (body.length > 2_048 && /<html/i.test(body)) {
    return "real-html";
  }
  return "unknown";
}

// ---- fetchOnce ------------------------------------------------------------

/**
 * Single GET. Captures status, elapsed ms, every response header, a
 * configurable subset as `headersOfInterest`, the first N body chars, and
 * the body classification. Errors (timeout / network) are returned as
 * `bodyClass: "error"` rather than thrown.
 */
export async function fetchOnce(
  url: string,
  mode: FetchMode,
  opts: FetchOnceOpts = {},
): Promise<SingleFetchResult> {
  const headersOfInterest =
    opts.headersOfInterest ?? DEFAULT_HEADERS_OF_INTEREST;
  const bodySnippetChars = opts.bodySnippetChars ?? BODY_SNIPPET_CHARS;
  const timeoutMs = opts.timeoutMs ?? REQUEST_TIMEOUT_MS;
  const redirect = opts.redirect ?? "manual";
  const requestHeaders = opts.headers ?? {};

  const start = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      method: "GET",
      redirect,
      headers: requestHeaders,
      signal: controller.signal,
    });

    const elapsedMs = Date.now() - start;

    const headers: Record<string, string> = {};
    res.headers.forEach((v, k) => {
      headers[k.toLowerCase()] = v;
    });

    const hoi: Record<string, string | null> = {};
    for (const name of headersOfInterest) {
      hoi[name] = headers[name] ?? null;
    }

    const body = await res.text();
    const snippet = body.slice(0, bodySnippetChars);

    const result: SingleFetchResult = {
      mode,
      status: res.status,
      statusText: res.statusText,
      elapsedMs,
      contentLength: body.length,
      headers,
      headersOfInterest: hoi,
      bodySnippet: snippet,
      bodySnippetBytes: snippet.length,
      error: null,
      bodyClass: classifyBody(res.status, headers, body),
    };
    if (opts.keepFullBody) {
      result.body =
        body.length > BUNDLE_FETCH_BYTES_MAX
          ? body.slice(0, BUNDLE_FETCH_BYTES_MAX)
          : body;
    }
    return result;
  } catch (err) {
    const elapsedMs = Date.now() - start;
    const message = err instanceof Error ? err.message : String(err);
    return {
      mode,
      status: null,
      statusText: null,
      elapsedMs,
      contentLength: null,
      headers: {},
      headersOfInterest: {},
      bodySnippet: "",
      bodySnippetBytes: 0,
      error: message,
      bodyClass: "error",
    };
  } finally {
    clearTimeout(timer);
  }
}

// ---- fetchBundleLike ------------------------------------------------------

export interface BundleFetchResult {
  status: number | null;
  statusText: string | null;
  contentType: string | null;
  contentLength: number | null;
  /** Body, capped at `BUNDLE_FETCH_BYTES_MAX` for memory safety. */
  body: string;
  error: string | null;
  headers: Record<string, string>;
}

export interface BundleFetchOpts {
  timeoutMs?: number;
  maxBytes?: number;
}

/**
 * GET an arbitrary asset (JS bundle, JSON API, etc.) and return the full
 * (capped) body for grep-time inspection. Always follows redirects and
 * always sends a UA — the goal is to look like a normal browser-loaded
 * sub-resource.
 */
export async function fetchBundleLike(
  url: string,
  acceptHeader: string,
  opts: BundleFetchOpts = {},
): Promise<BundleFetchResult> {
  const timeoutMs = opts.timeoutMs ?? REQUEST_TIMEOUT_MS;
  const maxBytes = opts.maxBytes ?? BUNDLE_FETCH_BYTES_MAX;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      redirect: "follow",
      headers: {
        "User-Agent": USER_AGENT,
        Accept: acceptHeader,
        "Accept-Language": "en-US,en;q=0.9",
      },
      signal: controller.signal,
    });
    const headers: Record<string, string> = {};
    res.headers.forEach((v, k) => {
      headers[k.toLowerCase()] = v;
    });
    const text = await res.text();
    const body = text.length > maxBytes ? text.slice(0, maxBytes) : text;
    return {
      status: res.status,
      statusText: res.statusText,
      contentType: headers["content-type"] ?? null,
      contentLength: text.length,
      body,
      error: null,
      headers,
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: null,
      statusText: null,
      contentType: null,
      contentLength: null,
      body: "",
      error: message,
      headers: {},
    };
  } finally {
    clearTimeout(timer);
  }
}

// ---- HTML / bundle inspection helpers -------------------------------------

/** Extract every `<script src="…">` URL from `html`, resolved against `baseUrl`. */
export function extractScriptSrcs(html: string, baseUrl: string): string[] {
  const out = new Set<string>();
  const re = /<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["'][^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    const raw = m[1]!;
    try {
      out.add(new URL(raw, baseUrl).toString());
    } catch {
      // Ignore unresolvable srcs (data: URIs, malformed).
    }
  }
  return Array.from(out);
}

export interface GrepOptions {
  /** Patterns to scan for. Default: `DEFAULT_API_PATTERNS`. */
  patterns?: ReadonlyArray<{ name: string; re: RegExp }>;
  /** Hosts to drop from `candidateUrls`. Default: `DEFAULT_DOC_HOST_DENYLIST`. */
  denylist?: readonly string[];
}

/**
 * Grep `body` for API-shape patterns. Returns per-pattern hit counts and a
 * deduped list of candidate URLs. Captured URLs are resolved against
 * `pageUrl` (so root-relative `/api/foo` becomes a fully-qualified URL the
 * caller can actually GET); URLs whose host appears in `denylist` are
 * dropped (framework-doc links sneaking out of vendor bundles).
 */
export function grepForApiPatterns(
  body: string,
  pageUrl: string,
  opts: GrepOptions = {},
): {
  patternCounts: Record<string, number>;
  candidateUrls: string[];
} {
  const patterns = opts.patterns ?? DEFAULT_API_PATTERNS;
  const denylist = opts.denylist ?? DEFAULT_DOC_HOST_DENYLIST;
  const patternCounts: Record<string, number> = {};
  const candidateUrls = new Set<string>();
  for (const p of patterns) {
    // Re-create the regex each time so .exec/.lastIndex state is clean.
    const re = new RegExp(p.re.source, p.re.flags);
    let count = 0;
    let m: RegExpExecArray | null;
    while ((m = re.exec(body)) !== null) {
      count++;
      const cap = m[1] ?? m[0];
      if (typeof cap === "string" && cap.length > 0 && cap.length < 300) {
        let resolved: string | null = null;
        if (cap.startsWith("http://") || cap.startsWith("https://")) {
          resolved = cap;
        } else if (cap.startsWith("/")) {
          try {
            resolved = new URL(cap, pageUrl).toString();
          } catch {
            resolved = null;
          }
        } else if (cap.includes("graphql") || cap.includes("/api/")) {
          resolved = cap;
        }
        if (resolved) {
          let host = "";
          try {
            host = new URL(resolved).host.toLowerCase();
          } catch {
            host = "";
          }
          if (!denylist.includes(host)) {
            candidateUrls.add(resolved);
          }
        }
      }
      if (!re.global) break;
    }
    if (count > 0) patternCounts[p.name] = count;
  }
  return { patternCounts, candidateUrls: Array.from(candidateUrls) };
}

// ---- Reporting helpers ----------------------------------------------------

/**
 * Format the per-fetch summary block — Status / Elapsed / Size / Class /
 * Headers of interest / Body snippet — used identically by every probe's
 * `printSummary`. Returns an array of lines, one per line, indented by
 * `indent`.
 */
export function formatFetchBlockLines(
  f: SingleFetchResult,
  indent = "    ",
): string[] {
  const lines: string[] = [];
  if (f.error) {
    lines.push(`${indent}ERROR:   ${f.error}`);
    lines.push(`${indent}Elapsed: ${f.elapsedMs}ms`);
    lines.push(`${indent}Class:   ${f.bodyClass}`);
    return lines;
  }
  lines.push(`${indent}Status:  ${f.status} ${f.statusText ?? ""}`);
  lines.push(`${indent}Elapsed: ${f.elapsedMs}ms`);
  lines.push(`${indent}Size:    ${f.contentLength} bytes`);
  lines.push(`${indent}Class:   ${f.bodyClass}`);
  const sig = Object.entries(f.headersOfInterest).filter(([, v]) => v != null);
  if (sig.length > 0) {
    lines.push(`${indent}Headers:`);
    for (const [k, v] of sig) lines.push(`${indent}  ${k}: ${v}`);
  }
  lines.push(`${indent}Snippet (first ${f.bodySnippetBytes} chars):`);
  lines.push(
    f.bodySnippet
      .split("\n")
      .map((l) => `${indent}  | ${l}`)
      .join("\n"),
  );
  return lines;
}

/**
 * Write `report` to `/tmp/<prefix>-<timestamp>.json` (pretty-printed) and
 * return the file path. Timestamp is ISO with `:` and `.` replaced by `-`.
 */
export function writeJsonReport(prefix: string, report: unknown): string {
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const outPath = join("/tmp", `${prefix}-${stamp}.json`);
  writeFileSync(outPath, JSON.stringify(report, null, 2), "utf8");
  return outPath;
}

// ---- CLI helpers ----------------------------------------------------------

export interface CommonProbeArgs {
  /** URLs from `--url <url>`. If non-empty, defaults are skipped. */
  onlyUrls: string[];
  /** URLs from `--extra-url <url>`. Always added on top. */
  extraUrls: string[];
}

/**
 * Parse the `--url` / `--extra-url` / `--help` / `-h` arguments common to
 * every probe. Probe-specific args can be intercepted via `customHandler`,
 * which receives `(arg, nextArg, advance)` and should return `true` if it
 * consumed the arg (and call `advance()` if it also consumed `nextArg`).
 *
 * On `--help` or `-h`, calls `printUsageAndExit(0)` — pass your own.
 */
export function parseCommonProbeArgs(
  argv: string[],
  printUsageAndExit: (code: number) => never,
  customHandler?: (
    arg: string,
    nextArg: string | undefined,
    advance: () => void,
  ) => boolean,
): CommonProbeArgs {
  const args: CommonProbeArgs = { onlyUrls: [], extraUrls: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!;
    const next = argv[i + 1];
    const advance = (): void => {
      i++;
    };
    if (a === "--url" && next !== undefined) {
      args.onlyUrls.push(next);
      advance();
    } else if (a === "--extra-url" && next !== undefined) {
      args.extraUrls.push(next);
      advance();
    } else if (a === "--help" || a === "-h") {
      printUsageAndExit(0);
    } else if (customHandler && customHandler(a, next, advance)) {
      // consumed by custom handler
    }
  }
  return args;
}

/**
 * Build the final URL list for a probe: if `--url` was given, use those
 * exclusively; otherwise use `defaults`. Then append any `--extra-url`s.
 * Labels are auto-assigned for custom URLs (`custom-N`, `extra-N`).
 */
export function buildProbeUrlList(
  args: CommonProbeArgs,
  defaults: ReadonlyArray<{ url: string; label: string }>,
): Array<{ url: string; label: string }> {
  const targets: Array<{ url: string; label: string }> =
    args.onlyUrls.length > 0
      ? args.onlyUrls.map((u, i) => ({ url: u, label: `custom-${i + 1}` }))
      : defaults.map((d) => ({ ...d }));
  for (const u of args.extraUrls) {
    targets.push({ url: u, label: `extra-${targets.length + 1}` });
  }
  return targets;
}

// ---- Common report header --------------------------------------------------

/**
 * The `ranAt` / `nodeVersion` / `platform` / `userAgent` block every probe
 * report carries. Probe-specific reports `extends` this with their own
 * fields.
 */
export interface BaseProbeReportHeader {
  ranAt: string;
  nodeVersion: string;
  platform: string;
  userAgent: string;
  note: string;
}

export function buildReportHeader(note: string): BaseProbeReportHeader {
  return {
    ranAt: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${process.platform} ${process.arch}`,
    userAgent: USER_AGENT,
    note,
  };
}
